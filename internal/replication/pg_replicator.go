package replication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"

	"git.famapp.in/fampay-inc/wal-cake/internal/ack"
	"git.famapp.in/fampay-inc/wal-cake/internal/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

const (
	KEY = 'K'
	ALL = 'O'
)

// PGReplicator interface defines methods for PostgreSQL logical replication
type PGReplicator interface {
	// Start streams CDC events to eventsCh and confirms acked to the slot
	// until ctx is cancelled. Before it returns, it sends a final status
	// update with the latest acked position.
	Start(ctx context.Context, eventsCh chan<- *model.CDCEvent, acked *ack.Position) error
	HealthCheck(ctx context.Context) error
}

type relationInfo struct {
	name    string
	columns []*pglogrepl.RelationMessageColumn
}

type pgReplicator struct {
	cfg          *config.Config
	repConn      *pgconn.PgConn
	queryConn    *pgx.Conn
	lastAckedLSN pglogrepl.LSN
	relations    map[uint32]relationInfo
	decoder      *TupleDecoder
	commitTime   time.Time // commit timestamp of the transaction being decoded
	// streaming and blockedSince are read by HealthCheck from other goroutines.
	streaming    atomic.Bool
	blockedSince atomic.Int64 // unix nanoseconds, 0 when delivery is not blocked
}

// NewPGReplicator creates a new PostgreSQL replicator
func NewPGReplicator(cfg *config.Config) PGReplicator {
	relations := make(map[uint32]relationInfo)
	defaultDecoder := NewTupleDecoder()
	return &pgReplicator{
		cfg:          cfg,
		repConn:      nil,
		queryConn:    nil,
		lastAckedLSN: 0,
		relations:    relations,
		decoder:      defaultDecoder,
	}
}

// ensurePublication ensures the publication exists
func (r *pgReplicator) ensurePublication(ctx context.Context) error {
	var exists bool
	err := r.queryConn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", r.cfg.Publication).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if publication exists: %w", err)
	}

	if !exists {
		_, err = r.queryConn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", r.cfg.Publication))
		if err != nil {
			return fmt.Errorf("failed to create publication: %w", err)
		}
		log.Info().Str("publication", r.cfg.Publication).Msg("Created publication")
	} else {
		log.Info().Str("publication", r.cfg.Publication).Msg("Publication already exists")
	}

	return nil
}

// ensureReplicationSlot ensures the replication slot exists and is not active
func (r *pgReplicator) ensureReplicationSlot(ctx context.Context) error {
	var exists bool
	err := r.queryConn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", r.cfg.Slot).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if replication slot exists: %w", err)
	}
	if !exists {
		_, err = pglogrepl.CreateReplicationSlot(ctx, r.repConn, r.cfg.Slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: false, Mode: pglogrepl.LogicalReplication})
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
		log.Info().Str("slot", r.cfg.Slot).Msg("Created replication slot")
	}
	return nil
}

// getStartLSN reads the slot's confirmed position. It fails closed: starting
// anywhere else could skip changes the slot still holds.
func (r *pgReplicator) getStartLSN(ctx context.Context) (pglogrepl.LSN, error) {
	var lsn pglogrepl.LSN
	err := r.queryConn.QueryRow(ctx, "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", r.cfg.Slot).Scan(&lsn)
	if err != nil {
		return 0, fmt.Errorf("read confirmed_flush_lsn of slot %q: %w", r.cfg.Slot, err)
	}
	return lsn, nil
}

func (r *pgReplicator) SendStandbyStatusUpdate(ctx context.Context, replyRequested bool) error {
	status := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: r.lastAckedLSN,
		WALFlushPosition: r.lastAckedLSN,
		WALApplyPosition: r.lastAckedLSN,
		ClientTime:       time.Now(),
		ReplyRequested:   replyRequested,
	}

	if err := pglogrepl.SendStandbyStatusUpdate(ctx, r.repConn, status); err != nil {
		return fmt.Errorf("send standby status update: %w", err)
	}
	log.Debug().Str("lsn", r.lastAckedLSN.String()).Bool("replyRequested", replyRequested).Msg("Sent standby status update")
	return nil
}

const (
	minReconnectBackoff = time.Second
	maxReconnectBackoff = 30 * time.Second
	// maxDeliveryBlock is how long event delivery may block on a full
	// eventsCh before HealthCheck reports the replicator as not ready.
	maxDeliveryBlock = time.Minute
)

// Start streams changes until ctx is cancelled. When the stream fails for any
// reason (connection loss, server error, a message it cannot parse), it
// reconnects with backoff and resumes from the slot's confirmed position, so
// nothing is skipped. It returns nil after ctx is cancelled.
func (r *pgReplicator) Start(ctx context.Context, ch chan<- *model.CDCEvent, acked *ack.Position) error {
	backoff := minReconnectBackoff
	for {
		streamed, err := r.stream(ctx, ch, acked)
		r.streaming.Store(false)
		if ctx.Err() != nil {
			return nil
		}
		if streamed {
			backoff = minReconnectBackoff
		}
		log.Error().Err(err).Dur("retryIn", backoff).Msg("Replication stream stopped, reconnecting from the slot position")
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxReconnectBackoff)
	}
}

// stream runs one replication session. streamed reports whether
// START_REPLICATION succeeded, so Start can reset its backoff.
func (r *pgReplicator) stream(ctx context.Context, ch chan<- *model.CDCEvent, acked *ack.Position) (streamed bool, err error) {
	log.Info().Str("slot", r.cfg.Slot).Str("publication", r.cfg.Publication).Msg("Starting PostgreSQL replication")

	r.repConn, err = pgconn.Connect(ctx, r.cfg.PGConn+"?replication=database")
	if err != nil {
		return false, fmt.Errorf("connect for replication: %w", err)
	}
	defer r.repConn.Close(context.Background())

	startLSN, err := r.prepare(ctx)
	if err != nil {
		return false, err
	}
	// A new session resends relation messages before it uses them.
	r.relations = make(map[uint32]relationInfo)
	r.lastAckedLSN = max(r.lastAckedLSN, startLSN)

	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", r.cfg.Publication),
	}
	if err := pglogrepl.StartReplication(ctx, r.repConn, r.cfg.Slot, startLSN, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		return false, fmt.Errorf("start replication: %w", err)
	}
	r.streaming.Store(true)
	log.Info().Str("lsn", startLSN.String()).Msg("Streaming from the slot's confirmed position")

	// On shutdown, confirm everything the ring finished. The connection is
	// still open here; the deferred Close above runs after this.
	defer func() {
		if ctx.Err() == nil {
			return
		}
		r.lastAckedLSN = max(r.lastAckedLSN, pglogrepl.LSN(acked.Load()))
		finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.SendStandbyStatusUpdate(finalCtx, false); err != nil {
			log.Warn().Err(err).Msg("Final status update failed; the slot will replay from the previous position")
			return
		}
		log.Info().Str("lsn", r.lastAckedLSN.String()).Msg("Sent final status update")
	}()

	const (
		// receiveTimeout bounds how long the loop waits for a message before
		// it forwards a new ACK or notices shutdown. The receive context is
		// not derived from ctx: cancelling a read closes the connection, and
		// the final status update needs it.
		receiveTimeout = time.Second
		statusInterval = 5 * time.Second
	)
	standbyMessageTicker := time.NewTicker(statusInterval)
	defer standbyMessageTicker.Stop()

	for {
		if ctx.Err() != nil {
			return true, nil
		}
		if lsn := pglogrepl.LSN(acked.Load()); lsn > r.lastAckedLSN {
			r.lastAckedLSN = lsn
			if err := r.SendStandbyStatusUpdate(ctx, false); err != nil {
				return true, err
			}
		}
		select {
		case <-standbyMessageTicker.C:
			if err := r.SendStandbyStatusUpdate(ctx, true); err != nil {
				return true, err
			}
		default:
		}

		receiveCtx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
		msg, err := r.repConn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return true, fmt.Errorf("receive message: %w", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.ErrorResponse:
			return true, fmt.Errorf("server error %s: %s", msg.Code, msg.Message)
		case *pgproto3.CopyData:
			if err := r.handleCopyData(ctx, msg.Data, ch); err != nil {
				return true, err
			}
		default:
			log.Warn().Msgf("received unexpected message type: %T", msg)
		}
	}
}

// prepare runs the setup queries on a short-lived connection and returns the
// slot's confirmed position.
func (r *pgReplicator) prepare(ctx context.Context) (pglogrepl.LSN, error) {
	var err error
	r.queryConn, err = pgx.Connect(ctx, r.cfg.PGConn)
	if err != nil {
		return 0, fmt.Errorf("connect for queries: %w", err)
	}
	defer r.queryConn.Close(context.Background())

	if err := r.ensurePublication(ctx); err != nil {
		return 0, fmt.Errorf("ensure publication: %w", err)
	}
	if err := r.ensureReplicationSlot(ctx); err != nil {
		return 0, fmt.Errorf("ensure replication slot: %w", err)
	}
	return r.getStartLSN(ctx)
}

// handleCopyData handles one CopyData frame. Any parse error stops the
// session: skipping a message would let a later ACK confirm it.
func (r *pgReplicator) handleCopyData(ctx context.Context, data []byte, ch chan<- *model.CDCEvent) error {
	if len(data) == 0 {
		return errors.New("empty CopyData message")
	}
	switch data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
		if err != nil {
			return fmt.Errorf("parse primary keepalive: %w", err)
		}
		if pkm.ReplyRequested {
			return r.SendStandbyStatusUpdate(ctx, false)
		}
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			return fmt.Errorf("parse XLogData: %w", err)
		}
		logicalMsg, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			return fmt.Errorf("parse logical replication message at %s: %w", xld.WALStart, err)
		}
		return r.proccessLogicalMsg(ctx, logicalMsg, xld.WALStart, ch)
	default:
		log.Warn().Msgf("received unexpected message byte ID: %d", data[0])
	}
	return nil
}

// deliver sends ev to ch. It records when delivery starts to block, so
// HealthCheck can report a replicator stuck behind a full pipeline.
func (r *pgReplicator) deliver(ctx context.Context, ch chan<- *model.CDCEvent, ev *model.CDCEvent) error {
	select {
	case ch <- ev:
		return nil
	default:
	}
	r.blockedSince.Store(time.Now().UnixNano())
	defer r.blockedSince.Store(0)
	select {
	case ch <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HealthCheck reports ready only while a replication session is streaming and
// event delivery is not stuck.
func (r *pgReplicator) HealthCheck(_ context.Context) error {
	if !r.streaming.Load() {
		return errors.New("replication stream is not running")
	}
	if since := r.blockedSince.Load(); since != 0 {
		if blocked := time.Since(time.Unix(0, since)); blocked > maxDeliveryBlock {
			return fmt.Errorf("event delivery blocked for %s", blocked.Round(time.Second))
		}
	}
	return nil
}

// proccessLogicalMsg turns one pgoutput message into a CDC event.
//
// walStart is the XLogData WALStart. For a row change it is the LSN of the
// change record. Every event LSN must be safe to confirm to the slot once the
// event and everything before it is durable:
//   - A row event uses its change LSN. Confirming it makes Postgres resend the
//     whole transaction on restart, which is at-least-once.
//   - A commit event uses the end of the commit record (TransactionEndLSN).
//     Do not add len(WALData): that lands past the end of the record, and if
//     the next transaction's commit record starts there, Postgres would skip
//     that transaction on restart.
func (r *pgReplicator) proccessLogicalMsg(ctx context.Context, logicalMsg pglogrepl.Message, walStart pglogrepl.LSN, ch chan<- *model.CDCEvent) error {
	// Handle different message types
	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		// Store relation info and column definitions for later use
		r.relations[msg.RelationID] = relationInfo{
			name:    msg.RelationName,
			columns: msg.Columns,
		}
		log.Debug().Str("relation", msg.RelationName).Uint32("id", msg.RelationID).Int("columns", len(msg.Columns)).Msg("relation message")

	case *pglogrepl.InsertMessage:
		// Create CDC event for insert
		relInfo, ok := r.relations[msg.RelationID]
		if !ok {
			log.Warn().Uint32("relationID", msg.RelationID).Msg("unknown relation ID for insert")
			relInfo = relationInfo{name: fmt.Sprintf("unknown-%d", msg.RelationID)}
		}

		// Create data map and extract values from tuple data
		data := r.decoder.ExtractTupleData(msg.Tuple, relInfo.columns)

		ev := &model.CDCEvent{
			Table:      relInfo.name,
			Operation:  model.InsertOp,
			Timestamp:  time.Now(),
			CommitTime: r.commitTime,
			LSN:        uint64(walStart),
			After:      data,
		}

		log.Debug().Str("table", relInfo.name).Str("op", string(model.InsertOp)).Int("after_fields", len(data)).Str("lsn", walStart.String()).Msg("insert event")
		return r.deliver(ctx, ch, ev)

	case *pglogrepl.UpdateMessage:
		// Create CDC event for update
		relInfo, ok := r.relations[msg.RelationID]
		if !ok {
			log.Warn().Uint32("relationID", msg.RelationID).Msg("unknown relation ID for update")
			relInfo = relationInfo{name: fmt.Sprintf("unknown-%d", msg.RelationID)}
		}

		// Extract the new data from the tuple
		newData := r.decoder.ExtractTupleData(msg.NewTuple, relInfo.columns)

		// Extract old data if available (OldTupleType can be 'K' for key or 'O' for old values)
		var oldData map[string]any
		if msg.OldTupleType == KEY {
			oldData = r.decoder.ExtractKeyOnlyTupleData(msg.OldTuple, relInfo.columns)
		} else if msg.OldTupleType == ALL {
			oldData = r.decoder.ExtractTupleData(msg.OldTuple, relInfo.columns)
		}

		ev := &model.CDCEvent{
			Table:      relInfo.name,
			Operation:  model.UpdateOp,
			Timestamp:  time.Now(),
			CommitTime: r.commitTime,
			LSN:        uint64(walStart),
			Before:     oldData,
			After:      newData,
		}

		log.Debug().Str("table", relInfo.name).Str("op", string(model.UpdateOp)).Int("before_fields", len(oldData)).Int("after_fields", len(newData)).Msg("update event")
		return r.deliver(ctx, ch, ev)

	case *pglogrepl.DeleteMessage:
		// Create CDC event for delete
		relInfo, ok := r.relations[msg.RelationID]
		if !ok {
			log.Warn().Uint32("relationID", msg.RelationID).Msg("unknown relation ID for delete")
			relInfo = relationInfo{name: fmt.Sprintf("unknown-%d", msg.RelationID)}
		}

		// For deletes, extract data from the old tuple if available
		var data map[string]any
		if msg.OldTupleType == KEY {
			data = r.decoder.ExtractKeyOnlyTupleData(msg.OldTuple, relInfo.columns)
		} else if msg.OldTupleType == ALL {
			data = r.decoder.ExtractTupleData(msg.OldTuple, relInfo.columns)
		}

		ev := &model.CDCEvent{
			Table:      relInfo.name,
			Operation:  model.DeleteOp,
			Timestamp:  time.Now(),
			CommitTime: r.commitTime,
			LSN:        uint64(walStart),
			Before:     data,
		}

		log.Debug().Str("table", relInfo.name).Str("op", string(model.DeleteOp)).Int("before_fields", len(data)).Msg("delete event")
		return r.deliver(ctx, ch, ev)

	case *pglogrepl.BeginMessage:
		r.commitTime = msg.CommitTime
		log.Debug().Uint32("xid", msg.Xid).Msg("begin transaction")

	case *pglogrepl.CommitMessage:
		ev := &model.CDCEvent{
			Table:      "_transaction", // Special table name for transaction events
			Operation:  model.CommitOp,
			Timestamp:  time.Now(),
			CommitTime: msg.CommitTime,
			LSN:        uint64(msg.TransactionEndLSN),
		}

		log.Debug().Str("lsn", msg.TransactionEndLSN.String()).Msg("commit transaction")
		return r.deliver(ctx, ch, ev)

	case *pglogrepl.TruncateMessage:
		log.Debug().Msg("truncate message")

	default:
		log.Debug().Str("type", fmt.Sprintf("%T", msg)).Msg("other message type")
	}
	return nil
}
