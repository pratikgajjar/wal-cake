package replication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"

	"git.famapp.in/fampay-inc/wal-cake/internal/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

const (
	KEY = 'K'
	ALL = 'O'
)

// PGReplicator interface defines methods for PostgreSQL logical replication
type PGReplicator interface {
	// Start begins replication and sends CDC events to the provided channel
	// It also listens for acknowledged LSNs on the ackCh to update the replication position
	Start(ctx context.Context, eventsCh chan<- *model.CDCEvent, ackCh <-chan uint64) error
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
}

// NewPGReplicator creates a new PostgreSQL replicator
func NewPGReplicator(cfg *config.Config) PGReplicator {
	relations := make(map[uint32]relationInfo)
	return &pgReplicator{
		cfg:       cfg,
		relations: relations,
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

// getStartLSN gets the confirmed LSN position from the replication slot
func (r *pgReplicator) getStartLSN(ctx context.Context) (pglogrepl.LSN, error) {
	// First try to get the confirmed LSN from the replication slot
	var lsn pglogrepl.LSN
	err := r.queryConn.QueryRow(ctx, "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", r.cfg.Slot).Scan(&lsn)
	if err == nil {
		log.Info().Str("lsn", lsn.String()).Msg("Starting replication from confirmed LSN position")
		return lsn, nil
	}

	// If we couldn't get a confirmed LSN, use the system position
	identifyResult, err := pglogrepl.IdentifySystem(ctx, r.repConn)
	if err != nil {
		return 0, fmt.Errorf("failed to identify system: %w", err)
	}

	log.Info().Str("lsn", identifyResult.XLogPos.String()).Msg("Starting replication from system XLogPos")
	return identifyResult.XLogPos, nil
}

func (r *pgReplicator) SendStandbyStatusUpdate(ctx context.Context, replyRequested bool) error {
	status := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: r.lastAckedLSN,
		WALFlushPosition: r.lastAckedLSN,
		WALApplyPosition: r.lastAckedLSN,
		ClientTime:       time.Now(),
		ReplyRequested:   replyRequested,
	}

	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.repConn, status)
	if err != nil {
		log.Warn().Err(err).Msg("failed to send standby status update")
		return err
	}
	// Log standby status updates appropriately
	if replyRequested {
		log.Info().Str("lsn", r.lastAckedLSN.String()).Msg("Sent requested standby status update")
	} else {
		log.Debug().Str("lsn", r.lastAckedLSN.String()).Msg("Sent periodic standby status update")
	}

	return nil
}

func (r *pgReplicator) Start(ctx context.Context, ch chan<- *model.CDCEvent, ackCh <-chan uint64) error {
	log.Info().Str("slot", r.cfg.Slot).Str("publication", r.cfg.Publication).Msg("starting PostgreSQL replication")

	var err error
	// Connect via pgconn for replication
	r.repConn, err = pgconn.Connect(ctx, r.cfg.PGConn+"?replication=database")
	if err != nil {
		return fmt.Errorf("failed to connect to database for replication: %w", err)
	}
	defer r.repConn.Close(ctx)

	// Create a separate connection for regular queries
	r.queryConn, err = pgx.Connect(ctx, r.cfg.PGConn)
	if err != nil {
		return fmt.Errorf("failed to connect to database for queries: %w", err)
	}
	defer r.queryConn.Close(ctx)

	// Ensure publication exists
	if err := r.ensurePublication(ctx); err != nil {
		return fmt.Errorf("failed to ensure publication: %w", err)
	}

	// Ensure replication slot exists
	if err := r.ensureReplicationSlot(ctx); err != nil {
		return fmt.Errorf("failed to ensure replication slot: %w", err)
	}

	// Get the starting LSN position
	r.lastAckedLSN, err = r.getStartLSN(ctx)
	if err != nil {
		return fmt.Errorf("failed to get starting LSN: %w", err)
	}

	// Set up plugin arguments
	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", r.cfg.Publication),
	}

	// Start replication
	if err := pglogrepl.StartReplication(ctx, r.repConn, r.cfg.Slot, r.lastAckedLSN, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	// Set up a ticker for sending standby status updates
	const receiveTimeout = 5 * time.Second
	standbyMessageTicker := time.NewTicker(receiveTimeout)
	defer standbyMessageTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Send a final status update before shutting down
			return nil
		case lsn := <-ackCh:
			// Update the last acknowledged LSN
			newLSN := pglogrepl.LSN(lsn)
			// if newLSN > r.lastAckedLSN {
			r.lastAckedLSN = newLSN
			log.Info().Uint64("lsn", lsn).Str("lsnS", newLSN.String()).Msg("Updated acknowledged LSN position")
			// Send a status update immediately after receiving an acknowledgment
			_ = r.SendStandbyStatusUpdate(ctx, false)
			// }
		case <-standbyMessageTicker.C:
			// Send periodic status updates
			_ = r.SendStandbyStatusUpdate(ctx, true)
		default:
			// Set up a timeout context for receiving messages
			receiveCtx, cancel := context.WithTimeout(ctx, receiveTimeout)
			msg, err := r.repConn.ReceiveMessage(receiveCtx)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					// This is just a timeout, continue
					continue
				} else if pgErr, ok := err.(*pgconn.PgError); ok {
					log.Error().Err(pgErr).Str("code", pgErr.Code).Msg("received PG error")
				} else if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					log.Error().Err(err).Msg("failed to receive message")
				}
				continue
			}
			// Check for error message
			if errMsg, ok := msg.(*pgproto3.ErrorResponse); ok {
				log.Error().Str("severity", errMsg.Severity).Str("code", errMsg.Code).Str("message", errMsg.Message).Msg("received Postgres error")
				continue
			}

			// Process the message based on its type
			copyData, ok := msg.(*pgproto3.CopyData)
			if !ok {
				log.Warn().Msgf("received unexpected message type: %T", msg)
				continue
			}

			// Handle message based on the first byte
			switch copyData.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				// Primary keepalive message
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					log.Error().Err(err).Msg("failed to parse primary keepalive message")
					continue
				}
				// log.Debug().Str("server_wal_end", pkm.ServerWALEnd.String()).Msg("primary keepalive message")
				// If the server requests a reply, send one immediately
				if pkm.ReplyRequested {
					_ = r.SendStandbyStatusUpdate(ctx, true)
				}
			case pglogrepl.XLogDataByteID:
				// Handle XLogData message (actual data)
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					log.Error().Err(err).Msg("failed to parse XLogData")
					continue
				}

				// Calculate the new WAL position
				xLogPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				// log.Debug().Str("xLogPos", xLogPos.String()).Msg("updated wal")
				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					log.Error().Err(err).Msg("failed to parse logical replication message")
					continue
				}
				r.proccessLogicalMsg(logicalMsg, xLogPos, ch)
			default:
				// Any other message type
				log.Warn().Msgf("received unexpected message byte ID: %d", copyData.Data[0])
			}
		}
	}
}

func (r *pgReplicator) proccessLogicalMsg(logicalMsg pglogrepl.Message, xLogPos pglogrepl.LSN, ch chan<- *model.CDCEvent) {
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
		data := extractTupleData(msg.Tuple, relInfo.columns)

		ev := &model.CDCEvent{
			Table:     relInfo.name,
			Operation: model.InsertOp,
			Timestamp: time.Now(),
			LSN:       uint64(xLogPos),
			After:     data,
		}

		log.Debug().Str("table", relInfo.name).Str("op", string(model.InsertOp)).Int("after_fields", len(data)).Str("lsn", xLogPos.String()).Msg("insert event")
		ch <- ev

	case *pglogrepl.UpdateMessage:
		// Create CDC event for update
		relInfo, ok := r.relations[msg.RelationID]
		if !ok {
			log.Warn().Uint32("relationID", msg.RelationID).Msg("unknown relation ID for update")
			relInfo = relationInfo{name: fmt.Sprintf("unknown-%d", msg.RelationID)}
		}

		// Extract the new data from the tuple
		newData := extractTupleData(msg.NewTuple, relInfo.columns)

		// Extract old data if available (OldTupleType can be 'K' for key or 'O' for old values)
		var oldData map[string]any
		if msg.OldTupleType == KEY {
			oldData = extractKeyOnlyTupleData(msg.OldTuple, relInfo.columns)
		} else if msg.OldTupleType == ALL {
			oldData = extractTupleData(msg.OldTuple, relInfo.columns)
		}

		ev := &model.CDCEvent{
			Table:     relInfo.name,
			Operation: model.UpdateOp,
			Timestamp: time.Now(),
			LSN:       uint64(xLogPos),
			Before:    oldData,
			After:     newData,
		}

		log.Debug().Str("table", relInfo.name).Str("op", string(model.UpdateOp)).Int("before_fields", len(oldData)).Int("after_fields", len(newData)).Msg("update event")
		ch <- ev

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
			data = extractKeyOnlyTupleData(msg.OldTuple, relInfo.columns)
		} else if msg.OldTupleType == ALL {
			data = extractTupleData(msg.OldTuple, relInfo.columns)
		}

		ev := &model.CDCEvent{
			Table:     relInfo.name,
			Operation: model.DeleteOp,
			Timestamp: time.Now(),
			LSN:       uint64(xLogPos),
			Before:    data,
		}

		log.Debug().Str("table", relInfo.name).Str("op", string(model.DeleteOp)).Int("before_fields", len(data)).Msg("delete event")
		ch <- ev

	case *pglogrepl.BeginMessage:
		log.Debug().Uint32("xid", msg.Xid).Msg("begin transaction")

	case *pglogrepl.CommitMessage:
		ev := &model.CDCEvent{
			Table:     "_transaction", // Special table name for transaction events
			Operation: model.CommitOp,
			Timestamp: time.Now(),
			LSN:       uint64(xLogPos),
		}

		log.Debug().Str("lsn", xLogPos.String()).Msg("commit transaction")
		ch <- ev

	case *pglogrepl.TruncateMessage:
		log.Debug().Msg("truncate message")

	default:
		log.Debug().Str("type", fmt.Sprintf("%T", msg)).Msg("other message type")
	}
}
