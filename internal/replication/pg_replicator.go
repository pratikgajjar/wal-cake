package replication

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog/log"

	"git.famapp.in/fampay-inc/wal-cake/internal/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

const statusInterval = 10 * time.Second

// PGReplicator interface defines methods for PostgreSQL logical replication
type PGReplicator interface {
	// Start begins replication and sends CDC events to the provided channel
	// It also listens for acknowledged LSNs on the ackCh to update the replication position
	Start(ctx context.Context, eventsCh chan<- *model.CDCEvent, ackCh <-chan uint64) error
}

type pgReplicator struct {
	cfg  *config.Config
	conn *pgconn.PgConn
	lastAckedLSN pglogrepl.LSN
}

// NewPGReplicator creates a new PostgreSQL replicator
func NewPGReplicator(cfg *config.Config) PGReplicator {
	return &pgReplicator{
		cfg: cfg,
		lastAckedLSN: pglogrepl.LSN(0),
	}
}

func (r *pgReplicator) Start(ctx context.Context, ch chan<- *model.CDCEvent, ackCh <-chan uint64) error {
	log.Info().Str("slot", r.cfg.Slot).Str("publication", r.cfg.Publication).Msg("starting PostgreSQL replication")
	
	var err error
	// Connect via pgconn for replication
	r.conn, err = pgconn.Connect(ctx, r.cfg.PGConn)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer r.conn.Close(ctx)

	// Create replication slot
	replSlot := r.cfg.Slot
	_, err = pglogrepl.CreateReplicationSlot(ctx, r.conn, replSlot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create slot: %w", err)
	}

	pluginArgs := []string{fmt.Sprintf("proto_version '1'"), fmt.Sprintf("publication_names '%s'", r.cfg.Publication)}

	// Start replication
	startLSN := pglogrepl.LSN(0)
	if r.cfg.StartLSN != "" {
		startLSN, err = pglogrepl.ParseLSN(r.cfg.StartLSN)
		if err != nil {
			return fmt.Errorf("invalid start-lsn: %w", err)
		}
	}
	if err := pglogrepl.StartReplication(ctx, r.conn, replSlot, startLSN, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	ticker := time.NewTicker(statusInterval)
	defer ticker.Stop()

	// Map to store relation info
	relations := make(map[uint32]string)
	
	// We'll use a direct approach for message handling instead of channels
	
	for {
		select {
		case <-ctx.Done():
			return nil
		
		case lsn := <-ackCh:
			// Update the last acknowledged LSN
			newLSN := pglogrepl.LSN(lsn)
			if newLSN > r.lastAckedLSN {
				r.lastAckedLSN = newLSN
				log.Info().Uint64("lsn", lsn).Msg("Updated acknowledged LSN position")
				
				// Send a standby status update with the new LSN
				status := pglogrepl.StandbyStatusUpdate{
					WALWritePosition: r.lastAckedLSN,
				}
				err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, status)
				if err != nil {
					log.Error().Err(err).Msg("failed to send standby status update after LSN acknowledgment")
				}
			}
		
		case <-ticker.C:
			// Send periodic standby status update
			// Only acknowledge up to the last confirmed LSN
			status := pglogrepl.StandbyStatusUpdate{
				WALWritePosition: r.lastAckedLSN,
			}
			err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, status)
			if err != nil {
				log.Warn().Err(err).Msg("failed to send standby status update")
			}
		
		default:
			msg, err := r.conn.ReceiveMessage(ctx)
			if err != nil {
				return fmt.Errorf("receive message error: %w", err)
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
				log.Debug().Uint64("server_wal_end", uint64(pkm.ServerWALEnd)).Msg("primary keepalive message")
				
				// If the server requests a reply, send one immediately
				if pkm.ReplyRequested {
					// When the server requests a reply, we still only acknowledge up to the last confirmed LSN
					// This ensures we don't acknowledge data that hasn't been safely stored in S3
					status := pglogrepl.StandbyStatusUpdate{WALWritePosition: r.lastAckedLSN}
					err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, status)
					if err != nil {
						log.Error().Err(err).Msg("failed to send requested status update")
					}
				}
			case pglogrepl.XLogDataByteID:
				// Handle XLogData message (actual data)
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					log.Error().Err(err).Msg("failed to parse XLogData")
					continue
				}

				// Update our current WAL position, but don't acknowledge it yet
				// We'll only acknowledge after successful S3 upload
				currentLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))

				// Process the message based on the logical replication protocol
				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					log.Error().Err(err).Msg("failed to parse logical replication message")
					continue
				}

				// Handle different message types
				switch msg := logicalMsg.(type) {
				case *pglogrepl.RelationMessage:
					// Store relation info for later use
					relations[msg.RelationID] = msg.RelationName
					log.Debug().Str("relation", msg.RelationName).Uint32("id", msg.RelationID).Msg("relation message")
					
				case *pglogrepl.InsertMessage:
					// Create CDC event for insert
					tableName := relations[msg.RelationID]
					if tableName == "" {
						tableName = fmt.Sprintf("unknown-%d", msg.RelationID)
					}
					
					ev := &model.CDCEvent{
						Table:     tableName,
						Operation: model.InsertOp,
						Timestamp: time.Now(),
						LSN:       uint64(currentLSN),
						Data:      map[string]interface{}{},
					}
					
					// TODO: Decode tuple data into the Data map
					// This requires tracking column info from relation messages
					
					log.Info().Str("table", tableName).Str("op", string(model.InsertOp)).Msg("insert event")
					ch <- ev
					
					case *pglogrepl.UpdateMessage:
					// Create CDC event for update
					tableName := relations[msg.RelationID]
					if tableName == "" {
						tableName = fmt.Sprintf("unknown-%d", msg.RelationID)
					}
					
					ev := &model.CDCEvent{
						Table:     tableName,
						Operation: model.UpdateOp,
						Timestamp: time.Now(),
						LSN:       uint64(currentLSN),
						Data:      map[string]interface{}{},
					}
					
					// TODO: Decode tuple data into the Data map
					// This requires tracking column info from relation messages
					
					log.Info().Str("table", tableName).Str("op", string(model.UpdateOp)).Msg("update event")
					ch <- ev
					
					case *pglogrepl.DeleteMessage:
					// Create CDC event for delete
					tableName := relations[msg.RelationID]
					if tableName == "" {
						tableName = fmt.Sprintf("unknown-%d", msg.RelationID)
					}
					
					ev := &model.CDCEvent{
						Table:     tableName,
						Operation: model.DeleteOp,
						Timestamp: time.Now(),
						LSN:       uint64(currentLSN),
						Data:      map[string]interface{}{},
					}
					
					// TODO: Decode tuple data into the Data map
					// This requires tracking column info from relation messages
					
					log.Info().Str("table", tableName).Str("op", string(model.DeleteOp)).Msg("delete event")
					ch <- ev
					
					case *pglogrepl.BeginMessage:
					log.Debug().Uint32("xid", msg.Xid).Msg("begin transaction")
				
				case *pglogrepl.CommitMessage:
					log.Debug().Msg("commit transaction")
				
				case *pglogrepl.TruncateMessage:
					log.Debug().Msg("truncate message")
				
				default:
					log.Debug().Str("type", fmt.Sprintf("%T", msg)).Msg("other message type")
				}
			default:
				// Any other message type
				log.Warn().Msgf("received unexpected message byte ID: %d", copyData.Data[0])
			}
		}
	}
}
