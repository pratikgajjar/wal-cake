// Package postgres provides adapters for interacting with PostgreSQL
package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"git.famapp.in/fampay-inc/wal-cake/internal/app/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/domain/entity"
	"git.famapp.in/fampay-inc/wal-cake/internal/domain/repository"
	"git.famapp.in/fampay-inc/wal-cake/internal/infrastructure/logger"
)

// CDCRepository implements the repository.WALRepository interface
// using direct PostgreSQL logical replication protocol
type CDCRepository struct {
	config              *config.PostgresConfig
	logger              *logger.Logger
	conn                *pgx.Conn
	replConn            *pgconn.PgConn
	changeChan          chan *entity.WALChange
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.RWMutex
	lastLSN             pglogrepl.LSN
	serverLSN           pglogrepl.LSN
	connected           bool
	statusUpdateAt      time.Time
	relationCache       map[uint32]*relationData
	publicationName     string
	slotName            string
	standbyMessageTimer *time.Timer
	standbyMsgTimeout   time.Duration
}

// relationData contains information about a PostgreSQL relation (table)
type relationData struct {
	relationID uint32
	namespace  string
	name       string
	columns    []pglogrepl.RelationMessageColumn
}

// NewCDCRepository creates a new CDCRepository
func NewCDCRepository(cfg *config.PostgresConfig, log *logger.Logger) repository.WALRepository {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &CDCRepository{
		config:            cfg,
		logger:            log,
		changeChan:        make(chan *entity.WALChange, 10000), // Buffer size can be configured
		ctx:               ctx,
		cancel:            cancel,
		relationCache:     make(map[uint32]*relationData),
		standbyMsgTimeout: 10 * time.Second, // Can be configured
	}
}

// StartReplication begins WAL replication from the specified position
func (r *CDCRepository) StartReplication(ctx context.Context, slot string, startLSN uint64) error {
	r.logger.Info("Starting WAL replication", map[string]interface{}{
		"slot":     slot,
		"startLSN": startLSN,
	})

	r.slotName = slot
	r.publicationName = r.config.Publications

	// Create connection string
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		r.config.User, r.config.Password, r.config.Host, r.config.Port, r.config.Database)

	// Connect to PostgreSQL
	var err error
	r.conn, err = pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	// Create replication connection
	connConfig, err := pgconn.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("failed to parse connection config: %w", err)
	}
	connConfig.RuntimeParams["replication"] = "database"
	
	r.replConn, err = pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("failed to create replication connection: %w", err)
	}

	// Check if slot exists, create if it doesn't
	var slotExists bool
	err = r.conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slot).Scan(&slotExists)
	if err != nil {
		return fmt.Errorf("failed to check if replication slot exists: %w", err)
	}

	if !slotExists {
		r.logger.Info("Creating replication slot", map[string]interface{}{
			"slot": slot,
		})
		_, err = pglogrepl.CreateReplicationSlot(ctx, r.replConn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: false})
		if err != nil {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
	}

	// Check if publication exists, create if it doesn't
	var pubExists bool
	err = r.conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", r.publicationName).Scan(&pubExists)
	if err != nil {
		return fmt.Errorf("failed to check if publication exists: %w", err)
	}

	if !pubExists {
		r.logger.Info("Creating publication", map[string]interface{}{
			"publication": r.publicationName,
		})
		_, err = r.conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", r.publicationName))
		if err != nil {
			return fmt.Errorf("failed to create publication: %w", err)
		}
	}

	// Start replication
	pluginArguments := []string{
		fmt.Sprintf("proto_version '1'"),
		fmt.Sprintf("publication_names '%s'", r.publicationName),
	}

	err = pglogrepl.StartReplication(ctx, r.replConn, slot, pglogrepl.LSN(startLSN), pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArguments,
	})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	r.mu.Lock()
	r.lastLSN = pglogrepl.LSN(startLSN)
	r.connected = true
	r.statusUpdateAt = time.Now()
	r.mu.Unlock()

	// Start background goroutine to process WAL messages
	go r.processWALMessages()

	// Start background goroutine to send standby status updates
	go r.sendPeriodicStandbyStatusUpdates()

	return nil
}

// StopReplication stops the WAL replication process
func (r *CDCRepository) StopReplication(ctx context.Context) error {
	r.logger.Info("Stopping WAL replication")
	
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.standbyMessageTimer != nil {
		r.standbyMessageTimer.Stop()
	}
	
	r.cancel() // Cancel the context to stop background goroutines
	
	// Close connections
	if r.replConn != nil {
		r.replConn.Close(ctx)
	}
	
	if r.conn != nil {
		r.conn.Close(ctx)
	}
	
	// Close the change channel after all messages are processed
	close(r.changeChan)
	
	r.connected = false
	return nil
}

// GetChanges returns a channel that will receive WAL changes
func (r *CDCRepository) GetChanges(ctx context.Context) (<-chan *entity.WALChange, error) {
	return r.changeChan, nil
}

// GetReplicationStatus returns the current replication status
func (r *CDCRepository) GetReplicationStatus(ctx context.Context) (repository.ReplicationStatus, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	status := repository.ReplicationStatus{
		CurrentLSN:       uint64(r.lastLSN),
		ServerLSN:        uint64(r.serverLSN),
		ReplicationLag:   uint64(r.serverLSN - r.lastLSN),
		Connected:        r.connected,
		SlotName:         r.slotName,
		LastStatusUpdate: r.statusUpdateAt.Unix(),
	}
	
	return status, nil
}

// SendStatusUpdate sends a status update to the PostgreSQL server
func (r *CDCRepository) SendStatusUpdate(ctx context.Context, lsn uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.replConn == nil {
		return fmt.Errorf("replication connection not initialized")
	}
	
	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(lsn),
	})
	
	if err != nil {
		r.connected = false
		return fmt.Errorf("failed to send standby status update: %w", err)
	}
	
	r.lastLSN = pglogrepl.LSN(lsn)
	r.statusUpdateAt = time.Now()
	r.connected = true
	return nil
}

// processWALMessages processes WAL messages from the replication connection
func (r *CDCRepository) processWALMessages() {
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
			msg, err := r.replConn.ReceiveMessage(ctx)
			cancel()

			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				r.logger.Error("Failed to receive replication message", err)
				time.Sleep(1 * time.Second)
				continue
			}

			switch msg := msg.(type) {
			case *pgproto3.CopyData:
				switch msg.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						r.logger.Error("Failed to parse primary keepalive message", err)
						continue
					}

					r.mu.Lock()
					r.serverLSN = pkm.ServerWALEnd
					r.mu.Unlock()

					if pkm.ReplyRequested {
						r.sendStandbyStatusUpdate()
					}

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						r.logger.Error("Failed to parse XLogData", err)
						continue
					}

					r.mu.Lock()
					r.serverLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
					r.mu.Unlock()

					if err := r.processXLogData(xld); err != nil {
						r.logger.Error("Failed to process XLogData", err)
					}
				}
			}
		}
	}
}

// processXLogData processes a logical replication XLogData message
func (r *CDCRepository) processXLogData(xld pglogrepl.XLogData) error {
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return fmt.Errorf("failed to parse logical replication message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		r.cacheRelation(msg)

	case *pglogrepl.BeginMessage:
		// Transaction begin, nothing to do

	case *pglogrepl.CommitMessage:
		// Transaction commit, update LSN
		r.mu.Lock()
		r.lastLSN = msg.CommitLSN
		r.mu.Unlock()

	case *pglogrepl.InsertMessage:
		if err := r.handleInsert(msg, xld.WALStart); err != nil {
			return err
		}

	case *pglogrepl.UpdateMessage:
		if err := r.handleUpdate(msg, xld.WALStart); err != nil {
			return err
		}

	case *pglogrepl.DeleteMessage:
		if err := r.handleDelete(msg, xld.WALStart); err != nil {
			return err
		}

	case *pglogrepl.TruncateMessage:
		// Truncate operation, not handling for now
		r.logger.Warn("Truncate operation not supported", map[string]interface{}{
			"relations": msg.RelationIDs,
		})

	case *pglogrepl.TypeMessage:
		// Type message, nothing to do

	case *pglogrepl.OriginMessage:
		// Origin message, nothing to do

	default:
		r.logger.Warn("Unhandled message type", map[string]interface{}{
			"type": fmt.Sprintf("%T", msg),
		})
	}

	return nil
}

// cacheRelation caches relation (table) metadata
func (r *CDCRepository) cacheRelation(msg *pglogrepl.RelationMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Convert []*pglogrepl.RelationMessageColumn to []pglogrepl.RelationMessageColumn
	columns := make([]pglogrepl.RelationMessageColumn, len(msg.Columns))
	for i, col := range msg.Columns {
		columns[i] = *col
	}

	r.relationCache[msg.RelationID] = &relationData{
		relationID: msg.RelationID,
		namespace:  msg.Namespace,
		name:       msg.RelationName,
		columns:    columns,
	}

	r.logger.Debug("Cached relation", map[string]interface{}{
		"relation_id": msg.RelationID,
		"namespace":   msg.Namespace,
		"name":        msg.RelationName,
		"columns":     len(columns),
	})
}

// handleInsert processes an insert message
func (r *CDCRepository) handleInsert(msg *pglogrepl.InsertMessage, walStart pglogrepl.LSN) error {
	r.mu.RLock()
	rel, ok := r.relationCache[msg.RelationID]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("relation %d not found in cache", msg.RelationID)
	}

	values, err := r.decodeRow(msg.Tuple, rel.columns)
	if err != nil {
		return fmt.Errorf("failed to decode row: %w", err)
	}

	change := &entity.WALChange{
		SchemaName:    rel.namespace,
		TableName:     rel.name,
		Operation:     "INSERT",
		CurrentValues: values,
		Timestamp:     time.Now().UTC(),
		LSN:           uint64(walStart),
	}

	select {
	case r.changeChan <- change:
		// Successfully sent
	case <-r.ctx.Done():
		// Context cancelled
		return r.ctx.Err()
	default:
		// Channel buffer full, log warning
		r.logger.Warn("Change channel buffer full, dropping change", map[string]interface{}{
			"table":     change.TableName,
			"operation": change.Operation,
		})
	}

	return nil
}

// handleUpdate processes an update message
func (r *CDCRepository) handleUpdate(msg *pglogrepl.UpdateMessage, walStart pglogrepl.LSN) error {
	r.mu.RLock()
	rel, ok := r.relationCache[msg.RelationID]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("relation %d not found in cache", msg.RelationID)
	}

	var oldValues map[string]interface{}
	var err error

	if msg.OldTuple != nil {
		oldValues, err = r.decodeRow(msg.OldTuple, rel.columns)
		if err != nil {
			return fmt.Errorf("failed to decode old row: %w", err)
		}
	}

	newValues, err := r.decodeRow(msg.NewTuple, rel.columns)
	if err != nil {
		return fmt.Errorf("failed to decode new row: %w", err)
	}

	change := &entity.WALChange{
		SchemaName:     rel.namespace,
		TableName:      rel.name,
		Operation:      "UPDATE",
		CurrentValues:  newValues,
		PreviousValues: oldValues,
		Timestamp:      time.Now().UTC(),
		LSN:            uint64(walStart),
	}

	select {
	case r.changeChan <- change:
		// Successfully sent
	case <-r.ctx.Done():
		// Context cancelled
		return r.ctx.Err()
	default:
		// Channel buffer full, log warning
		r.logger.Warn("Change channel buffer full, dropping change", map[string]interface{}{
			"table":     change.TableName,
			"operation": change.Operation,
		})
	}

	return nil
}

// handleDelete processes a delete message
func (r *CDCRepository) handleDelete(msg *pglogrepl.DeleteMessage, walStart pglogrepl.LSN) error {
	r.mu.RLock()
	rel, ok := r.relationCache[msg.RelationID]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("relation %d not found in cache", msg.RelationID)
	}

	values, err := r.decodeRow(msg.OldTuple, rel.columns)
	if err != nil {
		return fmt.Errorf("failed to decode row: %w", err)
	}

	change := &entity.WALChange{
		SchemaName:     rel.namespace,
		TableName:      rel.name,
		Operation:      "DELETE",
		PreviousValues: values,
		Timestamp:      time.Now().UTC(),
		LSN:            uint64(walStart),
	}

	select {
	case r.changeChan <- change:
		// Successfully sent
	case <-r.ctx.Done():
		// Context cancelled
		return r.ctx.Err()
	default:
		// Channel buffer full, log warning
		r.logger.Warn("Change channel buffer full, dropping change", map[string]interface{}{
			"table":     change.TableName,
			"operation": change.Operation,
		})
	}

	return nil
}

// decodeRow decodes a tuple data into a map of column name to value
func (r *CDCRepository) decodeRow(tuple *pglogrepl.TupleData, columns []pglogrepl.RelationMessageColumn) (map[string]interface{}, error) {
	if tuple == nil {
		return nil, nil
	}

	values := make(map[string]interface{})

	for i, col := range tuple.Columns {
		if i >= len(columns) {
			return nil, fmt.Errorf("column index %d out of range", i)
		}

		colName := columns[i].Name

		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 't': // text
			values[colName] = string(col.Data)
		case 'b': // binary
			// For binary data, we'd need to decode based on the PostgreSQL type
			// This is a simplified version that just stores the raw bytes
			values[colName] = col.Data
		case 'u': // unchanged toast
			// Value unchanged, not included in the payload
			// Skip for now
		default:
			return nil, fmt.Errorf("unknown column data type: %c", col.DataType)
		}
	}

	return values, nil
}

// sendStandbyStatusUpdate sends a standby status update to the server
func (r *CDCRepository) sendStandbyStatusUpdate() {
	r.mu.RLock()
	lastLSN := r.lastLSN
	r.mu.RUnlock()

	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()

	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lastLSN,
	})

	if err != nil {
		r.logger.Error("Failed to send standby status update", err)
		return
	}

	r.mu.Lock()
	r.statusUpdateAt = time.Now()
	r.mu.Unlock()

	r.logger.Debug("Sent standby status update", map[string]interface{}{
		"lsn": lastLSN,
	})
}

// sendPeriodicStandbyStatusUpdates sends standby status updates periodically
func (r *CDCRepository) sendPeriodicStandbyStatusUpdates() {
	ticker := time.NewTicker(r.standbyMsgTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.sendStandbyStatusUpdate()
		}
	}
}
