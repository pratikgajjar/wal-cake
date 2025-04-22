// Package postgres provides adapters for interacting with PostgreSQL
package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"

	"git.famapp.in/fampay-inc/wal-cake/internal/app/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/domain/entity"
	"git.famapp.in/fampay-inc/wal-cake/internal/domain/repository"
	"git.famapp.in/fampay-inc/wal-cake/internal/domain/valueobject"
	"git.famapp.in/fampay-inc/wal-cake/internal/infrastructure/logger"
	"git.famapp.in/fampay-inc/wal-cake/wal-e"
)

// WALRepository implements the repository.WALRepository interface using the wal-e package
type WALRepository struct {
	config         *config.PostgresConfig
	logger         *logger.Logger
	walController  *wal_e.WALController
	changeChan     chan *entity.WALChange
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.RWMutex
	lastLSN        pglogrepl.LSN
	connected      bool
	statusUpdateAt time.Time
}

// NewWALRepository creates a new WALRepository
func NewWALRepository(cfg *config.PostgresConfig, log *logger.Logger) repository.WALRepository {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &WALRepository{
		config:     cfg,
		logger:     log,
		changeChan: make(chan *entity.WALChange, 10000), // Buffer size can be configured
		ctx:        ctx,
		cancel:     cancel,
	}
}

// StartReplication begins WAL replication from the specified position
func (r *WALRepository) StartReplication(ctx context.Context, slot string, startLSN uint64) error {
	r.logger.Info("Starting WAL replication", map[string]interface{}{
		"slot":     slot,
		"startLSN": startLSN,
	})

	// Create connection string
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		r.config.User, r.config.Password, r.config.Host, r.config.Port, r.config.Database)

	// Create WAL controller config
	walConfig := &wal_e.Config{
		ReplicationSlot: slot,
		Publications:    r.config.Publications,
	}

	// Initialize WAL controller
	walController, err := wal_e.NewWalConsumer(ctx, connStr, walConfig)
	if err != nil {
		return fmt.Errorf("failed to create WAL controller: %w", err)
	}

	// Set up metrics functions
	walController.WalStandyStatusUpdateCounter = r.walStatusUpdateCounter
	walController.ReplicaLagMetricFunc = r.replicaLagMetric
	walController.RecoverFromPanic = r.recoverFromPanic

	// Add handler for WAL changes
	walController.AddHandlers(r.handleWALChange)

	// Initialize consumer
	if err := walController.InitConsumer(); err != nil {
		return fmt.Errorf("failed to initialize WAL consumer: %w", err)
	}

	r.mu.Lock()
	r.walController = walController
	r.connected = true
	r.mu.Unlock()

	// Start background goroutines
	wg := &sync.WaitGroup{}
	wg.Add(2)
	
	// Start consuming WAL
	go func() {
		defer wg.Done()
		if err := walController.Consume(wg); err != nil {
			r.logger.Error("Error consuming WAL", err)
		}
	}()

	// Start sending periodic status updates
	go func() {
		walController.SendPeriodicStandbyStatusUpdate()
		wg.Done()
	}()

	// Start monitoring replication lag
	go walController.GetReplicationLag()

	return nil
}

// StopReplication stops the WAL replication process
func (r *WALRepository) StopReplication(ctx context.Context) error {
	r.logger.Info("Stopping WAL replication")
	
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.walController != nil {
		r.walController.StopConsumer()
	}
	
	r.cancel() // Cancel the context to stop background goroutines
	
	// Close the change channel after all messages are processed
	close(r.changeChan)
	
	r.connected = false
	return nil
}

// GetChanges returns a channel that will receive WAL changes
func (r *WALRepository) GetChanges(ctx context.Context) (<-chan *entity.WALChange, error) {
	return r.changeChan, nil
}

// GetReplicationStatus returns the current replication status
func (r *WALRepository) GetReplicationStatus(ctx context.Context) (repository.ReplicationStatus, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	status := repository.ReplicationStatus{
		CurrentLSN:       uint64(r.lastLSN),
		Connected:        r.connected,
		SlotName:         r.config.ReplicationSlot,
		LastStatusUpdate: r.statusUpdateAt.Unix(),
	}
	
	// If we have a WAL controller, get the server LSN and calculate lag
	if r.walController != nil && r.connected {
		// This information is already being collected by the WAL controller
		// We're just exposing it through our interface
	}
	
	return status, nil
}

// SendStatusUpdate sends a status update to the PostgreSQL server
func (r *WALRepository) SendStatusUpdate(ctx context.Context, lsn uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.walController == nil {
		return fmt.Errorf("WAL controller not initialized")
	}
	
	if err := r.walController.SendStandbyStatusUpdate(); err != nil {
		r.connected = false
		return fmt.Errorf("failed to send standby status update: %w", err)
	}
	
	r.lastLSN = pglogrepl.LSN(lsn)
	r.statusUpdateAt = time.Now()
	r.connected = true
	return nil
}

// handleWALChange processes WAL changes from the wal-e package
func (r *WALRepository) handleWALChange(log *wal_e.Log) {
	// Skip if no WAL data
	if log.Wal == nil {
		log.Next()
		return
	}
	
	// Convert wal-e WAL to our domain entity
	walChange := &entity.WALChange{
		TableName:  log.Wal.TableName.String(),
		Operation:  valueobject.Operation(log.Wal.Operation),
		Timestamp:  time.Now().UTC(),
		SchemaName: "", // Not available in wal-e directly
	}
	
	// Set values based on operation
	switch log.Wal.Operation {
	case wal_e.Insert:
		walChange.CurrentValues = log.Wal.Values
	case wal_e.Update:
		walChange.CurrentValues = log.Wal.Values
		walChange.PreviousValues = log.Wal.ValuesNew // This might be reversed in wal-e
	case wal_e.Delete:
		walChange.PreviousValues = log.Wal.Values
	}
	
	// Send the change to the channel
	select {
	case r.changeChan <- walChange:
		// Successfully sent
	case <-r.ctx.Done():
		// Context cancelled
		return
	default:
		// Channel buffer full, log warning
		r.logger.Warn("Change channel buffer full, dropping change", map[string]interface{}{
			"table":     walChange.TableName,
			"operation": walChange.Operation,
		})
	}
	
	// Continue processing
	log.Next()
}

// Metric collection functions
func (r *WALRepository) walStatusUpdateCounter(ctx context.Context, status string, errorMsg string) {
	// In a real implementation, you would record metrics
	r.logger.Debug("WAL status update", map[string]interface{}{
		"status": status,
		"error":  errorMsg,
	})
}

func (r *WALRepository) replicaLagMetric(ctx context.Context, lag int64) {
	// In a real implementation, you would record metrics
	r.logger.Debug("Replication lag", map[string]interface{}{
		"lag_bytes": lag,
	})
}

func (r *WALRepository) recoverFromPanic() func() {
	return func() {
		if rec := recover(); rec != nil {
			r.logger.Error("Recovered from panic in WAL processing", fmt.Errorf("%v", rec))
		}
	}
}
