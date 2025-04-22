// Package worker provides the primary adapter for processing WAL changes and writing to S3 Tables
package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"git.famapp.in/fampay-inc/wal-cake/internal/app/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/domain/entity"
	"git.famapp.in/fampay-inc/wal-cake/internal/domain/repository"
	"git.famapp.in/fampay-inc/wal-cake/internal/infrastructure/logger"
)

// WALToS3Worker is responsible for processing WAL changes and writing them to S3 Tables
type WALToS3Worker struct {
	config         *config.Config
	walRepo        repository.WALRepository
	s3TablesRepo   repository.S3TablesRepository
	logger         *logger.Logger
	ctx            context.Context
	cancel         context.CancelFunc
	changeBuffers  map[string][]*entity.WALChange
	bufferMutex    sync.RWMutex
	lastFlushTime  time.Time
	metricsEnabled bool
	wg             sync.WaitGroup
}

// NewWALToS3Worker creates a new WALToS3Worker
func NewWALToS3Worker(
	cfg *config.Config,
	walRepo repository.WALRepository,
	s3TablesRepo repository.S3TablesRepository,
	log *logger.Logger,
) *WALToS3Worker {
	// Create a cancellable context for controlling the worker's lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	
	return &WALToS3Worker{
		config:         cfg,
		walRepo:        walRepo,
		s3TablesRepo:   s3TablesRepo,
		logger:         log,
		ctx:            ctx,
		cancel:         cancel,
		changeBuffers:  make(map[string][]*entity.WALChange),
		lastFlushTime:  time.Now(),
		metricsEnabled: cfg.Metrics.Enabled,
	}
}

// Start begins processing WAL changes
func (w *WALToS3Worker) Start() error {
	w.logger.Info("Starting WAL to S3 worker", map[string]interface{}{
		"replication_slot": w.config.Postgres.ReplicationSlot,
		"publications":     w.config.Postgres.Publications,
		"s3_bucket":        w.config.S3Tables.BucketName,
		"batch_size":       w.config.S3Tables.BatchSize,
		"flush_interval":   w.config.S3Tables.FlushInterval,
	})

	// Start replication from the last known position
	// In a production system, you would persist and recover the last LSN
	err := w.walRepo.StartReplication(w.ctx, w.config.Postgres.ReplicationSlot, 0)
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	// Get the changes channel
	changeChan, err := w.walRepo.GetChanges(w.ctx)
	if err != nil {
		return fmt.Errorf("failed to get changes channel: %w", err)
	}

	// Start background goroutines with proper wait group tracking
	w.wg.Add(3)
	go func() {
		defer w.wg.Done()
		w.processChanges(changeChan)
	}()
	
	go func() {
		defer w.wg.Done()
		w.flushBuffersRegularly()
	}()
	
	go func() {
		defer w.wg.Done()
		w.monitorReplicationStatus()
	}()

	return nil
}

// Stop gracefully stops the worker
func (w *WALToS3Worker) Stop() error {
	w.logger.Info("Stopping WAL to S3 worker")
	
	// Create a context with timeout for shutdown operations
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), w.config.Worker.ShutdownTimeout)
	defer shutdownCancel()
	
	// Cancel the main context to stop all goroutines
	w.cancel()
	
	// Wait for goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		w.logger.Info("All worker goroutines stopped gracefully")
	case <-shutdownCtx.Done():
		w.logger.Warn("Timed out waiting for worker goroutines to stop", map[string]interface{}{
			"timeout": w.config.Worker.ShutdownTimeout.String(),
		})
	}
	
	// Flush any remaining changes
	if err := w.flushAllBuffers(); err != nil {
		w.logger.Error("Error flushing buffers during shutdown", err)
	}
	
	// Stop replication
	if err := w.walRepo.StopReplication(shutdownCtx); err != nil {
		return fmt.Errorf("failed to stop replication: %w", err)
	}
	
	return nil
}

// processChanges processes WAL changes from the channel
func (w *WALToS3Worker) processChanges(changeChan <-chan *entity.WALChange) {
	// Use a ticker to periodically log processing statistics
	statsTicker := time.NewTicker(1 * time.Minute)
	defer statsTicker.Stop()
	
	processedCount := 0
	startTime := time.Now()
	
	for {
		select {
		case <-w.ctx.Done():
			w.logger.Info("Change processing stopped", map[string]interface{}{
				"total_processed": processedCount,
				"run_duration":   time.Since(startTime).String(),
			})
			return
			
		case change, ok := <-changeChan:
			if !ok {
				w.logger.Info("Change channel closed", map[string]interface{}{
					"total_processed": processedCount,
					"run_duration":   time.Since(startTime).String(),
				})
				return
			}
			
			// Process the change
			w.processChange(change)
			processedCount++
			
		case <-statsTicker.C:
			// Log processing statistics periodically
			w.logger.Info("Processing statistics", map[string]interface{}{
				"processed_count": processedCount,
				"running_for":    time.Since(startTime).String(),
				"buffer_sizes":   w.getBufferSizes(),
			})
		}
	}
}

// getBufferSizes returns the current size of each buffer
func (w *WALToS3Worker) getBufferSizes() map[string]int {
	w.bufferMutex.RLock()
	defer w.bufferMutex.RUnlock()
	
	sizes := make(map[string]int, len(w.changeBuffers))
	for table, buffer := range w.changeBuffers {
		sizes[table] = len(buffer)
	}
	
	return sizes
}

// processChange processes a single WAL change
func (w *WALToS3Worker) processChange(change *entity.WALChange) {
	tableName := change.GetTableFullName()
	
	w.logger.Debug("Processing change", map[string]interface{}{
		"table":     tableName,
		"operation": change.Operation,
		"lsn":       change.LSN,
	})
	
	// Add the change to the appropriate buffer
	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()
	
	if _, ok := w.changeBuffers[tableName]; !ok {
		w.changeBuffers[tableName] = make([]*entity.WALChange, 0, w.config.S3Tables.BatchSize)
	}
	
	w.changeBuffers[tableName] = append(w.changeBuffers[tableName], change)
	
	// If the buffer has reached the batch size, flush it
	if len(w.changeBuffers[tableName]) >= w.config.S3Tables.BatchSize {
		w.flushBuffer(tableName)
	}
}

// flushBuffersRegularly flushes buffers at regular intervals
func (w *WALToS3Worker) flushBuffersRegularly() {
	ticker := time.NewTicker(w.config.S3Tables.FlushInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if err := w.flushAllBuffers(); err != nil {
				w.logger.Error("Error flushing buffers", err)
			}
		}
	}
}

// flushAllBuffers flushes all change buffers
func (w *WALToS3Worker) flushAllBuffers() error {
	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()
	
	if len(w.changeBuffers) == 0 {
		return nil
	}
	
	w.logger.Info("Flushing all buffers", map[string]interface{}{
		"buffer_count": len(w.changeBuffers),
	})
	
	for tableName := range w.changeBuffers {
		if len(w.changeBuffers[tableName]) > 0 {
			w.flushBuffer(tableName)
		}
	}
	
	w.lastFlushTime = time.Now()
	return nil
}

// flushBuffer flushes a specific buffer
// Note: This method assumes the caller holds the bufferMutex lock
func (w *WALToS3Worker) flushBuffer(tableName string) {
	changes := w.changeBuffers[tableName]
	if len(changes) == 0 {
		return
	}
	
	w.logger.Info("Flushing buffer", map[string]interface{}{
		"table":        tableName,
		"change_count": len(changes),
	})
	
	// Create a copy of the changes to process
	changesToProcess := make([]*entity.WALChange, len(changes))
	copy(changesToProcess, changes)
	
	// Clear the buffer
	w.changeBuffers[tableName] = w.changeBuffers[tableName][:0]
	
	// Release the lock while writing to S3
	w.bufferMutex.Unlock()
	
	// Create a timeout context for the write operation
	writeCtx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()
	
	// Track the start time for metrics
	startTime := time.Now()
	
	// Write the changes to S3 Tables
	err := w.s3TablesRepo.WriteChanges(writeCtx, tableName, changesToProcess)
	
	// Calculate operation duration
	duration := time.Since(startTime)
	
	// Reacquire the lock
	w.bufferMutex.Lock()
	
	if err != nil {
		w.logger.Error("Failed to write changes to S3", err, map[string]interface{}{
			"table":        tableName,
			"change_count": len(changesToProcess),
			"duration_ms":  duration.Milliseconds(),
		})
		
		// In case of error, add the changes back to the buffer
		// This is a simplified retry mechanism
		// In a production system, you would implement a more sophisticated retry strategy
		w.changeBuffers[tableName] = append(w.changeBuffers[tableName], changesToProcess...)
	} else {
		w.logger.Info("Successfully wrote changes to S3", map[string]interface{}{
			"table":        tableName,
			"change_count": len(changesToProcess),
			"duration_ms":  duration.Milliseconds(),
		})
		
		if w.metricsEnabled {
			// Record metrics for successful flush
			w.recordFlushMetrics(tableName, len(changesToProcess), duration)
		}
	}
}

// monitorReplicationStatus monitors and logs replication status
func (w *WALToS3Worker) monitorReplicationStatus() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			status, err := w.walRepo.GetReplicationStatus(w.ctx)
			if err != nil {
				w.logger.Error("Failed to get replication status", err)
				continue
			}
			
			w.logger.Info("Replication status", map[string]interface{}{
				"current_lsn":  status.CurrentLSN,
				"server_lsn":   status.ServerLSN,
				"lag":          status.ReplicationLag,
				"connected":    status.Connected,
				"slot":         status.SlotName,
				"last_update":  time.Unix(status.LastStatusUpdate, 0),
			})
			
			if w.metricsEnabled {
				w.recordReplicationMetrics(status)
			}
		}
	}
}

// recordFlushMetrics records metrics for buffer flushes
func (w *WALToS3Worker) recordFlushMetrics(tableName string, changeCount int, duration time.Duration) {
	// In a real implementation, you would record metrics using your metrics system
	// For example, using Prometheus:
	// metrics.FlushCounter.WithLabelValues(tableName).Inc()
	// metrics.ChangeCounter.WithLabelValues(tableName).Add(float64(changeCount))
	// metrics.FlushLatency.WithLabelValues(tableName).Observe(duration.Seconds())
	
	// Log metrics for now
	w.logger.Debug("Flush metrics", map[string]interface{}{
		"table":         tableName,
		"change_count":  changeCount,
		"duration_ms":   duration.Milliseconds(),
		"changes_per_s": float64(changeCount) / duration.Seconds(),
	})
}

// recordReplicationMetrics records metrics for replication status
func (w *WALToS3Worker) recordReplicationMetrics(status repository.ReplicationStatus) {
	// In a real implementation, you would record metrics using your metrics system
	// For example, using Prometheus:
	// metrics.ReplicationLag.WithLabelValues(status.SlotName).Set(float64(status.ReplicationLag))
	// metrics.ReplicationConnected.WithLabelValues(status.SlotName).Set(boolToFloat64(status.Connected))
}
