package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"git.famapp.in/fampay-inc/wal-cake/internal/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
	"git.famapp.in/fampay-inc/wal-cake/internal/replication"
	"git.famapp.in/fampay-inc/wal-cake/internal/storage"
	"git.famapp.in/fampay-inc/wal-cake/internal/transform"
)

// CDCBatch represents a batch of CDC events with their max LSN
type CDCBatch struct {
	Events []*model.CDCEvent
}

// LastLSN returns the highest LSN in the batch
func (b *CDCBatch) LastLSN() uint64 {
	if len(b.Events) == 0 {
		return 0
	}
	return b.Events[len(b.Events)-1].LSN
}

// DatePartition represents a slice of events from the same date
type DatePartition struct {
	Date   string           // Date in YYYY/MM/DD format
	Events []*model.CDCEvent // Slice of events for this date (reference to original data)
}

// DatePartitionIterator allows iterating over date-partitioned batches of events
type DatePartitionIterator struct {
	batch     *CDCBatch
	cursorPos int
}

// NewDatePartitionIterator creates a new iterator for date-partitioned events
func (b *CDCBatch) NewDatePartitionIterator() *DatePartitionIterator {
	return &DatePartitionIterator{
		batch:     b,
		cursorPos: 0,
	}
}

// Next returns the next date partition or nil if there are no more partitions
func (it *DatePartitionIterator) Next() *DatePartition {
	if it.cursorPos >= len(it.batch.Events) {
		return nil // No more events
	}
	
	// Get the date for the current group of events
	currentDate := it.batch.Events[it.cursorPos].Timestamp.Format("2006/01/02")
	
	// Find all events with the same date
	startIdx := it.cursorPos
	for it.cursorPos < len(it.batch.Events) && 
		it.batch.Events[it.cursorPos].Timestamp.Format("2006/01/02") == currentDate {
		it.cursorPos++
	}
	endIdx := it.cursorPos
	
	// Return the partition with a slice reference to the original events
	return &DatePartition{
		Date:   currentDate,
		Events: it.batch.Events[startIdx:endIdx],
	}
}

func main() {
	cfg := config.LoadConfig()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Channel for CDC events from the replicator
	eventsCh := make(chan *model.CDCEvent, cfg.BatchSize*2)

	// Channel for acknowledging LSNs after successful S3 uploads
	ackCh := make(chan uint64, 10)
	defer close(ackCh)

	repl := replication.NewPGReplicator(cfg)
	transformer := transform.NewParquetWriter()

	// Add filter to exclude commit events from Parquet files
	transformer.AddFilter(func(event *model.CDCEvent) bool {
		return event.Operation != model.CommitOp
	})

	uploader := storage.NewS3Uploader(cfg)

	// Start the replicator with LSN acknowledgment
	go func() {
		if err := repl.Start(ctx, eventsCh, ackCh); err != nil {
			log.Fatal().Err(err).Msg("Replication error")
		}
	}()

	ticker := time.NewTicker(cfg.FlushInterval)
	defer ticker.Stop()

	// Use a WaitGroup to track in-flight batches
	var wg sync.WaitGroup

	// Buffer to collect events
	var buffer []*model.CDCEvent

	for {
		select {
		case <-ctx.Done():
			// Wait for all in-flight batches to complete before shutting down
			log.Info().Msg("Shutting down, waiting for in-flight batches to complete")
			wg.Wait()
			log.Info().Msg("All batches completed, shutting down")
			return

		case ev := <-eventsCh:
			// Add event to buffer and track max LSN
			buffer = append(buffer, ev)
			// Process batch when it reaches the configured size
			if len(buffer) >= cfg.BatchSize {
				batch := &CDCBatch{Events: buffer}
				wg.Add(1)
				go func() {
					defer wg.Done()
					processBatch(ctx, cfg, batch, transformer, uploader, ackCh)
				}()

				// Reset buffer and maxLSN
				buffer = make([]*model.CDCEvent, 0, cfg.BatchSize)
				// Reset ticker to avoid small batches right after this one
				ticker.Reset(cfg.FlushInterval)
			}

		case <-ticker.C:
			// Process any remaining events on flush interval
			if len(buffer) > 0 {
				batch := &CDCBatch{Events: buffer}
				wg.Add(1)
				go func() {
					defer wg.Done()
					processBatch(ctx, cfg, batch, transformer, uploader, ackCh)
				}()
				// Reset buffer and maxLSN
				buffer = make([]*model.CDCEvent, 0, cfg.BatchSize)
			}
		}
	}
}

// withRetry executes the given operation with exponential backoff retry logic
// It returns true if the operation succeeded, false if all retries failed
func withRetry[T any](ctx context.Context, maxRetries int, initialDelay time.Duration, operation func() (T, error), onError func(err error, attempt int) bool) (T, bool) {
	var result T
	retryDelay := initialDelay

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return result, false
		default:
			// Continue processing
		}

		// Execute the operation
		var err error
		result, err = operation()
		if err == nil {
			// Operation succeeded
			return result, true
		}

		// Handle the error
		shouldContinue := onError(err, attempt)
		
		// If this is the last attempt or we should not continue, give up
		if attempt == maxRetries || !shouldContinue {
			return result, false
		}

		// Wait before retrying
		time.Sleep(retryDelay)
		retryDelay *= 2 // Exponential backoff
	}

	return result, false
}

// processBatch processes a batch of CDC events, writing them directly to S3 and acknowledging LSN on success
func processBatch(ctx context.Context, cfg *config.Config, batch *CDCBatch, pw transform.ParquetWriter, up storage.S3Uploader, ackCh chan<- uint64) {
	// Skip empty batches
	if len(batch.Events) == 0 {
		return
	}

	// Define retry parameters
	maxRetries := 3
	initialDelay := 1 * time.Second
	
	// Create an iterator to process events by date partition
	iterator := batch.NewDatePartitionIterator()
	
	// Log the total events in the batch
	log.Info().Int("events", len(batch.Events)).Uint64("maxLSN", batch.LastLSN()).Msg("Processing batch")
	
	// Process each date partition
	var partition *DatePartition
	for partition = iterator.Next(); partition != nil; partition = iterator.Next() {
		// Log the events in this partition
		log.Info().Int("events", len(partition.Events)).Str("date", partition.Date).Msg("Processing events for date partition")
		
		// Generate a unique key for this date's batch
		timestamp := time.Now().UnixMicro() // Use current time for uniqueness
		key := fmt.Sprintf("%s/%s/%d.%s.parquet", cfg.Namespace, partition.Date, timestamp, pw.GetCompressionCodec())
		
		// Write events for this date directly to memory as parquet with retry
		parquetData, writeSuccess := withRetry(ctx, maxRetries, initialDelay, 
			func() ([]byte, error) {
				return pw.WriteToBuffer(partition.Events)
			},
			func(err error, attempt int) bool {
				log.Error().Err(err).Int("attempt", attempt).Str("date", partition.Date).Msg("Failed to write Parquet to memory buffer")
				if attempt == maxRetries {
					log.Error().Err(err).Str("date", partition.Date).Msg("All attempts to write Parquet failed for date")
				}
				return true // Always retry until max retries
			},
		)

		if !writeSuccess {
			// Skip to next date partition if writing failed
			continue
		}

		// Upload the parquet data directly to S3 with retry
		_, uploadSuccess := withRetry(ctx, maxRetries, initialDelay,
			func() (struct{}, error) {
				err := up.UploadBytes(ctx, key, parquetData)
				return struct{}{}, err
			},
			func(err error, attempt int) bool {
				log.Error().Err(err).Int("attempt", attempt).Str("date", partition.Date).Msg("Failed to upload Parquet to S3")
				if attempt == maxRetries {
					log.Error().Err(err).Str("date", partition.Date).Msg("All attempts to upload to S3 failed for date")
				}
				return true // Always retry until max retries
			},
		)

		if uploadSuccess {
			// Success for this date's batch!
			log.Info().Int("events", len(partition.Events)).Str("key", key).Str("date", partition.Date).Msg("Successfully uploaded batch to S3 for date")
		}
	}

	// Send the max LSN to the acknowledgment channel after all date partitions have been processed
	select {
	case ackCh <- batch.LastLSN():
		log.Debug().Uint64("lsn", batch.LastLSN()).Msg("Sent LSN Ack")
	case <-ctx.Done():
		log.Warn().Msg("Context cancelled before LSN could be acknowledged")
	}
}
