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

func (b *CDCBatch) LastLSN() uint64 {
	return b.Events[len(b.Events)-1].LSN
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
	transformer := transform.NewParquetWriter(cfg)
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

// processBatch processes a batch of CDC events, writing them directly to S3 and acknowledging LSN on success
func processBatch(ctx context.Context, cfg *config.Config, batch *CDCBatch, pw transform.ParquetWriter, up storage.S3Uploader, ackCh chan<- uint64) {
	// Skip empty batches
	if len(batch.Events) == 0 {
		return
	}

	// Generate a unique key for this batch
	timestamp := time.Now().UnixNano()
	key := fmt.Sprintf("%s/%s_%d.parquet", cfg.Slot, cfg.Slot, timestamp)

	log.Info().Int("events", len(batch.Events)).Uint64("maxLSN", batch.LastLSN()).Msg("Processing batch")

	// Implement retry logic with exponential backoff
	maxRetries := 3
	retryDelay := 1 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			log.Warn().Msg("Context cancelled during batch processing")
			return
		default:
			// Continue processing
		}
		// Write events directly to memory as parquet
		parquetData, err := pw.WriteToBuffer(batch.Events)
		if err != nil {
			log.Error().Err(err).Int("attempt", attempt).Msg("Failed to write Parquet to memory buffer")

			// If this is the last attempt, give up
			if attempt == maxRetries {
				log.Error().Err(err).Msg("All attempts to write Parquet failed")
				return
			}

			// Wait before retrying
			time.Sleep(retryDelay)
			retryDelay *= 2 // Exponential backoff
			continue
		}

		// Upload the parquet data directly to S3
		err = up.UploadBytes(ctx, key, parquetData)
		if err != nil {
			log.Error().Err(err).Int("attempt", attempt).Msg("Failed to upload Parquet to S3")

			// If this is the last attempt, give up
			if attempt == maxRetries {
				log.Error().Err(err).Msg("All attempts to upload to S3 failed")
				return
			}

			// Wait before retrying
			time.Sleep(retryDelay)
			retryDelay *= 2 // Exponential backoff
			continue
		}
		// Success! Acknowledge the LSN
		log.Info().Int("events", len(batch.Events)).Str("key", key).Msg("Successfully uploaded batch to S3")
		// Send the max LSN to the acknowledgment channel
		select {
		case ackCh <- batch.LastLSN():
			log.Debug().Uint64("lsn", batch.LastLSN()).Msg("Sent LSN Ack")
		case <-ctx.Done():
			log.Warn().Msg("Context cancelled before LSN could be acknowledged")
		}
		// We succeeded, no need to retry
		return
	}
}
