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

type Empty struct{}

// LastLSN returns the highest LSN in the batch
func (b *CDCBatch) LastLSN() uint64 {
	if len(b.Events) == 0 {
		return 0
	}
	return b.Events[len(b.Events)-1].LSN
}

type PartPos struct {
	StartIdx int
	EndIdx   int
	Date     string
}

type PartChan <-chan PartPos

func (b *CDCBatch) Iter(ctx context.Context) PartChan {
	posCh := make(chan PartPos)

	go func() {
		defer close(posCh)

		if len(b.Events) == 0 {
			return
		}

		cursorPos := 0
		for cursorPos < len(b.Events) {
			select {
			case <-ctx.Done():
				return
			default:
			}

			currentDate := b.Events[cursorPos].Timestamp.Format("2006/01/02")

			startIdx := cursorPos
			for cursorPos < len(b.Events) &&
				b.Events[cursorPos].Timestamp.Format("2006/01/02") == currentDate {
				cursorPos++
			}
			endIdx := cursorPos

			select {
			case posCh <- PartPos{StartIdx: startIdx, EndIdx: endIdx, Date: currentDate}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return posCh
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
	if len(batch.Events) == 0 {
		return
	}

	maxRetries := 3
	initialDelay := 1 * time.Second

	log.Info().Int("events", len(batch.Events)).Uint64("maxLSN", batch.LastLSN()).Msg("Processing batch")

	for pos := range batch.Iter(ctx) {
		eventsSlice := batch.Events[pos.StartIdx:pos.EndIdx]
		timestamp := eventsSlice[pos.StartIdx].Timestamp.UnixMicro()
		key := fmt.Sprintf("%s/%s/%d.%s.parquet", cfg.Namespace, pos.Date, timestamp, pw.GetCompressionCodec())

		parquetData, err := pw.WriteToBuffer(eventsSlice)
		if err != nil {
			log.Fatal().Err(err).Str("date", pos.Date).Msg("Failed to write Parquet to memory buffer")
		}
		_, uploadSuccess := withRetry(ctx, maxRetries, initialDelay,
			func() (Empty, error) {
				err := up.UploadBytes(ctx, key, parquetData)
				return Empty{}, err
			},
			func(err error, attempt int) bool {
				log.Error().Err(err).Int("attempt", attempt).Str("key", key).Msg("Failed to upload Parquet to S3")
				if attempt == maxRetries {
					log.Error().Err(err).Str("key", key).Msg("All attempts to upload to S3 failed")
				}
				return true
			},
		)

		if uploadSuccess {
			log.Info().Int("events", len(eventsSlice)).Str("key", key).Str("date", pos.Date).Msg("Successfully uploaded batch to S3 for date")
		} else {
			log.Fatal().Err(err).Str("key", key).Msg("Failed to upload Parquet to S3")
		}
	}

	select {
	case ackCh <- batch.LastLSN():
		log.Debug().Uint64("lsn", batch.LastLSN()).Msg("Sent LSN Ack")
	case <-ctx.Done():
		log.Warn().Msg("Context cancelled before LSN could be acknowledged")
	}
}
