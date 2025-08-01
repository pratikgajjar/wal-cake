package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"git.famapp.in/fampay-inc/wal-cake/internal/buffer"
	"git.famapp.in/fampay-inc/wal-cake/internal/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
	"git.famapp.in/fampay-inc/wal-cake/internal/replication"
	"git.famapp.in/fampay-inc/wal-cake/internal/server"
	"git.famapp.in/fampay-inc/wal-cake/internal/storage"
	"git.famapp.in/fampay-inc/wal-cake/internal/transform"
)

func main() {
	cfg := config.LoadConfig()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	logLevel := os.Getenv("LOG_LEVEL")
	level, err := zerolog.ParseLevel(logLevel)
	if err != nil || level == zerolog.NoLevel {
		level = zerolog.InfoLevel
	}
	log.Info().Str("logLevel", level.String()).Msg("Setting log level")
	log.Logger = log.Level(level)

	// Create context with cancellation on signals
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Channel for CDC events from the replicator
	eventsCh := make(chan *model.CDCEvent, cfg.BatchSize*cfg.Concurrency)

	// Channel for acknowledging LSNs after successful processing
	ackCh := make(chan uint64, cfg.Concurrency*2)
	defer close(ackCh)

	// Initialize components
	repl := replication.NewPGReplicator(cfg)
	transformer := transform.NewParquetWriter()
	uploader := storage.NewS3Uploader(cfg)
	httpServer := server.New(8080, repl)

	// Add filter to exclude commit events from Parquet files
	transformer.AddFilter(func(event *model.CDCEvent) bool {
		return event.Operation != model.CommitOp
	})

	// Create batch processor
	processor := buffer.NewParquetBatchProcessor(
		transformer,
		uploader,
		&buffer.BatchProcessorConfig{
			Namespace: cfg.Namespace,
		},
	)

	// Create ring buffer with the specified configuration
	rb := buffer.NewRingBuffer(
		cfg.BatchSize,
		cfg.Concurrency,
		cfg.FlushInterval,
		processor,
		ackCh,
	)

	// Start the replicator in a separate goroutine
	go func() {
		if err := repl.Start(ctx, eventsCh, ackCh); err != nil {
			log.Fatal().Err(err).Msg("Replication error")
		}
	}()

	// Start the ring buffer
	log.Info().
		Int("batchSize", cfg.BatchSize).
		Int("concurrency", cfg.Concurrency).
		Dur("flushInterval", cfg.FlushInterval).
		Msg("Starting ring buffer")

	// Start the HTTP server for health checks
	httpServer.Start()

	if err := rb.Start(ctx, eventsCh); err != nil {
		log.Fatal().Err(err).Msg("Ring buffer error")
	}

	// Wait for context cancellation
	<-ctx.Done()
	log.Info().Msg("Shutting down")

	// Shutdown the HTTP server gracefully
	if err := httpServer.Shutdown(context.Background()); err != nil {
		log.Error().Err(err).Msg("HTTP server shutdown error")
	}
}
