// Package main provides the entry point for the WAL-Cake application
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"git.famapp.in/fampay-inc/wal-cake/internal/adapter/primary/worker"
	"git.famapp.in/fampay-inc/wal-cake/internal/adapter/secondary/postgres"
	"git.famapp.in/fampay-inc/wal-cake/internal/adapter/secondary/s3"
	"git.famapp.in/fampay-inc/wal-cake/internal/app/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/infrastructure/logger"
)

// Application version
const version = "0.1.0"

func main() {
	// Initialize logger with logfmt format
	log := logger.New("wal-cake")
	log.Info("Starting WAL-Cake", map[string]interface{}{
		"version": version,
	})

	// Load configuration from environment variables
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Failed to load configuration", err)
	}

	// Initialize repositories
	walRepo := postgres.NewCDCRepository(&cfg.Postgres, log)
	s3TablesRepo, err := s3.NewS3TablesRepository(&cfg.S3Tables, &cfg.AWS, log, walRepo)
	if err != nil {
		log.Fatal("Failed to create S3 Tables repository", err)
	}

	// Create worker to process WAL changes and write to S3 Tables
	walToS3Worker := worker.NewWALToS3Worker(cfg, walRepo, s3TablesRepo, log)

	// Start the worker
	if err := walToS3Worker.Start(); err != nil {
		log.Fatal("Failed to start worker", err)
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	sig := <-sigChan
	log.Info("Received signal, shutting down", map[string]interface{}{
		"signal": sig.String(),
	})

	// Create a context with timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Worker.ShutdownTimeout)
	defer cancel()

	// Stop the worker
	if err := walToS3Worker.Stop(); err != nil {
		log.Error("Error stopping worker", err)
		os.Exit(1)
	}

	// Wait for context to be done (either timeout or clean shutdown)
	<-ctx.Done()
	if ctx.Err() == context.DeadlineExceeded {
		log.Error("Shutdown timed out", nil)
		os.Exit(1)
	}

	log.Info("Shutdown complete")
}
