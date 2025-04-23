package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"

    "git.famapp.in/fampay-inc/wal-cake/internal/config"
    "git.famapp.in/fampay-inc/wal-cake/internal/model"
    "git.famapp.in/fampay-inc/wal-cake/internal/replication"
    "git.famapp.in/fampay-inc/wal-cake/internal/transform"
    "git.famapp.in/fampay-inc/wal-cake/internal/storage"
)

func main() {
    cfg := config.LoadConfig()
    log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    eventsCh := make(chan *model.CDCEvent, cfg.BatchSize*2)
    defer close(eventsCh)

    repl := replication.NewPGReplicator(cfg)
    transformer := transform.NewParquetWriter(cfg)
    uploader := storage.NewS3Uploader(cfg)

    go func() {
        if err := repl.Start(ctx, eventsCh); err != nil {
            log.Fatal().Err(err).Msg("Replication error")
        }
    }()

    ticker := time.NewTicker(cfg.FlushInterval)
    defer ticker.Stop()

    var buffer []*model.CDCEvent
    for {
        select {
        case <-ctx.Done():
            log.Info().Msg("Shutting down")
            return
        case ev := <-eventsCh:
            buffer = append(buffer, ev)
            if len(buffer) >= cfg.BatchSize {
                processBatch(ctx, cfg, buffer, transformer, uploader)
                buffer = buffer[:0]
            }
        case <-ticker.C:
            if len(buffer) > 0 {
                processBatch(ctx, cfg, buffer, transformer, uploader)
                buffer = buffer[:0]
            }
        }
    }
}

func processBatch(ctx context.Context, cfg *config.Config, events []*model.CDCEvent, pw transform.ParquetWriter, up storage.S3Uploader) {
    fileName := fmt.Sprintf("%s_%d.parquet", cfg.Slot, time.Now().UnixNano())
    if err := pw.Write(events, fileName); err != nil {
        log.Error().Err(err).Msg("Failed to write Parquet")
        return
    }
    key := fmt.Sprintf("%s/%s", cfg.Slot, fileName)
    if err := up.UploadFile(ctx, key, fileName); err != nil {
        log.Error().Err(err).Msg("Failed to upload Parquet to S3")
    }
    os.Remove(fileName)
}
