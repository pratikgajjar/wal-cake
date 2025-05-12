package buffer

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"git.famapp.in/fampay-inc/wal-cake/internal/model"
	"git.famapp.in/fampay-inc/wal-cake/internal/storage"
	"git.famapp.in/fampay-inc/wal-cake/internal/transform"
)

// BatchProcessorConfig holds configuration for the batch processor
type BatchProcessorConfig struct {
	Namespace string
}

// ParquetBatchProcessor implements BatchProcessor for Parquet file generation
type ParquetBatchProcessor struct {
	transformer transform.ParquetWriter
	uploader    storage.S3Uploader
	config      *BatchProcessorConfig
}

// NewParquetBatchProcessor creates a new batch processor for Parquet files
func NewParquetBatchProcessor(
	transformer transform.ParquetWriter,
	uploader storage.S3Uploader,
	config *BatchProcessorConfig,
) *ParquetBatchProcessor {
	return &ParquetBatchProcessor{
		transformer: transformer,
		uploader:    uploader,
		config:      config,
	}
}

// Process processes a batch of CDC events, writing them to Parquet and uploading to S3
func (p *ParquetBatchProcessor) Process(ctx context.Context, events []*model.CDCEvent) error {
	if len(events) == 0 {
		return nil
	}

	log.Debug().
		Int("eventCount", len(events)).
		Str("firstTable", events[0].Table).
		Str("firstOp", string(events[0].Operation)).
		Str("start", model.LSNStr(events[0].LSN)).
		Str("end", model.LSNStr(events[len(events)-1].LSN)).
		Msg("Processing batch")

	parquetBytes, err := p.transformer.WriteToBuffer(events)
	if err != nil {
		log.Error().
			Err(err).
			Int("eventCount", len(events)).
			Msg("Failed to write events to Parquet")
		return err
	}
	if len(parquetBytes) == 0 {
		log.Info().
			Int("eventCount", len(events)).
			Msg("No data to upload")
		return nil
	}
	s3Key := p.generateS3Key(events[len(events)-1].Timestamp)
	time.Sleep(time.Second * 2)
	if err := p.uploader.UploadBytes(ctx, s3Key, parquetBytes); err != nil {
		log.Error().
			Err(err).
			Str("s3Key", s3Key).
			Int("size", len(parquetBytes)).
			Msg("Failed to upload Parquet file to S3")
		return err
	}

	return nil
}

// generateS3Key generates an S3 key for the Parquet file
func (p *ParquetBatchProcessor) generateS3Key(timestamp time.Time) string {
	key := fmt.Sprintf("%s/%s/%d.%s.parquet",
		p.config.Namespace,
		timestamp.Format("20060102"),
		timestamp.UnixMicro(),
		p.transformer.GetCompressionCodec(),
	)
	return key
}
