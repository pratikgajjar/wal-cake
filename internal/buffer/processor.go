package buffer

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	"git.famapp.in/fampay-inc/wal-cake/internal/model"
	"git.famapp.in/fampay-inc/wal-cake/internal/storage"
	"git.famapp.in/fampay-inc/wal-cake/internal/transform"
)

// BatchProcessorConfig holds configuration for the batch processor
type BatchProcessorConfig struct {
	// Any processor-specific configuration
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

	log.Info().
		Int("eventCount", len(events)).
		Str("firstTable", events[0].Table).
		Str("firstOp", string(events[0].Operation)).
		Msg("Processing batch")

	// Group events by date and table
	eventsByDateAndTable := make(map[string]map[string][]*model.CDCEvent)
	
	for _, event := range events {
		// Skip commit events if needed
		if event.Operation == model.CommitOp {
			continue
		}
		
		date := event.Timestamp.Format("2006/01/02")
		if _, ok := eventsByDateAndTable[date]; !ok {
			eventsByDateAndTable[date] = make(map[string][]*model.CDCEvent)
		}
		
		eventsByDateAndTable[date][event.Table] = append(eventsByDateAndTable[date][event.Table], event)
	}
	
	// Process each group
	for date, tableEvents := range eventsByDateAndTable {
		for table, events := range tableEvents {
			// Generate Parquet file
			parquetBytes, err := p.transformer.WriteToBuffer(events)
			if err != nil {
				log.Error().
					Err(err).
					Str("date", date).
					Str("table", table).
					Int("eventCount", len(events)).
					Msg("Failed to write events to Parquet")
				return err
			}
			
			// Upload to S3
			s3Key := p.generateS3Key(date, table, events[0].LSN, events[len(events)-1].LSN)
			if err := p.uploader.UploadBytes(ctx, s3Key, parquetBytes); err != nil {
				log.Error().
					Err(err).
					Str("s3Key", s3Key).
					Int("size", len(parquetBytes)).
					Msg("Failed to upload Parquet file to S3")
				return err
			}
			
			log.Info().
				Str("date", date).
				Str("table", table).
				Str("s3Key", s3Key).
				Int("eventCount", len(events)).
				Int("size", len(parquetBytes)).
				Msg("Successfully processed and uploaded batch")
		}
	}
	
	return nil
}

// generateS3Key generates an S3 key for the Parquet file
func (p *ParquetBatchProcessor) generateS3Key(date, table string, startLSN, endLSN uint64) string {
	return date + "/" + table + "/" + table + "_" + 
		formatLSN(startLSN) + "_" + formatLSN(endLSN) + ".parquet"
}

// formatLSN formats an LSN value as a string
func formatLSN(lsn uint64) string {
	return fmt.Sprintf("%016x", lsn)
}
