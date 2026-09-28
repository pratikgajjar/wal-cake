package buffer

import (
	"context"
	"fmt"
	"sync/atomic"
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
	seq         atomic.Uint64 // makes every key from this process unique
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

	// Upload one file per commit date. Commit timestamps are almost, but not
	// strictly, in commit order, so check every event rather than only the
	// first and last.
	curDate := events[0].Date()
	left := 0
	for i, e := range events {
		nextDate := e.Date()
		if !nextDate.Equal(curDate) {
			if err := p.Upload(ctx, curDate, events[left:i]); err != nil {
				return err
			}
			curDate = nextDate
			left = i
		}
	}

	return p.Upload(ctx, curDate, events[left:])
}

// Upload writes events, which all belong to date, to one Parquet file.
func (p *ParquetBatchProcessor) Upload(ctx context.Context, date time.Time, events []*model.CDCEvent) error {
	if len(events) == 0 {
		return nil
	}

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
	s3Key := p.generateS3Key(date, events[len(events)-1].Timestamp)
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

// generateS3Key returns namespace/YYYY/MM/DD/<decode-time-µs>-<seq>.<codec>.parquet.
// The folder is the UTC commit date. The decode time alone is not unique:
// events decode in well under a microsecond, so two files in one folder can
// end on the same microsecond, and the second PUT would overwrite the first.
// seq is unique within the process; a restarted process decodes later.
func (p *ParquetBatchProcessor) generateS3Key(date, lastDecoded time.Time) string {
	key := fmt.Sprintf("%s/%s/%d-%d.%s.parquet",
		p.config.Namespace,
		date.UTC().Format("2006/01/02"),
		lastDecoded.UnixMicro(),
		p.seq.Add(1),
		p.transformer.GetCompressionCodec(),
	)
	return key
}
