package transform

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"

	"git.famapp.in/fampay-inc/wal-cake/internal/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"

	"github.com/rs/zerolog/log"
)

// ParquetWriter writes CDC events to Parquet files
type ParquetWriter interface {
	Write(events []*model.CDCEvent, fileName string) error
}

type parquetWriter struct {
	compression compress.Compression
}

// writerTell is a wrapper that implements io.Writer and has a Tell method
type writerTell struct {
	w io.Writer
	pos int64
}

func (w *writerTell) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.pos += int64(n)
	return n, err
}

func (w *writerTell) Tell() int64 {
	return w.pos
}

// wrapWriter wraps an io.Writer to provide Tell functionality needed by parquet
func wrapWriter(w io.Writer) *writerTell {
	return &writerTell{w: w}
}

// NewParquetWriter creates a ParquetWriter with ZSTD compression.
func NewParquetWriter(cfg *config.Config) ParquetWriter {
	_ = cfg // not used currently
	return &parquetWriter{
		compression: compress.Codecs.Zstd,
	}
}

// Write writes the given events to a Parquet file using Apache Arrow's Parquet API.
func (w *parquetWriter) Write(events []*model.CDCEvent, fileName string) error {
	log.Info().Str("file", fileName).Int("events", len(events)).Msg("writing CDC events to parquet file")
	
	// Create output file
	f, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	// Create schema nodes for each column
	tableNode, err := schema.NewPrimitiveNode("table", parquet.Repetitions.Required, parquet.Types.ByteArray, -1, -1)
	if err != nil {
		return fmt.Errorf("create table schema node: %w", err)
	}

	opNode, err := schema.NewPrimitiveNode("operation", parquet.Repetitions.Required, parquet.Types.ByteArray, -1, -1)
	if err != nil {
		return fmt.Errorf("create operation schema node: %w", err)
	}

	tsNode, err := schema.NewPrimitiveNode("timestamp", parquet.Repetitions.Required, parquet.Types.Int64, -1, -1)
	if err != nil {
		return fmt.Errorf("create timestamp schema node: %w", err)
	}

	lsnNode, err := schema.NewPrimitiveNode("lsn", parquet.Repetitions.Required, parquet.Types.Int64, -1, -1)
	if err != nil {
		return fmt.Errorf("create lsn schema node: %w", err)
	}

	dataNode, err := schema.NewPrimitiveNode("data_json", parquet.Repetitions.Required, parquet.Types.ByteArray, -1, -1)
	if err != nil {
		return fmt.Errorf("create data_json schema node: %w", err)
	}

	// Create schema
	fields := []schema.Node{tableNode, opNode, tsNode, lsnNode, dataNode}
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// Create writer properties with ZSTD compression
	props := parquet.NewWriterProperties(
		parquet.WithCompression(w.compression),
		parquet.WithDictionaryDefault(true),
		parquet.WithStats(true),
		parquet.WithMaxRowGroupLength(parquet.DefaultMaxRowGroupLen),
		parquet.WithCreatedBy("wal-cake CDC to S3"),
	)

	// Create writer
	fileWriter := file.NewParquetWriter(wrapWriter(f), root, file.WithWriterProps(props))
	defer fileWriter.Close()

	// Process events in batches to keep memory usage reasonable
	batchSize := 1000
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}
		batch := events[i:end]

		// Create row group for this batch
		rgWriter := fileWriter.AppendRowGroup()

		// Write table column
		tableWriter, err := rgWriter.NextColumn()
		if err != nil {
			return fmt.Errorf("create table column writer: %w", err)
		}
		byteArrayWriter := tableWriter.(*file.ByteArrayColumnChunkWriter)

		for _, ev := range batch {
			// WriteBatch returns num values written and error
			_, err := byteArrayWriter.WriteBatch([]parquet.ByteArray{[]byte(ev.Table)}, nil, nil)
			if err != nil {
				return fmt.Errorf("write table column: %w", err)
			}
		}

		// Write operation column
		opWriter, err := rgWriter.NextColumn()
		if err != nil {
			return fmt.Errorf("create operation column writer: %w", err)
		}
		opByteArrayWriter := opWriter.(*file.ByteArrayColumnChunkWriter)

		for _, ev := range batch {
			_, err := opByteArrayWriter.WriteBatch([]parquet.ByteArray{[]byte(ev.Operation)}, nil, nil)
			if err != nil {
				return fmt.Errorf("write operation column: %w", err)
			}
		}

		// Write timestamp column
		tsWriter, err := rgWriter.NextColumn()
		if err != nil {
			return fmt.Errorf("create timestamp column writer: %w", err)
		}
		tsInt64Writer := tsWriter.(*file.Int64ColumnChunkWriter)

		for _, ev := range batch {
			ts := ev.Timestamp.UnixNano() / int64(time.Millisecond)
			_, err := tsInt64Writer.WriteBatch([]int64{ts}, nil, nil)
			if err != nil {
				return fmt.Errorf("write timestamp column: %w", err)
			}
		}

		// Write LSN column
		lsnWriter, err := rgWriter.NextColumn()
		if err != nil {
			return fmt.Errorf("create lsn column writer: %w", err)
		}
		lsnInt64Writer := lsnWriter.(*file.Int64ColumnChunkWriter)

		for _, ev := range batch {
			_, err := lsnInt64Writer.WriteBatch([]int64{int64(ev.LSN)}, nil, nil)
			if err != nil {
				return fmt.Errorf("write lsn column: %w", err)
			}
		}

		// Write data JSON column
		dataWriter, err := rgWriter.NextColumn()
		if err != nil {
			return fmt.Errorf("create data_json column writer: %w", err)
		}
		dataByteArrayWriter := dataWriter.(*file.ByteArrayColumnChunkWriter)

		for _, ev := range batch {
			jsonData, err := json.Marshal(ev.Data)
			if err != nil {
				return fmt.Errorf("marshal data to JSON: %w", err)
			}
			_, err = dataByteArrayWriter.WriteBatch([]parquet.ByteArray{jsonData}, nil, nil)
			if err != nil {
				return fmt.Errorf("write data_json column: %w", err)
			}
		}

		// Close row group
		if err := rgWriter.Close(); err != nil {
			return fmt.Errorf("close row group: %w", err)
		}
	}

	log.Info().Str("file", fileName).Msg("successfully wrote CDC events to parquet file")
	return nil
}
