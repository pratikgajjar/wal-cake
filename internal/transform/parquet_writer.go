package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/rs/zerolog/log"

	"git.famapp.in/fampay-inc/wal-cake/internal/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

// ParquetWriter writes CDC events to Parquet format
type ParquetWriter interface {
	// WriteToBuffer writes events to an in-memory buffer and returns the bytes
	WriteToBuffer(events []*model.CDCEvent) ([]byte, error)
}

type parquetWriter struct {
	compression compress.Compression
}

// writerTell is a wrapper that implements io.Writer and has a Tell method
type writerTell struct {
	w   io.Writer
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

// NewParquetWriter creates a ParquetWriter with ZSTD compression.
func NewParquetWriter(cfg *config.Config) ParquetWriter {
	_ = cfg // not used currently
	return &parquetWriter{
		compression: compress.Codecs.Zstd,
	}
}

// createSchema creates the Parquet schema for CDC events
func (w *parquetWriter) createSchema() *schema.Schema {
	// Create schema nodes for each column
	tableNode, err := schema.NewPrimitiveNode("table", parquet.Repetitions.Required, parquet.Types.ByteArray, -1, -1)
	if err != nil {
		log.Fatal().Err(err).Msg("create table schema node")
	}

	opNode, err := schema.NewPrimitiveNode("operation", parquet.Repetitions.Required, parquet.Types.ByteArray, -1, -1)
	if err != nil {
		log.Fatal().Err(err).Msg("create operation schema node")
	}

	tsNode, err := schema.NewPrimitiveNode("timestamp", parquet.Repetitions.Required, parquet.Types.Int64, -1, -1)
	if err != nil {
		log.Fatal().Err(err).Msg("create timestamp schema node")
	}

	lsnNode, err := schema.NewPrimitiveNode("lsn", parquet.Repetitions.Required, parquet.Types.Int64, -1, -1)
	if err != nil {
		log.Fatal().Err(err).Msg("create lsn schema node")
	}

	dataNode, err := schema.NewPrimitiveNode("data_json", parquet.Repetitions.Required, parquet.Types.ByteArray, -1, -1)
	if err != nil {
		log.Fatal().Err(err).Msg("create data_json schema node")
	}

	// Create schema
	fields := []schema.Node{tableNode, opNode, tsNode, lsnNode, dataNode}
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		log.Fatal().Err(err).Msg("create schema")
	}
	
	return schema.NewSchema(root)
}

// writeEventsToParquet writes events to a parquet file or buffer
func (w *parquetWriter) writeEventsToParquet(events []*model.CDCEvent, writer io.Writer) error {
	// Create a writer that can tell its position
	wt := &writerTell{w: writer}

	// Create parquet writer with schema
	schema := w.createSchema()
	
	// Create writer properties with ZSTD compression
	props := parquet.NewWriterProperties(
		parquet.WithCompression(w.compression),
		parquet.WithDictionaryDefault(true),
		parquet.WithStats(true),
		parquet.WithMaxRowGroupLength(parquet.DefaultMaxRowGroupLen),
		parquet.WithCreatedBy("wal-cake CDC to S3"),
	)

	// Create writer
	pw := file.NewParquetWriter(wt, schema.Root(), file.WithWriterProps(props))
	defer pw.Close()

	// Create row group
	rg := pw.AppendRowGroup()

	// Write table column
	tableWriter, err := rg.NextColumn()
	if err != nil {
		return fmt.Errorf("create table column writer: %w", err)
	}
	byteArrayWriter := tableWriter.(*file.ByteArrayColumnChunkWriter)

	for _, ev := range events {
		_, err := byteArrayWriter.WriteBatch([]parquet.ByteArray{[]byte(ev.Table)}, nil, nil)
		if err != nil {
			return fmt.Errorf("write table column: %w", err)
		}
	}

	// Write operation column
	opWriter, err := rg.NextColumn()
	if err != nil {
		return fmt.Errorf("create operation column writer: %w", err)
	}
	opByteArrayWriter := opWriter.(*file.ByteArrayColumnChunkWriter)

	for _, ev := range events {
		_, err := opByteArrayWriter.WriteBatch([]parquet.ByteArray{[]byte(ev.Operation)}, nil, nil)
		if err != nil {
			return fmt.Errorf("write operation column: %w", err)
		}
	}

	// Write timestamp column
	tsWriter, err := rg.NextColumn()
	if err != nil {
		return fmt.Errorf("create timestamp column writer: %w", err)
	}
	tsInt64Writer := tsWriter.(*file.Int64ColumnChunkWriter)

	for _, ev := range events {
		ts := ev.Timestamp.UnixNano() / int64(time.Millisecond)
		_, err := tsInt64Writer.WriteBatch([]int64{ts}, nil, nil)
		if err != nil {
			return fmt.Errorf("write timestamp column: %w", err)
		}
	}

	// Write LSN column
	lsnWriter, err := rg.NextColumn()
	if err != nil {
		return fmt.Errorf("create lsn column writer: %w", err)
	}
	lsnInt64Writer := lsnWriter.(*file.Int64ColumnChunkWriter)

	for _, ev := range events {
		_, err := lsnInt64Writer.WriteBatch([]int64{int64(ev.LSN)}, nil, nil)
		if err != nil {
			return fmt.Errorf("write lsn column: %w", err)
		}
	}

	// Write data JSON column
	dataWriter, err := rg.NextColumn()
	if err != nil {
		return fmt.Errorf("create data_json column writer: %w", err)
	}
	dataByteArrayWriter := dataWriter.(*file.ByteArrayColumnChunkWriter)

	for _, ev := range events {
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
	if err := rg.Close(); err != nil {
		return fmt.Errorf("close row group: %w", err)
	}

	return nil
}



// WriteToBuffer writes the given events to an in-memory buffer and returns the bytes.
func (w *parquetWriter) WriteToBuffer(events []*model.CDCEvent) ([]byte, error) {
	log.Info().Int("events", len(events)).Msg("writing CDC events to in-memory parquet buffer")
	
	// Create an in-memory buffer
	buf := new(bytes.Buffer)
	
	// Write events to the buffer
	if err := w.writeEventsToParquet(events, buf); err != nil {
		return nil, err
	}

	log.Info().Int("size", buf.Len()).Msg("successfully wrote CDC events to in-memory parquet buffer")
	return buf.Bytes(), nil
}
