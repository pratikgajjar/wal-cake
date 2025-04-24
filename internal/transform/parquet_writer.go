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

	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

// EventFilter defines a function type for filtering CDC events
type EventFilter func(event *model.CDCEvent) bool

// ParquetWriter interface defines methods for writing CDC events to Parquet format
type ParquetWriter interface {
	// WriteToBuffer writes events to an in-memory buffer and returns the bytes
	WriteToBuffer(events []*model.CDCEvent) ([]byte, error)
	GetCompressionCodec() string
	// AddFilter adds an event filter to the writer
	AddFilter(filter EventFilter)
}

type parquetWriter struct {
	props   *parquet.WriterProperties
	filters []EventFilter
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

// NewParquetWriter creates a new Parquet writer with ZSTD compression
func NewParquetWriter() ParquetWriter {
	// Create writer properties with ZSTD compression
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(3),
		parquet.WithDictionaryDefault(true),
		parquet.WithStats(true),
		parquet.WithCreatedBy("wal-cake"),
	)
	return &parquetWriter{
		props:   props,
		filters: make([]EventFilter, 0),
	}
}

func (w *parquetWriter) GetCompressionCodec() string {
	return w.props.Compression().String()
}

// AddFilter adds an event filter to the writer
func (w *parquetWriter) AddFilter(filter EventFilter) {
	w.filters = append(w.filters, filter)
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

// writeEventsToParquet writes the given events to a Parquet file, applying filters
func (w *parquetWriter) writeEventsToParquet(events []*model.CDCEvent, writer io.Writer) error {
	// First count how many events will pass the filter
	validCount := 0
	for _, ev := range events {
		if w.shouldIncludeEvent(ev) {
			validCount++
		}
	}

	// Log filter statistics
	if validCount < len(events) {
		log.Info().Int("total_events", len(events)).Int("filtered_events", len(events)-validCount).Int("written_events", validCount).Msg("filtered events before parquet writing")
	}

	// If no events after filtering, return early
	if validCount == 0 {
		log.Warn().Msg("no events to write after filtering")
		return nil
	}

	// Create a writer that can tell its position
	wt := &writerTell{w: writer}

	// Create parquet writer with schema
	schema := w.createSchema()
	// Create parquet file writer
	fileWriter := file.NewParquetWriter(wt, schema.Root(), file.WithWriterProps(w.props))

	// Create row group with a reasonable size
	rg := fileWriter.AppendRowGroup()

	// Prepare column data arrays once with the exact size needed
	tableData := make([]parquet.ByteArray, validCount)
	opData := make([]parquet.ByteArray, validCount)
	tsData := make([]int64, validCount)
	lsnData := make([]int64, validCount)
	dataJsonValues := make([]parquet.ByteArray, validCount)

	// Fill column data arrays in a single pass
	index := 0
	for _, ev := range events {
		if !w.shouldIncludeEvent(ev) {
			continue
		}

		// Table column
		tableData[index] = []byte(ev.Table)

		// Operation column
		opData[index] = []byte(ev.Operation)

		// Timestamp column
		tsData[index] = ev.Timestamp.UnixNano() / int64(time.Millisecond)

		// LSN column
		lsnData[index] = int64(ev.LSN)

		// Data JSON column
		jsonData, err := json.Marshal(ev.Data)
		if err != nil {
			return fmt.Errorf("marshal data to JSON: %w", err)
		}
		dataJsonValues[index] = jsonData

		index++
	}

	// Write table column
	tableWriter, err := rg.NextColumn()
	if err != nil {
		return fmt.Errorf("next column: %w", err)
	}
	byteArrayWriter := tableWriter.(*file.ByteArrayColumnChunkWriter)
	_, err = byteArrayWriter.WriteBatch(tableData, nil, nil)
	if err != nil {
		return fmt.Errorf("write table column: %w", err)
	}

	// Write operation column
	opWriter, err := rg.NextColumn()
	if err != nil {
		return fmt.Errorf("next column: %w", err)
	}
	opByteArrayWriter := opWriter.(*file.ByteArrayColumnChunkWriter)
	_, err = opByteArrayWriter.WriteBatch(opData, nil, nil)
	if err != nil {
		return fmt.Errorf("write operation column: %w", err)
	}

	// Write timestamp column
	tsWriter, err := rg.NextColumn()
	if err != nil {
		return fmt.Errorf("next column: %w", err)
	}
	tsInt64Writer := tsWriter.(*file.Int64ColumnChunkWriter)
	_, err = tsInt64Writer.WriteBatch(tsData, nil, nil)
	if err != nil {
		return fmt.Errorf("write timestamp column: %w", err)
	}

	// Write LSN column
	lsnWriter, err := rg.NextColumn()
	if err != nil {
		return fmt.Errorf("next column: %w", err)
	}
	lsnInt64Writer := lsnWriter.(*file.Int64ColumnChunkWriter)
	_, err = lsnInt64Writer.WriteBatch(lsnData, nil, nil)
	if err != nil {
		return fmt.Errorf("write lsn column: %w", err)
	}

	// Write data JSON column
	dataWriter, err := rg.NextColumn()
	if err != nil {
		return fmt.Errorf("next column: %w", err)
	}
	dataByteArrayWriter := dataWriter.(*file.ByteArrayColumnChunkWriter)
	_, err = dataByteArrayWriter.WriteBatch(dataJsonValues, nil, nil)
	if err != nil {
		return fmt.Errorf("write data column: %w", err)
	}

	// Close the row group
	if err := rg.Close(); err != nil {
		return fmt.Errorf("close row group: %w", err)
	}

	// Close the file writer
	if err := fileWriter.Close(); err != nil {
		return fmt.Errorf("close file writer: %w", err)
	}

	return nil
}

// shouldIncludeEvent applies all filters to determine if an event should be included
func (w *parquetWriter) shouldIncludeEvent(event *model.CDCEvent) bool {
	for _, filter := range w.filters {
		if !filter(event) {
			return false
		}
	}
	return true
}

// WriteToBuffer writes the given events to an in-memory buffer and returns the bytes.
func (w *parquetWriter) WriteToBuffer(events []*model.CDCEvent) ([]byte, error) {
	log.Info().Int("events", len(events)).Msg("writing CDC events to in-memory parquet buffer")

	// Create an in-memory buffer
	buf := new(bytes.Buffer)

	// Write events to the buffer (filtering happens during writing)
	if err := w.writeEventsToParquet(events, buf); err != nil {
		return nil, err
	}

	log.Info().Int("size", buf.Len()).Msg("successfully wrote CDC events to in-memory parquet buffer")
	return buf.Bytes(), nil
}
