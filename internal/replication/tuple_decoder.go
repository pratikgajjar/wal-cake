package replication

import (
	"encoding/json"
	"fmt"

	"github.com/jackc/pglogrepl"
)

// TypeDecoder defines an interface for decoding column values based on PostgreSQL types
type TypeDecoder interface {
	// Decode takes raw column data and returns the decoded value
	Decode(data []byte) any
}

// BasicDecoder handles simple data types
type BasicDecoder struct {
	decoder func([]byte) any
}

func (d *BasicDecoder) Decode(data []byte) any {
	return d.decoder(data)
}

// JSONDecoder handles JSON and JSONB types
type JSONDecoder struct{}

func (d *JSONDecoder) Decode(data []byte) any {
	var jsonData any
	if err := json.Unmarshal(data, &jsonData); err == nil {
		return jsonData
	}
	// Fall back to string if not valid JSON
	return string(data)
}

// TextDecoder handles text data types
type TextDecoder struct{}

func (d *TextDecoder) Decode(data []byte) any {
	return string(data)
}

// BinaryDecoder is a fallback for binary data without specific decoders
type BinaryDecoder struct{}

func (d *BinaryDecoder) Decode(data []byte) any {
	return fmt.Sprintf("binary(%d bytes): %x", len(data), data)
}

// TypeDecoderRegistry maintains mapping of PostgreSQL type OIDs to decoders
type TypeDecoderRegistry struct {
	decoders map[uint32]TypeDecoder
	fallback TypeDecoder
}

// NewTypeDecoderRegistry creates a registry with default decoders
func NewTypeDecoderRegistry() *TypeDecoderRegistry {
	registry := &TypeDecoderRegistry{
		decoders: make(map[uint32]TypeDecoder),
		fallback: &TextDecoder{},
	}

	// Register default decoders
	registry.RegisterDecoder(114, &JSONDecoder{})  // json
	registry.RegisterDecoder(3802, &JSONDecoder{}) // jsonb

	// Add more default decoders as needed

	return registry
}

// RegisterDecoder adds a decoder for a specific PostgreSQL type
func (r *TypeDecoderRegistry) RegisterDecoder(typeOID uint32, decoder TypeDecoder) {
	r.decoders[typeOID] = decoder
}

// GetDecoder returns the appropriate decoder for a given type
func (r *TypeDecoderRegistry) GetDecoder(typeOID uint32) TypeDecoder {
	if decoder, exists := r.decoders[typeOID]; exists {
		return decoder
	}
	return r.fallback
}

// TupleDecoder handles tuple data extraction
type TupleDecoder struct {
	registry *TypeDecoderRegistry
}

// NewTupleDecoder creates a new tuple decoder with default type decoders
func NewTupleDecoder() *TupleDecoder {
	return &TupleDecoder{
		registry: NewTypeDecoderRegistry(),
	}
}

// RegisterTypeDecoder adds a custom type decoder to the registry
func (d *TupleDecoder) RegisterTypeDecoder(typeOID uint32, decoder TypeDecoder) {
	d.registry.RegisterDecoder(typeOID, decoder)
}

// ExtractTupleData converts a tuple's data into a map of column name -> value
func (d *TupleDecoder) ExtractTupleData(tuple *pglogrepl.TupleData, columns []*pglogrepl.RelationMessageColumn) map[string]any {
	return d.extractTuple(tuple, columns, false)
}

// ExtractKeyOnlyTupleData extracts only key columns from a tuple
func (d *TupleDecoder) ExtractKeyOnlyTupleData(tuple *pglogrepl.TupleData, columns []*pglogrepl.RelationMessageColumn) map[string]any {
	return d.extractTuple(tuple, columns, true)
}

// extractTuple handles extracting data from tuples with options for key-only
func (d *TupleDecoder) extractTuple(tuple *pglogrepl.TupleData, columns []*pglogrepl.RelationMessageColumn, keyOnly bool) map[string]any {
	if tuple == nil || len(tuple.Columns) == 0 || len(columns) == 0 {
		return map[string]any{}
	}

	data := make(map[string]any)

	// Process each column in the tuple
	for i, col := range tuple.Columns {
		// Skip if we don't have column info or if the column index is out of range
		if i >= len(columns) {
			continue
		}

		// For key-only tuples, skip NULL columns as they're not part of the key
		if keyOnly && col.DataType == pglogrepl.TupleDataTypeNull {
			continue
		}

		colName := columns[i].Name
		pgType := columns[i].DataType

		// Handle different column types
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			data[colName] = nil

		case pglogrepl.TupleDataTypeToast:
			// TOAST data (Too Large Object Access Storage Technique)
			data[colName] = "<TOAST>"

		case pglogrepl.TupleDataTypeText:
			decoder := d.registry.GetDecoder(pgType)
			data[colName] = decoder.Decode(col.Data)

		case pglogrepl.TupleDataTypeBinary:
			// For binary, we might want specialized decoders based on the type
			data[colName] = d.decodeBinaryValue(col.Data, pgType)
		}
	}

	return data
}

// decodeBinaryValue handles decoding of binary data
func (d *TupleDecoder) decodeBinaryValue(data []byte, typeOID uint32) any {
	if len(data) == 0 {
		return nil
	}

	// In a real implementation, you would have specific decoders for binary data
	// This is a placeholder that should be expanded with proper binary decoders
	switch typeOID {
	// Add cases for specific binary types here
	default:
		return (&BinaryDecoder{}).Decode(data)
	}
}

// For backwards compatibility with existing code
var defaultDecoder = NewTupleDecoder()

// extractTupleData is kept for backwards compatibility
func extractTupleData(tuple *pglogrepl.TupleData, columns []*pglogrepl.RelationMessageColumn) map[string]any {
	return defaultDecoder.ExtractTupleData(tuple, columns)
}

// extractKeyOnlyTupleData is kept for backwards compatibility
func extractKeyOnlyTupleData(tuple *pglogrepl.TupleData, columns []*pglogrepl.RelationMessageColumn) map[string]any {
	return defaultDecoder.ExtractKeyOnlyTupleData(tuple, columns)
}
