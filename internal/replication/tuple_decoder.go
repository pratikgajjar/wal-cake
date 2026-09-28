package replication

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// TypeHandler defines an interface for handling column values based on PostgreSQL types
type TypeHandler interface {
	// Handle processes raw column data and returns the appropriate value
	Handle(data []byte) any
}

// DataTypeRegistry maintains mapping of PostgreSQL type OIDs to handlers
type DataTypeRegistry struct {
	handlers map[uint32]TypeHandler
}

// NewDataTypeRegistry creates a registry with default handlers
func NewDataTypeRegistry() *DataTypeRegistry {
	registry := &DataTypeRegistry{
		handlers: make(map[uint32]TypeHandler),
	}

	// Register numeric type handlers
	registry.RegisterHandler(pgtype.Int2OID, &IntegerHandler{})
	registry.RegisterHandler(pgtype.Int4OID, &IntegerHandler{})
	registry.RegisterHandler(pgtype.Int8OID, &IntegerHandler{})
	registry.RegisterHandler(pgtype.Float4OID, &FloatHandler{})
	registry.RegisterHandler(pgtype.Float8OID, &FloatHandler{})
	registry.RegisterHandler(pgtype.NumericOID, &NumericHandler{})
	registry.RegisterHandler(pgtype.BoolOID, &BooleanHandler{})

	return registry
}

// RegisterHandler adds a handler for a specific PostgreSQL type
func (r *DataTypeRegistry) RegisterHandler(typeOID uint32, handler TypeHandler) {
	r.handlers[typeOID] = handler
}

// GetHandler returns the appropriate handler for a given type
func (r *DataTypeRegistry) GetHandler(typeOID uint32) (TypeHandler, bool) {
	handler, exists := r.handlers[typeOID]
	return handler, exists
}

// IntegerHandler handles integer type conversions
type IntegerHandler struct{}

func (h *IntegerHandler) Handle(data []byte) any {
	if n, err := strconv.ParseInt(string(data), 10, 64); err == nil {
		return n
	}
	// Fall back to string if parsing fails
	return string(data)
}

// FloatHandler handles float type conversions
type FloatHandler struct{}

func (h *FloatHandler) Handle(data []byte) any {
	if n, err := strconv.ParseFloat(string(data), 64); err == nil {
		return n
	}
	// Fall back to string if parsing fails
	return string(data)
}

// NumericHandler keeps NUMERIC values exact. Postgres sends them as decimal
// text; converting to float64 rounded anything past about 15 significant
// digits (9999999999999999.99 became 10000000000000000). json.Number is
// written as a JSON number with the original digits. NaN and the infinities
// are not valid JSON numbers, so they stay strings.
type NumericHandler struct{}

func (h *NumericHandler) Handle(data []byte) any {
	s := string(data)
	switch s {
	case "NaN", "Infinity", "-Infinity":
		return s
	}
	return json.Number(s)
}

// BooleanHandler handles boolean type conversions
type BooleanHandler struct{}

func (h *BooleanHandler) Handle(data []byte) any {
	s := string(data)
	if s == "t" || s == "true" || s == "1" {
		return true
	}
	if s == "f" || s == "false" || s == "0" {
		return false
	}
	// Fall back to string if parsing fails
	return s
}

// TupleDecoder handles tuple data extraction
type TupleDecoder struct {
	registry *DataTypeRegistry
}

// NewTupleDecoder creates a new tuple decoder with default type handlers
func NewTupleDecoder() *TupleDecoder {
	return &TupleDecoder{
		registry: NewDataTypeRegistry(),
	}
}

// RegisterTypeHandler adds a custom type handler to the registry
func (d *TupleDecoder) RegisterTypeHandler(typeOID uint32, handler TypeHandler) {
	d.registry.RegisterHandler(typeOID, handler)
}

// ExtractTupleData converts a tuple data into a map of column name -> value
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
		// Skip if we do not have column info or if the column index is out of range
		if i >= len(columns) {
			continue
		}

		// For key-only tuples, skip NULL columns as they are not part of the key
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
			// Try to use a type-specific handler if one exists
			if handler, exists := d.registry.GetHandler(pgType); exists {
				data[colName] = handler.Handle(col.Data)
			} else {
				// Default to string for text data
				data[colName] = string(col.Data)
			}

		case pglogrepl.TupleDataTypeBinary:
			// For binary data, just return a string representation for now
			data[colName] = fmt.Sprintf("binary(%d bytes)", len(col.Data))
		}
	}

	return data
}
