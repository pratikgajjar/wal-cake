package replication

import (
	"encoding/json"
	"fmt"

	"github.com/jackc/pglogrepl"
)

// extractTupleData converts a tuple's data into a map of column name -> value
func extractTupleData(tuple *pglogrepl.TupleData, columns []*pglogrepl.RelationMessageColumn) map[string]any {
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

		colName := columns[i].Name
		dataType := columns[i].DataType

		// Handle different column types based on data type
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			// Column is NULL
			data[colName] = nil

		case pglogrepl.TupleDataTypeToast:
			// TOAST data (Too Large Object Access Storage Technique)
			// This means the actual data is stored separately
			data[colName] = "<TOAST>"

		case pglogrepl.TupleDataTypeText:
			// Text data
			if isJSONType(dataType) {
				// Try to parse as JSON
				var jsonData any
				if err := json.Unmarshal(col.Data, &jsonData); err == nil {
					data[colName] = jsonData
				} else {
					// Fall back to string if not valid JSON
					data[colName] = string(col.Data)
				}
			} else {
				data[colName] = string(col.Data)
			}

		case pglogrepl.TupleDataTypeBinary:
			// Binary data - decode based on PostgreSQL type
			value := decodeValue(col.Data, dataType)
			data[colName] = value
		}
	}

	return data
}

// extractKeyOnlyTupleData extracts only key columns from a tuple
func extractKeyOnlyTupleData(tuple *pglogrepl.TupleData, columns []*pglogrepl.RelationMessageColumn) map[string]any {
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
		if col.DataType == pglogrepl.TupleDataTypeNull {
			continue
		}

		colName := columns[i].Name
		dataType := columns[i].DataType

		// Handle different column types based on data type
		switch col.DataType {
		case pglogrepl.TupleDataTypeToast:
			// TOAST data (Too Large Object Access Storage Technique)
			data[colName] = "<TOAST>"

		case pglogrepl.TupleDataTypeText:
			// Text data
			if isJSONType(dataType) {
				// Try to parse as JSON
				var jsonData any
				if err := json.Unmarshal(col.Data, &jsonData); err == nil {
					data[colName] = jsonData
				} else {
					// Fall back to string if not valid JSON
					data[colName] = string(col.Data)
				}
			} else {
				data[colName] = string(col.Data)
			}

		case pglogrepl.TupleDataTypeBinary:
			// Binary data - decode based on PostgreSQL type
			value := decodeValue(col.Data, dataType)
			data[colName] = value
		}
	}

	return data
}

// decodeValue decodes binary value based on PostgreSQL type OID
func decodeValue(value []byte, typeOID uint32) any {
	if len(value) == 0 {
		return nil
	}

	// For binary data, we'll just return it as a string representation
	// In a production system, you would want to properly decode based on the type OID
	// This is a simplified implementation
	return fmt.Sprintf("binary(%d bytes): %x", len(value), value)

}

// isJSONType checks if the type OID is for JSON or JSONB
func isJSONType(typeOID uint32) bool {
	return typeOID == 114 || typeOID == 3802 // json or jsonb
}
