package entity

import (
	"time"

	"git.famapp.in/fampay-inc/wal-cake/internal/domain/valueobject"
)

// WALChange represents a change captured from the PostgreSQL WAL
type WALChange struct {
	// TableName is the name of the table that was changed
	TableName string
	
	// Operation is the type of operation that was performed
	Operation valueobject.Operation
	
	// CurrentValues contains the current values of the row (for INSERT and UPDATE)
	CurrentValues map[string]interface{}
	
	// PreviousValues contains the previous values of the row (for UPDATE and DELETE)
	PreviousValues map[string]interface{}
	
	// Timestamp is when the change was captured
	Timestamp time.Time
	
	// LSN (Log Sequence Number) is the position in the WAL
	LSN uint64
	
	// TransactionID is the ID of the transaction that made this change
	TransactionID uint32
	
	// SchemaName is the database schema name
	SchemaName string
}

// NewWALChange creates a new WALChange entity
func NewWALChange(
	tableName string,
	operation valueobject.Operation,
	currentValues map[string]interface{},
	previousValues map[string]interface{},
	lsn uint64,
	txID uint32,
	schemaName string,
) *WALChange {
	return &WALChange{
		TableName:      tableName,
		Operation:      operation,
		CurrentValues:  currentValues,
		PreviousValues: previousValues,
		Timestamp:      time.Now().UTC(),
		LSN:            lsn,
		TransactionID:  txID,
		SchemaName:     schemaName,
	}
}

// GetPrimaryKeyValues extracts the primary key values from the appropriate values map
func (w *WALChange) GetPrimaryKeyValues(primaryKeys []string) map[string]interface{} {
	result := make(map[string]interface{})
	
	// For DELETE operations, we need to use previous values since current values are empty
	sourceMap := w.CurrentValues
	if w.Operation == valueobject.Delete {
		sourceMap = w.PreviousValues
	}
	
	for _, key := range primaryKeys {
		if val, exists := sourceMap[key]; exists {
			result[key] = val
		}
	}
	
	return result
}

// GetTableFullName returns the fully qualified table name (schema.table)
func (w *WALChange) GetTableFullName() string {
	if w.SchemaName == "" {
		return w.TableName
	}
	return w.SchemaName + "." + w.TableName
}
