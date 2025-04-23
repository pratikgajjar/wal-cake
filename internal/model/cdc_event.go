package model

import "time"

// Operation represents the type of CDC change
type Operation string

const (
	InsertOp Operation = "insert"
	UpdateOp Operation = "update"
	DeleteOp Operation = "delete"
	DDLOp    Operation = "ddl"
)

// CDCEvent represents a change data capture event
type CDCEvent struct {
	Table     string                 `json:"table"`
	Operation Operation              `json:"operation"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
	LSN       uint64                 `json:"lsn"`
}

// CDCEventParquet is a Parquet schema for CDCEvent metadata
// Data field is not included for simplicity
type CDCEventParquet struct {
	Table     string `parquet:"name=table, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Operation string `parquet:"name=operation, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Timestamp int64  `parquet:"name=timestamp, type=INT64"`
	LSN       int64  `parquet:"name=lsn, type=INT64"`
}
