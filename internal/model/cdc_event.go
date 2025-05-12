package model

import (
	"fmt"
	"time"
)

// Operation represents the type of CDC operation
type Operation string

const (
	InsertOp Operation = "insert"
	UpdateOp Operation = "update"
	DeleteOp Operation = "delete"
	CommitOp Operation = "commit" // Special operation for transaction commits
	DDLOp    Operation = "ddl"
)

// CDCEvent represents a change data capture event
type CDCEvent struct {
	Table     string                 `json:"table"`
	Operation Operation              `json:"op"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
	LSN       uint64                 `json:"lsn"`
}

func LSNStr(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}
