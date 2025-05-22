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
	Table     string         `json:"table"`
	Operation Operation      `json:"op"`
	Before    map[string]any `json:"before,omitempty"`
	After     map[string]any `json:"after,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
	LSN       uint64         `json:"lsn"`
}

func (c *CDCEvent) Date() time.Time {
	return c.Timestamp.Truncate(24 * time.Hour)
}

func LSNStr(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}
