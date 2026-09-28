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
	Timestamp time.Time      `json:"timestamp"` // when WAL Cake decoded the change
	// CommitTime is the commit timestamp of the change's transaction, from
	// the pgoutput BEGIN message. It is the same on every replay.
	CommitTime time.Time `json:"commit_time"`
	LSN        uint64    `json:"lsn"`
}

// Date is the UTC day the change belongs to: the day its transaction
// committed. It falls back to the decode time only if the commit time is
// unknown.
func (c *CDCEvent) Date() time.Time {
	t := c.CommitTime
	if t.IsZero() {
		t = c.Timestamp
	}
	return t.UTC().Truncate(24 * time.Hour)
}

func LSNStr(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}
