package repository

import (
	"context"

	"git.famapp.in/fampay-inc/wal-cake/internal/domain/entity"
)

// WALRepository defines the interface for accessing WAL data
type WALRepository interface {
	// StartReplication begins WAL replication from the specified position
	StartReplication(ctx context.Context, slot string, startLSN uint64) error
	
	// StopReplication stops the WAL replication process
	StopReplication(ctx context.Context) error
	
	// GetChanges returns a channel that will receive WAL changes
	GetChanges(ctx context.Context) (<-chan *entity.WALChange, error)
	
	// GetReplicationStatus returns the current replication status
	GetReplicationStatus(ctx context.Context) (ReplicationStatus, error)
	
	// SendStatusUpdate sends a status update to the PostgreSQL server
	SendStatusUpdate(ctx context.Context, lsn uint64) error
}

// ReplicationStatus contains information about the current replication status
type ReplicationStatus struct {
	// CurrentLSN is the current position in the WAL
	CurrentLSN uint64
	
	// ServerLSN is the server's current WAL position
	ServerLSN uint64
	
	// ReplicationLag is the difference between ServerLSN and CurrentLSN
	ReplicationLag uint64
	
	// Connected indicates if the replication connection is active
	Connected bool
	
	// SlotName is the name of the replication slot
	SlotName string
	
	// LastStatusUpdate is the timestamp of the last status update
	LastStatusUpdate int64
}
