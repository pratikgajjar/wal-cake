package repository

import (
	"context"

	"git.famapp.in/fampay-inc/wal-cake/internal/domain/entity"
)

// S3TablesRepository defines the interface for interacting with AWS S3 Tables (Apache Iceberg)
type S3TablesRepository interface {
	// WriteChanges writes a batch of WAL changes to S3 Tables
	WriteChanges(ctx context.Context, tableName string, changes []*entity.WALChange) error
	
	// CreateTable creates a new table in S3 Tables if it doesn't exist
	CreateTable(ctx context.Context, tableName string, schema map[string]string, partitionKeys []string) error
	
	// GetTableMetadata retrieves metadata about an S3 Table
	GetTableMetadata(ctx context.Context, tableName string) (*TableMetadata, error)
	
	// CommitTransaction commits a transaction to S3 Tables
	CommitTransaction(ctx context.Context, txID string) error
	
	// AbortTransaction aborts a transaction
	AbortTransaction(ctx context.Context, txID string) error
	
	// BeginTransaction starts a new transaction
	BeginTransaction(ctx context.Context) (string, error)
}

// TableMetadata contains information about an S3 Table
type TableMetadata struct {
	// Name is the table name
	Name string
	
	// Schema is the table schema
	Schema map[string]string
	
	// PartitionKeys are the keys used for partitioning
	PartitionKeys []string
	
	// Location is the S3 location of the table
	Location string
	
	// Format is the table format (e.g., "iceberg")
	Format string
	
	// LastModified is the timestamp of the last modification
	LastModified int64
}
