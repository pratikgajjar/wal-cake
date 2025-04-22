// Package s3 provides adapters for interacting with AWS S3 Tables
package s3

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"

	appconfig "git.famapp.in/fampay-inc/wal-cake/internal/app/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/domain/entity"
	"git.famapp.in/fampay-inc/wal-cake/internal/domain/repository"
	"git.famapp.in/fampay-inc/wal-cake/internal/infrastructure/logger"
)

// S3TablesRepositoryImpl implements the S3TablesRepository interface
type S3TablesRepositoryImpl struct {
	config           *appconfig.S3TablesConfig
	awsConfig        *appconfig.AWSConfig
	s3Client         *s3.Client
	glueClient       *glue.Client
	s3TablesClient   *s3tables.Client
	logger           *logger.Logger
	tableCache       map[string]*repository.TableMetadata
	lastCommittedLSN uint64
	mu               sync.RWMutex
	walRepository    repository.WALRepository
}

// NewS3TablesRepository creates a new S3TablesRepositoryImpl
func NewS3TablesRepository(
	cfg *appconfig.S3TablesConfig,
	awsCfg *appconfig.AWSConfig,
	log *logger.Logger,
	walRepo repository.WALRepository,
) (repository.S3TablesRepository, error) {
	// Configure AWS SDK
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if awsCfg.Endpoint != "" {
			return aws.Endpoint{
				URL:           awsCfg.Endpoint,
				SigningRegion: region,
			}, nil
		}
		// Return EndpointNotFoundError to use the default endpoint resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	// Create AWS config
	awsSdkConfig, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(awsCfg.Region),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			awsCfg.AccessKeyID,
			awsCfg.SecretAccessKey,
			awsCfg.SessionToken,
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3, Glue, and S3Tables clients
	s3Client := s3.NewFromConfig(awsSdkConfig)
	glueClient := glue.NewFromConfig(awsSdkConfig)
	s3TablesClient := s3tables.NewFromConfig(awsSdkConfig)

	return &S3TablesRepositoryImpl{
		config:         cfg,
		awsConfig:      awsCfg,
		s3Client:       s3Client,
		glueClient:     glueClient,
		s3TablesClient: s3TablesClient,
		logger:         log,
		tableCache:     make(map[string]*repository.TableMetadata),
		walRepository:  walRepo,
	}, nil
}

// WriteChanges writes a batch of WAL changes to S3 Tables
func (r *S3TablesRepositoryImpl) WriteChanges(ctx context.Context, tableName string, changes []*entity.WALChange) error {
	if len(changes) == 0 {
		return nil
	}

	r.logger.Info("Writing changes to S3 Table", map[string]interface{}{
		"table":        tableName,
		"change_count": len(changes),
	})

	// Sort changes by LSN to ensure we process them in order
	sort.Slice(changes, func(i, j int) bool {
		return changes[i].LSN < changes[j].LSN
	})

	// Check if table exists and create if needed
	tableMetadata, err := r.GetTableMetadata(ctx, tableName)
	if err != nil {
		// Table doesn't exist, create it based on the first change
		if len(changes) > 0 {
			// Infer schema from the first change
			schema := r.inferSchemaFromChange(changes[0])
			
			// Determine partition keys (this is a simplified approach)
			// In a real implementation, you would use a more sophisticated strategy
			partitionKeys := []string{"year", "month", "day"}
			
			// Create the table
			if err := r.CreateTable(ctx, tableName, schema, partitionKeys); err != nil {
				return fmt.Errorf("failed to create table %s: %w", tableName, err)
			}
			
			// Refresh table metadata
			tableMetadata, err = r.GetTableMetadata(ctx, tableName)
			if err != nil {
				return fmt.Errorf("failed to get metadata for newly created table %s: %w", tableName, err)
			}
		} else {
			return fmt.Errorf("table %s does not exist and no changes provided to infer schema", tableName)
		}
	}

	// Start a transaction
	txID, err := r.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Track the highest LSN in this batch
	highestLSN := uint64(0)
	for _, change := range changes {
		if change.LSN > highestLSN {
			highestLSN = change.LSN
		}
	}

	// In a real implementation, you would:
	// 1. Convert changes to Iceberg format
	// 2. Write data files to S3
	// 3. Update Iceberg metadata
	// 4. Commit the transaction
	
	// For this example, we'll log what would happen
	r.logger.Info("Writing changes to S3 Table", map[string]interface{}{
		"table":        tableName,
		"change_count": len(changes),
		"transaction":  txID,
		"location":     tableMetadata.Location,
		"highest_lsn":  highestLSN,
	})

	// Simulate writing data to S3 Tables
	// In a real implementation, this would use the S3 Tables API
	// to write the data in Iceberg format

	// Commit the transaction
	if err := r.CommitTransaction(ctx, txID); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update the last committed LSN after successful write
	r.updateLastCommittedLSN(highestLSN)

	// Send acknowledgment to PostgreSQL to update the replication slot
	if err := r.acknowledgeChanges(ctx, highestLSN); err != nil {
		r.logger.Warn("Failed to acknowledge changes", map[string]interface{}{
			"error":       err.Error(),
			"highest_lsn": highestLSN,
		})
		// Don't return an error here, as the data has been successfully written
		// We'll retry acknowledgment in the next batch
	}

	return nil
}

// CreateTable creates a new table in S3 Tables if it doesn't exist
func (r *S3TablesRepositoryImpl) CreateTable(ctx context.Context, tableName string, schema map[string]string, partitionKeys []string) error {
	// In a real implementation, you would:
	// 1. Create the table in AWS Glue Catalog using the Iceberg format
	// 2. Initialize the Iceberg metadata files in S3
	
	// For this example, we'll log what would happen
	fullTableName := r.getFullTableName(tableName)
	s3Location := fmt.Sprintf("s3://%s/%s", r.config.BucketName, fullTableName)
	
	r.logger.Info("Creating S3 Table", map[string]interface{}{
		"table":          fullTableName,
		"catalog":        r.config.Catalog,
		"database":       r.config.Database,
		"location":       s3Location,
		"schema":         schema,
		"partition_keys": partitionKeys,
	})

	// Cache the table metadata
	r.tableCache[tableName] = &repository.TableMetadata{
		Name:          fullTableName,
		Schema:        schema,
		PartitionKeys: partitionKeys,
		Location:      s3Location,
		Format:        "iceberg",
		LastModified:  time.Now().Unix(),
	}

	return nil
}

// GetTableMetadata retrieves metadata about an S3 Table
func (r *S3TablesRepositoryImpl) GetTableMetadata(ctx context.Context, tableName string) (*repository.TableMetadata, error) {
	// Check cache first
	if metadata, ok := r.tableCache[tableName]; ok {
		return metadata, nil
	}

	// In a real implementation, you would:
	// 1. Get table metadata from AWS Glue Catalog
	// 2. Parse Iceberg metadata files from S3
	
	// For this example, we'll simulate a "table not found" error
	return nil, fmt.Errorf("table %s not found", tableName)
}

// CommitTransaction commits a transaction to S3 Tables
func (r *S3TablesRepositoryImpl) CommitTransaction(ctx context.Context, txID string) error {
	// In a real implementation, you would:
	// 1. Finalize the Iceberg transaction
	// 2. Update the Iceberg metadata files
	// using the S3 Tables API
	
	r.logger.Info("Committing transaction", map[string]interface{}{
		"transaction_id": txID,
	})

	// In a real implementation, you would use the S3 Tables API to commit the transaction
	// For example:
	// _, err := r.s3TablesClient.CommitTransaction(ctx, &s3tables.CommitTransactionInput{
	//     TransactionId: aws.String(txID),
	// })
	// return err

	return nil
}

// AbortTransaction aborts a transaction
func (r *S3TablesRepositoryImpl) AbortTransaction(ctx context.Context, txID string) error {
	// In a real implementation, you would:
	// 1. Abort the Iceberg transaction
	// 2. Clean up any temporary files
	
	r.logger.Info("Aborting transaction", map[string]interface{}{
		"transaction_id": txID,
	})

	return nil
}

// BeginTransaction starts a new transaction
func (r *S3TablesRepositoryImpl) BeginTransaction(ctx context.Context) (string, error) {
	// In a real implementation, you would:
	// 1. Start an Iceberg transaction using the S3 Tables API
	// 2. Return the transaction ID
	
	// For this example, we'll generate a simple transaction ID
	txID := fmt.Sprintf("tx-%d", time.Now().UnixNano())
	
	r.logger.Info("Beginning transaction", map[string]interface{}{
		"transaction_id": txID,
	})

	// In a real implementation, you would use the S3 Tables API to begin the transaction
	// For example:
	// result, err := r.s3TablesClient.BeginTransaction(ctx, &s3tables.BeginTransactionInput{
	//     Catalog:  aws.String(r.config.Catalog),
	//     Database: aws.String(r.config.Database),
	// })
	// if err != nil {
	//     return "", err
	// }
	// return *result.TransactionId, nil

	return txID, nil
}

// Helper methods

// getFullTableName returns the fully qualified table name with prefix
func (r *S3TablesRepositoryImpl) getFullTableName(tableName string) string {
	prefix := ""
	if r.config.TablePrefix != "" {
		prefix = r.config.TablePrefix + "_"
	}
	return prefix + tableName
}

// updateLastCommittedLSN updates the last committed LSN
func (r *S3TablesRepositoryImpl) updateLastCommittedLSN(lsn uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if lsn > r.lastCommittedLSN {
		r.lastCommittedLSN = lsn
		r.logger.Debug("Updated last committed LSN", map[string]interface{}{
			"lsn": lsn,
		})
	}
}

// GetLastCommittedLSN returns the last committed LSN
func (r *S3TablesRepositoryImpl) GetLastCommittedLSN() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	return r.lastCommittedLSN
}

// acknowledgeChanges sends an acknowledgment to PostgreSQL to update the replication slot
func (r *S3TablesRepositoryImpl) acknowledgeChanges(ctx context.Context, lsn uint64) error {
	if r.walRepository == nil {
		return fmt.Errorf("WAL repository not initialized")
	}
	
	r.logger.Info("Acknowledging changes to PostgreSQL", map[string]interface{}{
		"lsn": lsn,
	})
	
	return r.walRepository.SendStatusUpdate(ctx, lsn)
}

// inferSchemaFromChange infers a schema from a WAL change
func (r *S3TablesRepositoryImpl) inferSchemaFromChange(change *entity.WALChange) map[string]string {
	schema := make(map[string]string)
	
	// Use the appropriate values map based on the operation
	values := change.CurrentValues
	if change.Operation == "DELETE" {
		values = change.PreviousValues
	}
	
	// Infer types from values
	for key, value := range values {
		switch v := value.(type) {
		case int, int32, int64:
			schema[key] = "bigint"
		case float32, float64:
			schema[key] = "double"
		case bool:
			schema[key] = "boolean"
		case time.Time:
			schema[key] = "timestamp"
		case string:
			// Check if it might be JSON
			if strings.HasPrefix(v, "{") || strings.HasPrefix(v, "[") {
				schema[key] = "string" // or "json" if supported
			} else {
				schema[key] = "string"
			}
		default:
			schema[key] = "string" // Default to string for unknown types
		}
	}
	
	// Add partition columns if not present
	if _, ok := schema["year"]; !ok {
		schema["year"] = "int"
	}
	if _, ok := schema["month"]; !ok {
		schema["month"] = "int"
	}
	if _, ok := schema["day"]; !ok {
		schema["day"] = "int"
	}
	
	return schema
}
