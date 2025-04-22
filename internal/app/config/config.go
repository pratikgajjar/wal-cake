package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config holds the application configuration
type Config struct {
	// PostgreSQL configuration
	Postgres PostgresConfig
	
	// AWS configuration
	AWS AWSConfig
	
	// S3 Tables configuration
	S3Tables S3TablesConfig
	
	// Worker configuration
	Worker WorkerConfig
	
	// Logging configuration
	Logging LoggingConfig
	
	// Metrics configuration
	Metrics MetricsConfig
}

// PostgresConfig holds PostgreSQL-specific configuration
type PostgresConfig struct {
	// Host is the PostgreSQL host
	Host string
	
	// Port is the PostgreSQL port
	Port int
	
	// Database is the PostgreSQL database name
	Database string
	
	// User is the PostgreSQL user
	User string
	
	// Password is the PostgreSQL password
	Password string
	
	// ReplicationSlot is the name of the replication slot to use
	ReplicationSlot string
	
	// Publications is a comma-separated list of publications to subscribe to
	Publications string
	
	// MaxConnections is the maximum number of connections to the database
	MaxConnections int
	
	// ConnectionTimeout is the timeout for database connections
	ConnectionTimeout time.Duration
}

// AWSConfig holds AWS-specific configuration
type AWSConfig struct {
	// Region is the AWS region
	Region string
	
	// AccessKeyID is the AWS access key ID
	AccessKeyID string
	
	// SecretAccessKey is the AWS secret access key
	SecretAccessKey string
	
	// SessionToken is the AWS session token (optional)
	SessionToken string
	
	// Endpoint is the AWS endpoint (optional, for testing)
	Endpoint string
}

// S3TablesConfig holds S3 Tables-specific configuration
type S3TablesConfig struct {
	// Catalog is the AWS Glue catalog name
	Catalog string
	
	// Database is the AWS Glue database name
	Database string
	
	// TablePrefix is a prefix to add to all table names
	TablePrefix string
	
	// BucketName is the S3 bucket name for table data
	BucketName string
	
	// BatchSize is the number of changes to batch before writing
	BatchSize int
	
	// FlushInterval is how often to flush changes to S3
	FlushInterval time.Duration
}

// WorkerConfig holds worker-specific configuration
type WorkerConfig struct {
	// WorkerCount is the number of worker goroutines
	WorkerCount int
	
	// QueueSize is the size of the worker queue
	QueueSize int
	
	// ShutdownTimeout is the timeout for graceful shutdown
	ShutdownTimeout time.Duration
	
	// HeartbeatInterval is how often to send heartbeats
	HeartbeatInterval time.Duration
}

// LoggingConfig holds logging-specific configuration
type LoggingConfig struct {
	// Level is the logging level (debug, info, warn, error)
	Level string
	
	// Format is the logging format (json, logfmt)
	Format string
}

// MetricsConfig holds metrics-specific configuration
type MetricsConfig struct {
	// Enabled indicates if metrics collection is enabled
	Enabled bool
	
	// Port is the port for the metrics server
	Port int
	
	// Path is the path for the metrics endpoint
	Path string
}

// LoadConfig loads the configuration from environment variables
func LoadConfig() (*Config, error) {
	// Load .env file if it exists
	_ = godotenv.Load()
	
	config := &Config{
		Postgres: PostgresConfig{
			Host:              getEnv("POSTGRES_HOST", "localhost"),
			Port:              getEnvAsInt("POSTGRES_PORT", 5432),
			Database:          getEnv("POSTGRES_DB", ""),
			User:              getEnv("POSTGRES_USER", ""),
			Password:          getEnv("POSTGRES_PASSWORD", ""),
			ReplicationSlot:   getEnv("POSTGRES_REPLICATION_SLOT", "wal_cake_slot"),
			Publications:      getEnv("POSTGRES_PUBLICATIONS", "wal_cake_pub"),
			MaxConnections:    getEnvAsInt("POSTGRES_MAX_CONNECTIONS", 5),
			ConnectionTimeout: getEnvAsDuration("POSTGRES_CONNECTION_TIMEOUT", 30*time.Second),
		},
		AWS: AWSConfig{
			Region:          getEnv("AWS_REGION", "us-east-1"),
			AccessKeyID:     getEnv("AWS_ACCESS_KEY_ID", ""),
			SecretAccessKey: getEnv("AWS_SECRET_ACCESS_KEY", ""),
			SessionToken:    getEnv("AWS_SESSION_TOKEN", ""),
			Endpoint:        getEnv("AWS_ENDPOINT", ""),
		},
		S3Tables: S3TablesConfig{
			Catalog:       getEnv("S3_TABLES_CATALOG", "awsdatacatalog"),
			Database:      getEnv("S3_TABLES_DATABASE", "wal_cake"),
			TablePrefix:   getEnv("S3_TABLES_PREFIX", ""),
			BucketName:    getEnv("S3_BUCKET_NAME", ""),
			BatchSize:     getEnvAsInt("S3_TABLES_BATCH_SIZE", 1000),
			FlushInterval: getEnvAsDuration("S3_TABLES_FLUSH_INTERVAL", 30*time.Second),
		},
		Worker: WorkerConfig{
			WorkerCount:       getEnvAsInt("WORKER_COUNT", 1),
			QueueSize:         getEnvAsInt("WORKER_QUEUE_SIZE", 10000),
			ShutdownTimeout:   getEnvAsDuration("WORKER_SHUTDOWN_TIMEOUT", 30*time.Second),
			HeartbeatInterval: getEnvAsDuration("WORKER_HEARTBEAT_INTERVAL", 10*time.Second),
		},
		Logging: LoggingConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "logfmt"),
		},
		Metrics: MetricsConfig{
			Enabled: getEnvAsBool("METRICS_ENABLED", true),
			Port:    getEnvAsInt("METRICS_PORT", 9090),
			Path:    getEnv("METRICS_PATH", "/metrics"),
		},
	}
	
	// Validate required fields
	if err := config.validate(); err != nil {
		return nil, err
	}
	
	return config, nil
}

// validate checks if the configuration is valid
func (c *Config) validate() error {
	// Check required PostgreSQL configuration
	if c.Postgres.Database == "" {
		return fmt.Errorf("POSTGRES_DB is required")
	}
	if c.Postgres.User == "" {
		return fmt.Errorf("POSTGRES_USER is required")
	}
	
	// Check required AWS configuration
	if c.AWS.AccessKeyID == "" {
		return fmt.Errorf("AWS_ACCESS_KEY_ID is required")
	}
	if c.AWS.SecretAccessKey == "" {
		return fmt.Errorf("AWS_SECRET_ACCESS_KEY is required")
	}
	
	// Check required S3 Tables configuration
	if c.S3Tables.BucketName == "" {
		return fmt.Errorf("S3_BUCKET_NAME is required")
	}
	
	return nil
}

// Helper functions to get environment variables with default values
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}

func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.ParseBool(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}
	value, err := time.ParseDuration(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}

func getEnvAsSlice(key string, defaultValue []string, sep string) []string {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}
	return strings.Split(valueStr, sep)
}
