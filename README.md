# WAL-Cake

WAL-Cake is a high-performance, production-grade Go application that streams data from PostgreSQL's Write-Ahead Log (WAL) directly to AWS S3 Tables (Apache Iceberg) for data lake creation.

## Features

- Streams data changes from PostgreSQL WAL using logical replication
- Writes data directly to AWS S3 Tables in Apache Iceberg format
- Runs one worker per database for optimal scalability
- Implements hexagonal architecture and SOLID principles
- Uses zerolog for structured logging in logfmt format
- Provides metrics and health checks
- Optimized for performance and cost efficiency

## Architecture

WAL-Cake follows a hexagonal architecture with clear separation of concerns:

- **Domain Layer**: Core business logic and entities
- **Application Layer**: Use cases and ports (interfaces)
- **Adapter Layer**: Implementation of ports for external systems
- **Infrastructure Layer**: Cross-cutting concerns like logging and metrics

## Prerequisites

- Go 1.24 or later
- PostgreSQL 10 or later with logical replication enabled
- AWS account with S3 and Glue access

## Quick Start

### 1. Set up a test environment

Use the provided Docker Compose file to start a PostgreSQL instance with logical replication enabled:

```bash
docker-compose up -d postgres
```

For local S3 testing, you can use LocalStack:

```bash
docker-compose up -d localstack
```

### 2. Configure environment variables

Set the required environment variables or use the provided test script:

```bash
chmod +x scripts/run_test.sh
./scripts/run_test.sh
```

### 3. Build and run the application

```bash
go build -o bin/wal-cake ./cmd/wal-cake
./bin/wal-cake
```

## Configuration

WAL-Cake is configured via environment variables:

### PostgreSQL Configuration

- `POSTGRES_HOST`: PostgreSQL host (default: "localhost")
- `POSTGRES_PORT`: PostgreSQL port (default: "5432")
- `POSTGRES_DB`: PostgreSQL database name
- `POSTGRES_USER`: PostgreSQL user
- `POSTGRES_PASSWORD`: PostgreSQL password
- `POSTGRES_REPLICATION_SLOT`: Replication slot name (default: "wal_cake_slot")
- `POSTGRES_PUBLICATIONS`: Publications to subscribe to (default: "wal_cake_pub")

### AWS Configuration

- `AWS_REGION`: AWS region (default: "us-east-1")
- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key
- `AWS_ENDPOINT`: Optional custom endpoint for testing with LocalStack

### S3 Tables Configuration

- `S3_TABLES_CATALOG`: AWS Glue catalog name (default: "awsdatacatalog")
- `S3_TABLES_DATABASE`: AWS Glue database name (default: "wal_cake")
- `S3_TABLES_PREFIX`: Table name prefix (default: "")
- `S3_BUCKET_NAME`: S3 bucket name
- `S3_TABLES_BATCH_SIZE`: Number of changes to batch before writing (default: 1000)
- `S3_TABLES_FLUSH_INTERVAL`: How often to flush changes to S3 (default: "30s")

### Worker Configuration

- `WORKER_COUNT`: Number of worker goroutines (default: 1)
- `WORKER_QUEUE_SIZE`: Size of the worker queue (default: 10000)
- `WORKER_SHUTDOWN_TIMEOUT`: Timeout for graceful shutdown (default: "30s")
- `WORKER_HEARTBEAT_INTERVAL`: How often to send heartbeats (default: "10s")

### Logging Configuration

- `LOG_LEVEL`: Logging level (default: "info")
- `LOG_FORMAT`: Logging format (default: "logfmt")

### Metrics Configuration

- `METRICS_ENABLED`: Whether metrics collection is enabled (default: true)
- `METRICS_PORT`: Port for the metrics server (default: 9090)
- `METRICS_PATH`: Path for the metrics endpoint (default: "/metrics")

## PostgreSQL Setup

To use WAL-Cake, your PostgreSQL instance must have logical replication enabled:

1. Set the following parameters in `postgresql.conf`:
   ```
   wal_level = logical
   max_replication_slots = 10  # Adjust as needed
   max_wal_senders = 10        # Adjust as needed
   ```

2. Create a replication slot:
   ```sql
   SELECT pg_create_logical_replication_slot('wal_cake_slot', 'pgoutput');
   ```

3. Create a publication for the tables you want to replicate:
   ```sql
   CREATE PUBLICATION wal_cake_pub FOR TABLE table1, table2;
   -- Or for all tables:
   CREATE PUBLICATION wal_cake_pub FOR ALL TABLES;
   ```

## Testing

The repository includes a test script and Docker Compose file to help you set up a test environment:

1. Start the test environment:
   ```bash
   docker-compose up -d
   ```

2. Run the test script:
   ```bash
   chmod +x scripts/run_test.sh
   ./scripts/run_test.sh
   ```

## License

[MIT License](LICENSE)
