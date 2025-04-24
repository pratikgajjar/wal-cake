# WAL-Cake: PostgreSQL CDC to S3 Data Lake

WAL-Cake is a blazingly fast, production-grade Change Data Capture (CDC) service that streams events from PostgreSQL to an S3 data lake in Parquet format. It leverages PostgreSQL's logical replication to capture database changes in real-time and efficiently processes them for analytics and data warehousing.

## Features

- **Real-time CDC**: Capture INSERT, UPDATE, DELETE, and COMMIT operations from PostgreSQL WAL
- **Optimized Parquet Writing**: Efficiently converts CDC events to columnar Parquet format with ZSTD compression (level 3)
- **Reliable LSN Management**: Properly acknowledges LSN positions only after successful S3 uploads
- **Event Filtering**: Configurable filtering of events (e.g., excluding commit events from Parquet files)
- **Memory Efficient**: Minimizes memory allocations and optimizes for high throughput
- **Production Ready**: Resilient error handling, proper logging, and performance optimized

## Architecture

WAL-Cake follows SOLID principles and is built with a clean, modular architecture:

1. **Replication Layer** (`internal/replication`): Handles PostgreSQL logical replication using `pglogrepl`
2. **Transform Layer** (`internal/transform`): Converts CDC events to Parquet format
3. **Storage Layer** (`internal/storage`): Manages uploads to S3 data lake
4. **Model Layer** (`internal/model`): Defines core data structures
5. **Config Layer** (`internal/config`): Manages application configuration

## Prerequisites

- Go 1.21+
- PostgreSQL 13+ with logical replication enabled
- AWS S3 bucket or compatible storage service

## Configuration

WAL-Cake is configured via environment variables:

```
# PostgreSQL Configuration
PG_CONN_STRING=postgres://user:password@localhost:5432/dbname
PG_SLOT=wal_cake_slot
PG_PUBLICATION=wal_cake_pub

# S3 Configuration
S3_BUCKET_NAME=your-data-lake-bucket
NAMESPACE=your-namespace
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_ENDPOINT=https://s3.amazonaws.com (optional for S3-compatible services)
```

## Usage

### Dependencies
- postgres
- minio / s3

```bash
docker compose up -d
```

### Running

```bash
./run.sh
```

## PostgreSQL Setup

1. Enable logical replication in `postgresql.conf`:
   ```
   wal_level = logical
   ```

2. Create a publication:
   ```sql
   CREATE PUBLICATION wal_cake_pub FOR ALL TABLES;
   ```

3. Create a replication slot:
   ```sql
   SELECT pg_create_logical_replication_slot('wal_cake_slot', 'pgoutput');
   ```

## Monitoring

WAL-Cake uses zerolog with logfmt format for structured logging. Key metrics logged include:

- CDC event counts and types
- LSN positions and acknowledgments
- Parquet file sizes and compression ratios
- Error conditions and recovery actions

## Development

### Project Structure

```
wal-cake/
├── cmd/
│   └── cake/          # Main application entry point
├── internal/
│   ├── config/        # Configuration handling
│   ├── model/         # Data models
│   ├── replication/   # PostgreSQL replication logic
│   ├── storage/       # S3 storage interface
│   └── transform/     # Parquet transformation logic
├── docker-compose.yml # Local development environment
└── run.sh             # Convenience script for running the application
```
