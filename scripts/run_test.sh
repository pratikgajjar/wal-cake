#!/bin/bash
set -e

# PostgreSQL connection details
export POSTGRES_HOST="localhost"
export POSTGRES_PORT="1732"
export POSTGRES_DB="postgres"
export POSTGRES_USER="postgres"
export POSTGRES_PASSWORD="postgres"
export POSTGRES_REPLICATION_SLOT="wal_cake_slot"
export POSTGRES_PUBLICATIONS="wal_cake_pub"

# AWS credentials (read from environment or set defaults for testing)
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-dummy_access_key}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-dummy_secret_key}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

# S3 Tables configuration
export S3_TABLES_CATALOG="awsdatacatalog"
export S3_TABLES_DATABASE="wal_cake"
export S3_TABLES_PREFIX="test"
export S3_BUCKET_NAME="${S3_BUCKET_NAME:-your-test-bucket}"
export S3_TABLES_BATCH_SIZE="100"
export S3_TABLES_FLUSH_INTERVAL="10s"

# Worker configuration
export WORKER_COUNT="1"
export WORKER_QUEUE_SIZE="1000"
export WORKER_SHUTDOWN_TIMEOUT="30s"
export WORKER_HEARTBEAT_INTERVAL="10s"

# Logging configuration
export LOG_LEVEL="debug"
export LOG_FORMAT="logfmt"

# Metrics configuration
export METRICS_ENABLED="true"
export METRICS_PORT="9090"

# Check if AWS credentials are set
if [ "$AWS_ACCESS_KEY_ID" = "dummy_access_key" ] || [ "$AWS_SECRET_ACCESS_KEY" = "dummy_secret_key" ]; then
    echo "WARNING: Using dummy AWS credentials. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables for real testing."
fi

if [ "$S3_BUCKET_NAME" = "your-test-bucket" ]; then
    echo "WARNING: Using default S3 bucket name. Set S3_BUCKET_NAME environment variable for real testing."
fi

# Build the application if needed
echo "Building WAL-Cake application..."
go build -o bin/wal-cake ./cmd/wal-cake

# Ensure PostgreSQL has logical replication enabled and the publication exists
echo "Checking PostgreSQL replication configuration..."
PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT name, setting FROM pg_settings WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders');"

# Check if replication slot exists, create if not
SLOT_EXISTS=$(PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -t -c "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '$POSTGRES_REPLICATION_SLOT';")
if [ "$SLOT_EXISTS" -eq "0" ]; then
    echo "Creating replication slot $POSTGRES_REPLICATION_SLOT..."
    PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT pg_create_logical_replication_slot('$POSTGRES_REPLICATION_SLOT', 'pgoutput');"
else
    echo "Replication slot $POSTGRES_REPLICATION_SLOT already exists."
fi

# Check if publication exists, create if not
PUB_EXISTS=$(PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -t -c "SELECT COUNT(*) FROM pg_publication WHERE pubname = '$POSTGRES_PUBLICATIONS';")
if [ "$PUB_EXISTS" -eq "0" ]; then
    echo "Creating publication $POSTGRES_PUBLICATIONS..."
    PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "CREATE PUBLICATION $POSTGRES_PUBLICATIONS FOR ALL TABLES;"
else
    echo "Publication $POSTGRES_PUBLICATIONS already exists."
fi

# Run the application
echo "Starting WAL-Cake application..."
./bin/wal-cake
