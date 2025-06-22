export PG_CONN_STRING="postgres://postgres:postgres@localhost:5432/cake"
export PG_SLOT="wal_cake_slot"
export PG_PUBLICATION="wal_cake_pub"
export S3_BUCKET_NAME="wal-cake-bucket"
export AWS_REGION="ap-south-1"
export AWS_ACCESS_KEY_ID="access_key"
export AWS_SECRET_ACCESS_KEY="secret_key"
export AWS_ENDPOINT="http://localhost:9000"
export NAMESPACE="walcake"
export LOG_LEVEL="debug"

exec go run cmd/cake/main.go -batch-size 1000 -concurrency 1 -flush-interval 10s
