package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog/log"

	"git.famapp.in/fampay-inc/wal-cake/internal/config"
)

// S3Uploader uploads data to S3
type S3Uploader interface {
	// UploadBytes uploads data directly from memory to the specified S3 key
	UploadBytes(ctx context.Context, key string, data []byte) error
}

type s3Uploader struct {
	bucket   string
	region   string
	endpoint string
	client   *s3.Client
}

// NewS3Uploader creates a new S3Uploader
func NewS3Uploader(cfg *config.Config) S3Uploader {
	// Get endpoint from environment variable
	endpoint := os.Getenv("AWS_ENDPOINT")
	uploader := &s3Uploader{
		bucket:   cfg.S3Bucket,
		region:   cfg.Region,
		endpoint: endpoint,
	}

	client, err := uploader.getS3Client(context.Background())
	if err != nil {
		log.Error().Err(err).Msg("Failed to initialize S3 client")
	} else {
		uploader.client = client
	}

	return uploader
}

// getS3Client returns a configured S3 client
func (u *s3Uploader) getS3Client(ctx context.Context) (*s3.Client, error) {
	// Load AWS config with region
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(u.region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	// Create S3 client with the latest recommended configuration approach
	s3Options := func(o *s3.Options) {
		// Set base endpoint if specified (for MinIO)
		if u.endpoint != "" {
			log.Debug().Str("endpoint", u.endpoint).Msg("Using custom S3 endpoint")
			o.BaseEndpoint = aws.String(u.endpoint)
			o.UsePathStyle = true // Required for MinIO compatibility
		}
	}

	return s3.NewFromConfig(awsCfg, s3Options), nil
}

// UploadBytes uploads data directly from memory to the specified S3 key
func (u *s3Uploader) UploadBytes(ctx context.Context, key string, data []byte) error {
	log.Info().Str("bucket", u.bucket).Str("key", key).Int("size", len(data)).Msg("init")
	_, err := u.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
		ACL:    types.ObjectCannedACLPrivate,
	})
	if err != nil {
		return fmt.Errorf("uploaded bytes to S3: %w", err)
	}
	log.Info().Str("bucket", u.bucket).Str("key", key).Msg("success")
	return nil
}
