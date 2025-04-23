package storage

import (
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

// S3Uploader uploads files to S3
type S3Uploader interface {
	UploadFile(ctx context.Context, key, localPath string) error
}

type s3Uploader struct {
	bucket   string
	region   string
	endpoint string
}

// NewS3Uploader creates a new S3Uploader
func NewS3Uploader(cfg *config.Config) S3Uploader {
	// Get endpoint from environment variable
	endpoint := os.Getenv("AWS_ENDPOINT")
	return &s3Uploader{bucket: cfg.S3Bucket, region: cfg.Region, endpoint: endpoint}
}

// UploadFile uploads a local file to the specified S3 key
func (u *s3Uploader) UploadFile(ctx context.Context, key, localPath string) error {
	// Load AWS config with region
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(u.region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
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
	
	client := s3.NewFromConfig(awsCfg, s3Options)

	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open file %s: %w", localPath, err)
	}
	defer file.Close()

	log.Info().Str("bucket", u.bucket).Str("key", key).Msg("Uploading file to S3")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
		Body:   file,
		ACL:    types.ObjectCannedACLPrivate,
	})
	if err != nil {
		return fmt.Errorf("upload to S3: %w", err)
	}
	log.Info().Str("bucket", u.bucket).Str("key", key).Msg("Successfully uploaded to S3")
	return nil
}
