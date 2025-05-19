package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	bucket   = "wal-cake-bucket"
	region   = "ap-south-1"
	endpoint = "http://localhost:9000" // Set to empty string for AWS S3
)

func main() {
	// take key name from flag -key value
	var key string
	flag.StringVar(&key, "key", key, "Key name")
	flag.Parse()

	ctx := context.Background()
	// set os variables
	os.Setenv("S3_BUCKET_NAME", bucket)
	os.Setenv("AWS_REGION", region)
	os.Setenv("AWS_ACCESS_KEY_ID", "access_key")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "secret_key")
	os.Setenv("AWS_ENDPOINT", endpoint)

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	// Create S3 client with the latest recommended configuration approach
	s3Options := func(o *s3.Options) {
		// Set base endpoint if specified (for MinIO)
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true // Required for MinIO compatibility
		}
	}
	client := s3.NewFromConfig(awsCfg, s3Options)

	// Download file from S3 bucket
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Fatalf("Failed to get object %q from bucket %q: %v", key, bucket, err)
	}
	defer result.Body.Close()

	// Create output file
	fileDelim := strings.Split(key, "/")
	name := fileDelim[len(fileDelim)-1]
	outFile, err := os.Create(name)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	// Copy the file content to the output file
	if _, err := io.Copy(outFile, result.Body); err != nil {
		log.Fatalf("Failed to write to file: %v", err)
	}

	log.Printf("downloaded %q to %s", key, name)
}
