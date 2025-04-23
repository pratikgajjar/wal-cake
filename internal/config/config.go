package config

import (
	"flag"
	"os"
	"time"

	"github.com/rs/zerolog/log"
)

type Config struct {
	PGConn        string
	Slot          string
	Namespace     string
	Publication   string
	OutputPlugin  string
	S3Bucket      string
	Region        string
	BatchSize     int
	FlushInterval time.Duration
}

func LoadConfig() *Config {
	cfg := &Config{}

	var flush string
	flag.StringVar(&cfg.PGConn, "pg-conn", os.Getenv("PG_CONN_STRING"), "Postgres connection string")
	flag.StringVar(&cfg.Slot, "slot", os.Getenv("PG_SLOT"), "Replication slot name")
	flag.StringVar(&cfg.Namespace, "namespace", os.Getenv("NAMESPACE"), "Namespace for s3 bucket")
	flag.StringVar(&cfg.Publication, "publication", os.Getenv("PG_PUBLICATION"), "Publication name (default: alltables)")
	flag.StringVar(&cfg.OutputPlugin, "plugin", os.Getenv("PG_OUTPUT_PLUGIN"), "Logical decoding output plugin (default: pgoutput)")
	flag.StringVar(&cfg.S3Bucket, "s3-bucket", os.Getenv("S3_BUCKET_NAME"), "S3 bucket for data lake")
	flag.StringVar(&cfg.Region, "aws-region", os.Getenv("AWS_REGION"), "AWS region")
	flag.IntVar(&cfg.BatchSize, "batch-size", 1000, "Number of events per batch")
	flag.StringVar(&flush, "flush-interval", "30s", "Flush interval duration (e.g. 30s)")

	flag.Parse()

	if cfg.PGConn == "" {
		log.Fatal().Msg("pg-conn is required")
	}
	if cfg.Slot == "" {
		log.Fatal().Msg("slot is required")
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "default"
	}
	if cfg.Publication == "" {
		cfg.Publication = "alltables"
	}
	if cfg.OutputPlugin == "" {
		cfg.OutputPlugin = "pgoutput"
	}
	if cfg.S3Bucket == "" {
		log.Fatal().Msg("s3-bucket is required")
	}
	if cfg.Region == "" {
		log.Fatal().Msg("aws-region is required")
	}
	dur, err := time.ParseDuration(flush)
	if err != nil {
		log.Warn().Err(err).Msg("invalid flush-interval, using default 60s")
		dur = time.Second * 60
	}
	cfg.FlushInterval = dur

	return cfg
}
