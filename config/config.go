package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Source SourceConfig `yaml:"source"`
	Sink   SinkConfig   `yaml:"sink"`
	State  StateConfig  `yaml:"state"`
}

type SourceConfig struct {
	Mode    string         `yaml:"mode"` // "query" or "logical"
	Postgres PostgresConfig `yaml:"postgres"`
	Query   QueryConfig    `yaml:"query"`
	Logical LogicalConfig  `yaml:"logical"`
}

type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

func (p PostgresConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		p.Host, p.Port, p.Database, p.User, p.Password)
}

func (p PostgresConfig) ReplicationDSN() string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable replication=database",
		p.Host, p.Port, p.Database, p.User, p.Password)
}

type QueryConfig struct {
	Tables       []QueryTableConfig `yaml:"tables"`
	PollInterval string             `yaml:"poll_interval"`
}

func (q QueryConfig) PollDuration() time.Duration {
	d, err := time.ParseDuration(q.PollInterval)
	if err != nil {
		return 5 * time.Second
	}
	return d
}

type QueryTableConfig struct {
	Name            string   `yaml:"name"`
	PrimaryKey      []string `yaml:"primary_key"`
	WatermarkColumn string   `yaml:"watermark_column"`
}

type LogicalConfig struct {
	PublicationName string   `yaml:"publication_name"`
	SlotName        string   `yaml:"slot_name"`
	Tables          []string `yaml:"tables"`
}

type SinkConfig struct {
	CatalogURI    string `yaml:"catalog_uri"`
	Warehouse     string `yaml:"warehouse"`
	Namespace     string `yaml:"namespace"`
	S3Endpoint    string `yaml:"s3_endpoint"`
	S3AccessKey   string `yaml:"s3_access_key"`
	S3SecretKey   string `yaml:"s3_secret_key"`
	S3Region      string `yaml:"s3_region"`
	FlushInterval string `yaml:"flush_interval"`
	FlushRows     int    `yaml:"flush_rows"`
	FlushBytes    int64  `yaml:"flush_bytes"`    // flush when buffered bytes exceed this (default 64MB)
	TargetFileSize int64 `yaml:"target_file_size"` // max parquet file size before rolling (default 128MB)

	// Compaction settings
	CompactionInterval string `yaml:"compaction_interval"` // e.g. "5m" (default disabled)
	CompactionTargetSize int64 `yaml:"compaction_target_size"` // target merged file size (default 256MB)
	CompactionMinFiles   int   `yaml:"compaction_min_files"`   // minimum files to trigger (default 4)
	MaxSnapshots         int   `yaml:"max_snapshots"`          // pause writes when exceeded (default 0 = unlimited)
}

func (s SinkConfig) FlushBytesOrDefault() int64 {
	if s.FlushBytes > 0 {
		return s.FlushBytes
	}
	return 64 * 1024 * 1024 // 64MB
}

func (s SinkConfig) TargetFileSizeOrDefault() int64 {
	if s.TargetFileSize > 0 {
		return s.TargetFileSize
	}
	return 128 * 1024 * 1024 // 128MB
}

func (s SinkConfig) CompactionDuration() time.Duration {
	if s.CompactionInterval == "" {
		return 0 // disabled
	}
	d, err := time.ParseDuration(s.CompactionInterval)
	if err != nil {
		return 0
	}
	return d
}

func (s SinkConfig) CompactionTargetSizeOrDefault() int64 {
	if s.CompactionTargetSize > 0 {
		return s.CompactionTargetSize
	}
	return 256 * 1024 * 1024 // 256MB
}

func (s SinkConfig) CompactionMinFilesOrDefault() int {
	if s.CompactionMinFiles > 0 {
		return s.CompactionMinFiles
	}
	return 4
}

func (s SinkConfig) FlushDuration() time.Duration {
	d, err := time.ParseDuration(s.FlushInterval)
	if err != nil {
		return 10 * time.Second
	}
	return d
}

type StateConfig struct {
	Path string `yaml:"path"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if cfg.Source.Mode == "" {
		cfg.Source.Mode = "logical"
	}
	if cfg.Source.Postgres.Port == 0 {
		cfg.Source.Postgres.Port = 5432
	}
	if cfg.Sink.FlushRows == 0 {
		cfg.Sink.FlushRows = 1000
	}
	if cfg.Sink.S3Region == "" {
		cfg.Sink.S3Region = "us-east-1"
	}

	return &cfg, nil
}
