package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/bytefreezer/goodies/log"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

var k = koanf.New(".")

// Config holds all configuration for the connector
type Config struct {
	App             AppConfig             `mapstructure:"app"`
	Logging         LoggingConfig         `mapstructure:"logging"`
	Server          ServerConfig          `mapstructure:"server"`
	Control         ControlConfig         `mapstructure:"control"`
	S3              S3Config              `mapstructure:"s3"`
	HealthReporting HealthReportingConfig `mapstructure:"health_reporting"`
	Query           QueryConfig           `mapstructure:"query"`
	Destination     DestinationConfig     `mapstructure:"destination"`
	Cursor          CursorConfig          `mapstructure:"cursor"`
	Schedule        ScheduleConfig        `mapstructure:"schedule"`
}

// S3Config allows direct S3 credentials for on-prem deployments
// When set, bypasses control API query-credentials endpoint
type S3Config struct {
	Endpoint  string `mapstructure:"endpoint"`
	Bucket    string `mapstructure:"bucket"`
	Region    string `mapstructure:"region"`
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	UseSSL    bool   `mapstructure:"use_ssl"`
}

// HasS3Override returns true if local S3 config is provided
func (cfg *Config) HasS3Override() bool {
	return cfg.S3.Endpoint != "" && cfg.S3.Bucket != ""
}

type AppConfig struct {
	Name    string `mapstructure:"name"`
	Version string `mapstructure:"version"`
}

type LoggingConfig struct {
	Level string `mapstructure:"level"`
}

type ServerConfig struct {
	Port int `mapstructure:"port"` // HTTP port for interactive mode UI
}

type ControlConfig struct {
	URL       string `mapstructure:"url"`
	APIKey    string `mapstructure:"api_key"`
	AccountID string `mapstructure:"account_id"`
}

type HealthReportingConfig struct {
	Enabled        bool `mapstructure:"enabled"`
	ReportInterval int  `mapstructure:"report_interval"`
	TimeoutSeconds int  `mapstructure:"timeout_seconds"`
}

// QueryConfig defines what data to export
type QueryConfig struct {
	TenantID  string `mapstructure:"tenant_id"`
	DatasetID string `mapstructure:"dataset_id"`
	SQL       string `mapstructure:"sql"` // SQL query to run against parquet files
}

// DestinationConfig defines where to send data
type DestinationConfig struct {
	Type   string                 `mapstructure:"type"` // stdout, elasticsearch, webhook
	Config map[string]interface{} `mapstructure:"config"`
}

// CursorConfig defines cursor persistence
type CursorConfig struct {
	File string `mapstructure:"file"` // Path to cursor file
}

// ScheduleConfig defines how often to run in watch mode
type ScheduleConfig struct {
	IntervalSeconds int `mapstructure:"interval_seconds"` // Poll interval for watch mode
	BatchSize       int `mapstructure:"batch_size"`       // Records per batch sent to destination
}

// LoadConfig loads configuration from file with env overrides
func LoadConfig(cfgFile, envPrefix string, cfg *Config) error {
	if cfgFile == "" {
		cfgFile = "config.yaml"
	}

	if err := k.Load(file.Provider(cfgFile), yaml.Parser()); err != nil {
		return fmt.Errorf("failed to parse %s: %w", cfgFile, err)
	}

	envVars := make(map[string]string)
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, envPrefix) {
			parts := strings.SplitN(e, "=", 2)
			if len(parts) == 2 {
				envVars[parts[0]] = parts[1]
			}
		}
	}
	if len(envVars) > 0 {
		log.Infof("Found env overrides: %v", envVars)
	}

	if err := k.Load(env.Provider(envPrefix, ".", func(s string) string {
		return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s, envPrefix)), "_", ".")
	}), nil); err != nil {
		return fmt.Errorf("error loading config from env: %w", err)
	}

	if err := k.UnmarshalWithConf("", cfg, koanf.UnmarshalConf{Tag: "mapstructure"}); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Defaults
	if cfg.App.Name == "" {
		cfg.App.Name = "bytefreezer-connector"
	}
	if cfg.App.Version == "" {
		cfg.App.Version = "1.0.0"
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
	if cfg.Server.Port == 0 {
		cfg.Server.Port = 8090
	}
	if cfg.Cursor.File == "" {
		cfg.Cursor.File = "cursor.json"
	}
	if cfg.Schedule.IntervalSeconds == 0 {
		cfg.Schedule.IntervalSeconds = 60
	}
	if cfg.Schedule.BatchSize == 0 {
		cfg.Schedule.BatchSize = 1000
	}
	if cfg.HealthReporting.ReportInterval == 0 {
		cfg.HealthReporting.ReportInterval = 30
	}
	if cfg.HealthReporting.TimeoutSeconds == 0 {
		cfg.HealthReporting.TimeoutSeconds = 10
	}
	if cfg.Destination.Type == "" {
		cfg.Destination.Type = "stdout"
	}

	return nil
}

// Validate checks required configuration
func (cfg *Config) Validate(mode string) error {
	if cfg.Control.URL == "" {
		return fmt.Errorf("control.url is required")
	}
	if cfg.Control.APIKey == "" {
		return fmt.Errorf("control.api_key is required")
	}
	if cfg.Control.AccountID == "" {
		return fmt.Errorf("control.account_id is required")
	}

	// In batch/watch mode, query config is required
	if mode == "batch" || mode == "watch" {
		if cfg.Query.TenantID == "" {
			return fmt.Errorf("query.tenant_id is required for %s mode", mode)
		}
		if cfg.Query.DatasetID == "" {
			return fmt.Errorf("query.dataset_id is required for %s mode", mode)
		}
		if cfg.Query.SQL == "" {
			return fmt.Errorf("query.sql is required for %s mode", mode)
		}
	}

	return nil
}

// LogSummary logs the effective configuration
func (cfg *Config) LogSummary(mode string) {
	log.Infof("Config: mode=%s, control=%s, account=%s, destination=%s",
		mode, cfg.Control.URL, cfg.Control.AccountID, cfg.Destination.Type)
	if cfg.Query.DatasetID != "" {
		log.Infof("Query: tenant=%s, dataset=%s, sql=%q",
			cfg.Query.TenantID, cfg.Query.DatasetID, cfg.Query.SQL)
	}
	log.Infof("Schedule: interval=%ds, batch_size=%d, cursor=%s",
		cfg.Schedule.IntervalSeconds, cfg.Schedule.BatchSize, cfg.Cursor.File)
}
