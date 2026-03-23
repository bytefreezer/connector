package connector

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/bytefreezer/goodies/log"

	_ "github.com/marcboeker/go-duckdb"
)

// Connector is the core export engine
type Connector struct {
	controlClient *ControlClient
	cursor        *Cursor
	destination   Destination
	db            *sql.DB
	tenantID      string
	datasetID     string
	querySQL      string
	batchSize     int
	totalExported int64
	s3Override    *S3Credentials
}

// NewConnector creates a new connector instance
func NewConnector(client *ControlClient, cursor *Cursor, dest Destination, tenantID, datasetID, querySQL string, batchSize int) (*Connector, error) {
	// Ensure writable extension directory exists (default $HOME/.duckdb/ may not be writable in containers)
	duckdbHome := "/tmp/duckdb"
	if err := os.MkdirAll(duckdbHome, 0755); err != nil {
		log.Warnf("Failed to create DuckDB home directory %s: %v", duckdbHome, err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	// Install and load httpfs for S3 access
	for _, stmt := range []string{
		fmt.Sprintf("SET home_directory='%s'", duckdbHome),
		"INSTALL httpfs",
		"LOAD httpfs",
	} {
		if _, err := db.Exec(stmt); err != nil {
			log.Warnf("DuckDB setup %q: %v", stmt, err)
		}
	}

	return &Connector{
		controlClient: client,
		cursor:        cursor,
		destination:   dest,
		db:            db,
		tenantID:      tenantID,
		datasetID:     datasetID,
		querySQL:      querySQL,
		batchSize:     batchSize,
		totalExported: cursor.Get().TotalExported,
	}, nil
}

// SetS3Override sets local S3 credentials, bypassing control API query-credentials
func (c *Connector) SetS3Override(creds *S3Credentials) {
	c.s3Override = creds
}

// GetS3Bucket returns the configured S3 bucket name (from override or empty if not set)
func (c *Connector) GetS3Bucket() string {
	if c.s3Override != nil {
		return c.s3Override.Bucket
	}
	return ""
}

// BuildParquetPath returns the S3 parquet glob path for a given tenant/dataset
func (c *Connector) BuildParquetPath(bucket, tenantID, datasetID string) string {
	return fmt.Sprintf("s3://%s/%s/%s/data/parquet/**/*.parquet*", bucket, tenantID, datasetID)
}

// Close cleans up resources
func (c *Connector) Close() {
	if c.db != nil {
		c.db.Close()
	}
	if c.destination != nil {
		c.destination.Close()
	}
}

// RunOnce executes the export query once and sends all results to the destination
func (c *Connector) RunOnce(ctx context.Context) error {
	log.Infof("Running export: tenant=%s, dataset=%s", c.tenantID, c.datasetID)

	var creds *S3Credentials
	var err error

	if c.s3Override != nil {
		creds = c.s3Override
		log.Info("Using local S3 config override")
	} else {
		creds, err = c.controlClient.GetS3Credentials(ctx, c.tenantID, c.datasetID)
		if err != nil {
			return fmt.Errorf("failed to get S3 credentials: %w", err)
		}
	}

	// Configure DuckDB S3 access
	if err := c.configureS3(creds); err != nil {
		return fmt.Errorf("failed to configure S3: %w", err)
	}

	// Build the parquet path
	parquetPath := fmt.Sprintf("s3://%s/%s/%s/data/parquet/**/*.parquet*",
		creds.Bucket, c.tenantID, c.datasetID)

	// Replace PARQUET_PATH placeholder in SQL if present
	finalSQL := strings.ReplaceAll(c.querySQL, "PARQUET_PATH", parquetPath)

	// If cursor has a position, add partition filter
	cursorState := c.cursor.Get()
	if cursorState.LastPartition != "" {
		log.Infof("Resuming from cursor: partition=%s", cursorState.LastPartition)
	}

	log.Infof("Executing: %s", finalSQL)
	start := time.Now()

	rows, err := c.db.QueryContext(ctx, finalSQL)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Read and batch records
	var batch []Record
	var totalRows int64

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		record := make(Record, len(columns))
		for i, col := range columns {
			record[col] = values[i]
		}

		batch = append(batch, record)
		totalRows++

		// Flush batch when full
		if len(batch) >= c.batchSize {
			if err := c.destination.Send(ctx, Batch{Records: batch}); err != nil {
				return fmt.Errorf("destination send failed: %w", err)
			}
			c.totalExported += int64(len(batch))
			batch = batch[:0]
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	// Flush remaining records
	if len(batch) > 0 {
		if err := c.destination.Send(ctx, Batch{Records: batch}); err != nil {
			return fmt.Errorf("destination send failed: %w", err)
		}
		c.totalExported += int64(len(batch))
	}

	elapsed := time.Since(start)
	log.Infof("Export complete: %d records in %v (total exported: %d)", totalRows, elapsed, c.totalExported)

	// Update cursor
	if err := c.cursor.Update("", "", 0, c.totalExported); err != nil {
		log.Warnf("Failed to update cursor: %v", err)
	}

	return nil
}

// RunWatch runs the export loop continuously at the given interval
func (c *Connector) RunWatch(ctx context.Context, interval time.Duration) error {
	log.Infof("Starting watch mode: interval=%v", interval)

	// Run immediately on start
	if err := c.RunOnce(ctx); err != nil {
		log.Warnf("Export run failed: %v", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("Watch mode stopped")
			return nil
		case <-ticker.C:
			if err := c.RunOnce(ctx); err != nil {
				log.Warnf("Export run failed: %v", err)
			}
		}
	}
}

// ExecuteQuery runs an arbitrary SQL query and returns results (for interactive mode)
func (c *Connector) ExecuteQuery(ctx context.Context, sqlQuery string) ([]string, []Record, error) {
	rows, err := c.db.QueryContext(ctx, sqlQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var records []Record
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, fmt.Errorf("failed to scan: %w", err)
		}

		record := make(Record, len(columns))
		for i, col := range columns {
			record[col] = values[i]
		}
		records = append(records, record)
	}

	return columns, records, rows.Err()
}

// ConfigureDataset sets up S3 credentials for a specific dataset
func (c *Connector) ConfigureDataset(ctx context.Context, tenantID, datasetID string) error {
	creds, err := c.controlClient.GetS3Credentials(ctx, tenantID, datasetID)
	if err != nil {
		return err
	}
	c.tenantID = tenantID
	c.datasetID = datasetID
	return c.configureS3(creds)
}

// GetParquetPath returns the S3 parquet glob path for the current dataset.
// Tries the narrowest recent partition first: current hour, previous hour,
// today, yesterday. Falls back to full glob if nothing found.
func (c *Connector) GetParquetPath(ctx context.Context) (string, error) {
	creds, err := c.controlClient.GetS3Credentials(ctx, c.tenantID, c.datasetID)
	if err != nil {
		return "", err
	}
	base := fmt.Sprintf("s3://%s/%s/%s/data/parquet", creds.Bucket, c.tenantID, c.datasetID)
	now := time.Now().UTC()

	// Try current hour, then previous hour (narrowest — ~60 files max)
	for _, h := range []time.Time{now, now.Add(-1 * time.Hour)} {
		path := fmt.Sprintf("%s/year=%d/month=%02d/day=%d/hour=%02d/*.parquet*",
			base, h.Year(), h.Month(), h.Day(), h.Hour())
		if c.hasParquetFiles(ctx, path) {
			log.Infof("Using partition %d-%02d-%02d hour %02d", h.Year(), h.Month(), h.Day(), h.Hour())
			return path, nil
		}
	}

	// Try today, then yesterday (broader — all hours in a day)
	for _, d := range []time.Time{now, now.AddDate(0, 0, -1)} {
		path := fmt.Sprintf("%s/year=%d/month=%02d/day=%d/**/*.parquet*", base, d.Year(), d.Month(), d.Day())
		if c.hasParquetFiles(ctx, path) {
			log.Infof("Using partition %d-%02d-%02d (full day)", d.Year(), d.Month(), d.Day())
			return path, nil
		}
	}

	// Fallback to full glob
	log.Warn("No recent partitions found, using full glob (may be slow)")
	return base + "/**/*.parquet*", nil
}

func (c *Connector) hasParquetFiles(ctx context.Context, path string) bool {
	row := c.db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT 1 FROM read_parquet('%s', hive_partitioning=true, union_by_name=true) LIMIT 1", path))
	var dummy int
	return row.Scan(&dummy) == nil
}

func (c *Connector) configureS3(creds *S3Credentials) error {
	// DuckDB s3_endpoint expects host:port without scheme
	endpoint := creds.Endpoint
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

	settings := map[string]string{
		"s3_access_key_id":     creds.AccessKey,
		"s3_secret_access_key": creds.SecretKey,
		"s3_region":            creds.Region,
		"s3_endpoint":          endpoint,
		"s3_use_ssl":           "false",
		"s3_url_style":         "path",
	}
	if creds.UseSSL {
		settings["s3_use_ssl"] = "true"
	}

	for key, val := range settings {
		stmt := fmt.Sprintf("SET %s='%s'", key, val)
		if _, err := c.db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to set %s: %w", key, err)
		}
	}
	return nil
}
