# ByteFreezer Connector

A data export tool that reads parquet files from ByteFreezer datasets using DuckDB and sends subsets to external destinations (Elasticsearch, Splunk, webhooks, etc.). Minimizes SIEM costs by exporting only the data you need.

## How It Works

```
packer → parquet (S3/MinIO) → [CONNECTOR] → Elasticsearch / Splunk / webhook / stdout
```

1. Connector fetches S3 credentials from the ByteFreezer control API
2. DuckDB queries parquet files directly over S3 using httpfs
3. Query results are batched and sent to the configured destination
4. Cursor file tracks progress for at-least-once delivery

## Modes

- **interactive** (default): Web UI at `http://localhost:8090` — explore datasets, write queries, test destinations, run exports
- **batch**: Run a configured query once, send results to destination, exit
- **watch**: Run the query on a timer, continuously exporting new data

## Project Structure

```
├── main.go                 # Entry point, HTTP routes, mode switching
├── ui.go                   # Embedded interactive web UI (HTML)
├── config/config.go        # Config struct + koanf loader
├── connector/
│   ├── connector.go        # Core: DuckDB engine, S3 config, query execution
│   ├── control_client.go   # Control API client (S3 creds, health reporting)
│   ├── cursor.go           # Cursor persistence (JSON file)
│   └── destination.go      # Destination interface + registry
├── destinations/
│   ├── stdout.go           # JSON lines to stdout
│   ├── elasticsearch.go    # Elasticsearch bulk API
│   └── webhook.go          # Generic HTTP POST
├── config.yaml             # Example configuration
└── Dockerfile
```

## Adding a New Destination

1. Create `destinations/your_dest.go`
2. Implement the `Destination` interface:

```go
package destinations

import (
    "context"
    "github.com/bytefreezer/connector/connector"
)

func init() {
    connector.RegisterDestination("your_dest", func() connector.Destination {
        return &YourDestination{}
    })
}

type YourDestination struct {
    // config fields
}

func (d *YourDestination) Name() string { return "your_dest" }

func (d *YourDestination) Init(config map[string]interface{}) error {
    // Parse config, validate, create clients
    return nil
}

func (d *YourDestination) Send(ctx context.Context, batch connector.Batch) error {
    // Send batch.Records to your system
    return nil
}

func (d *YourDestination) Close() error { return nil }
```

3. The `init()` function auto-registers it. No other changes needed.

## ByteFreezer MCP Tools

Use these MCP tools to discover your data:

| Tool | Purpose |
|------|---------|
| `bf_whoami` | Get your account_id |
| `bf_list_tenants` | List tenants for your account |
| `bf_list_datasets` | List datasets for a tenant |
| `bf_dataset_parquet_files` | List parquet files for a dataset |
| `bf_transformation_schema` | Get field names and types for a dataset |
| `bf_dataset_statistics` | Get record counts and sizes |
| `bf_health_check` | Verify control API connectivity |

## SQL Query Patterns

Use `PARQUET_PATH` as placeholder — the connector replaces it with the actual S3 glob path.

```sql
-- All records (limited)
SELECT * FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true) LIMIT 100

-- Filter by time partition
SELECT * FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true)
WHERE year = 2026 AND month = 3 AND day = 5

-- Aggregate by hour
SELECT year, month, day, hour, COUNT(*) as count
FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true)
GROUP BY year, month, day, hour
ORDER BY year, month, day, hour

-- Filter specific fields (check schema with bf_transformation_schema)
SELECT timestamp, source_ip, message
FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true)
WHERE severity >= 4
LIMIT 1000
```

## Build & Run

```bash
# Build
go build -o bytefreezer-connector .

# Interactive mode (web UI)
./bytefreezer-connector --config config.yaml

# Batch mode (run once)
./bytefreezer-connector --config config.yaml --mode batch

# Watch mode (continuous)
./bytefreezer-connector --config config.yaml --mode watch

# Reset cursor (re-export from beginning)
./bytefreezer-connector --config config.yaml --mode batch --reset-cursor
```

## Configuration

Edit `config.yaml`. Required fields:
- `control.url`: ByteFreezer control API (default: `https://api.bytefreezer.com`)
- `control.api_key`: Your API key
- `control.account_id`: Your account ID

For batch/watch modes also set:
- `query.tenant_id`, `query.dataset_id`, `query.sql`
- `destination.type` and `destination.config`

## Example: Export to Elasticsearch

```yaml
query:
  tenant_id: "your-tenant-id"
  dataset_id: "your-dataset-id"
  sql: "SELECT * FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true) WHERE severity >= 4"

destination:
  type: elasticsearch
  config:
    url: "http://localhost:9200"
    index: "security-alerts"
    username: "elastic"
    password: "changeme"

schedule:
  interval_seconds: 300
  batch_size: 500
```

## Example: Add Splunk Destination

Ask Claude Code: "Add a Splunk HEC destination that sends events to Splunk's HTTP Event Collector"

It will create `destinations/splunk.go` following the pattern in `destinations/webhook.go`.
