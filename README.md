# ByteFreezer Connector

Export data from ByteFreezer parquet files into external systems. Query your data with SQL, send only what you need to Elasticsearch, Splunk, webhooks, or any custom destination.

## Overview

ByteFreezer stores all ingested data as Parquet files in S3/MinIO. The Connector reads those files using DuckDB and exports filtered subsets to external systems. Instead of sending everything to your SIEM, export only the 5% you need for active investigation.

```
packer --> parquet (S3/MinIO) --> [CONNECTOR] --> Elasticsearch / Splunk / webhook
```

## Modes

| Mode | Command | Description |
|------|---------|-------------|
| **interactive** | `--mode interactive` (default) | Web UI at `:8090` for exploring datasets and testing queries |
| **batch** | `--mode batch` | Run configured query once, send to destination, exit |
| **watch** | `--mode watch` | Poll for new data on a timer, continuously export |

## Quick Start

### Binary

```bash
go build -o bytefreezer-connector .
```

### Docker

```bash
docker pull ghcr.io/bytefreezer/bytefreezer-connector:latest
docker run -p 8090:8090 -v ./config.yaml:/app/config.yaml:ro ghcr.io/bytefreezer/bytefreezer-connector:latest
```

### Configure

Edit `config.yaml` with your control API credentials:

```yaml
control:
  url: "https://api.bytefreezer.com"
  api_key: "your-service-key"
  account_id: "your-account-id"
```

For batch/watch modes, also set:

```yaml
query:
  tenant_id: "your-tenant-id"
  dataset_id: "your-dataset-id"
  sql: >
    SELECT timestamp, source_ip, message
    FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true)
    WHERE severity >= 4

destination:
  type: elasticsearch
  config:
    url: "http://localhost:9200"
    index: "security-alerts"
```

### Run

```bash
# Interactive mode — open http://localhost:8090
./bytefreezer-connector --config config.yaml

# Batch export to stdout
./bytefreezer-connector --config config.yaml --mode batch

# Continuous watch mode
./bytefreezer-connector --config config.yaml --mode watch

# Re-export from beginning (reset cursor)
./bytefreezer-connector --config config.yaml --mode batch --reset-cursor
```

## SQL Queries

Use `PARQUET_PATH` as placeholder. The connector replaces it with the S3 glob path for your dataset.

```sql
-- All records
SELECT * FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true)
LIMIT 100

-- Filter by time partition
SELECT * FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true)
WHERE year = 2026 AND month = 3

-- Specific fields only
SELECT timestamp, source_ip, message
FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true)
WHERE severity >= 4
```

## Built-in Destinations

| Destination | Config Key | Description |
|-------------|------------|-------------|
| `stdout` | — | JSON lines to stdout |
| `elasticsearch` | `url`, `index`, `username`, `password` | Elasticsearch bulk API |
| `webhook` | `url`, `method`, `headers` | HTTP POST to any endpoint |

## Adding a Destination

Create `destinations/your_dest.go`:

```go
package destinations

import (
    "context"
    "github.com/bytefreezer/connector/connector"
)

func init() {
    connector.RegisterDestination("your_dest", func() connector.Destination {
        return &YourDest{}
    })
}

type YourDest struct{}

func (d *YourDest) Name() string                                          { return "your_dest" }
func (d *YourDest) Init(config map[string]interface{}) error              { return nil }
func (d *YourDest) Send(ctx context.Context, batch connector.Batch) error { return nil }
func (d *YourDest) Close() error                                          { return nil }
```

The `init()` function auto-registers the destination. Set `destination.type: "your_dest"` in config.

## Project Structure

```
├── main.go                    # Entry point, HTTP routes, mode switching
├── ui.go                      # Embedded interactive web UI
├── config/config.go           # Config struct + koanf loader
├── connector/
│   ├── connector.go           # DuckDB engine, S3 config, query execution
│   ├── control_client.go      # Control API client (S3 creds, health reporting)
│   ├── cursor.go              # Cursor persistence (JSON file)
│   └── destination.go         # Destination interface + registry
├── destinations/
│   ├── stdout.go              # JSON lines to stdout
│   ├── elasticsearch.go       # Elasticsearch bulk API
│   └── webhook.go             # Generic HTTP POST
├── config.yaml                # Example configuration
├── Dockerfile                 # Docker image (debian:bookworm-slim)
└── CLAUDE.md                  # Claude Code instructions
```

## Health Reporting

In watch and interactive modes, the connector registers with the ByteFreezer control plane as `bytefreezer-connector` and reports health every 30 seconds. It appears on the Health page in the UI alongside proxy, receiver, piper, packer, and query.

## Documentation

- [Connector docs](https://docs.bytefreezer.com/connector/) — full documentation
- [CLAUDE.md](CLAUDE.md) — instructions for Claude Code + MCP tools reference
