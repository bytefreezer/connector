package destinations

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/connector/connector"
	"github.com/bytefreezer/goodies/log"
)

func init() {
	connector.RegisterDestination("elasticsearch", func() connector.Destination {
		return &ElasticsearchDestination{}
	})
}

// ElasticsearchDestination sends records to Elasticsearch using the bulk API
type ElasticsearchDestination struct {
	url      string // e.g. "http://localhost:9200"
	index    string // e.g. "bytefreezer-logs"
	username string
	password string
	client   *http.Client
}

func (d *ElasticsearchDestination) Name() string { return "elasticsearch" }

func (d *ElasticsearchDestination) Init(config map[string]interface{}) error {
	url, _ := config["url"].(string)
	if url == "" {
		return fmt.Errorf("elasticsearch: url is required")
	}
	d.url = url

	index, _ := config["index"].(string)
	if index == "" {
		return fmt.Errorf("elasticsearch: index is required")
	}
	d.index = index

	d.username, _ = config["username"].(string)
	d.password, _ = config["password"].(string)

	d.client = &http.Client{Timeout: 30 * time.Second}

	log.Infof("Elasticsearch destination: %s/%s", d.url, d.index)
	return nil
}

func (d *ElasticsearchDestination) Send(ctx context.Context, batch connector.Batch) error {
	if len(batch.Records) == 0 {
		return nil
	}

	// Build bulk request body (NDJSON)
	var buf bytes.Buffer
	for _, record := range batch.Records {
		// Action line
		action := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": d.index,
			},
		}
		actionBytes, _ := sonic.Marshal(action)
		buf.Write(actionBytes)
		buf.WriteByte('\n')

		// Document line
		docBytes, err := sonic.Marshal(record)
		if err != nil {
			return fmt.Errorf("failed to marshal record: %w", err)
		}
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	// Send bulk request
	url := fmt.Sprintf("%s/_bulk", d.url)
	req, err := http.NewRequestWithContext(ctx, "POST", url, &buf)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-ndjson")
	if d.username != "" {
		req.SetBasicAuth(d.username, d.password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("bulk request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("elasticsearch returned %d: %s", resp.StatusCode, string(body))
	}

	log.Debugf("Elasticsearch: indexed %d records to %s", len(batch.Records), d.index)
	return nil
}

func (d *ElasticsearchDestination) Close() error { return nil }
