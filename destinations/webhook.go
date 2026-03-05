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
	connector.RegisterDestination("webhook", func() connector.Destination {
		return &WebhookDestination{}
	})
}

// WebhookDestination sends batches as JSON POST to a URL
type WebhookDestination struct {
	url     string
	headers map[string]string
	client  *http.Client
}

func (d *WebhookDestination) Name() string { return "webhook" }

func (d *WebhookDestination) Init(config map[string]interface{}) error {
	url, _ := config["url"].(string)
	if url == "" {
		return fmt.Errorf("webhook: url is required")
	}
	d.url = url

	d.headers = make(map[string]string)
	if headers, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range headers {
			if s, ok := v.(string); ok {
				d.headers[k] = s
			}
		}
	}

	d.client = &http.Client{Timeout: 30 * time.Second}

	log.Infof("Webhook destination: %s", d.url)
	return nil
}

func (d *WebhookDestination) Send(ctx context.Context, batch connector.Batch) error {
	if len(batch.Records) == 0 {
		return nil
	}

	payload := map[string]interface{}{
		"records": batch.Records,
		"count":   len(batch.Records),
	}

	body, err := sonic.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", d.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range d.headers {
		req.Header.Set(k, v)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("webhook returned %d: %s", resp.StatusCode, string(respBody))
	}

	log.Debugf("Webhook: sent %d records to %s", len(batch.Records), d.url)
	return nil
}

func (d *WebhookDestination) Close() error { return nil }
