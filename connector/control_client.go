package connector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
)

// ControlClient communicates with the ByteFreezer control API
type ControlClient struct {
	baseURL    string
	apiKey     string
	accountID  string
	httpClient *http.Client
}

// Tenant represents a tenant
type Tenant struct {
	ID        string `json:"id"`
	AccountID string `json:"account_id"`
	Name      string `json:"name"`
	Active    bool   `json:"active"`
}

// Dataset represents a dataset
type Dataset struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
	Name     string `json:"name"`
	Active   bool   `json:"active"`
}

// S3Credentials for accessing parquet files
type S3Credentials struct {
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	UseSSL    bool   `json:"use_ssl"`
	Path      string `json:"path"`
}

type tenantsResponse struct {
	Items []Tenant `json:"items"`
}

type datasetsResponse struct {
	Items []Dataset `json:"items"`
}

// NewControlClient creates a new control API client
func NewControlClient(baseURL, apiKey, accountID string) *ControlClient {
	return &ControlClient{
		baseURL:   baseURL,
		apiKey:    apiKey,
		accountID: accountID,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *ControlClient) doRequest(ctx context.Context, method, url string, body []byte) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return c.httpClient.Do(req)
}

// GetTenants returns all tenants for this account
func (c *ControlClient) GetTenants(ctx context.Context) ([]Tenant, error) {
	url := fmt.Sprintf("%s/api/v1/accounts/%s/tenants", c.baseURL, c.accountID)
	resp, err := c.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tenants: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("control API returned %d", resp.StatusCode)
	}

	var result tenantsResponse
	if err := sonic.ConfigDefault.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode: %w", err)
	}
	return result.Items, nil
}

// GetDatasets returns all datasets for a tenant
func (c *ControlClient) GetDatasets(ctx context.Context, tenantID string) ([]Dataset, error) {
	url := fmt.Sprintf("%s/api/v1/tenants/%s/datasets", c.baseURL, tenantID)
	resp, err := c.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch datasets: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("control API returned %d", resp.StatusCode)
	}

	var result datasetsResponse
	if err := sonic.ConfigDefault.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode: %w", err)
	}
	return result.Items, nil
}

// GetS3Credentials returns S3 credentials for accessing a dataset's parquet files
func (c *ControlClient) GetS3Credentials(ctx context.Context, tenantID, datasetID string) (*S3Credentials, error) {
	url := fmt.Sprintf("%s/api/v1/tenants/%s/datasets/%s/query-credentials", c.baseURL, tenantID, datasetID)
	resp, err := c.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch S3 credentials: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("control API returned %d", resp.StatusCode)
	}

	var creds S3Credentials
	if err := sonic.ConfigDefault.NewDecoder(resp.Body).Decode(&creds); err != nil {
		return nil, fmt.Errorf("failed to decode: %w", err)
	}
	return &creds, nil
}

// --- Health Reporting ---

type serviceRegistrationReq struct {
	ServiceType   string         `json:"service_type"`
	InstanceID    string         `json:"instance_id"`
	InstanceAPI   string         `json:"instance_api"`
	Status        string         `json:"status"`
	Configuration map[string]any `json:"configuration,omitempty"`
}

type healthReportReq struct {
	ServiceName string         `json:"service_name"`
	ServiceID   string         `json:"service_id"`
	InstanceAPI string         `json:"instance_api"`
	Healthy     bool           `json:"healthy"`
	Status      string         `json:"status"`
	Metrics     map[string]any `json:"metrics,omitempty"`
}

// RegisterService registers this connector with control plane
func (c *ControlClient) RegisterService(ctx context.Context, instanceID, instanceAPI string, config map[string]any) error {
	body, _ := sonic.Marshal(serviceRegistrationReq{
		ServiceType:   "bytefreezer-connector",
		InstanceID:    instanceID,
		InstanceAPI:   instanceAPI,
		Status:        "Starting",
		Configuration: config,
	})

	url := fmt.Sprintf("%s/api/v1/accounts/%s/services/register", c.baseURL, c.accountID)
	resp, err := c.doRequest(ctx, "POST", url, body)
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registration returned %d", resp.StatusCode)
	}

	log.Infof("Registered as bytefreezer-connector (instance: %s)", instanceID)
	return nil
}

// SendHealthReport sends a health report to control plane
func (c *ControlClient) SendHealthReport(ctx context.Context, instanceID, instanceAPI string, healthy bool, status string, metrics map[string]any) error {
	body, _ := sonic.Marshal(healthReportReq{
		ServiceName: "bytefreezer-connector",
		ServiceID:   instanceID,
		InstanceAPI: instanceAPI,
		Healthy:     healthy,
		Status:      status,
		Metrics:     metrics,
	})

	url := fmt.Sprintf("%s/api/v1/accounts/%s/services/report", c.baseURL, c.accountID)
	resp, err := c.doRequest(ctx, "POST", url, body)
	if err != nil {
		return fmt.Errorf("health report failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health report returned %d", resp.StatusCode)
	}

	log.Debug("Health report sent")
	return nil
}

// BuildInstanceID generates an instance ID for this connector
func BuildInstanceID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	hostHostname := os.Getenv("HOST_HOSTNAME")
	if hostHostname != "" {
		if _, err := os.Stat("/.dockerenv"); err == nil {
			return fmt.Sprintf("%s:%s", hostHostname, hostname)
		}
	}
	return hostname
}
