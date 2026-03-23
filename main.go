package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/connector/config"
	"github.com/bytefreezer/connector/connector"
	"github.com/bytefreezer/goodies/log"

	// Register destination implementations
	_ "github.com/bytefreezer/connector/destinations"
)

var (
	version   = "dev"
	buildTime = "unknown"
)

func main() {
	var (
		cfgFile     = flag.String("config", "config.yaml", "Path to config file")
		mode        = flag.String("mode", "interactive", "Run mode: batch, watch, interactive")
		resetCursor = flag.Bool("reset-cursor", false, "Reset cursor and re-export from beginning")
		showVersion = flag.Bool("version", false, "Show version")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("bytefreezer-connector %s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	log.Infof("ByteFreezer Connector %s — mode: %s", version, *mode)

	// Load config
	var cfg config.Config
	if err := config.LoadConfig(*cfgFile, "BYTEFREEZER_CONNECTOR_", &cfg); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if err := cfg.Validate(*mode); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}

	setLogLevel(cfg.Logging.Level)
	cfg.LogSummary(*mode)

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Info("Shutdown signal received")
		cancel()
	}()

	// Create control client
	client := connector.NewControlClient(cfg.Control.URL, cfg.Control.APIKey, cfg.Control.AccountID)

	switch *mode {
	case "interactive":
		runInteractive(ctx, &cfg, client)
	case "batch":
		runBatch(ctx, &cfg, client, *resetCursor)
	case "watch":
		runWatch(ctx, &cfg, client, *resetCursor)
	default:
		log.Fatalf("Unknown mode: %s (use: batch, watch, interactive)", *mode)
	}
}

func runBatch(ctx context.Context, cfg *config.Config, client *connector.ControlClient, resetCursor bool) {
	cursor := connector.NewCursor(cfg.Cursor.File)
	if resetCursor {
		log.Info("Resetting cursor — will re-export all data")
		cursor.Reset()
	}

	dest, err := connector.GetDestination(cfg.Destination.Type)
	if err != nil {
		log.Fatalf("Failed to create destination: %v", err)
	}
	if err := dest.Init(cfg.Destination.Config); err != nil {
		log.Fatalf("Failed to init destination: %v", err)
	}

	conn, err := connector.NewConnector(client, cursor, dest, cfg.Query.TenantID, cfg.Query.DatasetID, cfg.Query.SQL, cfg.Schedule.BatchSize)
	if err != nil {
		log.Fatalf("Failed to create connector: %v", err)
	}
	defer conn.Close()

	if cfg.HasS3Override() {
		conn.SetS3Override(s3OverrideFromConfig(cfg))
	}

	if err := conn.RunOnce(ctx); err != nil {
		log.Fatalf("Export failed: %v", err)
	}
}

func runWatch(ctx context.Context, cfg *config.Config, client *connector.ControlClient, resetCursor bool) {
	cursor := connector.NewCursor(cfg.Cursor.File)
	if resetCursor {
		log.Info("Resetting cursor — will re-export all data")
		cursor.Reset()
	}

	dest, err := connector.GetDestination(cfg.Destination.Type)
	if err != nil {
		log.Fatalf("Failed to create destination: %v", err)
	}
	if err := dest.Init(cfg.Destination.Config); err != nil {
		log.Fatalf("Failed to init destination: %v", err)
	}

	conn, err := connector.NewConnector(client, cursor, dest, cfg.Query.TenantID, cfg.Query.DatasetID, cfg.Query.SQL, cfg.Schedule.BatchSize)
	if err != nil {
		log.Fatalf("Failed to create connector: %v", err)
	}
	defer conn.Close()

	if cfg.HasS3Override() {
		conn.SetS3Override(s3OverrideFromConfig(cfg))
	}

	// Register with control plane
	instanceID := connector.BuildInstanceID()
	client.RegisterService(ctx, instanceID, fmt.Sprintf("http://localhost:%d", cfg.Server.Port), map[string]any{
		"mode":        "watch",
		"version":     version,
		"destination": cfg.Destination.Type,
		"dataset_id":  cfg.Query.DatasetID,
	})

	// Start health reporting in background
	go healthReportingLoop(ctx, client, instanceID, cfg)

	interval := time.Duration(cfg.Schedule.IntervalSeconds) * time.Second
	if err := conn.RunWatch(ctx, interval); err != nil {
		log.Fatalf("Watch mode failed: %v", err)
	}
}

func runInteractive(ctx context.Context, cfg *config.Config, client *connector.ControlClient) {
	// Create a connector with no query (interactive mode)
	cursor := connector.NewCursor(cfg.Cursor.File)

	// Use stdout as default destination for interactive preview
	dest, _ := connector.GetDestination("stdout")
	dest.Init(nil)

	conn, err := connector.NewConnector(client, cursor, dest, cfg.Query.TenantID, cfg.Query.DatasetID, "", cfg.Schedule.BatchSize)
	if err != nil {
		log.Fatalf("Failed to create connector: %v", err)
	}
	defer conn.Close()

	if cfg.HasS3Override() {
		conn.SetS3Override(s3OverrideFromConfig(cfg))
	}

	// Register with control plane
	instanceID := connector.BuildInstanceID()
	client.RegisterService(ctx, instanceID, fmt.Sprintf("http://localhost:%d", cfg.Server.Port), map[string]any{
		"mode":    "interactive",
		"version": version,
	})

	// Start HTTP server for interactive UI
	mux := http.NewServeMux()
	setupInteractiveRoutes(mux, cfg, client, conn)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.Port),
		Handler: corsMiddleware(mux),
	}

	go func() {
		log.Infof("Interactive UI available at http://localhost:%d", cfg.Server.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Start health reporting in background
	go healthReportingLoop(ctx, client, instanceID, cfg)

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)
}

func setupInteractiveRoutes(mux *http.ServeMux, cfg *config.Config, client *connector.ControlClient, conn *connector.Connector) {
	// Health check
	mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})

	// List datasets
	mux.HandleFunc("GET /api/v1/datasets", func(w http.ResponseWriter, r *http.Request) {
		tenants, err := client.GetTenants(r.Context())
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}

		type datasetInfo struct {
			TenantID    string `json:"tenant_id"`
			TenantName  string `json:"tenant_name"`
			DatasetID   string `json:"dataset_id"`
			Name        string `json:"name"`
			ParquetPath string `json:"parquet_path"`
		}
		var all []datasetInfo

		bucket := conn.GetS3Bucket()
		for _, t := range tenants {
			datasets, err := client.GetDatasets(r.Context(), t.ID)
			if err != nil {
				continue
			}
			for _, d := range datasets {
				ppath := ""
				if bucket != "" {
					ppath = conn.BuildParquetPath(bucket, t.ID, d.ID)
				}
				all = append(all, datasetInfo{
					TenantID:    t.ID,
					TenantName:  t.Name,
					DatasetID:   d.ID,
					Name:        d.Name,
					ParquetPath: ppath,
				})
			}
		}

		writeJSON(w, 200, map[string]interface{}{"datasets": all})
	})

	// Resolve the optimal parquet path for a dataset (scoped to latest partition)
	mux.HandleFunc("GET /api/v1/parquet-path", func(w http.ResponseWriter, r *http.Request) {
		tenantID := r.URL.Query().Get("tenant_id")
		datasetID := r.URL.Query().Get("dataset_id")
		if tenantID == "" || datasetID == "" {
			writeJSON(w, 400, map[string]string{"error": "tenant_id and dataset_id are required"})
			return
		}

		if err := conn.ConfigureDataset(r.Context(), tenantID, datasetID); err != nil {
			writeJSON(w, 500, map[string]string{"error": "Failed to configure dataset: " + err.Error()})
			return
		}

		path, err := conn.GetParquetPath(r.Context())
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": "Failed to resolve path: " + err.Error()})
			return
		}

		writeJSON(w, 200, map[string]interface{}{"parquet_path": path})
	})

	// Execute SQL query (preview data)
	mux.HandleFunc("POST /api/v1/query", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TenantID  string `json:"tenant_id"`
			DatasetID string `json:"dataset_id"`
			SQL       string `json:"sql"`
		}
		if err := sonic.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid request"})
			return
		}

		if req.TenantID == "" || req.DatasetID == "" || req.SQL == "" {
			writeJSON(w, 400, map[string]string{"error": "tenant_id, dataset_id, and sql are required"})
			return
		}

		// Configure S3 for this dataset
		if err := conn.ConfigureDataset(r.Context(), req.TenantID, req.DatasetID); err != nil {
			writeJSON(w, 500, map[string]string{"error": "Failed to configure dataset: " + err.Error()})
			return
		}

		// Get parquet path and substitute
		parquetPath, err := conn.GetParquetPath(r.Context())
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": "Failed to get parquet path: " + err.Error()})
			return
		}
		finalSQL := strings.ReplaceAll(req.SQL, "PARQUET_PATH", parquetPath)

		start := time.Now()
		columns, records, err := conn.ExecuteQuery(r.Context(), finalSQL)
		elapsed := time.Since(start)

		if err != nil {
			writeJSON(w, 200, map[string]interface{}{
				"error":             err.Error(),
				"execution_time_ms": elapsed.Milliseconds(),
			})
			return
		}

		writeJSON(w, 200, map[string]interface{}{
			"columns":           columns,
			"rows":              records,
			"row_count":         len(records),
			"execution_time_ms": elapsed.Milliseconds(),
		})
	})

	// Test destination
	mux.HandleFunc("POST /api/v1/test-destination", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Type   string                 `json:"type"`
			Config map[string]interface{} `json:"config"`
		}
		if err := sonic.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid request"})
			return
		}

		dest, err := connector.GetDestination(req.Type)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if err := dest.Init(req.Config); err != nil {
			writeJSON(w, 400, map[string]string{"error": "init failed: " + err.Error()})
			return
		}
		defer dest.Close()

		// Send a test record
		testBatch := connector.Batch{
			Records: []connector.Record{
				{"_test": true, "_timestamp": time.Now().UTC().Format(time.RFC3339), "_source": "bytefreezer-connector"},
			},
		}
		if err := dest.Send(r.Context(), testBatch); err != nil {
			writeJSON(w, 200, map[string]string{"error": "send failed: " + err.Error()})
			return
		}

		writeJSON(w, 200, map[string]string{"status": "ok", "message": "Test record sent to " + req.Type})
	})

	// Export (run query and send to destination)
	mux.HandleFunc("POST /api/v1/export", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TenantID  string                 `json:"tenant_id"`
			DatasetID string                 `json:"dataset_id"`
			SQL       string                 `json:"sql"`
			DestType  string                 `json:"destination_type"`
			DestCfg   map[string]interface{} `json:"destination_config"`
		}
		if err := sonic.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid request"})
			return
		}

		// Get or create destination
		destType := req.DestType
		if destType == "" {
			destType = cfg.Destination.Type
		}
		dest, err := connector.GetDestination(destType)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		destCfg := req.DestCfg
		if destCfg == nil {
			destCfg = cfg.Destination.Config
		}
		if err := dest.Init(destCfg); err != nil {
			writeJSON(w, 400, map[string]string{"error": "destination init failed: " + err.Error()})
			return
		}
		defer dest.Close()

		// Create temporary connector for this export
		cursor := connector.NewCursor("")
		exportConn, err := connector.NewConnector(client, cursor, dest, req.TenantID, req.DatasetID, req.SQL, cfg.Schedule.BatchSize)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": "connector init failed: " + err.Error()})
			return
		}
		defer exportConn.Close()

		start := time.Now()
		if err := exportConn.RunOnce(r.Context()); err != nil {
			writeJSON(w, 200, map[string]interface{}{
				"error":             err.Error(),
				"execution_time_ms": time.Since(start).Milliseconds(),
			})
			return
		}

		writeJSON(w, 200, map[string]interface{}{
			"status":            "ok",
			"destination":       destType,
			"execution_time_ms": time.Since(start).Milliseconds(),
		})
	})

	// Serve static UI
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && !strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(interactiveHTML))
	})
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	sonic.ConfigDefault.NewEncoder(w).Encode(data)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func healthReportingLoop(ctx context.Context, client *connector.ControlClient, instanceID string, cfg *config.Config) {
	if !cfg.HealthReporting.Enabled {
		return
	}

	ticker := time.NewTicker(time.Duration(cfg.HealthReporting.ReportInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics := map[string]any{
				"timestamp":         time.Now().Unix(),
				"last_health_check": time.Now().UTC().Format(time.RFC3339),
			}
			client.SendHealthReport(ctx, instanceID, fmt.Sprintf("http://localhost:%d", cfg.Server.Port), true, "healthy", metrics)
		}
	}
}

func s3OverrideFromConfig(cfg *config.Config) *connector.S3Credentials {
	return &connector.S3Credentials{
		Endpoint:  cfg.S3.Endpoint,
		Bucket:    cfg.S3.Bucket,
		Region:    cfg.S3.Region,
		AccessKey: cfg.S3.AccessKey,
		SecretKey: cfg.S3.SecretKey,
		UseSSL:    cfg.S3.UseSSL,
	}
}

func setLogLevel(levelStr string) {
	switch strings.ToLower(levelStr) {
	case "debug":
		log.SetMinLogLevel(log.MinLevelDebug)
	case "info":
		log.SetMinLogLevel(log.MinLevelInfo)
	case "warn":
		log.SetMinLogLevel(log.MinLevelWarn)
	case "error":
		log.SetMinLogLevel(log.MinLevelError)
	}
}
