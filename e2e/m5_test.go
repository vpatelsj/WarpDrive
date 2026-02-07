package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/warpdrive/warpdrive/pkg/metrics"
)

// ─── M5.3 Monitoring & Alerting ────────────────────────────────────

func TestM5MetricsEndpoint(t *testing.T) {
	// Verify Prometheus /metrics endpoint serves all warpdrive_* metrics.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Generate some metric data.
	metrics.CacheHits.Inc()
	metrics.CacheMisses.Inc()
	metrics.CacheEvictions.Inc()
	metrics.CacheSize.Set(1024)
	metrics.CacheUtilization.Set(0.75)
	metrics.BackendRequestDuration.WithLabelValues("s3", "read").Observe(0.05)
	metrics.BackendErrors.WithLabelValues("s3", "timeout").Inc()
	metrics.BackendBytesRead.WithLabelValues("s3").Add(4096)
	metrics.FUSEOperations.WithLabelValues("read").Inc()
	metrics.FUSELatency.WithLabelValues("read").Observe(0.001)
	metrics.ReadaheadHits.Inc()
	metrics.ReadaheadWasted.Inc()
	metrics.AuthRefreshes.WithLabelValues("entra", "success").Inc()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body := make([]byte, 64*1024)
	n, _ := resp.Body.Read(body)
	text := string(body[:n])

	required := []string{
		"warpdrive_cache_hit_total",
		"warpdrive_cache_miss_total",
		"warpdrive_cache_evictions_total",
		"warpdrive_cache_size_bytes",
		"warpdrive_cache_utilization_ratio",
		"warpdrive_backend_request_duration_seconds",
		"warpdrive_backend_errors_total",
		"warpdrive_backend_bytes_read_total",
		"warpdrive_fuse_operations_total",
		"warpdrive_fuse_operation_duration_seconds",
		"warpdrive_readahead_hit_total",
		"warpdrive_readahead_wasted_total",
		"warpdrive_auth_refresh_total",
	}

	for _, name := range required {
		if !strings.Contains(text, name) {
			t.Errorf("missing metric %q in /metrics output", name)
		}
	}
}

func TestM5HealthzEndpoint(t *testing.T) {
	// Test healthz endpoint lifecycle: healthy -> degraded -> healthy.

	// Reset health checks.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", metrics.HealthzHandler)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// 1. No checks registered should be OK.
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	var status metrics.HealthStatus
	json.NewDecoder(resp.Body).Decode(&status)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("no-checks healthz: expected 200, got %d", resp.StatusCode)
	}

	// 2. Register a healthy check.
	metrics.RegisterHealthCheck("test-fuse", func() error { return nil })

	resp, err = http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&status)
	resp.Body.Close()
	if status.Status != "ok" {
		t.Errorf("healthy check: expected ok, got %q", status.Status)
	}

	// 3. Register a failing check.
	metrics.RegisterHealthCheck("test-db", func() error {
		return fmt.Errorf("connection refused")
	})

	resp, err = http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&status)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("degraded check: expected 503, got %d", resp.StatusCode)
	}
	if status.Status != "degraded" {
		t.Errorf("degraded check: expected degraded, got %q", status.Status)
	}
	if status.Checks["test-db"] != "connection refused" {
		t.Errorf("degraded check: expected error message, got %q", status.Checks["test-db"])
	}
}

func TestM5MetricsServerLifecycle(t *testing.T) {
	// Verify MetricsServer starts and stops gracefully.
	stop := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		errCh <- metrics.MetricsServer(":0", stop)
	}()

	// Let it start.
	time.Sleep(200 * time.Millisecond)

	// Stop.
	close(stop)

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("MetricsServer returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("MetricsServer did not shut down within 5s")
	}
}

func TestM5PrometheusRegistrations(t *testing.T) {
	// Verify all expected metric families are registered.
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}

	required := map[string]bool{
		"warpdrive_cache_hit_total":                  false,
		"warpdrive_cache_miss_total":                 false,
		"warpdrive_cache_evictions_total":            false,
		"warpdrive_cache_size_bytes":                 false,
		"warpdrive_cache_utilization_ratio":          false,
		"warpdrive_backend_request_duration_seconds": false,
		"warpdrive_backend_errors_total":             false,
		"warpdrive_backend_bytes_read_total":         false,
		"warpdrive_fuse_operations_total":            false,
		"warpdrive_fuse_operation_duration_seconds":  false,
		"warpdrive_readahead_hit_total":              false,
		"warpdrive_readahead_wasted_total":           false,
		"warpdrive_auth_refresh_total":               false,
	}

	for _, fam := range families {
		if _, exists := required[fam.GetName()]; exists {
			required[fam.GetName()] = true
		}
	}

	for name, found := range required {
		if !found {
			t.Errorf("metric family %q not registered with Prometheus", name)
		}
	}
}

// ─── M5.1 Kubernetes Deployment Artifacts ─────────────────────────

func TestM5HelmChartStructure(t *testing.T) {
	// Verify Helm chart directory structure exists.
	projectDir := findProjectDir(t)
	helmDir := filepath.Join(projectDir, "deploy", "helm", "warpdrive")

	requiredFiles := []string{
		"Chart.yaml",
		"values.yaml",
		"README.md",
		"templates/_helpers.tpl",
		"templates/daemonset.yaml",
		"templates/deployment.yaml",
		"templates/service.yaml",
		"templates/configmap.yaml",
		"templates/secret.yaml",
		"templates/serviceaccount.yaml",
		"templates/clusterrole.yaml",
		"templates/clusterrolebinding.yaml",
		"templates/pdb.yaml",
		"templates/NOTES.txt",
	}

	for _, f := range requiredFiles {
		path := filepath.Join(helmDir, f)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("missing Helm chart file: %s", f)
		}
	}
}

func TestM5HelmChartContent(t *testing.T) {
	// Verify critical Helm chart content.
	projectDir := findProjectDir(t)

	// DaemonSet should have privileged + mountPropagation.
	ds, err := os.ReadFile(filepath.Join(projectDir, "deploy", "helm", "warpdrive", "templates", "daemonset.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	dsStr := string(ds)
	for _, kw := range []string{"privileged", "Bidirectional", "SYS_ADMIN", "/dev/fuse", "mountPropagation"} {
		if !strings.Contains(dsStr, kw) {
			t.Errorf("daemonset.yaml missing keyword: %s", kw)
		}
	}

	// values.yaml should have nodeSelector for GPU.
	vals, err := os.ReadFile(filepath.Join(projectDir, "deploy", "helm", "warpdrive", "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	valsStr := string(vals)
	for _, kw := range []string{"nvidia.com/gpu", "mountPoint", "cachePath", "maxSize"} {
		if !strings.Contains(valsStr, kw) {
			t.Errorf("values.yaml missing keyword: %s", kw)
		}
	}
}

// ─── M5.2 Slurm Integration ─────────────────────────────────────

func TestM5SlurmDeploymentArtifacts(t *testing.T) {
	projectDir := findProjectDir(t)

	artifacts := []struct {
		path     string
		keywords []string
	}{
		{
			path:     "deploy/systemd/warpdrive-agent.service",
			keywords: []string{"ExecStart", "warpdrive-mount", "CAP_SYS_ADMIN", "LimitNOFILE"},
		},
		{
			path:     "deploy/slurm/warpdrive-prolog.sh",
			keywords: []string{"SLURM", "warpdrive", "warm", "mountpoint"},
		},
		{
			path:     "deploy/slurm/warpdrive-epilog.sh",
			keywords: []string{"SLURM", "warpdrive-ctl", "epilog"},
		},
		{
			path:     "deploy/slurm/ansible/install-warpdrive.yaml",
			keywords: []string{"warpdrive-mount", "systemd", "prolog", "epilog"},
		},
	}

	for _, a := range artifacts {
		fullPath := filepath.Join(projectDir, a.path)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			t.Errorf("missing artifact %s: %v", a.path, err)
			continue
		}
		content := string(data)
		for _, kw := range a.keywords {
			if !strings.Contains(content, kw) {
				t.Errorf("%s missing keyword: %s", a.path, kw)
			}
		}
	}
}

// ─── M5.3 Monitoring Artifacts ────────────────────────────────────

func TestM5MonitoringArtifacts(t *testing.T) {
	projectDir := findProjectDir(t)

	// Alerts YAML.
	alerts, err := os.ReadFile(filepath.Join(projectDir, "deploy", "monitoring", "alerts.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	alertsStr := string(alerts)
	for _, rule := range []string{"WarpDriveCacheHitRateLow", "WarpDriveBackendErrorRate", "WarpDriveCacheFull", "WarpDriveMountDown"} {
		if !strings.Contains(alertsStr, rule) {
			t.Errorf("alerts.yaml missing rule: %s", rule)
		}
	}

	// Grafana dashboard.
	dash, err := os.ReadFile(filepath.Join(projectDir, "deploy", "monitoring", "grafana-dashboard.json"))
	if err != nil {
		t.Fatal(err)
	}
	var dashMap map[string]interface{}
	if err := json.Unmarshal(dash, &dashMap); err != nil {
		t.Fatalf("grafana-dashboard.json is not valid JSON: %v", err)
	}
	dashStr := string(dash)
	for _, panel := range []string{"Cache Hit Rate", "Cache Size", "Backend Latency", "Backend Errors", "FUSE Throughput", "FUSE Operation Latency"} {
		if !strings.Contains(dashStr, panel) {
			t.Errorf("grafana-dashboard.json missing panel: %s", panel)
		}
	}
}

// ─── Helpers ──────────────────────────────────────────────────────

func findProjectDir(t *testing.T) string {
	t.Helper()
	// Walk up from the test file until we find go.mod.
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find project root (go.mod)")
		}
		dir = parent
	}
}
