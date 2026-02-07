package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Cache metrics
	CacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "warpdrive_cache_hit_total",
		Help: "Total cache hits",
	})
	CacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "warpdrive_cache_miss_total",
		Help: "Total cache misses",
	})
	CacheSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "warpdrive_cache_size_bytes",
		Help: "Current cache size in bytes",
	})
	CacheEvictions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "warpdrive_cache_evictions_total",
		Help: "Total cache evictions",
	})
	CacheUtilization = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "warpdrive_cache_utilization_ratio",
		Help: "Cache utilization (0-1)",
	})

	// Backend metrics
	BackendRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "warpdrive_backend_request_duration_seconds",
		Help:    "Backend request duration",
		Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5, 10, 30},
	}, []string{"backend", "operation"})

	BackendErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "warpdrive_backend_errors_total",
		Help: "Backend errors by type",
	}, []string{"backend", "error_type"})

	BackendBytesRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "warpdrive_backend_bytes_read_total",
		Help: "Total bytes read from backends",
	}, []string{"backend"})

	// FUSE metrics
	FUSEOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "warpdrive_fuse_operations_total",
		Help: "FUSE operations by type",
	}, []string{"operation"})

	FUSELatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "warpdrive_fuse_operation_duration_seconds",
		Help:    "FUSE operation latency",
		Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1},
	}, []string{"operation"})

	// Readahead metrics
	ReadaheadHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "warpdrive_readahead_hit_total",
		Help: "Readahead-prefetched blocks that were subsequently read",
	})
	ReadaheadWasted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "warpdrive_readahead_wasted_total",
		Help: "Readahead-prefetched blocks that were evicted before being read",
	})

	// Auth metrics
	AuthRefreshes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "warpdrive_auth_refresh_total",
		Help: "Credential refreshes",
	}, []string{"provider", "status"})
)

func init() {
	// Pre-initialize Vec metrics so they appear in /metrics output before first use.
	FUSEOperations.WithLabelValues("lookup")
	FUSEOperations.WithLabelValues("readdir")
	FUSEOperations.WithLabelValues("read")
	FUSELatency.WithLabelValues("read")
	BackendRequestDuration.WithLabelValues("", "read")
	BackendErrors.WithLabelValues("", "io")
	BackendBytesRead.WithLabelValues("")
	AuthRefreshes.WithLabelValues("", "success")
}

// HealthCheck holds a single health check function.
type HealthCheck struct {
	Name  string
	Check func() error
}

// HealthStatus represents the health response.
type HealthStatus struct {
	Status string            `json:"status"` // "ok" or "degraded"
	Checks map[string]string `json:"checks"`
}

// healthChecker holds registered health checks.
type healthChecker struct {
	mu     sync.RWMutex
	checks []HealthCheck
}

var defaultHealthChecker = &healthChecker{}

// RegisterHealthCheck adds a health check.
func RegisterHealthCheck(name string, check func() error) {
	defaultHealthChecker.mu.Lock()
	defer defaultHealthChecker.mu.Unlock()
	defaultHealthChecker.checks = append(defaultHealthChecker.checks, HealthCheck{
		Name:  name,
		Check: check,
	})
}

// runChecks runs all registered health checks.
func runChecks() HealthStatus {
	defaultHealthChecker.mu.RLock()
	checks := make([]HealthCheck, len(defaultHealthChecker.checks))
	copy(checks, defaultHealthChecker.checks)
	defaultHealthChecker.mu.RUnlock()

	status := HealthStatus{
		Status: "ok",
		Checks: make(map[string]string),
	}

	for _, hc := range checks {
		if err := hc.Check(); err != nil {
			status.Status = "degraded"
			status.Checks[hc.Name] = err.Error()
		} else {
			status.Checks[hc.Name] = "ok"
		}
	}
	return status
}

// HealthzHandler handles GET /healthz requests.
func HealthzHandler(w http.ResponseWriter, r *http.Request) {
	status := runChecks()
	w.Header().Set("Content-Type", "application/json")
	if status.Status != "ok" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(status)
}

// MountHealthCheck returns a check function for FUSE mount point existence.
func MountHealthCheck(mountPoint string) func() error {
	return func() error {
		cmd := exec.Command("mountpoint", "-q", mountPoint)
		if err := cmd.Run(); err != nil {
			return err
		}
		return nil
	}
}

// MetricsServer starts an HTTP server for /metrics and /healthz on the given addr.
// It blocks until the provided stop channel is closed, then shuts down gracefully.
func MetricsServer(addr string, stop <-chan struct{}) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", HealthzHandler)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-stop:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(ctx)
	case err := <-errCh:
		return err
	}
}
