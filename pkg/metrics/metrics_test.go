package metrics

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthzHandler_AllHealthy(t *testing.T) {
	defaultHealthChecker.mu.Lock()
	defaultHealthChecker.checks = nil
	defaultHealthChecker.mu.Unlock()
	RegisterHealthCheck("test-ok", func() error { return nil })
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	HealthzHandler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var status HealthStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Status != "ok" {
		t.Fatalf("expected ok, got %q", status.Status)
	}
}

func TestHealthzHandler_Degraded(t *testing.T) {
	defaultHealthChecker.mu.Lock()
	defaultHealthChecker.checks = nil
	defaultHealthChecker.mu.Unlock()
	RegisterHealthCheck("healthy", func() error { return nil })
	RegisterHealthCheck("broken", func() error { return errors.New("db down") })
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	HealthzHandler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
	var status HealthStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Status != "degraded" {
		t.Fatalf("expected degraded, got %q", status.Status)
	}
}

func TestHealthzHandler_NoChecks(t *testing.T) {
	defaultHealthChecker.mu.Lock()
	defaultHealthChecker.checks = nil
	defaultHealthChecker.mu.Unlock()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	HealthzHandler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestMetricsCounters(t *testing.T) {
	CacheHits.Inc()
	CacheMisses.Inc()
	CacheEvictions.Inc()
	CacheSize.Set(1024)
	CacheUtilization.Set(0.5)
	BackendRequestDuration.WithLabelValues("test-be", "read").Observe(0.01)
	BackendErrors.WithLabelValues("test-be", "timeout").Inc()
	BackendBytesRead.WithLabelValues("test-be").Add(4096)
	FUSEOperations.WithLabelValues("read").Inc()
	FUSELatency.WithLabelValues("read").Observe(0.001)
	ReadaheadHits.Inc()
	ReadaheadWasted.Inc()
	AuthRefreshes.WithLabelValues("managed_identity", "success").Inc()
}

func TestRegisterHealthCheck_Concurrent(t *testing.T) {
	defaultHealthChecker.mu.Lock()
	defaultHealthChecker.checks = nil
	defaultHealthChecker.mu.Unlock()
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			RegisterHealthCheck("test", func() error { return nil })
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	status := runChecks()
	if status.Status != "ok" {
		t.Fatalf("expected ok, got %s", status.Status)
	}
}
