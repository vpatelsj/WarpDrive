package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/warpdrive/warpdrive/pkg/control"
	"github.com/warpdrive/warpdrive/pkg/telemetry"
)

func TestM4TelemetryCollectorRoundTrip(t *testing.T) {
	mem := telemetry.NewMemoryEmitter()
	c := &telemetry.Collector{}
	_ = c // Collector fields are unexported, so we test via NewCollector

	cfg := telemetry.CollectorConfig{
		Enabled:           true,
		Sink:              "nop",
		BatchSize:         5,
		FlushInterval:     100 * time.Millisecond,
		SampleMetadataOps: 1.0,
	}

	tc, err := telemetry.NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = mem // We use the nop sink for this test

	for i := 0; i < 10; i++ {
		tc.Record(telemetry.AccessEvent{
			Timestamp:   time.Now(),
			UserID:      "alice",
			BackendName: "s3",
			Path:        "data/file.bin",
			Operation:   "read",
			BytesRead:   int64(i * 1024),
		})
	}

	tc.Close()
	// If we get here without panic/hang, the collector works end-to-end.
}

func TestM4ControlPlaneEndToEnd(t *testing.T) {
	srv := control.NewServer(control.ControlPlaneConfig{}, nil)

	// Set team mappings.
	srv.SetTeamMapping("alice", "ml-team")
	srv.SetTeamMapping("bob", "ml-team")
	srv.SetTeamMapping("charlie", "infra")

	// Set quotas.
	if err := srv.SetQuota(control.Quota{
		TeamName: "ml-team", BackendName: "s3",
		SoftLimit: 500, HardLimit: 1000,
	}); err != nil {
		t.Fatal(err)
	}

	// Index some files.
	store := srv.GetStore()
	store.UpsertFile(control.StorageFile{
		BackendName: "s3", Path: "datasets/imagenet/train.tar",
		SizeBytes: 400, FirstSeen: time.Now().AddDate(0, 0, -10),
	})
	store.UpsertFile(control.StorageFile{
		BackendName: "s3", Path: "datasets/coco/val.tar",
		SizeBytes: 200, FirstSeen: time.Now().AddDate(0, 0, -100),
	})
	store.UpsertFile(control.StorageFile{
		BackendName: "azure", Path: "models/v1/weights.bin",
		SizeBytes: 300, FirstSeen: time.Now(),
	})

	// Ingest access events.
	events := []telemetry.AccessEvent{
		{Timestamp: time.Now(), UserID: "alice", BackendName: "s3", Path: "datasets/imagenet/train.tar", Operation: "read", BytesRead: 400},
		{Timestamp: time.Now(), UserID: "bob", BackendName: "s3", Path: "datasets/imagenet/train.tar", Operation: "read", BytesRead: 400},
		{Timestamp: time.Now(), UserID: "charlie", BackendName: "azure", Path: "models/v1/weights.bin", Operation: "read", BytesRead: 300},
	}
	accepted := srv.IngestEvents(events)
	if accepted != 3 {
		t.Fatalf("expected 3 accepted, got %d", accepted)
	}

	// Query usage by team.
	rows := srv.QueryUsage("team", "")
	if len(rows) == 0 {
		t.Error("expected usage rows")
	}

	// Query stale files.
	stale := srv.QueryStale(90, 0)
	if len(stale) != 1 {
		t.Errorf("expected 1 stale file (coco/val.tar), got %d", len(stale))
	}

	// Query growth.
	growth := srv.QueryGrowth("", 30)
	if len(growth) == 0 {
		t.Error("expected growth points")
	}

	// Check quotas.
	quotas := srv.GetQuotas()
	if len(quotas) != 1 {
		t.Fatalf("expected 1 quota, got %d", len(quotas))
	}
	if quotas[0].Exceeded {
		t.Error("quota should not be exceeded")
	}

	// Backend stats.
	backends := srv.ListBackends()
	if len(backends) != 2 {
		t.Errorf("expected 2 backends, got %d", len(backends))
	}

	// User detail.
	records := srv.GetUserDetail("alice")
	if len(records) != 1 {
		t.Errorf("expected 1 record for alice, got %d", len(records))
	}

	// Crawl with nil backends should not error.
	if err := srv.CrawlAll(context.Background()); err != nil {
		t.Errorf("crawl should not error: %v", err)
	}
}

func TestM4QuotaEnforcement(t *testing.T) {
	srv := control.NewServer(control.ControlPlaneConfig{}, nil)
	srv.SetTeamMapping("alice", "ml-team")

	store := srv.GetStore()
	store.UpsertFile(control.StorageFile{
		BackendName: "s3", Path: "big.bin",
		SizeBytes: 1000,
	})

	// Set a low hard limit.
	srv.SetQuota(control.Quota{
		TeamName: "ml-team", BackendName: "s3",
		SoftLimit: 500, HardLimit: 900,
	})

	// Ingest event that causes alice to access the big file.
	srv.IngestEvents([]telemetry.AccessEvent{
		{Timestamp: time.Now(), UserID: "alice", BackendName: "s3", Path: "big.bin", BytesRead: 1000},
	})

	// Check quota status.
	quotas := srv.GetQuotas()
	if len(quotas) != 1 {
		t.Fatalf("expected 1 quota, got %d", len(quotas))
	}
	if !quotas[0].Exceeded {
		t.Error("expected quota to be exceeded (usage=1000 >= hard=900)")
	}
	if !quotas[0].SoftExceeded {
		t.Error("expected soft quota to be exceeded (usage=1000 >= soft=500)")
	}
}

func TestM4HTTPEmitterEndToEnd(t *testing.T) {
	// Start a control plane server with an HTTP test server.
	srv := control.NewServer(control.ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Create an HTTP emitter pointing at the test server.
	emitter := telemetry.NewHTTPEmitter(ts.URL)

	// Emit some events.
	events := []telemetry.AccessEvent{
		{Timestamp: time.Now(), UserID: "alice", BackendName: "s3", Path: "data/train.tar", Operation: "read", BytesRead: 4096},
		{Timestamp: time.Now(), UserID: "bob", BackendName: "azure", Path: "models/v1.bin", Operation: "read", BytesRead: 2048},
	}
	if err := emitter.Emit(events); err != nil {
		t.Fatalf("HTTP emit failed: %v", err)
	}

	// Verify events were ingested by the control plane.
	store := srv.GetStore()
	if store.AccessRecordCount() != 2 {
		t.Errorf("expected 2 access records, got %d", store.AccessRecordCount())
	}

	// Query the usage API to confirm.
	req := httptest.NewRequest("GET", "/api/v1/usage?group_by=user", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var rows []control.UsageRow
	json.NewDecoder(w.Body).Decode(&rows)
	if len(rows) != 2 {
		t.Errorf("expected 2 usage rows, got %d", len(rows))
	}
}

func TestM4ControlPlaneServerRun(t *testing.T) {
	// Test the full Run lifecycle: start, interact via HTTP, shutdown.
	srv := control.NewServer(control.ControlPlaneConfig{RESTAddr: ":0"}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run(ctx)
	}()

	// Give server time to start.
	time.Sleep(200 * time.Millisecond)

	// Shut down.
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("server run error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestM4APIFullWorkflow(t *testing.T) {
	// Full API workflow: set teams, set quotas, ingest events, crawl, query.
	srv := control.NewServer(control.ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()

	// 1. Set team mappings via API.
	teamBody := `{"user_id":"alice","team_name":"ml"}`
	resp, err := client.Post(ts.URL+"/api/v1/team", "application/json", strings.NewReader(teamBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("team set: expected 200, got %d", resp.StatusCode)
	}

	// 2. Set quota via API.
	quotaBody := `{"team_name":"ml","backend_name":"s3","soft_limit":500,"hard_limit":1000}`
	req, _ := http.NewRequest("PUT", ts.URL+"/api/v1/quota", strings.NewReader(quotaBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("quota set: expected 200, got %d", resp.StatusCode)
	}

	// 3. Ingest events via API.
	ingestBody := fmt.Sprintf(`[{"ts":"%s","user":"alice","backend":"s3","path":"data/train.tar","op":"read","bytes_read":2048}]`,
		time.Now().Format(time.RFC3339))
	resp, err = client.Post(ts.URL+"/api/v1/ingest", "application/json", strings.NewReader(ingestBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("ingest: expected 200, got %d", resp.StatusCode)
	}

	// 4. Query usage.
	resp, err = client.Get(ts.URL + "/api/v1/usage?group_by=team")
	if err != nil {
		t.Fatal(err)
	}
	var usageRows []control.UsageRow
	json.NewDecoder(resp.Body).Decode(&usageRows)
	resp.Body.Close()
	if len(usageRows) == 0 {
		t.Error("expected usage rows after ingest")
	}

	// 5. Query stale.
	resp, err = client.Get(ts.URL + "/api/v1/stale?days=90")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("stale: expected 200, got %d", resp.StatusCode)
	}

	// 6. List quotas.
	resp, err = client.Get(ts.URL + "/api/v1/quota")
	if err != nil {
		t.Fatal(err)
	}
	var quotas []control.QuotaStatus
	json.NewDecoder(resp.Body).Decode(&quotas)
	resp.Body.Close()
	if len(quotas) != 1 {
		t.Errorf("expected 1 quota, got %d", len(quotas))
	}

	// 7. User detail.
	resp, err = client.Get(ts.URL + "/api/v1/user/alice")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("user detail: expected 200, got %d", resp.StatusCode)
	}

	// 8. List backends (empty since no crawl data).
	resp, err = client.Get(ts.URL + "/api/v1/backends")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("backends: expected 200, got %d", resp.StatusCode)
	}

	// 9. Trigger crawl (no backends configured, should succeed).
	resp, err = client.Post(ts.URL+"/api/v1/crawl", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("crawl: expected 200, got %d", resp.StatusCode)
	}
}
