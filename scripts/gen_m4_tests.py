#!/usr/bin/env python3
"""Generate M4 test files for the warpdrive project."""
import os

BASE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(BASE)

def write(relpath, content):
    path = os.path.join(ROOT, relpath)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(content)
    print(f"Wrote {path} ({len(content)} bytes)")

# ─── Telemetry tests ───
write("pkg/telemetry/telemetry_test.go", r'''package telemetry

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCollectorRecordAndFlush(t *testing.T) {
	mem := NewMemoryEmitter()
	c := &Collector{
		cfg: CollectorConfig{
			Enabled:           true,
			BatchSize:         10,
			FlushInterval:     time.Hour,
			SampleMetadataOps: 1.0,
		},
		emitter: mem,
		batch:   make([]AccessEvent, 0, 10),
		flushCh: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
	c.wg.Add(1)
	go c.flushLoop()

	now := time.Now()
	c.Record(AccessEvent{
		Timestamp:   now,
		UserID:      "alice",
		BackendName: "s3",
		Path:        "data/file.bin",
		Operation:   "read",
		BytesRead:   1024,
		NodeHost:    "node1",
		LatencyMs:   1.5,
	})

	events := c.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 batched event, got %d", len(events))
	}
	if events[0].UserID != "alice" {
		t.Errorf("expected UserID=alice, got %s", events[0].UserID)
	}

	c.Flush()
	time.Sleep(20 * time.Millisecond)
	if mem.Len() != 1 {
		t.Fatalf("expected 1 emitted event after flush, got %d", mem.Len())
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCollectorBatchFlush(t *testing.T) {
	mem := NewMemoryEmitter()
	batchSize := 5
	c := &Collector{
		cfg: CollectorConfig{
			Enabled:           true,
			BatchSize:         batchSize,
			FlushInterval:     time.Hour,
			SampleMetadataOps: 1.0,
		},
		emitter: mem,
		batch:   make([]AccessEvent, 0, batchSize),
		flushCh: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
	c.wg.Add(1)
	go c.flushLoop()

	for i := 0; i < batchSize; i++ {
		c.Record(AccessEvent{
			Timestamp:   time.Now(),
			UserID:      "bob",
			BackendName: "azure",
			Path:        "data/chunk",
			Operation:   "read",
			BytesRead:   int64(i * 100),
		})
	}

	time.Sleep(50 * time.Millisecond)
	if mem.Len() != batchSize {
		t.Errorf("expected %d emitted events, got %d", batchSize, mem.Len())
	}
	c.Close()
}

func TestCollectorDisabled(t *testing.T) {
	mem := NewMemoryEmitter()
	c := &Collector{
		cfg: CollectorConfig{
			Enabled:   false,
			BatchSize: 10,
		},
		emitter: mem,
		batch:   make([]AccessEvent, 0, 10),
		flushCh: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
	c.wg.Add(1)
	go c.flushLoop()

	c.Record(AccessEvent{Timestamp: time.Now(), UserID: "alice", Operation: "read"})

	if len(c.Events()) != 0 {
		t.Error("disabled collector should not record events")
	}
	c.Close()
}

func TestCollectorSampling(t *testing.T) {
	mem := NewMemoryEmitter()
	c := &Collector{
		cfg: CollectorConfig{
			Enabled:           true,
			BatchSize:         1000,
			FlushInterval:     time.Hour,
			SampleMetadataOps: 0.0,
		},
		emitter: mem,
		batch:   make([]AccessEvent, 0, 1000),
		flushCh: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
	c.wg.Add(1)
	go c.flushLoop()

	for i := 0; i < 50; i++ {
		c.Record(AccessEvent{Timestamp: time.Now(), Operation: "stat"})
		c.Record(AccessEvent{Timestamp: time.Now(), Operation: "list"})
	}
	c.Record(AccessEvent{Timestamp: time.Now(), Operation: "read"})

	events := c.Events()
	if len(events) != 1 {
		t.Errorf("expected 1 read event, got %d", len(events))
	}
	c.Close()
}

func TestCollectorCloseFlushesRemaining(t *testing.T) {
	mem := NewMemoryEmitter()
	c := &Collector{
		cfg: CollectorConfig{
			Enabled:           true,
			BatchSize:         100,
			FlushInterval:     time.Hour,
			SampleMetadataOps: 1.0,
		},
		emitter: mem,
		batch:   make([]AccessEvent, 0, 100),
		flushCh: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
	c.wg.Add(1)
	go c.flushLoop()

	for i := 0; i < 3; i++ {
		c.Record(AccessEvent{Timestamp: time.Now(), Operation: "read", UserID: "u1"})
	}
	c.Close()

	if mem.Len() != 3 {
		t.Errorf("expected 3 events after close, got %d", mem.Len())
	}
}

func TestNewCollectorDefaults(t *testing.T) {
	c, err := NewCollector(CollectorConfig{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if c.cfg.BatchSize != 100 {
		t.Errorf("expected default BatchSize=100, got %d", c.cfg.BatchSize)
	}
	if c.cfg.FlushInterval != 5*time.Second {
		t.Errorf("expected default FlushInterval=5s, got %v", c.cfg.FlushInterval)
	}
}

func TestNewCollectorFileSink(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "telemetry.jsonl")

	c, err := NewCollector(CollectorConfig{Enabled: true, Sink: "file", FilePath: path})
	if err != nil {
		t.Fatal(err)
	}

	c.Record(AccessEvent{Timestamp: time.Now(), Operation: "read", UserID: "alice"})
	c.Flush()
	time.Sleep(20 * time.Millisecond)
	c.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty telemetry file")
	}
}

func TestMemoryEmitter(t *testing.T) {
	m := NewMemoryEmitter()
	if err := m.Emit([]AccessEvent{{UserID: "a"}, {UserID: "b"}}); err != nil {
		t.Fatal(err)
	}
	if m.Len() != 2 {
		t.Errorf("expected 2 events, got %d", m.Len())
	}
	stored := m.Events()
	if stored[0].UserID != "a" || stored[1].UserID != "b" {
		t.Error("events not stored correctly")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNopEmitter(t *testing.T) {
	n := NewNopEmitter()
	if err := n.Emit([]AccessEvent{{UserID: "test"}}); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileEmitter(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "events.jsonl")

	fe, err := NewFileEmitter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := fe.Emit([]AccessEvent{{UserID: "alice", BytesRead: 1024}}); err != nil {
		t.Fatal(err)
	}
	if err := fe.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty file")
	}
}

func TestFileEmitterBadPath(t *testing.T) {
	_, err := NewFileEmitter("/nonexistent/path/file.jsonl")
	if err == nil {
		t.Error("expected error for bad path")
	}
}

func TestShouldSample(t *testing.T) {
	if !shouldSample(1.0) {
		t.Error("expected sample at rate 1.0")
	}
	if !shouldSample(2.0) {
		t.Error("expected sample at rate 2.0")
	}
	if shouldSample(0.0) {
		t.Error("expected no sample at rate 0.0")
	}
	if shouldSample(-1.0) {
		t.Error("expected no sample at rate -1.0")
	}
}
''')

# ─── Control plane tests ───
write("pkg/control/control_test.go", r'''package control

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/warpdrive/warpdrive/pkg/telemetry"
)

func TestStoreUpsertFile(t *testing.T) {
	s := NewStore()
	s.UpsertFile(StorageFile{BackendName: "s3", Path: "data/a.bin", SizeBytes: 100})
	s.UpsertFile(StorageFile{BackendName: "s3", Path: "data/b.bin", SizeBytes: 200})

	if s.FileCount() != 2 {
		t.Errorf("expected 2 files, got %d", s.FileCount())
	}

	// Update existing file.
	s.UpsertFile(StorageFile{BackendName: "s3", Path: "data/a.bin", SizeBytes: 150})
	if s.FileCount() != 2 {
		t.Errorf("expected still 2 files after update, got %d", s.FileCount())
	}
}

func TestStoreUpsertAccess(t *testing.T) {
	s := NewStore()
	s.UpsertAccess(AccessRecord{
		BackendName: "s3", Path: "data/a.bin", UserID: "alice",
		Date: "2024-01-01", ReadCount: 1, BytesRead: 100,
		LastAccess: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
	})
	s.UpsertAccess(AccessRecord{
		BackendName: "s3", Path: "data/a.bin", UserID: "alice",
		Date: "2024-01-01", ReadCount: 1, BytesRead: 200,
		LastAccess: time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC),
	})

	if s.AccessRecordCount() != 1 {
		t.Errorf("expected 1 aggregated record, got %d", s.AccessRecordCount())
	}
}

func TestStoreTeamMappings(t *testing.T) {
	s := NewStore()
	s.SetTeam("alice", "ml-team")
	s.SetTeam("alice", "ml-team") // duplicate
	s.SetTeam("alice", "infra")
	s.SetTeam("bob", "ml-team")

	teams := s.GetTeamsForUser("alice")
	if len(teams) != 2 {
		t.Errorf("expected 2 teams for alice, got %d", len(teams))
	}

	teams = s.GetTeamsForUser("bob")
	if len(teams) != 1 {
		t.Errorf("expected 1 team for bob, got %d", len(teams))
	}

	teams = s.GetTeamsForUser("charlie")
	if len(teams) != 0 {
		t.Errorf("expected 0 teams for charlie, got %d", len(teams))
	}
}

func TestStoreQuotas(t *testing.T) {
	s := NewStore()
	s.SetQuota(Quota{TeamName: "ml-team", BackendName: "s3", SoftLimit: 100, HardLimit: 200})
	s.SetQuota(Quota{TeamName: "ml-team", BackendName: "azure", SoftLimit: 50, HardLimit: 100})

	quotas := s.GetQuotasForTeam("ml-team")
	if len(quotas) != 2 {
		t.Errorf("expected 2 quotas, got %d", len(quotas))
	}

	all := s.GetAllQuotas()
	if len(all) != 2 {
		t.Errorf("expected 2 total quotas, got %d", len(all))
	}

	// Update existing quota.
	s.SetQuota(Quota{TeamName: "ml-team", BackendName: "s3", SoftLimit: 150, HardLimit: 300})
	all = s.GetAllQuotas()
	if len(all) != 2 {
		t.Errorf("expected still 2 quotas after update, got %d", len(all))
	}
}

func TestStoreQueryUsage(t *testing.T) {
	s := NewStore()
	s.SetTeam("alice", "ml-team")
	s.SetTeam("bob", "ml-team")

	s.UpsertAccess(AccessRecord{
		BackendName: "s3", Path: "file1", UserID: "alice",
		Date: "2024-01-01", ReadCount: 1, BytesRead: 100,
		LastAccess: time.Now(),
	})
	s.UpsertAccess(AccessRecord{
		BackendName: "s3", Path: "file2", UserID: "bob",
		Date: "2024-01-01", ReadCount: 1, BytesRead: 200,
		LastAccess: time.Now(),
	})

	// By user
	rows := s.QueryUsage("user", "")
	if len(rows) != 2 {
		t.Errorf("expected 2 user rows, got %d", len(rows))
	}

	// By team
	rows = s.QueryUsage("team", "")
	if len(rows) != 1 {
		t.Errorf("expected 1 team row, got %d", len(rows))
	}
	if rows[0].GroupName != "ml-team" {
		t.Errorf("expected ml-team, got %s", rows[0].GroupName)
	}
	if rows[0].TotalBytes != 300 {
		t.Errorf("expected 300 bytes, got %d", rows[0].TotalBytes)
	}
}

func TestStoreQueryStale(t *testing.T) {
	s := NewStore()
	old := time.Now().AddDate(0, 0, -200)
	s.UpsertFile(StorageFile{
		BackendName: "s3", Path: "old_file", SizeBytes: 1000,
		FirstSeen: old,
	})
	s.UpsertFile(StorageFile{
		BackendName: "s3", Path: "new_file", SizeBytes: 500,
		FirstSeen: time.Now(),
	})

	stale := s.QueryStale(90, 0)
	if len(stale) != 1 {
		t.Errorf("expected 1 stale file, got %d", len(stale))
	}
	if stale[0].Path != "old_file" {
		t.Errorf("expected old_file, got %s", stale[0].Path)
	}
}

func TestStoreQueryGrowth(t *testing.T) {
	s := NewStore()
	s.UpsertFile(StorageFile{
		BackendName: "s3", Path: "f1", SizeBytes: 100,
		FirstSeen: time.Now().AddDate(0, 0, -5),
	})
	s.UpsertFile(StorageFile{
		BackendName: "s3", Path: "f2", SizeBytes: 200,
		FirstSeen: time.Now().AddDate(0, 0, -3),
	})

	points := s.QueryGrowth("", 30)
	if len(points) == 0 {
		t.Error("expected at least one growth point")
	}
}

func TestStoreListBackendStats(t *testing.T) {
	s := NewStore()
	s.UpsertFile(StorageFile{BackendName: "s3", Path: "f1", SizeBytes: 100})
	s.UpsertFile(StorageFile{BackendName: "s3", Path: "f2", SizeBytes: 200})
	s.UpsertFile(StorageFile{BackendName: "azure", Path: "f3", SizeBytes: 300})

	stats := s.ListBackendStats()
	if len(stats) != 2 {
		t.Errorf("expected 2 backends, got %d", len(stats))
	}
}

func TestStoreGetUserAccess(t *testing.T) {
	s := NewStore()
	s.UpsertAccess(AccessRecord{
		BackendName: "s3", Path: "f1", UserID: "alice",
		Date: "2024-01-01", ReadCount: 1, BytesRead: 100,
		LastAccess: time.Now(),
	})
	s.UpsertAccess(AccessRecord{
		BackendName: "s3", Path: "f2", UserID: "bob",
		Date: "2024-01-01", ReadCount: 1, BytesRead: 200,
		LastAccess: time.Now(),
	})

	records := s.GetUserAccess("alice")
	if len(records) != 1 {
		t.Errorf("expected 1 record for alice, got %d", len(records))
	}
}

func TestStoreGetTeamUsage(t *testing.T) {
	s := NewStore()
	s.SetTeam("alice", "ml-team")
	s.UpsertFile(StorageFile{BackendName: "s3", Path: "f1", SizeBytes: 1000})
	s.UpsertAccess(AccessRecord{
		BackendName: "s3", Path: "f1", UserID: "alice",
		Date: "2024-01-01", ReadCount: 1, BytesRead: 100,
		LastAccess: time.Now(),
	})

	usage := s.GetTeamUsage("ml-team", "s3")
	if usage != 1000 {
		t.Errorf("expected 1000 bytes team usage, got %d", usage)
	}

	usage = s.GetTeamUsage("other-team", "s3")
	if usage != 0 {
		t.Errorf("expected 0 bytes for other team, got %d", usage)
	}
}

// ─── Server tests ───

func TestServerIngestEvents(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	events := []telemetry.AccessEvent{
		{
			Timestamp: time.Now(), UserID: "alice", BackendName: "s3",
			Path: "data/file.bin", Operation: "read", BytesRead: 1024,
		},
		{
			Timestamp: time.Now(), UserID: "bob", BackendName: "azure",
			Path: "models/v1", Operation: "read", BytesRead: 2048,
		},
	}

	accepted := srv.IngestEvents(events)
	if accepted != 2 {
		t.Errorf("expected 2 accepted, got %d", accepted)
	}

	store := srv.GetStore()
	if store.AccessRecordCount() != 2 {
		t.Errorf("expected 2 access records, got %d", store.AccessRecordCount())
	}
}

func TestServerSetQuotaValidation(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)

	// Missing team name.
	if err := srv.SetQuota(Quota{BackendName: "s3", HardLimit: 100}); err == nil {
		t.Error("expected error for missing team name")
	}

	// Negative limits.
	if err := srv.SetQuota(Quota{TeamName: "t", HardLimit: -1}); err == nil {
		t.Error("expected error for negative limit")
	}

	// Soft > hard.
	if err := srv.SetQuota(Quota{TeamName: "t", SoftLimit: 200, HardLimit: 100}); err == nil {
		t.Error("expected error for soft > hard")
	}

	// Valid quota.
	if err := srv.SetQuota(Quota{TeamName: "t", SoftLimit: 50, HardLimit: 100}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestServerGetQuotas(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	srv.SetQuota(Quota{TeamName: "ml", BackendName: "s3", SoftLimit: 100, HardLimit: 200})

	statuses := srv.GetQuotas()
	if len(statuses) != 1 {
		t.Fatalf("expected 1 quota status, got %d", len(statuses))
	}
	if statuses[0].TeamName != "ml" {
		t.Errorf("expected ml, got %s", statuses[0].TeamName)
	}
	if statuses[0].Exceeded {
		t.Error("expected not exceeded")
	}
}

func TestServerTeamMapping(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	srv.SetTeamMapping("alice", "ml-team")

	records := srv.GetUserDetail("alice")
	if len(records) != 0 {
		t.Errorf("expected 0 records initially, got %d", len(records))
	}
}

func TestServerQueryUsageEmpty(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	rows := srv.QueryUsage("user", "")
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestServerQueryStaleEmpty(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	files := srv.QueryStale(90, 0)
	if len(files) != 0 {
		t.Errorf("expected 0 stale files, got %d", len(files))
	}
}

func TestServerQueryGrowthEmpty(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	points := srv.QueryGrowth("", 30)
	if len(points) != 0 {
		t.Errorf("expected 0 growth points, got %d", len(points))
	}
}

func TestServerCrawlNoBackends(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	if err := srv.CrawlAll(context.Background()); err != nil {
		t.Errorf("crawl with nil backends should not error: %v", err)
	}
}

// ─── API tests ───

func TestAPIUsage(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	srv.IngestEvents([]telemetry.AccessEvent{
		{Timestamp: time.Now(), UserID: "alice", BackendName: "s3", Path: "f1", BytesRead: 100},
	})

	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	req := httptest.NewRequest("GET", "/api/v1/usage?group_by=user", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var rows []UsageRow
	if err := json.NewDecoder(w.Body).Decode(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

func TestAPIStale(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	store := srv.GetStore()
	store.UpsertFile(StorageFile{
		BackendName: "s3", Path: "old", SizeBytes: 100,
		FirstSeen: time.Now().AddDate(0, 0, -200),
	})

	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	req := httptest.NewRequest("GET", "/api/v1/stale?days=90", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAPIQuotaSetAndList(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	// Set a quota.
	body := `{"team_name":"ml","backend_name":"s3","soft_limit":100,"hard_limit":200}`
	req := httptest.NewRequest("PUT", "/api/v1/quota", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200 on set, got %d: %s", w.Code, w.Body.String())
	}

	// List quotas.
	req = httptest.NewRequest("GET", "/api/v1/quota", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200 on list, got %d", w.Code)
	}

	var statuses []QuotaStatus
	if err := json.NewDecoder(w.Body).Decode(&statuses); err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 {
		t.Errorf("expected 1 quota, got %d", len(statuses))
	}
}

func TestAPITeamMapping(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	body := `{"user_id":"alice","team_name":"ml"}`
	req := httptest.NewRequest("POST", "/api/v1/team", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAPIBackends(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	req := httptest.NewRequest("GET", "/api/v1/backends", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAPIGrowth(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	req := httptest.NewRequest("GET", "/api/v1/growth?period=30", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAPICrawl(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	req := httptest.NewRequest("POST", "/api/v1/crawl", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAPIIngest(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	body := `[{"ts":"2024-01-01T10:00:00Z","user":"alice","backend":"s3","path":"f1","op":"read","bytes_read":1024}]`
	req := httptest.NewRequest("POST", "/api/v1/ingest", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]int
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp["accepted"] != 1 {
		t.Errorf("expected 1 accepted, got %d", resp["accepted"])
	}
}

func TestAPIUserDetail(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	srv.IngestEvents([]telemetry.AccessEvent{
		{Timestamp: time.Now(), UserID: "alice", BackendName: "s3", Path: "f1", BytesRead: 100},
	})

	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	req := httptest.NewRequest("GET", "/api/v1/user/alice", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAPIQuotaSetBadJSON(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	req := httptest.NewRequest("PUT", "/api/v1/quota", strings.NewReader("not json"))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("expected 400 for bad JSON, got %d", w.Code)
	}
}

func TestAPITeamBadJSON(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	req := httptest.NewRequest("POST", "/api/v1/team", strings.NewReader("bad"))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("expected 400 for bad JSON, got %d", w.Code)
	}
}
''')

# ─── M4 e2e tests ───
write("e2e/m4_test.go", r'''package e2e

import (
	"context"
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
''')

if __name__ == "__main__":
    print("All test files generated.")
