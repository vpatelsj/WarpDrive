package control

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

// ─── Additional edge-case tests ───

func TestServerRun(t *testing.T) {
	// Test that Server.Run starts and can be shut down.
	srv := NewServer(ControlPlaneConfig{RESTAddr: ":0"}, nil)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run(ctx)
	}()

	// Give the server time to start.
	time.Sleep(100 * time.Millisecond)

	// Cancel to trigger shutdown.
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("unexpected error from Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down in time")
	}
}

func TestServerSetRESTAddr(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	srv.SetRESTAddr(":9999")

	if srv.cfg.RESTAddr != ":9999" {
		t.Errorf("expected :9999, got %s", srv.cfg.RESTAddr)
	}
}

func TestAPIParseSizeParam(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"", 0},
		{"1024", 1024},
		{"1KB", 1024},
		{"1MB", 1024 * 1024},
		{"1GB", 1024 * 1024 * 1024},
		{"1TB", 1024 * 1024 * 1024 * 1024},
		{"2.5GB", int64(2.5 * 1024 * 1024 * 1024)},
		{"invalid", 0},
	}

	for _, tt := range tests {
		r := httptest.NewRequest("GET", "/?size="+tt.input, nil)
		got := parseSizeParam(r, "size", 0)
		if got != tt.expected {
			t.Errorf("parseSizeParam(%q) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

func TestAPIParseTimestamp(t *testing.T) {
	// Valid RFC3339.
	ts, err := parseTimestamp("2024-01-15T10:30:00Z")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ts.Year() != 2024 || ts.Month() != 1 || ts.Day() != 15 {
		t.Errorf("unexpected date: %v", ts)
	}

	// Valid RFC3339Nano.
	ts, err = parseTimestamp("2024-01-15T10:30:00.123456789Z")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ts.Nanosecond() != 123456789 {
		t.Errorf("expected nanoseconds, got %d", ts.Nanosecond())
	}

	// Invalid.
	_, err = parseTimestamp("not-a-timestamp")
	if err == nil {
		t.Error("expected error for invalid timestamp")
	}

	// Empty string.
	_, err = parseTimestamp("")
	if err == nil {
		t.Error("expected error for empty timestamp")
	}
}

func TestAPIParseIntParam(t *testing.T) {
	r := httptest.NewRequest("GET", "/?days=45", nil)
	if got := parseIntParam(r, "days", 90); got != 45 {
		t.Errorf("expected 45, got %d", got)
	}

	r = httptest.NewRequest("GET", "/", nil)
	if got := parseIntParam(r, "days", 90); got != 90 {
		t.Errorf("expected default 90, got %d", got)
	}

	r = httptest.NewRequest("GET", "/?days=abc", nil)
	if got := parseIntParam(r, "days", 90); got != 90 {
		t.Errorf("expected default 90 for invalid, got %d", got)
	}
}

func TestAPIQuotaSetValidationError(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	// Empty team name should get a 400 from SetQuota validation.
	body := `{"team_name":"","backend_name":"s3","hard_limit":100}`
	req := httptest.NewRequest("PUT", "/api/v1/quota", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("expected 400 for empty team_name, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAPITeamMissingFields(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	// Missing user_id and team_name.
	body := `{"user_id":"","team_name":""}`
	req := httptest.NewRequest("POST", "/api/v1/team", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("expected 400 for empty fields, got %d", w.Code)
	}
}

func TestAPIIngestMultipleEvents(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	body := `[
		{"ts":"2024-01-01T10:00:00Z","user":"alice","backend":"s3","path":"f1","op":"read","bytes_read":100},
		{"ts":"2024-01-01T11:00:00Z","user":"bob","backend":"azure","path":"f2","op":"read","bytes_read":200},
		{"ts":"","user":"charlie","backend":"gcs","path":"f3","op":"stat","bytes_read":0}
	]`
	req := httptest.NewRequest("POST", "/api/v1/ingest", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]int
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["accepted"] != 3 {
		t.Errorf("expected 3 accepted, got %d", resp["accepted"])
	}

	// Verify the store has the records.
	if srv.GetStore().AccessRecordCount() != 3 {
		t.Errorf("expected 3 access records, got %d", srv.GetStore().AccessRecordCount())
	}
}

func TestAPIUsageWithBackendFilter(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	srv.IngestEvents([]telemetry.AccessEvent{
		{Timestamp: time.Now(), UserID: "alice", BackendName: "s3", Path: "f1", BytesRead: 100},
		{Timestamp: time.Now(), UserID: "alice", BackendName: "azure", Path: "f2", BytesRead: 200},
	})

	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	req := httptest.NewRequest("GET", "/api/v1/usage?group_by=user&backend=s3", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var rows []UsageRow
	json.NewDecoder(w.Body).Decode(&rows)
	if len(rows) != 1 {
		t.Errorf("expected 1 row for s3 filter, got %d", len(rows))
	}
}

func TestAPIGrowthWithBackend(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	store := srv.GetStore()
	store.UpsertFile(StorageFile{
		BackendName: "s3", Path: "f1", SizeBytes: 100,
		FirstSeen: time.Now().AddDate(0, 0, -5),
	})
	store.UpsertFile(StorageFile{
		BackendName: "azure", Path: "f2", SizeBytes: 200,
		FirstSeen: time.Now().AddDate(0, 0, -3),
	})

	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	req := httptest.NewRequest("GET", "/api/v1/growth?backend=s3&period=30", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var points []GrowthPoint
	json.NewDecoder(w.Body).Decode(&points)
	// Should only have s3 data.
	for _, p := range points {
		if p.TotalBytes <= 0 {
			t.Errorf("expected positive bytes in growth point")
		}
	}
}

func TestAPIStaleWithMinSize(t *testing.T) {
	srv := NewServer(ControlPlaneConfig{}, nil)
	store := srv.GetStore()
	store.UpsertFile(StorageFile{
		BackendName: "s3", Path: "small", SizeBytes: 100,
		FirstSeen: time.Now().AddDate(0, 0, -200),
	})
	store.UpsertFile(StorageFile{
		BackendName: "s3", Path: "big", SizeBytes: 2 * 1024 * 1024 * 1024, // 2GB
		FirstSeen: time.Now().AddDate(0, 0, -200),
	})

	mux := http.NewServeMux()
	srv.RegisterAPIRoutes(mux)

	// min_size=1GB should only return the big file.
	req := httptest.NewRequest("GET", "/api/v1/stale?days=90&min_size=1GB", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var files []StaleFile
	json.NewDecoder(w.Body).Decode(&files)
	if len(files) != 1 {
		t.Fatalf("expected 1 stale file with min_size=1GB, got %d", len(files))
	}
	if files[0].Path != "big" {
		t.Errorf("expected 'big' file, got %s", files[0].Path)
	}
}
