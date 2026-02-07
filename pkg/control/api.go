package control

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RegisterAPIRoutes registers all REST API routes on the given mux.
func (s *Server) RegisterAPIRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/usage", s.handleUsage)
	mux.HandleFunc("GET /api/v1/stale", s.handleStale)
	mux.HandleFunc("GET /api/v1/growth", s.handleGrowth)
	mux.HandleFunc("GET /api/v1/quota", s.handleQuotaList)
	mux.HandleFunc("PUT /api/v1/quota", s.handleQuotaSet)
	mux.HandleFunc("GET /api/v1/user/{userId}", s.handleUserDetail)
	mux.HandleFunc("GET /api/v1/backends", s.handleBackendList)
	mux.HandleFunc("POST /api/v1/ingest", s.handleIngest)
	mux.HandleFunc("POST /api/v1/crawl", s.handleCrawl)
	mux.HandleFunc("POST /api/v1/team", s.handleSetTeam)
}

// GET /api/v1/usage?group_by=team|user&backend=all|<name>
func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	groupBy := r.URL.Query().Get("group_by")
	if groupBy == "" {
		groupBy = "user"
	}
	backendFilter := r.URL.Query().Get("backend")
	rows := s.QueryUsage(groupBy, backendFilter)
	writeJSON(w, rows)
}

// GET /api/v1/stale?days=90&min_size=0
func (s *Server) handleStale(w http.ResponseWriter, r *http.Request) {
	days := parseIntParam(r, "days", 90)
	minSize := parseSizeParam(r, "min_size", 0)
	files := s.QueryStale(days, minSize)
	writeJSON(w, files)
}

// GET /api/v1/growth?backend=<name>&period=30
func (s *Server) handleGrowth(w http.ResponseWriter, r *http.Request) {
	backendName := r.URL.Query().Get("backend")
	period := parseIntParam(r, "period", 30)
	points := s.QueryGrowth(backendName, period)
	writeJSON(w, points)
}

// GET /api/v1/quota
func (s *Server) handleQuotaList(w http.ResponseWriter, r *http.Request) {
	quotas := s.GetQuotas()
	writeJSON(w, quotas)
}

// PUT /api/v1/quota
func (s *Server) handleQuotaSet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TeamName    string `json:"team_name"`
		BackendName string `json:"backend_name"`
		SoftLimit   int64  `json:"soft_limit"`
		HardLimit   int64  `json:"hard_limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	err := s.SetQuota(Quota{
		TeamName:    req.TeamName,
		BackendName: req.BackendName,
		SoftLimit:   req.SoftLimit,
		HardLimit:   req.HardLimit,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

// GET /api/v1/user/{userId}
func (s *Server) handleUserDetail(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userId")
	if userID == "" {
		http.Error(w, "userId is required", http.StatusBadRequest)
		return
	}
	records := s.GetUserDetail(userID)
	writeJSON(w, records)
}

// GET /api/v1/backends
func (s *Server) handleBackendList(w http.ResponseWriter, r *http.Request) {
	backends := s.ListBackends()
	writeJSON(w, backends)
}

// POST /api/v1/ingest — receives a batch of telemetry events.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var events []ingestEvent
	if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Convert to telemetry events
	telEvents := make([]telemetryAccessEvent, 0, len(events))
	for _, e := range events {
		telEvents = append(telEvents, telemetryAccessEvent(e))
	}

	accepted := s.IngestEventsFromHTTP(telEvents)
	writeJSON(w, map[string]int{"accepted": accepted})
}

// ingestEvent matches the telemetry AccessEvent JSON format for HTTP ingestion.
type ingestEvent struct {
	Timestamp   string  `json:"ts"`
	UserID      string  `json:"user"`
	BackendName string  `json:"backend"`
	Path        string  `json:"path"`
	Operation   string  `json:"op"`
	BytesRead   int64   `json:"bytes_read"`
	CacheHit    bool    `json:"cache_hit"`
	NodeHost    string  `json:"node"`
	LatencyMs   float64 `json:"latency_ms"`
	Error       string  `json:"error,omitempty"`
}

type telemetryAccessEvent = ingestEvent

// IngestEventsFromHTTP processes HTTP-submitted events.
func (s *Server) IngestEventsFromHTTP(events []ingestEvent) int {
	accepted := 0
	for _, evt := range events {
		ts, _ := parseTimestamp(evt.Timestamp)
		if ts.IsZero() {
			ts = timeNow()
		}

		date := ts.Format("2006-01-02")
		s.store.UpsertAccess(AccessRecord{
			BackendName: evt.BackendName,
			Path:        evt.Path,
			UserID:      evt.UserID,
			Date:        date,
			ReadCount:   1,
			BytesRead:   evt.BytesRead,
			LastAccess:  ts,
		})
		accepted++
	}
	return accepted
}

// POST /api/v1/crawl — triggers a storage crawl.
func (s *Server) handleCrawl(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := s.CrawlAll(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

// POST /api/v1/team — set a team mapping.
func (s *Server) handleSetTeam(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID   string `json:"user_id"`
		TeamName string `json:"team_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}
	if req.UserID == "" || req.TeamName == "" {
		http.Error(w, "user_id and team_name are required", http.StatusBadRequest)
		return
	}
	s.SetTeamMapping(req.UserID, req.TeamName)
	writeJSON(w, map[string]string{"status": "ok"})
}

// ─── Helpers ──────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func parseIntParam(r *http.Request, name string, defaultVal int) int {
	s := r.URL.Query().Get(name)
	if s == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return n
}

func parseSizeParam(r *http.Request, name string, defaultVal int64) int64 {
	s := r.URL.Query().Get(name)
	if s == "" {
		return defaultVal
	}
	// Try parsing as integer first
	n, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return n
	}
	// Try human-readable size (e.g., "1GB")
	s = strings.ToUpper(strings.TrimSpace(s))
	multipliers := []struct {
		suffix string
		mult   int64
	}{
		{"TB", 1024 * 1024 * 1024 * 1024},
		{"GB", 1024 * 1024 * 1024},
		{"MB", 1024 * 1024},
		{"KB", 1024},
	}
	for _, m := range multipliers {
		if strings.HasSuffix(s, m.suffix) {
			numStr := strings.TrimSuffix(s, m.suffix)
			num, err := strconv.ParseFloat(numStr, 64)
			if err == nil {
				return int64(num * float64(m.mult))
			}
		}
	}
	return defaultVal
}

func parseTimestamp(s string) (time.Time, error) {
	// Try RFC3339 first
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		return t, nil
	}
	// Try RFC3339Nano
	t, err = time.Parse(time.RFC3339Nano, s)
	if err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp %q", s)
}

// timeNow is a variable for testing.
var timeNow = time.Now
