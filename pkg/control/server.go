package control

// The data layer uses an in-memory store for local/testing use.
// For production, swap with a PostgreSQL-backed implementation.

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/telemetry"
)

// ControlPlaneConfig configures the control plane server.
type ControlPlaneConfig struct {
	RESTAddr             string        `yaml:"rest_addr"`
	StorageCrawlInterval time.Duration `yaml:"storage_crawl_interval"`
}

// StorageFile represents one indexed file across any backend.
type StorageFile struct {
	ID           int64
	BackendName  string
	Path         string
	SizeBytes    int64
	ETag         string
	LastModified time.Time
	FirstSeen    time.Time
	LastCrawled  time.Time
}

// AccessRecord represents aggregated access per file per user per day.
type AccessRecord struct {
	BackendName string
	Path        string
	UserID      string
	Date        string // YYYY-MM-DD
	ReadCount   int
	BytesRead   int64
	LastAccess  time.Time
}

// TeamMapping maps a user to a team.
type TeamMapping struct {
	UserID   string
	TeamName string
}

// Quota defines storage limits for a team.
type Quota struct {
	ID          int
	TeamName    string
	BackendName string // "" = all backends
	SoftLimit   int64  // bytes
	HardLimit   int64  // bytes
}

// UsageRow is a row returned by usage queries.
type UsageRow struct {
	GroupName   string    `json:"group_name"`
	BackendName string    `json:"backend_name"`
	FileCount   int64     `json:"file_count"`
	TotalBytes  int64     `json:"total_bytes"`
	LastAccess  time.Time `json:"last_access,omitempty"`
}

// StaleFile represents a file not accessed in a long time.
type StaleFile struct {
	BackendName  string    `json:"backend_name"`
	Path         string    `json:"path"`
	SizeBytes    int64     `json:"size_bytes"`
	LastModified time.Time `json:"last_modified"`
	LastAccess   time.Time `json:"last_access"`
}

// GrowthPoint is a time-series data point for storage growth.
type GrowthPoint struct {
	Date       string `json:"date"` // YYYY-MM-DD
	TotalBytes int64  `json:"total_bytes"`
	FileCount  int64  `json:"file_count"`
}

// BackendStats summarizes a single backend.
type BackendStats struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	FileCount  int64  `json:"file_count"`
	TotalBytes int64  `json:"total_bytes"`
}

// QuotaStatus shows current usage against quota limits.
type QuotaStatus struct {
	Quota
	CurrentUsage int64   `json:"current_usage"`
	UsagePercent float64 `json:"usage_percent"`
	Exceeded     bool    `json:"exceeded"`
	SoftExceeded bool    `json:"soft_exceeded"`
}

// Server is the WarpDrive control plane server.
type Server struct {
	store    *Store
	backends *backend.Registry
	cfg      ControlPlaneConfig
	mu       sync.Mutex
	crawler  *StorageCrawler
	httpSrv  *http.Server
}

// NewServer creates a control plane server.
func NewServer(cfg ControlPlaneConfig, backends *backend.Registry) *Server {
	if cfg.StorageCrawlInterval <= 0 {
		cfg.StorageCrawlInterval = 24 * time.Hour
	}
	s := &Server{
		store:    NewStore(),
		backends: backends,
		cfg:      cfg,
	}
	s.crawler = &StorageCrawler{
		server:   s,
		interval: cfg.StorageCrawlInterval,
	}
	return s
}

// Run starts the control plane HTTP server and the storage crawler.
// It blocks until ctx is cancelled, then shuts down gracefully.
func (s *Server) Run(ctx context.Context) error {
	addr := s.cfg.RESTAddr
	if addr == "" {
		addr = ":8080"
	}

	mux := http.NewServeMux()
	s.RegisterAPIRoutes(mux)

	s.httpSrv = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Start storage crawler in background.
	crawlCtx, crawlCancel := context.WithCancel(ctx)
	defer crawlCancel()
	go s.crawler.Run(crawlCtx)

	// Start HTTP server in background.
	errCh := make(chan error, 1)
	go func() {
		slog.Info("control plane listening", "addr", addr)
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	// Wait for context cancellation or server error.
	select {
	case <-ctx.Done():
		slog.Info("control plane shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// Shutdown gracefully shuts down the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpSrv != nil {
		return s.httpSrv.Shutdown(ctx)
	}
	return nil
}

// SetRESTAddr overrides the REST listen address.
func (s *Server) SetRESTAddr(addr string) {
	s.cfg.RESTAddr = addr
}

// IngestEvents processes a batch of telemetry events, aggregating into access records.
func (s *Server) IngestEvents(events []telemetry.AccessEvent) int {
	accepted := 0
	for _, evt := range events {
		date := evt.Timestamp.Format("2006-01-02")
		s.store.UpsertAccess(AccessRecord{
			BackendName: evt.BackendName,
			Path:        evt.Path,
			UserID:      evt.UserID,
			Date:        date,
			ReadCount:   1,
			BytesRead:   evt.BytesRead,
			LastAccess:  evt.Timestamp,
		})
		s.checkQuota(evt)
		accepted++
	}
	return accepted
}

// checkQuota logs warnings when a team exceeds soft/hard limits.
func (s *Server) checkQuota(evt telemetry.AccessEvent) {
	teams := s.store.GetTeamsForUser(evt.UserID)
	for _, team := range teams {
		quotas := s.store.GetQuotasForTeam(team)
		for _, q := range quotas {
			if q.BackendName != "" && q.BackendName != evt.BackendName {
				continue
			}
			usage := s.store.GetTeamUsage(team, q.BackendName)
			if q.HardLimit > 0 && usage >= q.HardLimit {
				slog.Warn("quota hard limit exceeded",
					"team", team, "backend", q.BackendName,
					"usage", usage, "limit", q.HardLimit)
			} else if q.SoftLimit > 0 && usage >= q.SoftLimit {
				slog.Warn("quota soft limit exceeded",
					"team", team, "backend", q.BackendName,
					"usage", usage, "limit", q.SoftLimit)
			}
		}
	}
}

// QueryUsage returns storage usage grouped by team or user.
func (s *Server) QueryUsage(groupBy, backendFilter string) []UsageRow {
	return s.store.QueryUsage(groupBy, backendFilter)
}

// QueryStale returns files not accessed within the given number of days.
func (s *Server) QueryStale(days int, minSize int64) []StaleFile {
	return s.store.QueryStale(days, minSize)
}

// QueryGrowth returns storage growth over time for a backend.
func (s *Server) QueryGrowth(backendName string, periodDays int) []GrowthPoint {
	return s.store.QueryGrowth(backendName, periodDays)
}

// ListBackends returns stats for all backends.
func (s *Server) ListBackends() []BackendStats {
	result := s.store.ListBackendStats()
	if s.backends != nil {
		for i, bs := range result {
			if be, err := s.backends.Get(bs.Name); err == nil {
				result[i].Type = be.Type()
			}
		}
	}
	return result
}

// GetQuotas returns all quotas with current usage status.
func (s *Server) GetQuotas() []QuotaStatus {
	quotas := s.store.GetAllQuotas()
	result := make([]QuotaStatus, 0, len(quotas))
	for _, q := range quotas {
		usage := s.store.GetTeamUsage(q.TeamName, q.BackendName)
		qs := QuotaStatus{
			Quota:        q,
			CurrentUsage: usage,
		}
		if q.HardLimit > 0 {
			qs.UsagePercent = float64(usage) / float64(q.HardLimit) * 100
			qs.Exceeded = usage >= q.HardLimit
		}
		if q.SoftLimit > 0 {
			qs.SoftExceeded = usage >= q.SoftLimit
		}
		result = append(result, qs)
	}
	return result
}

// SetQuota creates or updates a quota.
func (s *Server) SetQuota(q Quota) error {
	if q.TeamName == "" {
		return fmt.Errorf("control.SetQuota: team_name is required")
	}
	if q.SoftLimit < 0 || q.HardLimit < 0 {
		return fmt.Errorf("control.SetQuota: limits must be non-negative")
	}
	if q.SoftLimit > 0 && q.HardLimit > 0 && q.SoftLimit > q.HardLimit {
		return fmt.Errorf("control.SetQuota: soft_limit must be <= hard_limit")
	}
	s.store.SetQuota(q)
	return nil
}

// SetTeamMapping sets a user-to-team mapping.
func (s *Server) SetTeamMapping(userID, teamName string) {
	s.store.SetTeam(userID, teamName)
}

// GetUserDetail returns access details for a specific user.
func (s *Server) GetUserDetail(userID string) []AccessRecord {
	return s.store.GetUserAccess(userID)
}

// CrawlAll triggers a full crawl of all backends.
func (s *Server) CrawlAll(ctx context.Context) error {
	return s.crawler.crawlAll(ctx)
}

// GetStore returns the underlying store (for testing).
func (s *Server) GetStore() *Store {
	return s.store
}

// ─────────────────────── In-Memory Store ───────────────────────

// Store is an in-memory data store for telemetry and governance data.
type Store struct {
	mu sync.RWMutex

	files  map[string]*StorageFile  // key: backend:path
	access map[string]*AccessRecord // key: backend:path:user:date
	teams  map[string][]string      // userID -> []teamName
	quotas map[string]*Quota        // key: team:backend

	nextID  int64
	quotaID int
}

// NewStore creates an empty in-memory store.
func NewStore() *Store {
	return &Store{
		files:  make(map[string]*StorageFile),
		access: make(map[string]*AccessRecord),
		teams:  make(map[string][]string),
		quotas: make(map[string]*Quota),
		nextID: 1,
	}
}

func fileKey(backendName, path string) string {
	return backendName + ":" + path
}

func accessKey(backendName, path, userID, date string) string {
	return backendName + ":" + path + ":" + userID + ":" + date
}

func quotaKey(teamName, backendName string) string {
	return teamName + ":" + backendName
}

// UpsertFile inserts or updates a storage file record.
func (s *Store) UpsertFile(sf StorageFile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fileKey(sf.BackendName, sf.Path)
	if existing, ok := s.files[key]; ok {
		existing.SizeBytes = sf.SizeBytes
		existing.ETag = sf.ETag
		existing.LastModified = sf.LastModified
		existing.LastCrawled = time.Now()
	} else {
		sf.ID = s.nextID
		s.nextID++
		if sf.FirstSeen.IsZero() {
			sf.FirstSeen = time.Now()
		}
		sf.LastCrawled = time.Now()
		s.files[key] = &sf
	}
}

// UpsertAccess inserts or updates an access record (aggregates).
func (s *Store) UpsertAccess(rec AccessRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := accessKey(rec.BackendName, rec.Path, rec.UserID, rec.Date)
	if existing, ok := s.access[key]; ok {
		existing.ReadCount += rec.ReadCount
		existing.BytesRead += rec.BytesRead
		if rec.LastAccess.After(existing.LastAccess) {
			existing.LastAccess = rec.LastAccess
		}
	} else {
		s.access[key] = &rec
	}
}

// SetTeam adds a user-to-team mapping.
func (s *Store) SetTeam(userID, teamName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	teams := s.teams[userID]
	for _, t := range teams {
		if t == teamName {
			return
		}
	}
	s.teams[userID] = append(teams, teamName)
}

// GetTeamsForUser returns the teams a user belongs to.
func (s *Store) GetTeamsForUser(userID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.teams[userID]
}

// SetQuota creates or updates a quota.
func (s *Store) SetQuota(q Quota) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := quotaKey(q.TeamName, q.BackendName)
	if existing, ok := s.quotas[key]; ok {
		existing.SoftLimit = q.SoftLimit
		existing.HardLimit = q.HardLimit
	} else {
		s.quotaID++
		q.ID = s.quotaID
		s.quotas[key] = &q
	}
}

// GetQuotasForTeam returns quotas for a specific team.
func (s *Store) GetQuotasForTeam(teamName string) []Quota {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []Quota
	for _, q := range s.quotas {
		if q.TeamName == teamName {
			result = append(result, *q)
		}
	}
	return result
}

// GetAllQuotas returns all quotas.
func (s *Store) GetAllQuotas() []Quota {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Quota, 0, len(s.quotas))
	for _, q := range s.quotas {
		result = append(result, *q)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].TeamName < result[j].TeamName
	})
	return result
}

// GetTeamUsage returns total bytes stored by a team on a backend.
func (s *Store) GetTeamUsage(teamName, backendName string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	users := make(map[string]bool)
	for uid, teams := range s.teams {
		for _, t := range teams {
			if t == teamName {
				users[uid] = true
			}
		}
	}

	accessedFiles := make(map[string]bool)
	for _, rec := range s.access {
		if !users[rec.UserID] {
			continue
		}
		if backendName != "" && rec.BackendName != backendName {
			continue
		}
		fk := fileKey(rec.BackendName, rec.Path)
		accessedFiles[fk] = true
	}

	var total int64
	for fk := range accessedFiles {
		if f, ok := s.files[fk]; ok {
			total += f.SizeBytes
		}
	}
	return total
}

// QueryUsage returns usage grouped by team or user.
func (s *Store) QueryUsage(groupBy, backendFilter string) []UsageRow {
	s.mu.RLock()
	defer s.mu.RUnlock()

	type groupKey struct {
		name    string
		backend string
	}
	groups := make(map[groupKey]*UsageRow)

	for _, rec := range s.access {
		if backendFilter != "" && backendFilter != "all" && rec.BackendName != backendFilter {
			continue
		}
		var name string
		switch groupBy {
		case "team":
			teams := s.teams[rec.UserID]
			if len(teams) == 0 {
				name = "unassigned"
			} else {
				name = teams[0]
			}
		case "user":
			name = rec.UserID
		default:
			name = rec.UserID
		}
		key := groupKey{name: name, backend: rec.BackendName}
		if row, ok := groups[key]; ok {
			row.FileCount++
			row.TotalBytes += rec.BytesRead
			if rec.LastAccess.After(row.LastAccess) {
				row.LastAccess = rec.LastAccess
			}
		} else {
			groups[key] = &UsageRow{
				GroupName:   name,
				BackendName: rec.BackendName,
				FileCount:   1,
				TotalBytes:  rec.BytesRead,
				LastAccess:  rec.LastAccess,
			}
		}
	}

	result := make([]UsageRow, 0, len(groups))
	for _, row := range groups {
		result = append(result, *row)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].TotalBytes > result[j].TotalBytes
	})
	return result
}

// QueryStale returns files not accessed in the last N days.
func (s *Store) QueryStale(days int, minSize int64) []StaleFile {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cutoff := time.Now().AddDate(0, 0, -days)
	var result []StaleFile

	for _, f := range s.files {
		if f.SizeBytes < minSize {
			continue
		}
		lastAccess := f.FirstSeen
		for _, rec := range s.access {
			if rec.BackendName == f.BackendName && rec.Path == f.Path {
				if rec.LastAccess.After(lastAccess) {
					lastAccess = rec.LastAccess
				}
			}
		}
		if lastAccess.Before(cutoff) {
			result = append(result, StaleFile{
				BackendName:  f.BackendName,
				Path:         f.Path,
				SizeBytes:    f.SizeBytes,
				LastModified: f.LastModified,
				LastAccess:   lastAccess,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].SizeBytes > result[j].SizeBytes
	})
	if len(result) > 1000 {
		result = result[:1000]
	}
	return result
}

// QueryGrowth returns storage growth bucketed by day.
func (s *Store) QueryGrowth(backendName string, periodDays int) []GrowthPoint {
	s.mu.RLock()
	defer s.mu.RUnlock()

	since := time.Now().AddDate(0, 0, -periodDays)
	dailyBytes := make(map[string]int64)
	dailyFiles := make(map[string]int64)

	for _, f := range s.files {
		if backendName != "" && backendName != "all" && f.BackendName != backendName {
			continue
		}
		if f.FirstSeen.Before(since) {
			dateKey := since.Format("2006-01-02")
			dailyBytes[dateKey] += f.SizeBytes
			dailyFiles[dateKey]++
		} else {
			dateKey := f.FirstSeen.Format("2006-01-02")
			dailyBytes[dateKey] += f.SizeBytes
			dailyFiles[dateKey]++
		}
	}

	var points []GrowthPoint
	for date, bytes := range dailyBytes {
		points = append(points, GrowthPoint{
			Date:       date,
			TotalBytes: bytes,
			FileCount:  dailyFiles[date],
		})
	}
	sort.Slice(points, func(i, j int) bool {
		return points[i].Date < points[j].Date
	})
	for i := 1; i < len(points); i++ {
		points[i].TotalBytes += points[i-1].TotalBytes
		points[i].FileCount += points[i-1].FileCount
	}
	return points
}

// ListBackendStats returns stats for each backend that has indexed files.
func (s *Store) ListBackendStats() []BackendStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make(map[string]*BackendStats)
	for _, f := range s.files {
		if bs, ok := stats[f.BackendName]; ok {
			bs.FileCount++
			bs.TotalBytes += f.SizeBytes
		} else {
			stats[f.BackendName] = &BackendStats{
				Name:       f.BackendName,
				FileCount:  1,
				TotalBytes: f.SizeBytes,
			}
		}
	}

	result := make([]BackendStats, 0, len(stats))
	for _, bs := range stats {
		result = append(result, *bs)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// GetUserAccess returns all access records for a user.
func (s *Store) GetUserAccess(userID string) []AccessRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []AccessRecord
	for _, rec := range s.access {
		if rec.UserID == userID {
			result = append(result, *rec)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].LastAccess.After(result[j].LastAccess)
	})
	return result
}

// FileCount returns the number of indexed files.
func (s *Store) FileCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.files)
}

// AccessRecordCount returns the number of access records.
func (s *Store) AccessRecordCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.access)
}
