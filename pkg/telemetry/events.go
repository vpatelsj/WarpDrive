package telemetry

import "time"

// AccessEvent records a single file access.
type AccessEvent struct {
	Timestamp   time.Time `json:"ts"`
	UserID      string    `json:"user"`
	BackendName string    `json:"backend"`
	Path        string    `json:"path"`
	Operation   string    `json:"op"` // "read", "list", "stat", "write"
	BytesRead   int64     `json:"bytes_read"`
	CacheHit    bool      `json:"cache_hit"`
	NodeHost    string    `json:"node"`
	LatencyMs   float64   `json:"latency_ms"`
	Error       string    `json:"error,omitempty"`
}
