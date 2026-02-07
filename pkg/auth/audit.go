package auth

import (
	"log/slog"
	"sync"
	"time"
)

// AuditEntry records a credential resolution event.
type AuditEntry struct {
	Timestamp   time.Time `json:"ts"`
	BackendName string    `json:"backend"`
	Provider    string    `json:"provider"`
	UserID      string    `json:"user,omitempty"`
	Success     bool      `json:"success"`
	Error       string    `json:"error,omitempty"`
}

// AuditLogger records all credential operations in a ring buffer.
type AuditLogger struct {
	mu      sync.Mutex
	entries []AuditEntry
	maxSize int
	sink    func(AuditEntry) // Optional external sink (e.g., telemetry emitter)
}

// NewAuditLogger creates a new audit logger with the given ring buffer size.
func NewAuditLogger(maxSize int, sink func(AuditEntry)) *AuditLogger {
	return &AuditLogger{
		entries: make([]AuditEntry, 0, maxSize),
		maxSize: maxSize,
		sink:    sink,
	}
}

// Log records an audit entry.
func (al *AuditLogger) Log(entry AuditEntry) {
	al.mu.Lock()
	defer al.mu.Unlock()

	al.entries = append(al.entries, entry)
	if len(al.entries) > al.maxSize {
		// Trim to maxSize, keeping most recent entries
		al.entries = al.entries[len(al.entries)-al.maxSize:]
	}

	// Structured log for external aggregation
	if entry.Success {
		slog.Info("Auth credential resolved",
			"backend", entry.BackendName,
			"provider", entry.Provider)
	} else {
		slog.Warn("Auth credential failed",
			"backend", entry.BackendName,
			"provider", entry.Provider,
			"error", entry.Error)
	}

	if al.sink != nil {
		al.sink(entry)
	}
}

// Recent returns the last N entries.
func (al *AuditLogger) Recent(limit int) []AuditEntry {
	al.mu.Lock()
	defer al.mu.Unlock()

	if limit > len(al.entries) {
		limit = len(al.entries)
	}
	if limit == 0 {
		return nil
	}
	result := make([]AuditEntry, limit)
	copy(result, al.entries[len(al.entries)-limit:])
	return result
}

// Len returns the current number of entries.
func (al *AuditLogger) Len() int {
	al.mu.Lock()
	defer al.mu.Unlock()
	return len(al.entries)
}
