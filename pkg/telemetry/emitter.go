package telemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// Emitter sends batches of events to a sink.
type Emitter interface {
	Emit(events []AccessEvent) error
	Close() error
}

// StdoutEmitter writes JSON lines to stdout (for K8s log aggregation).
type StdoutEmitter struct {
	encoder *json.Encoder
	mu      sync.Mutex
}

// NewStdoutEmitter creates a stdout emitter.
func NewStdoutEmitter() *StdoutEmitter {
	return &StdoutEmitter{
		encoder: json.NewEncoder(os.Stdout),
	}
}

// Emit writes events as JSON lines to stdout.
func (e *StdoutEmitter) Emit(events []AccessEvent) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, evt := range events {
		if err := e.encoder.Encode(evt); err != nil {
			return fmt.Errorf("telemetry.StdoutEmitter: %w", err)
		}
	}
	return nil
}

// Close is a no-op for stdout.
func (e *StdoutEmitter) Close() error {
	return nil
}

// FileEmitter writes JSON lines to a file.
type FileEmitter struct {
	file    *os.File
	encoder *json.Encoder
	mu      sync.Mutex
}

// NewFileEmitter creates a file emitter that writes JSONL to the given path.
func NewFileEmitter(path string) (*FileEmitter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("telemetry.NewFileEmitter: %w", err)
	}
	return &FileEmitter{
		file:    f,
		encoder: json.NewEncoder(f),
	}, nil
}

// Emit writes events as JSON lines to file.
func (e *FileEmitter) Emit(events []AccessEvent) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, evt := range events {
		if err := e.encoder.Encode(evt); err != nil {
			return fmt.Errorf("telemetry.FileEmitter: %w", err)
		}
	}
	return nil
}

// Close closes the file.
func (e *FileEmitter) Close() error {
	return e.file.Close()
}

// HTTPEmitter sends events to the control plane via HTTP POST.
type HTTPEmitter struct {
	addr   string
	client *http.Client
}

// NewHTTPEmitter creates an emitter that POSTs events to the control plane ingest endpoint.
func NewHTTPEmitter(addr string) *HTTPEmitter {
	return &HTTPEmitter{
		addr: addr,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Emit sends events to the control plane via HTTP POST /api/v1/ingest.
func (e *HTTPEmitter) Emit(events []AccessEvent) error {
	data, err := json.Marshal(events)
	if err != nil {
		return fmt.Errorf("telemetry.HTTPEmitter: marshal: %w", err)
	}

	url := e.addr + "/api/v1/ingest"
	resp, err := e.client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("telemetry.HTTPEmitter: post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telemetry.HTTPEmitter: unexpected status %d", resp.StatusCode)
	}
	return nil
}

// Close is a no-op for HTTP emitter.
func (e *HTTPEmitter) Close() error {
	return nil
}

// NopEmitter discards all events.
type NopEmitter struct{}

// NewNopEmitter creates a no-op emitter.
func NewNopEmitter() *NopEmitter {
	return &NopEmitter{}
}

// Emit discards events.
func (e *NopEmitter) Emit(events []AccessEvent) error {
	return nil
}

// Close is a no-op.
func (e *NopEmitter) Close() error {
	return nil
}

// MemoryEmitter stores events in memory (for testing).
type MemoryEmitter struct {
	mu     sync.Mutex
	events []AccessEvent
}

// NewMemoryEmitter creates a memory-backed emitter.
func NewMemoryEmitter() *MemoryEmitter {
	return &MemoryEmitter{}
}

// Emit stores events.
func (e *MemoryEmitter) Emit(events []AccessEvent) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, events...)
	return nil
}

// Close is a no-op.
func (e *MemoryEmitter) Close() error {
	return nil
}

// Events returns all stored events.
func (e *MemoryEmitter) Events() []AccessEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]AccessEvent, len(e.events))
	copy(out, e.events)
	return out
}

// Len returns the number of stored events.
func (e *MemoryEmitter) Len() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.events)
}
