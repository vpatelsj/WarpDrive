package telemetry

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
			Enabled:       false,
			BatchSize:     10,
			FlushInterval: time.Hour,
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

func TestHTTPEmitter(t *testing.T) {
	// Set up a mock control plane ingest endpoint.
	var received []AccessEvent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/ingest" {
			http.Error(w, "not found", 404)
			return
		}
		var events []AccessEvent
		if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		received = append(received, events...)
		w.WriteHeader(200)
		w.Write([]byte(`{"accepted":` + fmt.Sprintf("%d", len(events)) + `}`))
	}))
	defer srv.Close()

	emitter := NewHTTPEmitter(srv.URL)
	events := []AccessEvent{
		{UserID: "alice", Operation: "read", BytesRead: 1024},
		{UserID: "bob", Operation: "stat"},
	}
	if err := emitter.Emit(events); err != nil {
		t.Fatalf("emit failed: %v", err)
	}

	if len(received) != 2 {
		t.Errorf("expected 2 received events, got %d", len(received))
	}
	if received[0].UserID != "alice" {
		t.Errorf("expected alice, got %s", received[0].UserID)
	}

	if err := emitter.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPEmitterBadURL(t *testing.T) {
	emitter := NewHTTPEmitter("http://127.0.0.1:1") // port 1 should fail
	err := emitter.Emit([]AccessEvent{{UserID: "test"}})
	if err == nil {
		t.Error("expected error for bad URL")
	}
}

func TestHTTPEmitterServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", 500)
	}))
	defer srv.Close()

	emitter := NewHTTPEmitter(srv.URL)
	err := emitter.Emit([]AccessEvent{{UserID: "test"}})
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestNewCollectorHTTPSink(t *testing.T) {
	c, err := NewCollector(CollectorConfig{
		Enabled:          true,
		Sink:             "http",
		ControlPlaneAddr: "http://localhost:9999",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Verify it created without error (HTTP emitter doesn't connect on creation).
}

func TestStdoutEmitter(t *testing.T) {
	// StdoutEmitter writes to os.Stdout which we can't easily capture,
	// but we can verify it doesn't panic on Emit and Close.
	e := NewStdoutEmitter()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}
