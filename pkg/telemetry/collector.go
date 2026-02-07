package telemetry

import (
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// CollectorConfig configures telemetry collection.
type CollectorConfig struct {
	Enabled           bool          `yaml:"enabled"`
	Sink              string        `yaml:"sink"` // "stdout", "file", "nop"
	FilePath          string        `yaml:"file_path"`
	ControlPlaneAddr  string        `yaml:"control_plane_addr"`
	SampleMetadataOps float64       `yaml:"sample_metadata_ops"` // 0.1 = 10%
	BatchSize         int           `yaml:"batch_size"`
	FlushInterval     time.Duration `yaml:"flush_interval"`
}

// Collector collects and batches access events.
type Collector struct {
	cfg     CollectorConfig
	emitter Emitter

	batch []AccessEvent
	mu    sync.Mutex

	// Async flush
	flushCh chan struct{}
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// NewCollector creates a telemetry collector.
func NewCollector(cfg CollectorConfig) (*Collector, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	if cfg.SampleMetadataOps <= 0 {
		cfg.SampleMetadataOps = 0.1
	}

	var emitter Emitter
	switch cfg.Sink {
	case "stdout":
		emitter = NewStdoutEmitter()
	case "file":
		var err error
		path := cfg.FilePath
		if path == "" {
			path = "/var/log/warpdrive/telemetry.jsonl"
		}
		emitter, err = NewFileEmitter(path)
		if err != nil {
			return nil, err
		}
	case "http":
		addr := cfg.ControlPlaneAddr
		if addr == "" {
			addr = "http://localhost:8080"
		}
		emitter = NewHTTPEmitter(addr)
	default:
		emitter = NewNopEmitter()
	}

	c := &Collector{
		cfg:     cfg,
		emitter: emitter,
		batch:   make([]AccessEvent, 0, cfg.BatchSize),
		flushCh: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}

	c.wg.Add(1)
	go c.flushLoop()
	return c, nil
}

// Record adds an access event. Non-blocking.
func (c *Collector) Record(evt AccessEvent) {
	if !c.cfg.Enabled {
		return
	}

	// Sampling for metadata ops
	if (evt.Operation == "list" || evt.Operation == "stat") && !shouldSample(c.cfg.SampleMetadataOps) {
		return
	}

	c.mu.Lock()
	c.batch = append(c.batch, evt)
	shouldFlush := len(c.batch) >= c.cfg.BatchSize
	c.mu.Unlock()

	if shouldFlush {
		select {
		case c.flushCh <- struct{}{}:
		default:
		}
	}
}

// Flush forces a flush of the current batch.
func (c *Collector) Flush() {
	c.flush()
}

// Close flushes remaining events and closes the emitter.
func (c *Collector) Close() error {
	close(c.closeCh)
	c.wg.Wait()
	return c.emitter.Close()
}

// Events returns all currently batched events (for testing).
func (c *Collector) Events() []AccessEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]AccessEvent, len(c.batch))
	copy(out, c.batch)
	return out
}

func (c *Collector) flushLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeCh:
			c.flush() // Final flush
			return
		case <-c.flushCh:
			c.flush()
		case <-ticker.C:
			c.flush()
		}
	}
}

func (c *Collector) flush() {
	c.mu.Lock()
	if len(c.batch) == 0 {
		c.mu.Unlock()
		return
	}
	batch := c.batch
	c.batch = make([]AccessEvent, 0, c.cfg.BatchSize)
	c.mu.Unlock()

	// Send to emitter (non-blocking, drop on error)
	if err := c.emitter.Emit(batch); err != nil {
		slog.Warn("telemetry flush failed", "count", len(batch), "error", err)
	}
}

func shouldSample(rate float64) bool {
	if rate >= 1.0 {
		return true
	}
	if rate <= 0 {
		return false
	}
	return rand.Float64() < rate
}
