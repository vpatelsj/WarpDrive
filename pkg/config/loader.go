package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Load reads and parses a WarpDrive configuration file.
// Supports environment variable expansion in string values via ${VAR} syntax.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config.Load: read %s: %w", path, err)
	}

	expanded := os.ExpandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("config.Load: parse %s: %w", path, err)
	}

	cfg.applyDefaults()
	if err := cfg.parseSizes(); err != nil {
		return nil, fmt.Errorf("config.Load: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config.Load: %w", err)
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.MountPoint == "" {
		c.MountPoint = "/data"
	}
	// Path and Dir are aliases; prefer Path.
	if c.Cache.Path == "" && c.Cache.Dir != "" {
		c.Cache.Path = c.Cache.Dir
	}
	if c.Cache.Path == "" {
		c.Cache.Path = "/nvme/warpdrive-cache"
	}
	if c.Cache.MaxSizeRaw == "" {
		c.Cache.MaxSizeRaw = "2TB"
	}
	if c.Cache.BlockSizeRaw == "" {
		c.Cache.BlockSizeRaw = "4MB"
	}
	// Parse human-readable sizes to int64.
	// Errors are surfaced by parseSizes(), called separately.
	if c.Cache.ReadaheadBlocks == 0 {
		c.Cache.ReadaheadBlocks = 4
	}
	if c.Cache.MaxParallelFetch == 0 {
		c.Cache.MaxParallelFetch = 16
	}
	if c.Cache.StaleTTL == 0 {
		c.Cache.StaleTTL = 60 * time.Second
	}
	if c.Cache.EvictHighWater == 0 {
		c.Cache.EvictHighWater = 0.90
	}
	if c.Cache.EvictLowWater == 0 {
		c.Cache.EvictLowWater = 0.80
	}
	if c.Metrics.Addr == "" {
		c.Metrics.Addr = ":9090"
	}
}

// parseSizes converts human-readable size strings to int64 bytes.
// Returns an error if any user-provided size string is invalid.
func (c *Config) parseSizes() error {
	v, err := ParseSize(c.Cache.MaxSizeRaw)
	if err != nil {
		return fmt.Errorf("config: invalid cache.max_size %q: %w", c.Cache.MaxSizeRaw, err)
	}
	c.Cache.MaxSize = v

	v, err = ParseSize(c.Cache.BlockSizeRaw)
	if err != nil {
		return fmt.Errorf("config: invalid cache.block_size %q: %w", c.Cache.BlockSizeRaw, err)
	}
	c.Cache.BlockSize = v
	return nil
}

// ParseSize converts a human-readable size like "2TB", "500GB", "4MB" to bytes.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" || s == "0" {
		return 0, nil
	}

	multipliers := []struct {
		suffix string
		mult   int64
	}{
		{"PB", 1024 * 1024 * 1024 * 1024 * 1024},
		{"TB", 1024 * 1024 * 1024 * 1024},
		{"GB", 1024 * 1024 * 1024},
		{"MB", 1024 * 1024},
		{"KB", 1024},
		{"B", 1},
	}

	for _, m := range multipliers {
		if strings.HasSuffix(s, m.suffix) {
			numStr := strings.TrimSuffix(s, m.suffix)
			num, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return 0, fmt.Errorf("config.ParseSize: invalid size %q: %w", s, err)
			}
			return int64(num * float64(m.mult)), nil
		}
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("config.ParseSize: invalid size %q: %w", s, err)
	}
	return n, nil
}
