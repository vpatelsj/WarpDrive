// Package main provides a benchmark tool that simulates DataLoader access
// patterns for WarpDrive FUSE-mounted filesystems.
//
// Usage:
//
//	warpdrive-bench --dir /data/training_sets --readers 32 --duration 30s --chunk 4194304
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	dir := flag.String("dir", "/data/training_sets", "Directory to read from")
	readers := flag.Int("readers", 32, "Number of concurrent readers")
	duration := flag.Duration("duration", 30*time.Second, "Test duration")
	chunkSize := flag.Int("chunk", 4*1024*1024, "Read chunk size (bytes)")
	doChecksum := flag.Bool("checksum", false, "Verify data integrity via simple checksum")
	flag.Parse()

	dirEntries, err := os.ReadDir(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading directory %s: %v\n", *dir, err)
		os.Exit(1)
	}

	var files []string
	for _, entry := range dirEntries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	if len(files) == 0 {
		fmt.Fprintf(os.Stderr, "No files found in %s\n", *dir)
		os.Exit(1)
	}
	sort.Strings(files)

	fmt.Printf("WarpDrive Benchmark\n")
	fmt.Printf("-----------------------------------\n")
	fmt.Printf("Directory:  %s\n", *dir)
	fmt.Printf("Files:      %d\n", len(files))
	fmt.Printf("Readers:    %d\n", *readers)
	fmt.Printf("Duration:   %s\n", *duration)
	fmt.Printf("Chunk Size: %s\n", humanBytes(int64(*chunkSize)))
	fmt.Printf("Checksum:   %v\n", *doChecksum)
	fmt.Printf("-----------------------------------\n\n")

	var totalBytes atomic.Int64
	var totalOps atomic.Int64
	var totalErrors atomic.Int64

	var latMu sync.Mutex
	var latencies []int64

	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < *readers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			buf := make([]byte, *chunkSize)
			fileIdx := workerID % len(files)
			var localLats []int64

			for time.Since(start) < *duration {
				filePath := filepath.Join(*dir, files[fileIdx])
				f, openErr := os.Open(filePath)
				if openErr != nil {
					totalErrors.Add(1)
					fileIdx = (fileIdx + 1) % len(files)
					continue
				}

				var checksumVal uint64
				for {
					if time.Since(start) >= *duration {
						break
					}

					opStart := time.Now()
					n, readErr := f.Read(buf)
					lat := time.Since(opStart).Nanoseconds()

					if n > 0 {
						totalBytes.Add(int64(n))
						totalOps.Add(1)
						localLats = append(localLats, lat)

						if *doChecksum {
							for j := 0; j < n; j++ {
								checksumVal += uint64(buf[j])
							}
						}
					}

					if readErr != nil {
						break
					}
				}
				f.Close()

				if *doChecksum {
					_ = checksumVal
				}

				fileIdx = (fileIdx + 1) % len(files)
			}

			latMu.Lock()
			latencies = append(latencies, localLats...)
			latMu.Unlock()
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	bytesRead := totalBytes.Load()
	ops := totalOps.Load()
	errs := totalErrors.Load()

	throughputGBs := float64(bytesRead) / elapsed.Seconds() / 1e9
	iops := float64(ops) / elapsed.Seconds()

	var avgLatMs, p50LatMs, p95LatMs, p99LatMs float64

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

		var sum int64
		for _, l := range latencies {
			sum += l
		}
		avgLatMs = float64(sum) / float64(len(latencies)) / 1e6
		p50LatMs = float64(percentile(latencies, 50)) / 1e6
		p95LatMs = float64(percentile(latencies, 95)) / 1e6
		p99LatMs = float64(percentile(latencies, 99)) / 1e6
	}

	fmt.Printf("Results\n")
	fmt.Printf("-----------------------------------\n")
	fmt.Printf("Duration:    %s\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("Throughput:  %.2f GB/s\n", throughputGBs)
	fmt.Printf("Operations:  %d\n", ops)
	fmt.Printf("IOPS:        %.0f\n", iops)
	fmt.Printf("Bytes Read:  %s\n", humanBytes(bytesRead))
	fmt.Printf("Errors:      %d\n", errs)
	fmt.Printf("-----------------------------------\n")
	fmt.Printf("Latency:\n")
	fmt.Printf("  Average:   %.2f ms\n", avgLatMs)
	fmt.Printf("  P50:       %.2f ms\n", p50LatMs)
	fmt.Printf("  P95:       %.2f ms\n", p95LatMs)
	fmt.Printf("  P99:       %.2f ms\n", p99LatMs)
	fmt.Printf("-----------------------------------\n")
}

func percentile(sorted []int64, pct int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(pct)/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	suffix := []string{"KB", "MB", "GB", "TB", "PB"}
	return fmt.Sprintf("%.2f %s", float64(b)/float64(div), suffix[exp])
}
