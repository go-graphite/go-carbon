package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

type resourceLease struct {
	Version          int    `json:"version"`
	SourceID         string `json:"source_id"`
	Generation       uint64 `json:"generation"`
	ExpiresAt        int64  `json:"expires_at"`
	CPU              int    `json:"cpu"`
	Workers          int    `json:"workers"`
	MemoryLimitBytes int64  `json:"memory_limit_bytes"`
}

type workerGate struct {
	mu            sync.Mutex
	limit, active int
	wake          chan struct{}
}

func newWorkerGate(limit int) *workerGate {
	return &workerGate{limit: limit, wake: make(chan struct{}, 1)}
}

func (g *workerGate) signal() {
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

func (g *workerGate) acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		g.mu.Lock()
		if g.active < g.limit {
			g.active++
			if g.active < g.limit {
				g.signal()
			}
			g.mu.Unlock()
			return nil
		}
		wake := g.wake
		g.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *workerGate) release() {
	g.mu.Lock()
	g.active--
	g.signal()
	g.mu.Unlock()
}

func (g *workerGate) setLimit(limit int) {
	g.mu.Lock()
	g.limit = limit
	close(g.wake)
	g.wake = make(chan struct{}, 1)
	g.mu.Unlock()
}

func (g *workerGate) activeCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active
}

type resourceControl struct {
	mu             sync.Mutex
	current        resourceLease
	fallback       resourceLease
	gate           *workerGate
	maxCPU         int
	maxWorkers     int
	lastGeneration uint64
}

func newResourceControl(sourceID string, cpu, workers int, memory int64, maxCPU, maxWorkers int) *resourceControl {
	initial := resourceLease{Version: 1, SourceID: sourceID, CPU: cpu, Workers: workers, MemoryLimitBytes: memory}
	fallback := initial
	fallback.CPU = min(cpu, 8)
	fallback.Workers = min(workers, 8)
	if fallback.MemoryLimitBytes > 8<<30 {
		fallback.MemoryLimitBytes = 8 << 30
	}
	r := &resourceControl{current: initial, fallback: fallback, gate: newWorkerGate(workers), maxCPU: maxCPU, maxWorkers: maxWorkers}
	runtime.GOMAXPROCS(cpu)
	if memory > 0 {
		debug.SetMemoryLimit(memory)
	}
	return r
}

func readResourceLease(path, sourceID string, now time.Time, maxCPU, maxWorkers int) (resourceLease, error) {
	f, err := os.Open(path)
	if err != nil {
		return resourceLease{}, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return resourceLease{}, err
	}
	if len(data) > 4096 {
		return resourceLease{}, errors.New("resource lease exceeds 4096 bytes")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var lease resourceLease
	if err := decoder.Decode(&lease); err != nil {
		return lease, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return lease, errors.New("resource lease contains trailing data")
	}
	if lease.Version != 1 || lease.SourceID != sourceID || lease.Generation == 0 || lease.CPU < 1 || lease.CPU > maxCPU || lease.Workers < 0 || lease.Workers > maxWorkers || lease.MemoryLimitBytes < 64<<20 || lease.MemoryLimitBytes > 128<<30 {
		return lease, errors.New("resource lease identity or budgets are invalid")
	}
	if lease.ExpiresAt <= now.Unix() || lease.ExpiresAt > now.Unix()+300 {
		return lease, errors.New("resource lease expired or exceeds five minutes")
	}
	return lease, nil
}

func (r *resourceControl) apply(lease resourceLease) {
	r.mu.Lock()
	defer r.mu.Unlock()
	changed := lease.CPU != r.current.CPU || lease.Workers != r.current.Workers || lease.MemoryLimitBytes != r.current.MemoryLimitBytes
	if changed {
		r.gate.setLimit(lease.Workers)
		runtime.GOMAXPROCS(lease.CPU)
		if lease.MemoryLimitBytes > 0 {
			debug.SetMemoryLimit(lease.MemoryLimitBytes)
		}
		data, _ := json.Marshal(lease)
		fmt.Fprintf(os.Stderr, "resource-control applied %s\n", data)
	}
	r.current = lease
}

func (r *resourceControl) snapshot() resourceLease {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current
}

func (r *resourceControl) watch(ctx context.Context, path string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastError := ""
	for {
		lease, err := readResourceLease(path, r.fallback.SourceID, time.Now(), r.maxCPU, r.maxWorkers)
		if err == nil {
			r.mu.Lock()
			if lease.Generation < r.lastGeneration {
				err = errors.New("resource lease generation regressed")
			} else {
				r.lastGeneration = lease.Generation
			}
			r.mu.Unlock()
		}
		if err != nil {
			lease = r.fallback
			if err.Error() != lastError {
				fmt.Fprintf(os.Stderr, "resource-control fallback: %v\n", err)
				lastError = err.Error()
			}
		} else {
			lastError = ""
		}
		r.apply(lease)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
