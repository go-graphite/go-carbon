package downsize

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

type hostSample struct {
	DiskFreeBytes, DiskBytes                                             uint64
	StoragePhase                                                         string
	CachePoints                                                          int64
	ReadSamples                                                          int
	Time                                                                 float64
	Generation                                                           string
	PhysicalCores                                                        int
	CPUCores, DiskUtil, DiskAwaitMS, NICBytesPerSecond, NICCapacityBytes float64
	NICIngressBytes, NICEgressBytes, DiskWriteBytes, DiskWriteCapacity   float64
	RSS, MemoryBytes, AvailableBytes                                     uint64
	Threads                                                              int
	WriteErrors, Unlogged                                                uint64
	ReadP99MS                                                            float64
	ReadsBlocked, Offline                                                bool
}
type storageSample struct {
	Hostname                    string
	MaxTransfers, CPULimit      int
	L0StopWrites                int64
	MaxBodyBytes                int64
	Protocol                    int
	L0Sublevels                 int64
	CompactionDebt, WriteStalls uint64
	WriteStallActive            bool
}
type capacity struct {
	Streams, Batch int
	BatchBytes     int64
	DeleteRate     int
}
type hostLimit struct {
	readBaseline float64
	deleteAt     time.Time
	capacity
	active              int
	previous            *hostSample
	storePrevious       *storageSample
	healthy             int
	ingressAt, egressAt time.Time
	backgroundCPU       float64
}
type tuner struct {
	mu            sync.Mutex
	changed       chan struct{}
	hosts         map[string]*hostLimit
	maximum       capacity
	mbps, readP99 float64
}

func newTuner(hosts []string, maximum capacity, auto bool, mbps, p99 float64) *tuner {
	t := &tuner{changed: make(chan struct{}), hosts: map[string]*hostLimit{}, maximum: maximum, mbps: mbps, readP99: p99}
	for _, h := range hosts {
		c := maximum
		if auto {
			c = capacity{Streams: 1, Batch: min(32, maximum.Batch), BatchBytes: min(1<<20, maximum.BatchBytes), DeleteRate: min(256, maximum.DeleteRate)}
		}
		t.hosts[h] = &hostLimit{capacity: c}
	}
	return t
}
func (t *tuner) batch(source, target string) (int, int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	a, b := t.hosts[source], t.hosts[target]
	return min(a.Batch, b.Batch), min(a.BatchBytes, b.BatchBytes)
}
func (t *tuner) admit(ctx context.Context, source, target string) (func(), error) {
	for {
		t.mu.Lock()
		a, b := t.hosts[source], t.hosts[target]
		now := time.Now()
		delay := max(a.egressAt.Sub(now), b.ingressAt.Sub(now))
		if a.active < a.Streams && b.active < b.Streams && delay <= 0 {
			a.active++
			b.active++
			t.mu.Unlock()
			return func() {
				t.mu.Lock()
				a.active--
				b.active--
				close(t.changed)
				t.changed = make(chan struct{})
				t.mu.Unlock()
			}, nil
		}
		changed := t.changed
		t.mu.Unlock()
		if delay <= 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-changed:
			}
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-changed:
			timer.Stop()
		case <-timer.C:
		}
	}
}
func (t *tuner) charge(source, target string, n int64, started time.Time) {
	if t.mbps <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	duration := time.Duration(float64(n) / (t.mbps * 1e6) * float64(time.Second))
	a, b := t.hosts[source], t.hosts[target]
	a.egressAt = maxTime(a.egressAt, started).Add(duration)
	b.ingressAt = maxTime(b.ingressAt, started).Add(duration)
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func guard(sample hostSample, store storageSample, previous *hostSample, old *storageSample, p99 float64) (hard, soft string) {
	if math.IsNaN(sample.Time) || time.Since(time.UnixMilli(int64(sample.Time*1000))) > 30*time.Second || sample.Time > float64(time.Now().Unix())+5 {
		return "stale host telemetry", ""
	}
	if sample.StoragePhase != "shared" {
		return "store has not completed conversion", ""
	}
	if sample.DiskBytes == 0 || sample.DiskFreeBytes < max(sample.DiskBytes/100, uint64(8<<30)) {
		return "hard data-drive free-space reserve", ""
	}
	if sample.CachePoints > 500000000 {
		return "hard ingest backlog", ""
	}
	if sample.PhysicalCores < 1 || sample.MemoryBytes == 0 || sample.NICCapacityBytes <= 0 {
		return "incomplete host telemetry", ""
	}
	if sample.Threads >= 8000 || sample.AvailableBytes < sample.MemoryBytes/20 || sample.RSS > sample.MemoryBytes*9/10 {
		return "hard memory/thread reserve", ""
	}
	if previous != nil {
		if sample.Generation != previous.Generation || sample.WriteErrors < previous.WriteErrors || sample.Unlogged < previous.Unlogged {
			return "host restart/counter reset", ""
		}
		if sample.WriteErrors > previous.WriteErrors || sample.Unlogged > previous.Unlogged {
			return "write errors or unlogged input points", ""
		}
	}
	if old != nil && store.WriteStalls < old.WriteStalls {
		return "store restart/counter reset", ""
	}
	stopL0 := store.L0StopWrites
	if stopL0 <= 0 {
		stopL0 = 12
	}
	if store.L0Sublevels >= stopL0-1 || store.CompactionDebt > 64<<30 {
		return "hard storage pressure", ""
	}
	if store.WriteStallActive || (old != nil && store.WriteStalls > old.WriteStalls) {
		return "", "write stall"
	}
	if sample.Threads > 6000 && previous != nil && sample.Threads-previous.Threads > 64 {
		return "", "OS thread growth"
	}
	if sample.DiskFreeBytes < max(sample.DiskBytes/10, uint64(64<<30)) {
		return "", "data-drive free-space reserve"
	}
	if previous != nil && sample.Time > previous.Time && (float64(sample.CachePoints-previous.CachePoints)/(sample.Time-previous.Time)) > 100000 && sample.CachePoints > previous.CachePoints*3/2+1000000 {
		return "", "ingest backlog growth"
	}
	if sample.CPUCores > float64(sample.PhysicalCores)-cpuReserve(sample.PhysicalCores) {
		return "", "CPU reserve"
	}
	if sample.DiskUtil > 90 && sample.DiskAwaitMS > 2 || sample.DiskAwaitMS > 10 {
		return "", "data-drive latency"
	}
	if sample.DiskWriteCapacity > 0 && sample.DiskWriteBytes >= sample.DiskWriteCapacity*.98 {
		return "", "disk write bandwidth"
	}
	if sample.NICIngressBytes >= min(1e9, sample.NICCapacityBytes)*.98 {
		return "", "NIC ingress"
	}
	if sample.NICEgressBytes >= min(1e9, sample.NICCapacityBytes)*.98 {
		return "", "NIC egress"
	}
	if sample.NICIngressBytes == 0 && sample.NICEgressBytes == 0 && sample.NICBytesPerSecond >= min(1e9, sample.NICCapacityBytes)*.98 {
		return "", "NIC bandwidth"
	}
	if sample.RSS > sample.MemoryBytes*7/10 || sample.AvailableBytes < sample.MemoryBytes/10 {
		return "", "memory reserve"
	}
	if store.L0Sublevels >= max(2, stopL0/2) || store.CompactionDebt > 8<<30 {
		return "", "compaction pressure"
	}
	if !sample.ReadsBlocked && p99 > 0 && (math.IsNaN(sample.ReadP99MS) || sample.ReadP99MS > p99) {
		return "", "read p99"
	}
	return "", ""
}
func (t *tuner) observe(host string, sample hostSample, store storageSample) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.hosts[host]
	readLimit := t.readP99
	if h.readBaseline > 0 {
		relative := max(2, h.readBaseline*1.5)
		if readLimit <= 0 || relative < readLimit {
			readLimit = relative
		}
	}
	hard, soft := guard(sample, store, h.previous, h.storePrevious, readLimit)
	if h.previous == nil {
		h.backgroundCPU = sample.CPUCores
	}
	h.previous, h.storePrevious = &sample, &store
	ceiling := t.maximum
	binding := "operator concurrency ceiling"
	limit := func(streams int, resource string) {
		if streams < ceiling.Streams {
			ceiling.Streams = streams
			binding = resource
		}
	}
	limit(max(1, int(float64(sample.PhysicalCores)-h.backgroundCPU-cpuReserve(sample.PhysicalCores))), "foreground CPU reserve")
	if store.CPULimit > 0 {
		limit(max(1, 2*store.CPULimit), "backend CPU configuration")
	}
	// Export, wire decode and staged tables can coexist on one host.
	if ceiling.BatchBytes > 0 && sample.AvailableBytes > sample.MemoryBytes/10 {
		limit(max(1, int((sample.AvailableBytes-sample.MemoryBytes/10)/uint64(4*ceiling.BatchBytes))), "memory capacity")
	}
	if store.MaxTransfers > 0 {
		limit(store.MaxTransfers, "server concurrency ceiling")
	}
	if store.MaxBodyBytes > 0 {
		ceiling.BatchBytes = min(ceiling.BatchBytes, store.MaxBodyBytes)
	}
	h.Streams = min(h.Streams, ceiling.Streams)
	h.BatchBytes = min(h.BatchBytes, ceiling.BatchBytes)
	reason := "headroom"
	if h.Streams >= ceiling.Streams {
		reason = binding
	}
	if hard != "" {
		return fmt.Errorf("%s: %s", host, hard)
	}
	if soft != "" {
		reason = soft
		h.healthy = 0
		h.Streams = max(0, h.Streams/2)
		h.Batch = max(1, h.Batch/2)
		h.BatchBytes = max(64<<10, h.BatchBytes/2)
		h.DeleteRate = max(1, h.DeleteRate/2)
	} else if sample.ReadsBlocked || sample.ReadSamples >= 20 || sample.ReadSamples == 0 {
		h.healthy++
		if h.healthy >= 2 {
			h.healthy = 0
			h.Streams = min(ceiling.Streams, h.Streams+max(1, h.Streams/2))
			h.Batch = min(ceiling.Batch, h.Batch*2)
			h.BatchBytes = min(ceiling.BatchBytes, h.BatchBytes*2)
			h.DeleteRate = min(ceiling.DeleteRate, h.DeleteRate*2)
		}
	}
	event("tune", map[string]any{"host": host, "limits": h.capacity, "reason": reason, "binding_resource": reason, "ceiling": ceiling, "background_cpu_cores": h.backgroundCPU, "sample": sample, "storage": store})
	close(t.changed)
	t.changed = make(chan struct{})
	return nil
}
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var errGuards = errors.New("host guards unavailable")

func (t *tuner) backoff(host string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.hosts[host]
	if h == nil {
		return
	}
	h.Streams = max(0, h.Streams/2)
	h.Batch = max(1, h.Batch/2)
	h.BatchBytes = max(65536, h.BatchBytes/2)
	h.DeleteRate = max(1, h.DeleteRate/2)
	h.healthy = 0
}

func (t *tuner) paceDelete(ctx context.Context, host string, count int) error {
	t.mu.Lock()
	h := t.hosts[host]
	start := maxTime(h.deleteAt, time.Now())
	h.deleteAt = start.Add(time.Duration(float64(count) / float64(max(1, h.DeleteRate)) * float64(time.Second)))
	t.mu.Unlock()
	return sleepContext(ctx, time.Until(start))
}

func (t *tuner) captureReadBaselines() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for host, h := range t.hosts {
		if h.previous != nil {
			h.backgroundCPU = h.previous.CPUCores
		}
		if h.previous != nil && h.previous.ReadSamples >= 20 && h.previous.ReadP99MS > 0 {
			h.readBaseline = h.previous.ReadP99MS
			event("read_baseline", map[string]any{"host": host, "p99_ms": h.readBaseline, "limit_ms": max(2, h.readBaseline*1.5)})
		}
	}
}

func cpuReserve(cores int) float64 {
	return min(float64(cores)/4, max(2, math.Ceil(float64(cores)/12)))
}
