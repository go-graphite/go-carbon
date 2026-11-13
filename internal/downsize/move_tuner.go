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
	OldestUnpersistedAgeSeconds                                          *float64 `json:",omitempty"`
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
	WritePoints                                                          uint64
	ForegroundMeasured                                                   bool
	IngestPoints, IngestRelogged, IngestCycles                           uint64
	IngestMeasured                                                       bool
	ReadP99MS                                                            float64
	ReadP99Rolling5mMS, ReadWindowSeconds                                float64
	ReadRolling5mSamples                                                 int
	ReadWindowReady                                                      bool
	ReadP99Rolling1mMS, Read1mWindowSeconds, ProcessUptimeSeconds        float64
	ReadRolling1mSamples                                                 int
	ReadDiagnostics                                                      []string `json:",omitempty"`
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
	PendingDeleteBatches        bool
	StagedCopyVerification      bool
}
type capacity struct {
	Streams, Batch int
	BatchBytes     int64
	DeleteRate     int
}
type hostLimit struct {
	readBaseline float64
	readSamples  []float64
	deleteAt     time.Time
	capacity
	active                    int
	previous                  *hostSample
	storePrevious             *storageSample
	healthy                   int
	ingressAt, egressAt       time.Time
	backgroundCPU             float64
	writeRate, foregroundRate float64
	baseline                  *backlogBaseline
	backlogSamples            []int64
	backlogStart              float64
	growth                    int
	debtBaseline              *backlogBaseline
	debtSamples               []int64
	debtStart                 float64
	debtGrowth, debtHigh      int
	latencyBaseline           *backlogBaseline
	latencySamples            []int64
	latencyStart              float64
	latencyHigh               int
	hard                      string
	maxSpeed                  *maxSpeedHostState
	foregroundReady           bool
	// downSince marks a host whose telemetry or status is unreachable (a
	// go-carbon restart); admission is parked, never failed, until it is back.
	downSince   time.Time
	downStreams int
}
type tuner struct {
	mu             sync.Mutex
	changed        chan struct{}
	hosts          map[string]*hostLimit
	maximum        capacity
	mbps           float64
	read           readPolicy
	overrides      []policyOverride
	backlog        backlogPolicy
	collecting     bool
	collectingDebt bool
	debtGuardOff   bool
	maxSpeed       *maxSpeedGovernor
}

func newTuner(hosts []string, maximum capacity, auto bool, mbps, p99 float64) *tuner {
	read := defaultReadPolicy()
	read.CeilingMS = p99
	t := &tuner{changed: make(chan struct{}), hosts: map[string]*hostLimit{}, maximum: maximum, mbps: mbps, read: read, backlog: defaultBacklogPolicy()}
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

// A single metric cannot be split. Wait for healthy observations to grow the
// adaptive byte budget, without exceeding either server's configured ceiling.
func (t *tuner) waitLargerBatch(ctx context.Context, source, target string, previous int64) (bool, error) {
	for {
		t.mu.Lock()
		ceiling := t.maximum.BatchBytes
		for _, host := range []string{source, target} {
			if store := t.hosts[host].storePrevious; store != nil && store.MaxBodyBytes > 0 {
				ceiling = min(ceiling, store.MaxBodyBytes)
			}
		}
		budget := min(t.hosts[source].BatchBytes, t.hosts[target].BatchBytes)
		changed := t.changed
		t.mu.Unlock()
		if previous >= ceiling {
			return false, nil
		}
		if budget > previous {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-changed:
		}
	}
}
func (t *tuner) admit(ctx context.Context, source, target string) (func(), error) {
	for {
		t.mu.Lock()
		a, b := t.hosts[source], t.hosts[target]
		for _, h := range []*hostLimit{a, b} {
			if t.maxSpeed == nil && (h.hard != "" || h.baseline == nil || h.debtBaseline == nil || h.latencyBaseline == nil || t.collectingDebt) {
				t.mu.Unlock()
				return nil, fmt.Errorf("copy admission unavailable: hard=%q; pre-copy backlog baseline required", h.hard)
			}
		}
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
	if math.IsNaN(sample.Time) || math.IsInf(sample.Time, 0) || time.Since(time.UnixMilli(int64(sample.Time*1000))) > 30*time.Second || sample.Time > float64(time.Now().Unix())+5 {
		return "stale host telemetry", ""
	}
	if sample.StoragePhase != "shared" {
		return "store has not completed conversion", ""
	}
	if sample.DiskBytes == 0 || sample.DiskFreeBytes < max(sample.DiskBytes/100, uint64(8<<30)) {
		return "hard data-drive free-space reserve", ""
	}
	if sample.CachePoints < 0 {
		return "invalid negative cache points", ""
	}
	if sample.PhysicalCores < 1 || sample.MemoryBytes == 0 || sample.NICCapacityBytes <= 0 {
		return "incomplete host telemetry", ""
	}
	if sample.Threads >= 8000 || sample.AvailableBytes < sample.MemoryBytes/20 || sample.RSS > sample.MemoryBytes*9/10 {
		return "hard memory/thread reserve", ""
	}
	// A restart or counter reset is re-baselined by the caller (previous and
	// old arrive nil), never a hard stop: the copy waits and continues.
	if previous != nil {
		if sample.WriteErrors > previous.WriteErrors || sample.Unlogged > previous.Unlogged {
			return "write errors or unlogged input points", ""
		}
		if sample.Time <= previous.Time {
			return "non-advancing host telemetry", ""
		}
	}
	stopL0 := store.L0StopWrites
	if stopL0 <= 0 {
		stopL0 = 12
	}
	if store.L0Sublevels >= stopL0-1 || store.CompactionDebt > 64<<30 {
		return "hard storage pressure", ""
	}
	if store.WriteStallActive || (old != nil && store.WriteStalls > old.WriteStalls) {
		return "write stall", ""
	}
	if sample.Threads > 6000 && previous != nil && sample.Threads-previous.Threads > 64 {
		return "", "OS thread growth"
	}
	if sample.DiskFreeBytes < max(sample.DiskBytes/10, uint64(64<<30)) {
		return "", "data-drive free-space reserve"
	}
	if sample.CPUCores > float64(sample.PhysicalCores)-cpuReserve(sample.PhysicalCores) {
		return "", "CPU reserve"
	}
	if sample.DiskWriteCapacity > 0 && sample.DiskWriteBytes >= sample.DiskWriteCapacity*.98 {
		return "", "disk write bandwidth"
	}
	if sample.NICIngressBytes >= sample.NICCapacityBytes*.98 {
		return "", "NIC ingress"
	}
	if sample.NICEgressBytes >= sample.NICCapacityBytes*.98 {
		return "", "NIC egress"
	}
	if sample.NICIngressBytes == 0 && sample.NICEgressBytes == 0 && sample.NICBytesPerSecond >= sample.NICCapacityBytes*.98 {
		return "", "NIC bandwidth"
	}
	if sample.RSS > sample.MemoryBytes*7/10 || sample.AvailableBytes < sample.MemoryBytes/10 {
		return "", "memory reserve"
	}
	if store.L0Sublevels >= max(2, stopL0/2) {
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
	if t.maxSpeed != nil {
		return t.observeMaxSpeedLocked(host, sample, store)
	}
	if !h.downSince.IsZero() {
		event("host_back", map[string]any{"host": host, "down_seconds": time.Since(h.downSince).Seconds(), "streams": max(1, h.downStreams/2)})
		h.Streams = max(1, h.downStreams/2)
		h.downSince, h.downStreams = time.Time{}, 0
	}
	rebaselined := h.rebaseline(host, sample, store)
	hard, soft := guard(sample, store, h.previous, h.storePrevious, t.read.limit(h.readBaseline))
	if rebaselined != "" && soft == "" {
		soft = "host restart: re-baselined"
	}
	if (t.collecting || t.collectingDebt) && validReadSample(sample) {
		h.readSamples = append(h.readSamples, sample.ReadP99MS)
	}
	debt := h.observeDebt(sample, store, t.backlog.GrowthSamples, t.collectingDebt || t.collecting)
	latency := h.observeLatency(sample, t.collectingDebt || t.collecting)
	if latency.Soft != "" && soft == "" {
		soft = latency.Soft
	}
	if !t.debtGuardOff && !t.collectingDebt && !t.collecting && h.debtBaseline == nil && hard == "" {
		hard = "missing pre-copy compaction debt baseline; run --observe-only before copying"
	}
	if !t.collectingDebt && !t.collecting && h.latencyBaseline == nil && hard == "" {
		hard = "missing pre-copy disk latency baseline; run --observe-only before copying"
	}
	if debt.Soft != "" && soft == "" && !t.debtGuardOff {
		soft = debt.Soft
	}
	if hard == "" && h.previous != nil && sample.ForegroundMeasured && sample.Time > h.previous.Time {
		h.writeRate = float64(sample.WritePoints-h.previous.WritePoints) / (sample.Time - h.previous.Time)
	}
	if beyondBacklogSwing(sample.CachePoints, h.baseline) {
		if h.previous != nil && beyondBacklogSwing(h.previous.CachePoints, h.baseline) && sample.CachePoints > h.previous.CachePoints {
			h.growth++
		} else {
			h.growth = 1
		}
	} else {
		h.growth = 0
	}
	backlogHard, backlogSoft, backlog := backlogGuard(sample, h.baseline, t.backlog, h.growth)
	if t.collecting && h.baseline == nil {
		// Collection runs the other guards but cannot admit any copy work.
		// The memory-derived bound applies even before an envelope exists.
		backlogHard = ""
		if backlog.EstimatedBytes > backlog.MemoryLimit {
			backlogHard = "hard estimated cache memory reserve"
		}
		if hard == "" && backlogHard == "" {
			if len(h.backlogSamples) == 0 {
				h.backlogStart = sample.Time
			}
			h.backlogSamples = append(h.backlogSamples, sample.CachePoints)
		}
	}
	if hard == "" {
		hard = backlogHard
	}
	if backlogSoft != "" {
		soft = backlogSoft
	}
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
		h.hard, h.Streams = hard, 0
		event("tune", map[string]any{"host": host, "decision": "hard", "reason": hard, "binding_resource": hard, "limits": h.capacity, "backlog": backlog, "debt": debt, "latency": latency, "sample": sample, "storage": store})
		close(t.changed)
		t.changed = make(chan struct{})
		return fmt.Errorf("%s: %s (baseline=%+v current=%d ratio=%.4f soft_ratio=%.4f hard_ratio=%.4f)", host, hard, h.baseline, sample.CachePoints, backlog.Ratio, t.backlog.SoftRatio, t.backlog.HardRatio)
	}
	if soft != "" {
		reason = soft
		h.healthy = 0
		h.Streams = max(0, h.Streams/2)
		if soft == "foreground write floor" || soft == "ingest backlog ratio" || soft == "ingest backlog growth" || soft == "write stall" {
			h.Streams = 0
		}
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
	decision := "healthy"
	if soft != "" {
		decision = "soft"
	}
	event("tune", map[string]any{"host": host, "decision": decision, "limits": h.capacity, "reason": reason, "binding_resource": reason, "ceiling": ceiling, "background_cpu_cores": h.backgroundCPU, "backlog": backlog, "debt": debt, "latency": latency, "sample": sample, "storage": store})
	close(t.changed)
	t.changed = make(chan struct{})
	return nil
}

// counterReset reports why sample cannot be compared with previous: a new
// process generation or a cumulative counter that went backwards.
func counterReset(sample, previous hostSample) string {
	switch {
	case sample.Generation != previous.Generation:
		return "process generation changed"
	case sample.WriteErrors < previous.WriteErrors || sample.Unlogged < previous.Unlogged || sample.WritePoints < previous.WritePoints:
		return "host counter reset"
	case sample.ForegroundMeasured != previous.ForegroundMeasured:
		return "foreground measurement changed"
	}
	return ""
}

// rebaseline drops the previous samples a restart made incomparable (also a
// pre-restart Previous restored from the journal), so this tick's delta
// guards, write rate and debt growth start fresh instead of aborting.
func (h *hostLimit) rebaseline(host string, sample hostSample, store storageSample) string {
	reason := ""
	fields := map[string]any{"host": host, "new_generation": sample.Generation}
	if h.previous != nil {
		if why := counterReset(sample, *h.previous); why != "" {
			reason = why
			fields["old_generation"] = h.previous.Generation
			h.previous = nil
		}
	}
	if h.storePrevious != nil && store.WriteStalls < h.storePrevious.WriteStalls {
		if reason == "" {
			reason = "store counter reset"
		}
		fields["old_write_stalls"], fields["write_stalls"] = h.storePrevious.WriteStalls, store.WriteStalls
		h.storePrevious = nil
	}
	if reason != "" {
		fields["reason"] = reason
		event("rebaseline", fields)
	}
	return reason
}

// hostUnavailable parks admission for a host whose telemetry or status is
// unreachable (restart in progress). It never fails: the copy waits.
func (t *tuner) hostUnavailable(host string, cause error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.hosts[host]
	if h == nil {
		return
	}
	if h.downSince.IsZero() {
		h.downSince, h.downStreams = time.Now(), h.Streams
	}
	h.Streams, h.healthy = 0, 0
	event("host_unavailable", map[string]any{"level": "warn", "host": host, "down_seconds": time.Since(h.downSince).Seconds(), "error": cause.Error()})
	close(t.changed)
	t.changed = make(chan struct{})
}

// hostUp reports whether the tuner currently sees host as reachable.
func (t *tuner) hostUp(host string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.hosts[host]
	return h == nil || h.downSince.IsZero()
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
	if t.maxSpeed != nil {
		if h.Streams > 0 {
			h.Streams = max(1, h.Streams/2)
		}
		h.healthy = 0
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
		// The median of the observation window's samples, not one reading at
		// its end; later captures (read isolation, whole-store) use the latest.
		samples, source := h.readSamples, "observation median"
		h.readSamples = nil
		if len(samples) == 0 && h.previous != nil && h.previous.ReadSamples >= 20 && h.previous.ReadP99MS > 0 {
			samples, source = []float64{h.previous.ReadP99MS}, "latest sample"
		}
		if len(samples) > 0 {
			h.readBaseline = medianFloat(samples)
			event("read_baseline", map[string]any{"host": host, "p99_ms": h.readBaseline, "samples": len(samples), "source": source, "limit_ms": t.read.limit(h.readBaseline), "policy": t.read})
		}
	}
}

func validReadSample(s hostSample) bool {
	return !s.ReadsBlocked && s.ReadSamples >= 20 && s.ReadP99MS > 0 && !math.IsInf(s.ReadP99MS, 0)
}

func cpuReserve(cores int) float64 {
	return min(float64(cores)/4, max(2, math.Ceil(float64(cores)/12)))
}
