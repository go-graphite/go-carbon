package downsize

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
)

// backlogPolicy is saved with the run: resuming cannot relax the envelope or
// learn a new baseline from work that the controller already admitted.
type backlogPolicy struct {
	Window        time.Duration `json:"baseline_window"`
	SoftRatio     float64       `json:"soft_ratio"`
	HardRatio     float64       `json:"hard_ratio"`
	GrowthSamples int           `json:"growth_samples"`
	BytesPerPoint float64       `json:"estimated_bytes_per_point"`
}

func defaultBacklogPolicy() backlogPolicy {
	return backlogPolicy{5 * time.Minute, 1.25, 1.6, 5, 64}
}

func (p backlogPolicy) validate() error {
	if p.Window < 3*time.Minute || p.Window > 24*time.Hour ||
		math.IsNaN(p.SoftRatio) || math.IsInf(p.SoftRatio, 0) || p.SoftRatio <= 1 ||
		math.IsNaN(p.HardRatio) || math.IsInf(p.HardRatio, 0) || p.HardRatio <= p.SoftRatio ||
		p.GrowthSamples < 2 || p.GrowthSamples > 10000 ||
		math.IsNaN(p.BytesPerPoint) || math.IsInf(p.BytesPerPoint, 0) || p.BytesPerPoint < 16 || p.BytesPerPoint > 1<<20 {
		return errors.New("invalid backlog guard policy: window must be 3m..24h, 1 < soft ratio < hard ratio, growth samples 2..10000, cache bytes/point 16..1048576")
	}
	return nil
}

type backlogBaseline struct {
	Min, Median, Max int64
	Start, End       float64
	Samples          int
}

func (b *backlogBaseline) valid(window time.Duration, synthetic bool) bool {
	if b == nil || b.Min < 0 || b.Median < b.Min || b.Max < b.Median ||
		math.IsNaN(b.Start) || math.IsInf(b.Start, 0) || math.IsNaN(b.End) || math.IsInf(b.End, 0) || b.End < b.Start {
		return false
	}
	return synthetic && b.Samples >= 1 || b.Samples >= 10 && b.End-b.Start >= window.Seconds()
}

// Last observations and trend survive controller restart as well as the immutable
// baseline. This preserves counter-reset checks and a partly observed rise.
type hostAdmission struct {
	MaxSpeed                     *maxSpeedHostState `json:",omitempty"`
	Baseline                     *backlogBaseline
	Previous                     hostSample
	StorePrevious                storageSample
	Growth                       int
	ForegroundRate, ReadBaseline float64
	BackgroundCPU                float64
	DebtBaseline                 *backlogBaseline
	DebtHigh, DebtGrowth         int
	LatencyBaseline              *backlogBaseline
	LatencyHigh                  int
}

type admissionState struct {
	Policy    backlogPolicy
	Synthetic bool
	Hosts     map[string]hostAdmission
}

type backlogStatus struct {
	Baseline       *backlogBaseline `json:"baseline"`
	Current        int64            `json:"current_points"`
	Ratio          float64          `json:"ratio"`
	SoftRatio      float64          `json:"soft_ratio"`
	HardRatio      float64          `json:"hard_ratio"`
	Swing          int64            `json:"swing_points"`
	Growth         int              `json:"rising_samples"`
	GrowthRequired int              `json:"growth_samples"`
	EstimatedBytes float64          `json:"estimated_cache_bytes"`
	MemoryLimit    float64          `json:"cache_memory_limit_bytes"`
}

func backlogGuard(sample hostSample, b *backlogBaseline, p backlogPolicy, growth int) (hard, soft string, status backlogStatus) {
	status = backlogStatus{Baseline: b, Current: sample.CachePoints, SoftRatio: p.SoftRatio, HardRatio: p.HardRatio,
		Growth: growth, GrowthRequired: p.GrowthSamples, EstimatedBytes: float64(sample.CachePoints) * p.BytesPerPoint,
		MemoryLimit: float64(sample.MemoryBytes) * .9}
	if b == nil {
		return "missing pre-copy backlog baseline; observe and persist a baseline before admission", "", status
	}
	status.Ratio = float64(sample.CachePoints) / float64(max(1, b.Max))
	status.Swing = b.Max - b.Min
	if sample.CachePoints < 0 {
		return "invalid negative cache points", "", status
	}
	if status.EstimatedBytes > status.MemoryLimit {
		return "hard estimated cache memory reserve", "", status
	}
	if status.Ratio > p.HardRatio {
		return "hard ingest backlog ratio", "", status
	}
	if status.Ratio > p.SoftRatio {
		return "", "ingest backlog ratio", status
	}
	if growth >= p.GrowthSamples {
		return "", "ingest backlog growth", status
	}
	return "", "", status
}

func beyondBacklogSwing(current int64, b *backlogBaseline) bool {
	// Subtraction after the comparison avoids overflow at large point counts.
	return b != nil && current > b.Max && current-b.Max > b.Max-b.Min
}

func (t *tuner) snapshotAdmission(synthetic bool) *admissionState {
	t.mu.Lock()
	defer t.mu.Unlock()
	state := &admissionState{Policy: t.backlog, Synthetic: synthetic, Hosts: map[string]hostAdmission{}}
	for host, h := range t.hosts {
		var previous hostSample
		var store storageSample
		if h.previous != nil {
			previous = *h.previous
		}
		if h.storePrevious != nil {
			store = *h.storePrevious
		}
		var speed *maxSpeedHostState
		if h.maxSpeed != nil {
			copy := *h.maxSpeed
			speed = &copy
		}
		state.Hosts[host] = hostAdmission{MaxSpeed: speed, Baseline: h.baseline, Previous: previous, StorePrevious: store,
			Growth: h.growth, ForegroundRate: h.foregroundRate, ReadBaseline: h.readBaseline, BackgroundCPU: h.backgroundCPU,
			DebtBaseline: h.debtBaseline, DebtHigh: h.debtHigh, DebtGrowth: h.debtGrowth, LatencyBaseline: h.latencyBaseline, LatencyHigh: h.latencyHigh}
	}
	return state
}

func (t *tuner) restoreAdmission(state *admissionState, synthetic bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if state.Policy != t.backlog || state.Synthetic != synthetic || len(state.Hosts) != len(t.hosts) {
		return errors.New("saved backlog policy/hosts differ; resume with the original flags and plan")
	}
	for host, h := range t.hosts {
		saved, ok := state.Hosts[host]
		if !ok || !saved.Baseline.valid(t.backlog.Window, synthetic) || saved.Growth < 0 {
			return fmt.Errorf("%s: missing or invalid saved pre-copy backlog baseline", host)
		}
		h.baseline, h.previous, h.storePrevious = saved.Baseline, &saved.Previous, &saved.StorePrevious
		h.growth, h.foregroundRate, h.readBaseline, h.backgroundCPU = saved.Growth, saved.ForegroundRate, saved.ReadBaseline, saved.BackgroundCPU
		h.foregroundReady = saved.Previous.ForegroundMeasured
		if saved.DebtBaseline == nil && !t.collectingDebt || saved.DebtBaseline != nil && !saved.DebtBaseline.valid(t.backlog.Window, synthetic) || saved.DebtHigh < 0 || saved.DebtGrowth < 0 {
			return fmt.Errorf("%s: missing or invalid saved compaction debt baseline; run --observe-only with copying off", host)
		}
		h.debtBaseline, h.debtHigh, h.debtGrowth = saved.DebtBaseline, saved.DebtHigh, saved.DebtGrowth
		if saved.LatencyBaseline == nil && !t.collectingDebt || saved.LatencyBaseline != nil && !saved.LatencyBaseline.valid(t.backlog.Window, synthetic) || saved.LatencyHigh < 0 {
			return fmt.Errorf("%s: missing or invalid disk latency baseline; run --observe-only", host)
		}
		h.latencyBaseline, h.latencyHigh = saved.LatencyBaseline, saved.LatencyHigh
		// A resumed controller earns admission again from fresh healthy samples.
		h.Streams, h.healthy = 0, 0
		event("backlog_baseline_restored", map[string]any{"host": host, "baseline": h.baseline, "policy": t.backlog, "rising_samples": h.growth})
		if h.debtBaseline != nil {
			event("debt_baseline_restored", map[string]any{"host": host, "baseline": h.debtBaseline, "high_samples": h.debtHigh, "rising_samples": h.debtGrowth})
		}
		if h.latencyBaseline != nil {
			event("latency_baseline_restored", map[string]any{"host": host, "baseline": h.latencyBaseline, "high_samples": h.latencyHigh})
		}
	}
	return nil
}

func (t *tuner) finishBacklogObservation(synthetic bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for host, h := range t.hosts {
		if h.latencyBaseline == nil {
			b := observationBaseline(h.latencySamples, h.latencyStart, h.previous)
			if !b.valid(t.backlog.Window, synthetic) {
				return fmt.Errorf("%s: incomplete pre-copy disk latency window", host)
			}
			h.latencyBaseline, h.latencySamples = b, nil
			event("latency_baseline", map[string]any{"host": host, "baseline": b, "unit": "microseconds", "high_samples_required": 3})
		}
		if h.debtBaseline == nil {
			b := observationBaseline(h.debtSamples, h.debtStart, h.previous)
			if !b.valid(t.backlog.Window, synthetic) {
				return fmt.Errorf("%s: incomplete pre-copy compaction debt window", host)
			}
			h.debtBaseline, h.debtSamples = b, nil
			event("debt_baseline", map[string]any{"host": host, "baseline": b, "soft_limit_bytes": max(float64(8<<30), float64(b.Max)*1.25), "high_samples_required": 3, "growth_samples": t.backlog.GrowthSamples})
		}
		if h.baseline != nil {
			continue
		}
		if len(h.backlogSamples) == 0 || h.previous == nil {
			return fmt.Errorf("%s: missing pre-copy backlog observations", host)
		}
		b := observationBaseline(h.backlogSamples, h.backlogStart, h.previous)
		if !b.valid(t.backlog.Window, synthetic) {
			return fmt.Errorf("%s: incomplete pre-copy backlog window: %d samples over %.1fs; need >=10 samples over %s", host, b.Samples, b.End-b.Start, t.backlog.Window)
		}
		h.baseline, h.backlogSamples = b, nil
		h.foregroundRate, h.foregroundReady = h.writeRate, h.previous.ForegroundMeasured
		event("backlog_baseline", map[string]any{"host": host, "baseline": b, "policy": t.backlog, "synthetic": synthetic})
	}
	t.collecting = false
	t.collectingDebt = false
	return nil
}

func observationBaseline(samples []int64, start float64, previous *hostSample) *backlogBaseline {
	if len(samples) == 0 || previous == nil {
		return nil
	}
	values := slices.Clone(samples)
	slices.Sort(values)
	mid := len(values) / 2
	median := values[mid]
	if len(values)%2 == 0 {
		median = values[mid-1] + (values[mid]-values[mid-1])/2
	}
	return &backlogBaseline{Min: values[0], Median: median, Max: values[len(values)-1], Start: start, End: previous.Time, Samples: len(values)}
}

func (m *mover) saveAdmission() error {
	state := m.tuner.snapshotAdmission(m.localLab)
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.journal.saveAdmission(state)
}

func (m *mover) prepareAdmission(ctx context.Context, hosts []string) error {
	if m.journal.admission != nil {
		// Legacy journals may learn only the new debt envelope, and only during
		// an explicit observation with all transfer workers disabled.
		m.tuner.collectingDebt = m.observeOnly > 0
		if err := m.tuner.restoreAdmission(m.journal.admission, m.localLab); err != nil {
			return err
		}
		missingDebt := false
		for _, h := range m.tuner.hosts {
			missingDebt = missingDebt || h.debtBaseline == nil || h.latencyBaseline == nil
		}
		m.tuner.collectingDebt = missingDebt
		if missingDebt {
			return m.observeAdmission(ctx, hosts)
		}
		if err := m.poll(ctx, hosts); err != nil {
			return err
		}
		// Whole-store workers need their initial lease before the monitoring
		// loop starts. Earn the same two healthy samples as range admission.
		if !m.localLab {
			if err := sleepContext(ctx, m.tick); err != nil {
				return err
			}
		}
		return m.poll(ctx, hosts)
	}
	if len(m.journal.tasks) > 0 && !m.journal.nativeOnlyWork() {
		return errors.New("missing saved pre-copy backlog baseline in existing run; cannot re-baseline after copy work")
	}
	// Native-only work never enters the cache/persister. Observe the legacy
	// envelope before its first deferred metric import, without governing SSTs.
	m.tuner.collecting = true
	m.tuner.collectingDebt = true
	return m.observeAdmission(ctx, hosts)
}

func (m *mover) observeAdmission(ctx context.Context, hosts []string) error {
	event("backlog_observation", map[string]any{"window": m.tuner.backlog.Window.String(), "synthetic": m.localLab, "admission": "blocked until baseline is durable"})
	if err := m.poll(ctx, hosts); err != nil {
		return err
	}
	if !m.localLab {
		started := time.Now()
		for time.Since(started) < m.tuner.backlog.Window {
			if err := sleepContext(ctx, m.tick); err != nil {
				return err
			}
			if err := m.poll(ctx, hosts); err != nil {
				return err
			}
		}
	}
	if err := m.tuner.finishBacklogObservation(m.localLab); err != nil {
		return err
	}
	m.tuner.captureReadBaselines()
	// No transfer worker is started until the baseline has been fsynced.
	return m.saveAdmission()
}
