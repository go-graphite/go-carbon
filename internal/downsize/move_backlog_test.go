package downsize

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Other tuner tests start with an explicitly supplied empty-store baseline.
// Production construction never supplies one implicitly.
func newTestTuner(hosts []string, maximum capacity, auto bool, mbps, p99 float64) *tuner {
	t := newTuner(hosts, maximum, auto, mbps, p99)
	for _, h := range t.hosts {
		h.baseline = &backlogBaseline{Samples: 1}
		h.debtBaseline = &backlogBaseline{Samples: 10, Start: 1, End: 301}
		h.latencyBaseline = &backlogBaseline{Samples: 10, Start: 1, End: 301}
	}
	return t
}

func backlogTestSample(points int64) hostSample {
	return hostSample{Time: float64(time.Now().UnixNano())/1e9 - 2, Generation: "one", StoragePhase: "shared",
		DiskFreeBytes: 500 << 30, DiskBytes: 1 << 40, PhysicalCores: 96,
		MemoryBytes: 1536 << 30, AvailableBytes: 768 << 30, NICCapacityBytes: 3e9,
		ForegroundMeasured: true, WritePoints: 1000000, CachePoints: points}
}

func TestBacklogGuardClusterBaselines(t *testing.T) {
	// Model temporal envelopes with synthetic host ranges; these fixtures do not claim that a cross-host range is a real
	// pre-copy temporal baseline. Tuning rounds up to the requested 250M case.
	for _, cluster := range []struct {
		name     string
		min, max int64
	}{
		{"pool-a", 788119826, 819004345},
		{"pool-slow", 1390815083, 1406582395},
		{"pool-e", 965764725, 997695599},
		{"pool-d", 237441989, 250000000},
		{"pool-c", 115305761, 115822979},
	} {
		t.Run(cluster.name, func(t *testing.T) {
			b := &backlogBaseline{Min: cluster.min, Median: cluster.min + (cluster.max-cluster.min)/2, Max: cluster.max}
			p := defaultBacklogPolicy()
			for _, tc := range []struct {
				name       string
				current    int64
				hard, soft string
			}{
				{"steady-min", b.Min, "", ""},
				{"steady-max", b.Max, "", ""},
				{"above-old-five-percent-envelope", int64(float64(b.Max) * 1.1), "", ""},
				{"soft-boundary", int64(float64(b.Max) * p.SoftRatio), "", ""},
				{"soft-growth", int64(float64(b.Max)*p.SoftRatio) + 1, "", "ingest backlog ratio"},
				{"hard-boundary", int64(float64(b.Max) * p.HardRatio), "", "ingest backlog ratio"},
				{"hard-growth", int64(float64(b.Max)*p.HardRatio) + 1, "hard ingest backlog ratio", ""},
			} {
				t.Run(tc.name, func(t *testing.T) {
					hard, soft, status := backlogGuard(backlogTestSample(tc.current), b, p, 0)
					if hard != tc.hard || soft != tc.soft || status.Current != tc.current || status.Baseline != b || status.Ratio == 0 {
						t.Fatalf("hard=%q soft=%q status=%+v", hard, soft, status)
					}
				})
			}
		})
	}
}

func TestBacklogMissingBaselineAndMemory(t *testing.T) {
	for _, tc := range []struct {
		name     string
		baseline *backlogBaseline
		points   int64
		memory   uint64
		want     string
	}{
		{"missing", nil, 1400000000, 1536 << 30, "missing pre-copy backlog baseline"},
		{"memory-derived-ceiling", &backlogBaseline{Max: 1400000000}, 1400000000, 64 << 30, "hard estimated cache memory reserve"},
		{"no-fixed-point-cap", &backlogBaseline{Max: 1400000000}, 1400000000, 1536 << 30, ""},
		{"empty-baseline", &backlogBaseline{}, 0, 1536 << 30, ""},
		{"empty-baseline-growth", &backlogBaseline{}, 100, 1536 << 30, "hard ingest backlog ratio"},
		{"negative", &backlogBaseline{Max: 10}, -1, 1536 << 30, "invalid negative cache points"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sample := backlogTestSample(tc.points)
			sample.MemoryBytes = tc.memory
			hard, _, status := backlogGuard(sample, tc.baseline, defaultBacklogPolicy(), 0)
			if tc.want == "" && hard != "" || tc.want != "" && !strings.Contains(hard, tc.want) {
				t.Fatalf("hard=%q status=%+v", hard, status)
			}
		})
	}
	missing := newTuner([]string{"a", "b"}, capacity{8, 32, 1 << 20, 100}, true, 0, 100)
	if err := missing.observe("a", backlogTestSample(1400000000), storageSample{}); err == nil || !strings.Contains(err.Error(), "missing pre-copy") {
		t.Fatal("observation silently accepted missing baseline", err)
	}
	if _, err := missing.admit(context.Background(), "a", "b"); err == nil {
		t.Fatal("admitted work without baseline")
	}
}

func TestBacklogObservationWindowAndMedian(t *testing.T) {
	tune := newTuner([]string{"a"}, capacity{8, 32, 1 << 20, 100}, true, 0, 100)
	tune.collecting = true
	sample := backlogTestSample(100)
	for _, points := range []int64{80, 110, 100, 90, 105, 100, 110, 95, 90, 100} {
		sample.Time += .01
		sample.CachePoints = points
		if err := tune.observe("a", sample, storageSample{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tune.finishBacklogObservation(false); err == nil {
		t.Fatal("accepted a sub-minute observation window")
	}
	// Simulate elapsed observation time without making the test sleep for 5m.
	tune.hosts["a"].backlogStart = sample.Time - tune.backlog.Window.Seconds()
	tune.hosts["a"].debtStart = tune.hosts["a"].backlogStart
	tune.hosts["a"].latencyStart = tune.hosts["a"].backlogStart
	if err := tune.finishBacklogObservation(false); err != nil {
		t.Fatal(err)
	}
	b := tune.hosts["a"].baseline
	if b.Min != 80 || b.Median != 100 || b.Max != 110 || b.Samples != 10 || len(tune.hosts["a"].backlogSamples) != 0 {
		t.Fatalf("wrong envelope: %+v", b)
	}
	if b.valid(6*time.Minute, false) || (&backlogBaseline{Samples: 100, Min: 2, Median: 1, Max: 3}).valid(0, true) {
		t.Fatal("accepted insufficient window or corrupt baseline")
	}
}

func TestBacklogTunerDBSteadyGrowthRecoveryAndResume(t *testing.T) {
	tune := newTestTuner([]string{"a", "b"}, capacity{16, 8192, 64 << 20, 10000}, true, 0, 100)
	sample := backlogTestSample(1400000000)
	tune.hosts["a"].baseline = &backlogBaseline{Min: 1390000000, Median: 1400000000, Max: 1407000000, Samples: 16, Start: sample.Time - 300, End: sample.Time}
	tune.hosts["b"].baseline = tune.hosts["a"].baseline
	observe := func(points int64) {
		t.Helper()
		sample.Time += .01
		sample.WritePoints += 1000
		sample.CachePoints = points
		if err := tune.observe("a", sample, storageSample{}); err != nil {
			t.Fatal(err)
		}
	}
	for range 16 {
		observe(1400000000)
	}
	if tune.hosts["a"].Streams != 16 {
		t.Fatal("steady 1.4B db backlog failed to ramp", tune.hosts["a"].Streams)
	}
	// A normal baseline swing and a non-monotonic spike do not trip the trend.
	for _, points := range []int64{1401000000, 1406000000, 1400000000, 1425000000, 1426000000, 1425000000} {
		observe(points)
	}
	if tune.hosts["a"].Streams == 0 || tune.hosts["a"].growth != 1 {
		t.Fatal("normal swing/non-monotonic spike paused admission")
	}
	for _, points := range []int64{1426000000, 1427000000, 1428000000, 1429000000} {
		observe(points)
	}
	if tune.hosts["a"].Streams != 0 || tune.hosts["a"].growth != 5 {
		t.Fatal("sustained growth below soft ratio did not pause", tune.hosts["a"].growth)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := tune.admit(ctx, "a", "b"); err == nil {
		t.Fatal("growth pause admitted copy")
	}
	// Paused state and its original envelope survive fsync, compaction, a torn
	// final write and a new controller. No inflated baseline is learned.
	tune.hosts["b"].previous, tune.hosts["b"].storePrevious = &sample, &storageSample{}
	path := filepath.Join(t.TempDir(), "state")
	j, err := openJournal(path, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if err := j.saveAdmission(tune.snapshotAdmission(false)); err != nil {
		t.Fatal(err)
	}
	if err := j.compact(); err != nil {
		t.Fatal(err)
	}
	j.close()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString(`{"Admission":`)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = openJournal(path, "plan")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	tune = newTuner([]string{"a", "b"}, tune.maximum, true, 0, 100)
	if err := tune.restoreAdmission(j.admission, false); err != nil {
		t.Fatal(err)
	}
	observe(1430000000)
	if tune.hosts["a"].Streams != 0 || tune.hosts["a"].baseline.Max != 1407000000 || tune.hosts["a"].growth != 6 {
		t.Fatal("restart forgot growth or relearned backlog")
	}
	observe(1400000000)
	if tune.hosts["a"].Streams != 0 {
		t.Fatal("resumed before two healthy samples")
	}
	observe(1400000000)
	if tune.hosts["a"].Streams != 1 {
		t.Fatal("failed conservative recovery")
	}
	observe(1800000000)
	if tune.hosts["a"].Streams != 0 {
		t.Fatal("soft ratio did not pause")
	}
	sample.CachePoints = 2300000000
	sample.Time += .01
	if err := tune.observe("a", sample, storageSample{}); err == nil || !strings.Contains(err.Error(), "hard ingest backlog ratio") {
		t.Fatal("hard ratio did not abort", err)
	}
	if _, err := tune.admit(context.Background(), "a", "b"); err == nil {
		t.Fatal("hard stop admitted work")
	}
}

func TestBacklogRestartAndCounterReset(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*hostSample, *storageSample)
	}{
		{"generation", func(s *hostSample, _ *storageSample) { s.Generation = "two" }},
		{"committed-points", func(s *hostSample, _ *storageSample) { s.WritePoints-- }},
		{"write-errors-reset", func(s *hostSample, _ *storageSample) { s.WriteErrors-- }},
		{"unlogged-reset", func(s *hostSample, _ *storageSample) { s.Unlogged-- }},
		{"write-errors-growth", func(s *hostSample, _ *storageSample) { s.WriteErrors++ }},
		{"unlogged-growth", func(s *hostSample, _ *storageSample) { s.Unlogged++ }},
		{"store-reset", func(_ *hostSample, s *storageSample) { s.WriteStalls-- }},
		{"memory-reserve", func(s *hostSample, _ *storageSample) { s.AvailableBytes = 1 }},
		{"rss-reserve", func(s *hostSample, _ *storageSample) { s.RSS = s.MemoryBytes }},
		{"repeated-sample", func(s *hostSample, _ *storageSample) { s.Time -= .01 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tune := newTestTuner([]string{"a"}, capacity{8, 32, 1 << 20, 100}, true, 0, 100)
			sample := backlogTestSample(1400000000)
			sample.WriteErrors, sample.Unlogged = 2, 2
			store := storageSample{WriteStalls: 2}
			h := tune.hosts["a"]
			h.baseline = &backlogBaseline{Min: sample.CachePoints, Median: sample.CachePoints, Max: sample.CachePoints, Samples: 16, Start: sample.Time - 300, End: sample.Time}
			if err := tune.observe("a", sample, store); err != nil {
				t.Fatal(err)
			}
			saved := tune.snapshotAdmission(false)
			tune = newTuner([]string{"a"}, tune.maximum, true, 0, 100)
			if err := tune.restoreAdmission(saved, false); err != nil {
				t.Fatal(err)
			}
			sample.Time += .01
			tc.change(&sample, &store)
			if err := tune.observe("a", sample, store); err == nil {
				t.Fatal("reset/durability/memory guard passed after controller restart")
			}
		})
	}
}

func TestBacklogPolicyAndLegacyResume(t *testing.T) {
	for _, change := range []func(*backlogPolicy){
		func(p *backlogPolicy) { p.Window = time.Minute },
		func(p *backlogPolicy) { p.SoftRatio = 1 },
		func(p *backlogPolicy) { p.HardRatio = p.SoftRatio },
		func(p *backlogPolicy) { p.HardRatio = math.Inf(1) },
		func(p *backlogPolicy) { p.SoftRatio = math.NaN() },
		func(p *backlogPolicy) { p.GrowthSamples = 1 },
		func(p *backlogPolicy) { p.BytesPerPoint = 15 },
	} {
		p := defaultBacklogPolicy()
		change(&p)
		if p.validate() == nil {
			t.Fatal("invalid guard policy accepted", p)
		}
	}
	root := t.TempDir()
	j, err := openJournal(filepath.Join(root, "state"), "plan")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err := j.save(task{Start: 0, End: 1, Stream: "copy", Phase: "done"}); err != nil {
		t.Fatal(err)
	}
	m := &mover{journal: j, tuner: newTuner([]string{"a"}, capacity{}, true, 0, 100)}
	if err := m.prepareAdmission(context.Background(), []string{"a"}); err == nil || !strings.Contains(err.Error(), "cannot re-baseline") {
		t.Fatal("legacy run re-baselined", err)
	}
	// Numeric status must remain encodable for the controller's JSON output.
	_, _, status := backlogGuard(backlogTestSample(0), &backlogBaseline{}, defaultBacklogPolicy(), 0)
	if _, err := json.Marshal(status); err != nil {
		t.Fatal(err)
	}
}

func TestBacklogRestoreRejectsChangedOrMissingBaseline(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*admissionState)
	}{
		{"changed-policy", func(s *admissionState) { s.Policy.SoftRatio = 1.5 }},
		{"missing-host", func(s *admissionState) { delete(s.Hosts, "a") }},
		{"missing-baseline", func(s *admissionState) { h := s.Hosts["a"]; h.Baseline = nil; s.Hosts["a"] = h }},
		{"short-window", func(s *admissionState) { s.Hosts["a"].Baseline.End = s.Hosts["a"].Baseline.Start + 30 }},
		{"too-few-samples", func(s *admissionState) { s.Hosts["a"].Baseline.Samples = 2 }},
		{"synthetic-in-production", func(s *admissionState) { s.Synthetic = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tune := newTestTuner([]string{"a"}, capacity{8, 32, 1 << 20, 100}, true, 0, 100)
			sample := backlogTestSample(1400000000)
			tune.hosts["a"].baseline = &backlogBaseline{Min: sample.CachePoints, Median: sample.CachePoints, Max: sample.CachePoints, Samples: 16, Start: sample.Time - 300, End: sample.Time}
			if err := tune.observe("a", sample, storageSample{}); err != nil {
				t.Fatal(err)
			}
			state := tune.snapshotAdmission(false)
			tc.change(state)
			restored := newTuner([]string{"a"}, tune.maximum, true, 0, 100)
			if err := restored.restoreAdmission(state, false); err == nil {
				t.Fatal("invalid saved baseline/policy accepted")
			}
		})
	}
}

func TestBacklogDecisionsAreLoggedWithNumbers(t *testing.T) {
	// Validate the actual controller event contract, including fatal decisions
	// which used to return before emitting a tune event.
	f, err := os.Create(filepath.Join(t.TempDir(), "events"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	stdout := os.Stdout
	os.Stdout = f
	defer func() { os.Stdout = stdout }()
	tune := newTestTuner([]string{"a"}, capacity{8, 32, 1 << 20, 100}, true, 0, 100)
	tune.hosts["a"].baseline = &backlogBaseline{Min: 1390000000, Median: 1400000000, Max: 1407000000}
	sample := backlogTestSample(1800000000)
	if err := tune.observe("a", sample, storageSample{}); err != nil {
		t.Fatal(err)
	}
	sample.Time += .01
	sample.CachePoints = 2300000000
	if err := tune.observe("a", sample, storageSample{}); err == nil {
		t.Fatal("missing hard error")
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(f)
	for _, decision := range []string{"soft", "hard"} {
		var row struct {
			Event, Decision string
			Backlog         backlogStatus
		}
		if err := decoder.Decode(&row); err != nil {
			t.Fatal(err)
		}
		if row.Event != "tune" || row.Decision != decision || row.Backlog.Baseline.Max != 1407000000 || row.Backlog.Ratio <= 1.25 || row.Backlog.Current == 0 || row.Backlog.SoftRatio != 1.25 || row.Backlog.HardRatio != 1.6 || row.Backlog.EstimatedBytes == 0 {
			t.Fatalf("incomplete decision event: %+v", row)
		}
	}
}
