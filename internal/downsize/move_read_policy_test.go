package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReadPolicyFloorAndRatio(t *testing.T) {
	p := defaultReadPolicy()
	for _, tc := range []struct {
		name     string
		policy   readPolicy
		baseline float64
		want     float64
	}{
		{"floor wins over 1.5x a 1.26 ms baseline", p, 1.26, 50},
		{"ratio wins for a large baseline", p, 40, 60},
		{"ceiling caps the relative limit", p, 80, 100},
		{"no ceiling leaves the relative limit", readPolicy{0, 50, 1.5}, 80, 120},
		{"no baseline uses the ceiling", p, 0, 100},
		{"raised ceiling and floor", readPolicy{500, 200, 2}, 1.26, 200},
	} {
		if got := tc.policy.limit(tc.baseline); got != tc.want {
			t.Errorf("%s: limit %g, want %g", tc.name, got, tc.want)
		}
	}
	for _, bad := range []readPolicy{{-1, 50, 1.5}, {100, -1, 1.5}, {100, 50, 0.5}, {100, 50, 0}} {
		if bad.validate() == nil {
			t.Error("invalid read policy accepted", bad)
		}
	}

	// The tuner pauses on the effective limit, not the 2 ms legacy one.
	tune := newTestTuner([]string{"a"}, capacity{8, 8192, 64 << 20, 65536}, true, 0, 100)
	sample := hostSample{Time: float64(time.Now().Unix()), Generation: "one", StoragePhase: "shared", DiskFreeBytes: 500 << 30, DiskBytes: 1 << 40, PhysicalCores: 96, MemoryBytes: 128 << 30, AvailableBytes: 64 << 30, NICCapacityBytes: 1e9, ReadSamples: 100, ReadP99MS: 1.26}
	observe := func(p99 float64) {
		t.Helper()
		sample.ReadP99MS = p99
		sample.Time += .01
		if err := tune.observe("a", sample, storageSample{}); err != nil {
			t.Fatal(err)
		}
	}
	observe(1.26)
	tune.captureReadBaselines()
	for range 4 {
		before := tune.hosts["a"].Streams
		observe(30)
		if tune.hosts["a"].Streams < before {
			t.Fatal("30 ms read p99 throttled a copy under the 50 ms floor")
		}
	}
	before := tune.hosts["a"].Streams
	observe(51)
	if before < 2 || tune.hosts["a"].Streams >= before {
		t.Fatal("read p99 above the floor did not throttle", before, tune.hosts["a"].Streams)
	}
}

func TestReadBaselineUsesObservationMedian(t *testing.T) {
	tune := newTestTuner([]string{"a"}, capacity{8, 8192, 64 << 20, 65536}, true, 0, 0)
	tune.collecting = true
	sample := hostSample{Time: float64(time.Now().Unix()), Generation: "one", StoragePhase: "shared", DiskFreeBytes: 500 << 30, DiskBytes: 1 << 40, PhysicalCores: 96, MemoryBytes: 128 << 30, AvailableBytes: 64 << 30, NICCapacityBytes: 1e9, ReadSamples: 100}
	// The last reading (1.26) is unrepresentative; blocked or thin samples
	// are not baseline evidence.
	for _, s := range []struct {
		p99     float64
		blocked bool
		reads   int
	}{{4, false, 100}, {30, false, 100}, {5, false, 100}, {900, true, 100}, {700, false, 3}, {3, false, 100}, {1.26, false, 100}} {
		sample.ReadP99MS, sample.ReadsBlocked, sample.ReadSamples = s.p99, s.blocked, s.reads
		sample.Time += .01
		if err := tune.observe("a", sample, storageSample{}); err != nil {
			t.Fatal(err)
		}
	}
	tune.collecting = false
	tune.captureReadBaselines()
	if got := tune.hosts["a"].readBaseline; got != 4 {
		t.Fatalf("read baseline %g, want median 4 of the observation window", got)
	}
	// Later captures (read isolation, whole-store) use the latest sample.
	sample.ReadP99MS, sample.ReadsBlocked, sample.ReadSamples = 9, false, 100
	sample.Time += .01
	if err := tune.observe("a", sample, storageSample{}); err != nil {
		t.Fatal(err)
	}
	tune.captureReadBaselines()
	if got := tune.hosts["a"].readBaseline; got != 9 {
		t.Fatalf("read baseline %g, want latest 9", got)
	}
}

func TestResumePolicyLegacyAndInvalidOverrides(t *testing.T) {
	now := time.Unix(1794570000, 0)
	saved := admissionState{Policy: backlogPolicy{3 * time.Minute, 4, 8, 30, 64}}
	// A journal written before the read policy was saved adopts this run's
	// read flags and records the replaced 2 ms legacy floor.
	merged, changes, err := resumePolicy(saved, defaultBacklogPolicy(), defaultReadPolicy(), map[string]bool{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if merged.Policy != saved.Policy || *merged.Read != defaultReadPolicy() {
		t.Fatal("legacy resume changed the backlog policy or ignored read flags", merged.Policy, *merged.Read)
	}
	if len(changes) != 1 || changes[0].Flag != "read-p99-floor-ms" || changes[0].From != 2 || changes[0].To != 50 || changes[0].Note == "" || len(merged.Overrides) != 1 {
		t.Fatal("legacy floor change not recorded", changes)
	}
	// Overriding the soft ratio above the inherited hard ratio is refused.
	soft := defaultBacklogPolicy()
	soft.SoftRatio = 9
	if _, _, err := resumePolicy(saved, soft, defaultReadPolicy(), map[string]bool{"backlog-soft-ratio": true}, now); err == nil {
		t.Fatal("soft ratio above the saved hard ratio accepted")
	}
	// Programmatic callers (nil explicit) override every differing value.
	read := readPolicy{100, 50, 1.5}
	saved.Read = &read
	want := backlogPolicy{3 * time.Minute, 2, 3, 6, 64}
	merged, changes, err = resumePolicy(saved, want, readPolicy{200, 50, 1.5}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if merged.Policy != want || merged.Read.CeilingMS != 200 || len(changes) != 4 {
		t.Fatal("programmatic overrides not applied", merged.Policy, *merged.Read, changes)
	}
}

func TestResumeOverridesSavedGovernorPolicy(t *testing.T) {
	ctx := context.Background()
	source, target := localStore(t), localStore(t)
	addPoints(t, source, "resume.policy", 2)
	src := httptest.NewServer(localService(t, source))
	defer src.Close()
	dst := httptest.NewServer(localService(t, target))
	defer dst.Close()
	root := fixturePlan(t, src.URL, dst.URL, []string{"resume.policy"})
	o := testOptions(root)
	o.explicit = map[string]bool{}
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	status := func() (out struct {
		Backlog   backlogPolicy    `json:"backlog_policy"`
		Read      readPolicy       `json:"read_p99_policy"`
		Overrides []policyOverride `json:"policy_overrides"`
	}) {
		t.Helper()
		var buf bytes.Buffer
		if err := Status(root, o.state, &buf); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if s := status(); s.Read != defaultReadPolicy() || s.Backlog != defaultBacklogPolicy() || len(s.Overrides) != 0 {
		t.Fatal("fresh run did not save its policy", s)
	}

	// Only the named flags override; an unnamed changed value is ignored.
	resumed := o
	resumed.backlog = defaultBacklogPolicy()
	resumed.backlog.SoftRatio, resumed.backlog.HardRatio, resumed.read.FloorMS = 1.5, 3, 80
	resumed.explicit = map[string]bool{"backlog-soft-ratio": true, "read-p99-floor-ms": true}
	if err := run(ctx, resumed); err != nil {
		t.Fatal("resume with overrides", err)
	}
	// A plain resume keeps the overridden journal values without new records.
	if err := run(ctx, o); err != nil {
		t.Fatal("plain resume", err)
	}
	s := status()
	if s.Backlog.SoftRatio != 1.5 || s.Backlog.HardRatio != defaultBacklogPolicy().HardRatio || s.Read.FloorMS != 80 || s.Read.CeilingMS != 100 {
		t.Fatal("override not applied or not inherited", s)
	}
	if len(s.Overrides) != 2 || s.Overrides[0].Flag != "backlog-soft-ratio" || s.Overrides[0].From != 1.25 || s.Overrides[0].To != 1.5 ||
		s.Overrides[1].Flag != "read-p99-floor-ms" || s.Overrides[1].From != 50 || s.Overrides[1].To != 80 || s.Overrides[0].At == "" {
		t.Fatal("override not recorded in the journal", s.Overrides)
	}

	// The learned baseline's window is fixed.
	window := o
	window.backlog = defaultBacklogPolicy()
	window.backlog.Window = 10 * time.Minute
	window.explicit = map[string]bool{"backlog-baseline-window": true}
	if err := run(ctx, window); err == nil || !strings.Contains(err.Error(), "backlog-baseline-window") {
		t.Fatal("changed baseline window accepted on resume", err)
	}
}
