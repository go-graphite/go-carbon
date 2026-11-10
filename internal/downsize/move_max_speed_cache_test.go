package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestMaxSpeedCacheThresholdsAreStrictAndDestinationOnly(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cache int64
		age   *float64
		want  int
	}{
		{"12B equality", 12_000_000_000, cacheAge(0), 16},
		{"above12B", 12_000_000_001, cacheAge(0), 8},
		{"16B equality", 16_000_000_000, cacheAge(0), 8},
		{"above16B", 16_000_000_001, cacheAge(0), 0},
		{"3000 equality", 0, cacheAge(3000), 16},
		{"above3000", 0, cacheAge(3000.001), 0},
		{"missing age", 0, nil, 8},
		{"negative age", 0, cacheAge(-1), 8},
		{"nonfinite age", 0, cacheAge(math.NaN()), 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newMaxSpeedRig(t)
			h := r.tune.hosts["dst"]
			h.Streams = 16
			s := r.samples["dst"]
			s.CachePoints, s.OldestUnpersistedAgeSeconds, s.CPUCores = tc.cache, tc.age, 12
			r.samples["dst"] = s
			r.tick(t, "dst", 5)
			if h.Streams != tc.want {
				t.Fatal("wrong cache decision", h.Streams, tc.want)
			}
		})
	}
	r := newMaxSpeedRig(t)
	s := r.samples["srcA"]
	s.CachePoints = 17_000_000_000
	s.OldestUnpersistedAgeSeconds = cacheAge(4000)
	r.samples["srcA"] = s
	r.tick(t, "srcA", 5)
	if r.tune.hosts["srcA"].Streams == 0 || r.tune.hosts["srcA"].maxSpeed.CachePaused {
		t.Fatal("destination cache guard affected donor")
	}
}

func cacheAge(value float64) *float64 { return &value }

func TestMaxSpeedCacheHysteresisResumesAtFullReportedCeiling(t *testing.T) {
	for _, initiallyPaused := range []bool{false, true} {
		t.Run(fmt.Sprintf("paused=%t", initiallyPaused), func(t *testing.T) {
			r := newMaxSpeedRig(t)
			h := r.tune.hosts["dst"]
			h.Streams = 16
			storage := r.stores["dst"]
			storage.MaxTransfers = 16
			r.stores["dst"] = storage
			s := r.samples["dst"]
			s.CachePoints = 12_000_000_001
			if initiallyPaused {
				s.CachePoints = 16_000_000_001
			}
			r.samples["dst"] = s
			r.tick(t, "dst", 5)
			want := 8
			if initiallyPaused {
				want = 0
			}
			if h.Streams != want {
				t.Fatal("initial pressure", h.Streams)
			}
			for _, v := range []struct {
				cache int64
				age   *float64
			}{{11_000_000_000, cacheAge(0)}, {10_000_000_000, cacheAge(0)}, {9_999_999_999, cacheAge(2400)}, {9_999_999_999, nil}} {
				s = r.samples["dst"]
				s.CachePoints, s.OldestUnpersistedAgeSeconds = v.cache, v.age
				r.samples["dst"] = s
				r.tick(t, "dst", 5)
				if initiallyPaused && h.Streams != 0 {
					t.Fatal("unsafe cache pause recovery", h.Streams)
				}
				if !initiallyPaused && h.Streams > want {
					t.Fatal("cache hysteresis raised before both recovery thresholds", h.Streams)
				}
				want = h.Streams
			}
			s = r.samples["dst"]
			s.CachePoints = 9_999_999_999
			s.OldestUnpersistedAgeSeconds = cacheAge(2399.999)
			r.samples["dst"] = s
			r.tick(t, "dst", 5)
			if h.Streams != 16 || h.maxSpeed.CachePaused || h.maxSpeed.CacheLimited {
				t.Fatal("did not recover to full reported ceiling", h.Streams)
			}
		})
	}
}

func TestMaxSpeedCacheRecoveryStillAppliesOtherGuards(t *testing.T) {
	r := newMaxSpeedRig(t)
	h := r.tune.hosts["dst"]
	h.maxSpeed.CachePaused = true
	h.Streams = 0
	s := r.samples["dst"]
	s.CPUCores = 16 * .86
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if h.Streams != 64 {
		t.Fatal("cache recovery waived CPU throttle", h.Streams)
	}
	h.maxSpeed.CachePaused = true
	h.Streams = 0
	s = r.samples["dst"]
	s.DiskFreeBytes = 1000
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if h.Streams != 0 {
		t.Fatal("cache recovery waived disk hard pause", h.Streams)
	}
}

func TestMaxSpeedOwnerReadBaselineIsIndependentOfSavedAdmission(t *testing.T) {
	for _, tc := range []struct {
		cluster                     string
		baseline, soft, hard, boost float64
	}{{"pool-f", 4, 15, 25, 8}, {"pool-slow", 22, 33, 55, 22}} {
		baseline, err := maximumSpeedClusterReadBaseline(tc.cluster, []string{"http://production:4242"})
		if err != nil || baseline != tc.baseline {
			t.Fatal(baseline, err)
		}
		g := maxSpeedGovernor{readBaselineMS: baseline}
		soft, hard, boost := g.readThresholds()
		if soft != tc.soft || hard != tc.hard || boost != tc.boost {
			t.Fatal("relative thresholds", soft, hard, boost)
		}
	}
	if _, err := maximumSpeedClusterReadBaseline("unknown", []string{"http://production:4242"}); err == nil {
		t.Fatal("unknown production baseline accepted")
	}
	r := newMaxSpeedRig(t)
	h := r.tune.hosts["dst"]
	h.readBaseline = 22.7
	r.tune.maxSpeed.readBaselineMS = 22
	s := r.samples["dst"]
	s.ReadRolling1mSamples = 100
	s.Read1mWindowSeconds = 60
	s.ReadP99Rolling1mMS = 55
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if h.maxSpeed.ReadHighSince != 0 {
		t.Fatal("hard equality started timer")
	}
	s = r.samples["dst"]
	s.ReadP99Rolling1mMS = 55.001
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	for range 36 {
		r.tick(t, "dst", 5)
	}
	if h.Streams != 0 {
		t.Fatal("relative hard threshold did not pause")
	}
	s = r.samples["dst"]
	s.ReadP99Rolling1mMS = 32.999
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	for range 24 {
		r.tick(t, "dst", 5)
	}
	if h.Streams == 0 || h.readBaseline != 22.7 {
		t.Fatal("relative recovery changed frozen admission or stayed paused")
	}
}

func TestMaxSpeedCacheStateSurvivesResumeAndCompaction(t *testing.T) {
	m, original := nativeMaxPolicyFixture(t)
	decision, sha := maxDecisionFixture(t)
	t.Setenv("RESIZE_MAX_SPEED_DECISION", decision)
	t.Setenv("RESIZE_MAX_SPEED_DECISION_SHA256", sha)
	t.Setenv("RESIZE_MAX_SPEED_WRITE_OK", filepath.Join(m.planData.root, "WRITE-OK"))
	t.Setenv("RESIZE_MAX_SPEED_WRITE_OK_SHA256", original.ApprovalSHA256)
	saved := &admissionState{Policy: defaultBacklogPolicy(), Hosts: map[string]hostAdmission{}}
	for host := range m.tuner.hosts {
		saved.Hosts[host] = hostAdmission{Baseline: &backlogBaseline{Max: 123}, ReadBaseline: 22.7, MaxSpeed: &maxSpeedHostState{CachePaused: true, CacheLimited: true}}
	}
	if err := m.journal.saveAdmission(saved); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := m.configureMaximumSpeed(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, h := range m.tuner.hosts {
		if !h.maxSpeed.CachePaused || !h.maxSpeed.CacheLimited || h.Streams != 0 || h.readBaseline != 22.7 || h.baseline.Max != 123 {
			t.Fatal("resume lost cache state or original normal")
		}
	}
	if err := m.journal.compact(); err != nil {
		t.Fatal(err)
	}
	path, plan := m.journal.path, m.journal.plan
	m.journal.close()
	j, err := openJournal(path, plan)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	for _, h := range j.admission.Hosts {
		if !h.MaxSpeed.CachePaused || !h.MaxSpeed.CacheLimited || h.ReadBaseline != 22.7 {
			t.Fatal("compaction lost cache state/normal")
		}
	}
}

func TestMaxSpeedSoftCacheLimitIsNotLostOnEnable(t *testing.T) {
	r := newMaxSpeedRig(t)
	h := r.tune.hosts["dst"]
	h.maxSpeed.CacheLimited, h.maxSpeed.CacheStreams = true, 8
	r.tune.enableMaxSpeed([]string{"dst"}, "same-authority")
	r.tune.maxSpeed.now = func() float64 { return r.now }
	if h.Streams != 8 {
		t.Fatal("resume bypassed the last measured cache reduction", h.Streams)
	}
	s := r.samples["dst"]
	s.CachePoints = 11_000_000_000
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if h.Streams != 8 {
		t.Fatal("soft cache recovery grew before strict recovery thresholds", h.Streams)
	}
	s = r.samples["dst"]
	s.CachePoints = 9_999_999_999
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if h.Streams != 128 || h.maxSpeed.CacheStreams != 0 {
		t.Fatal("cache recovery did not restore full capacity", h.Streams)
	}
}

func TestMaxSpeedLatestAmendmentsRequired(t *testing.T) {
	for _, mode := range []string{"missing relative amendment", "missing cache amendment", "old unlogged", "wrong cache threshold"} {
		value := maximumSpeedDecisionFixtureValue()
		amendments := value["amendments"].([]map[string]string)
		switch mode {
		case "missing relative amendment":
			value["amendments"] = append(amendments[:1], amendments[2:]...)
		case "missing cache amendment":
			value["amendments"] = amendments[:2]
		case "old unlogged":
			value["throttle_not_stop"].(map[string]string)["unlogged"] = "halve concurrency"
		case "wrong cache threshold":
			value["throttle_not_stop"].(map[string]string)["cache_headroom"] = "cache_points > 16B -> halve"
		}
		raw, _ := json.Marshal(value)
		path := filepath.Join(t.TempDir(), "decision.json")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if err := verifyMaxSpeedDecision(path, fmt.Sprintf("%x", sum)); err == nil {
			t.Fatal("outdated authority accepted", mode)
		}
	}
}

func TestMaxSpeedOperatorCachePauseHandoffIsDurableAndNotRearmed(t *testing.T) {
	m, original := nativeMaxPolicyFixture(t)
	decision, sha := maxDecisionFixture(t)
	t.Setenv("RESIZE_MAX_SPEED_DECISION", decision)
	t.Setenv("RESIZE_MAX_SPEED_DECISION_SHA256", sha)
	t.Setenv("RESIZE_MAX_SPEED_WRITE_OK", filepath.Join(m.planData.root, "WRITE-OK"))
	t.Setenv("RESIZE_MAX_SPEED_WRITE_OK_SHA256", original.ApprovalSHA256)
	t.Setenv("RESIZE_CACHE_PAUSED_ON_START", "1")
	if _, err := m.configureMaximumSpeed(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	destination, source := "http://keep:4242", "http://drop:4242"
	h := m.tuner.hosts[destination]
	if !h.maxSpeed.CachePaused || !h.maxSpeed.CacheGuardInitialized || h.Streams != 0 || m.tuner.hosts[source].maxSpeed.CachePaused {
		t.Fatal("operator pause was not destination-only")
	}
	if m.journal.admission == nil || !m.journal.admission.Hosts[destination].MaxSpeed.CachePaused {
		t.Fatal("operator pause was not fsynced before first poll")
	}
	now := float64(1000)
	m.tuner.maxSpeed.now = func() float64 { return now }
	s := hostSample{Time: now, Generation: "same", ProcessUptimeSeconds: 3600, PhysicalCores: 16, CPUCores: 1, DiskBytes: 10000, DiskFreeBytes: 5000,
		MemoryBytes: 128 << 30, AvailableBytes: 64 << 30, NICCapacityBytes: 1e9, CachePoints: 9_000_000_000, OldestUnpersistedAgeSeconds: cacheAge(2500)}
	storage := storageSample{MaxTransfers: 8, MaxBodyBytes: 1 << 20}
	if err := m.tuner.observe(destination, s, storage); err != nil {
		t.Fatal(err)
	}
	if h.Streams != 0 {
		t.Fatal("adopted pause admitted at age2500")
	}
	now += 5
	s.Time = now
	s.OldestUnpersistedAgeSeconds = cacheAge(2399)
	if err := m.tuner.observe(destination, s, storage); err != nil {
		t.Fatal(err)
	}
	if h.Streams != 8 || h.maxSpeed.CachePaused {
		t.Fatal("eligible handoff did not resume full reported ceiling", h.Streams)
	}
	if err := m.saveAdmission(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.configureMaximumSpeed(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if h.maxSpeed.CachePaused || !h.maxSpeed.CacheGuardInitialized || h.Streams != 8 {
		t.Fatal("retained startup flag rearmed an initialized recovered guard", h.Streams)
	}
	t.Setenv("RESIZE_CACHE_PAUSED_ON_START", "")
	if _, err := m.configureMaximumSpeed(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if h.maxSpeed.CachePaused {
		t.Fatal("unsetting startup flag changed recovered state")
	}
}

func TestMaxSpeedCachePauseStartupRequiresExplicitValidMode(t *testing.T) {
	for _, mode := range []string{"no MAX", "bad value"} {
		t.Run(mode, func(t *testing.T) {
			m, _ := nativeMaxPolicyFixture(t)
			t.Setenv("RESIZE_CACHE_PAUSED_ON_START", "1")
			t.Setenv("RESIZE_MAX_SPEED_DECISION", "")
			if mode == "bad value" {
				t.Setenv("RESIZE_CACHE_PAUSED_ON_START", "true")
				t.Setenv("RESIZE_MAX_SPEED_DECISION", "explicit")
			}
			if _, err := m.configureMaximumSpeed(context.Background(), nil); err == nil {
				t.Fatal("invalid operator startup mode accepted")
			}
		})
	}
}
