package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

type maxSpeedRig struct {
	tune    *tuner
	now     float64
	samples map[string]hostSample
	stores  map[string]storageSample
}

func newMaxSpeedRig(t *testing.T) *maxSpeedRig {
	t.Helper()
	r := &maxSpeedRig{tune: newTuner([]string{"srcA", "srcB", "dst"}, capacity{128, 8192, 64 << 20, 65536}, true, 0, 2), now: 1000, samples: map[string]hostSample{}, stores: map[string]storageSample{}}
	r.tune.enableMaxSpeed([]string{"dst"}, "owner-sha")
	r.tune.maxSpeed.now = func() float64 { return r.now }
	for _, host := range []string{"srcA", "srcB", "dst"} {
		age := float64(0)
		r.samples[host] = hostSample{OldestUnpersistedAgeSeconds: &age, Time: r.now, Generation: host + ":one", ProcessUptimeSeconds: 3600, PhysicalCores: 16, CPUCores: 1, DiskBytes: 10000, DiskFreeBytes: 5000, MemoryBytes: 128 << 30, AvailableBytes: 64 << 30, NICCapacityBytes: 1e9, ReadP99MS: 10000}
		r.stores[host] = storageSample{MaxTransfers: 128, CPULimit: 1, MaxBodyBytes: 64 << 20}
		r.observe(t, host)
	}
	return r
}

func (r *maxSpeedRig) observe(t *testing.T, host string) {
	t.Helper()
	s := r.samples[host]
	s.Time = r.now
	r.samples[host] = s
	if err := r.tune.observe(host, s, r.stores[host]); err != nil {
		t.Fatal("MAX observation exited block", host, err)
	}
}

func (r *maxSpeedRig) tick(t *testing.T, host string, seconds float64) {
	t.Helper()
	r.now += seconds
	s := r.samples[host]
	s.ProcessUptimeSeconds += seconds
	r.samples[host] = s
	r.observe(t, host)
}

func TestMaxSpeedNoLegacyWarmupOrStaticCoreCeiling(t *testing.T) {
	r := newMaxSpeedRig(t)
	h := r.tune.hosts["dst"]
	if h.Streams <= 2 || h.baseline != nil || h.debtBaseline != nil || h.latencyBaseline != nil || h.Batch != 8192 {
		t.Fatal("legacy gate/static CPU cap survived", h.capacity)
	}
	release, err := r.tune.admit(context.Background(), "srcA", "dst")
	if err != nil {
		t.Fatal(err)
	}
	release()
	for range 20 {
		r.tune.backoff("dst")
	}
	if h.Streams != 1 {
		t.Fatal("backpressure parked MAX copy", h.Streams)
	}
	// Old hard memory/L0/backlog/read limits can only throttle this mode.
	s := r.samples["dst"]
	s.AvailableBytes = 1
	s.RSS = s.MemoryBytes
	s.CachePoints = 12_000_000_000
	s.Threads = 9000
	r.samples["dst"] = s
	store := r.stores["dst"]
	store.L0StopWrites = 12
	store.L0Sublevels = 30
	store.CompactionDebt = 100 << 30
	r.stores["dst"] = store
	r.tick(t, "dst", 5)
	if h.Streams != 1 || h.hard != "" {
		t.Fatal("removed legacy rule stopped MAX copy", h.capacity, h.hard)
	}
}

func TestMaxSpeedWriteErrorsPauseOnlyAffectedPairsForQuiet120s(t *testing.T) {
	r := newMaxSpeedRig(t)
	s := r.samples["srcA"]
	s.WriteErrors = 1
	r.samples["srcA"] = s
	r.tick(t, "srcA", 5)
	if r.tune.hosts["srcA"].Streams != 0 || r.tune.hosts["dst"].Streams == 0 || r.tune.hosts["srcB"].Streams == 0 {
		t.Fatal("write pause was not pair-local")
	}
	release, err := r.tune.admit(context.Background(), "srcB", "dst")
	if err != nil {
		t.Fatal(err)
	}
	release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.tune.admit(ctx, "srcA", "dst"); !errors.Is(err, context.Canceled) {
		t.Fatal("paused pair did not wait", err)
	}
	for range 23 {
		r.tick(t, "srcA", 5)
	}
	if r.tune.hosts["srcA"].Streams != 0 {
		t.Fatal("resumed before120s quiet")
	}
	r.tick(t, "srcA", 5)
	if r.tune.hosts["srcA"].Streams < 1 {
		t.Fatal("did not auto-resume at120s quiet")
	}
}

func TestMaxSpeedRestartAndUnavailableArePairLocal(t *testing.T) {
	r := newMaxSpeedRig(t)
	s := r.samples["srcA"]
	s.Generation = "srcA:two"
	s.ProcessUptimeSeconds = 0
	r.samples["srcA"] = s
	r.tick(t, "srcA", 1)
	if r.tune.hosts["srcA"].Streams != 0 || r.tune.hosts["srcB"].Streams == 0 {
		t.Fatal("restart did not pause only affected host")
	}
	for range 23 {
		r.tick(t, "srcA", 5)
	}
	if r.tune.hosts["srcA"].Streams != 0 {
		t.Fatal("process resumed before120s uptime")
	}
	r.tick(t, "srcA", 4)
	if r.tune.hosts["srcA"].Streams < 1 {
		t.Fatal("process did not resume at120s uptime")
	}
	r.tune.maxSpeedUnavailable("srcA", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED})
	if r.tune.hosts["srcA"].Streams != 0 {
		t.Fatal("unavailable process admitted copy")
	}
	r.tick(t, "srcA", 5)
	if r.tune.hosts["srcA"].Streams < 1 {
		t.Fatal("healthy mature process stayed paused")
	}
}

func TestMaxSpeedMissingTelemetryOnlyThrottlesAndRetainsRealPauses(t *testing.T) {
	r := newMaxSpeedRig(t)
	h := r.tune.hosts["srcA"]
	for range 20 {
		r.tune.maxSpeedUnavailable("srcA", context.DeadlineExceeded)
	}
	if h.Streams != 1 || h.maxSpeed.ProcessDown {
		t.Fatal("generic telemetry timeout parked a live pair", h.Streams)
	}
	r.tune.maxSpeedUnavailable("srcA", fmt.Errorf("%w: %w", errGuards, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}))
	if h.Streams != 1 || h.maxSpeed.ProcessDown {
		t.Fatal("helper port refusal was mistaken for app down", h.Streams)
	}
	h.Streams = 0
	h.maxSpeed.WriteQuietUntil = r.now + 120
	r.tune.maxSpeedUnavailable("srcA", context.DeadlineExceeded)
	if h.Streams != 0 {
		t.Fatal("missing telemetry lifted an existing write-error pause")
	}
	r.tune.maxSpeedUnavailable("srcA", fmt.Errorf("%w: worker process is not live", errGuards))
	if h.Streams != 0 || !h.maxSpeed.ProcessDown {
		t.Fatal("explicit missing process did not pause its pair")
	}
}

func TestMaxSpeedReadPause180sAndRecovery120s(t *testing.T) {
	r := newMaxSpeedRig(t)
	s := r.samples["dst"]
	s.ReadRolling1mSamples = 1
	s.Read1mWindowSeconds = 1
	s.ReadP99Rolling1mMS = 25
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if r.tune.hosts["dst"].maxSpeed.ReadHighSince != 0 {
		t.Fatal("25ms equality started hard read timer")
	}
	s = r.samples["dst"]
	s.ReadP99Rolling1mMS = 25.001
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	for range 35 {
		r.tick(t, "dst", 5)
	}
	if r.tune.hosts["dst"].Streams < 1 {
		t.Fatal("read pause before180s")
	}
	r.tick(t, "dst", 5)
	if r.tune.hosts["dst"].Streams != 0 {
		t.Fatal("missing sustained read pause")
	}
	s = r.samples["dst"]
	s.ReadP99Rolling1mMS = 15
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if r.tune.hosts["dst"].maxSpeed.ReadLowSince != 0 {
		t.Fatal("15ms equality qualified recovery")
	}
	s = r.samples["dst"]
	s.ReadP99Rolling1mMS = 14.999
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	for range 23 {
		r.tick(t, "dst", 5)
	}
	if r.tune.hosts["dst"].Streams != 0 {
		t.Fatal("read pause recovered before120s")
	}
	r.tick(t, "dst", 5)
	if r.tune.hosts["dst"].Streams < 1 {
		t.Fatal("read pause did not recover at120s")
	}
}

func TestMaxSpeedThrottleAndDestinationOnlyBoundaries(t *testing.T) {
	r := newMaxSpeedRig(t)
	h := r.tune.hosts["dst"]
	s := r.samples["dst"]
	s.CPUCores = 16 * .85
	r.samples["dst"] = s
	h.Streams = 8
	r.tick(t, "dst", 5)
	if h.Streams != 8 {
		t.Fatal("85% equality throttled", h.Streams)
	}
	s = r.samples["dst"]
	s.CPUCores = 16 * .851
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if h.Streams != 4 {
		t.Fatal("CPU above85% did not halve", h.Streams)
	}
	s = r.samples["dst"]
	s.CPUCores = 16 * .70
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if h.Streams != 4 {
		t.Fatal("70% equality raised concurrency", h.Streams)
	}
	s = r.samples["dst"]
	s.CPUCores = 16 * .699
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if h.Streams <= 4 {
		t.Fatal("CPU below70% did not raise", h.Streams)
	}
	h.debtBaseline = &backlogBaseline{Max: 100}
	store := r.stores["dst"]
	store.CompactionDebt = 201
	r.stores["dst"] = store
	h.Streams = 8
	r.tick(t, "dst", 5)
	if h.Streams <= 8 || h.debtBaseline.Max != 100 {
		t.Fatal("removed debt rule suppressed growth or changed normal", h.Streams)
	}
	store.CompactionDebt = 0
	r.stores["dst"] = store
	s = r.samples["dst"]
	s.Unlogged++
	r.samples["dst"] = s
	h.Streams = 8
	r.tick(t, "dst", 5)
	if h.Streams <= 8 {
		t.Fatal("removed unlogged throttle suppressed measured headroom", h.Streams)
	}
	s = r.samples["dst"]
	s.DiskFreeBytes = 1500
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if h.Streams == 0 {
		t.Fatal("15% disk equality paused")
	}
	s = r.samples["dst"]
	s.DiskFreeBytes = 1499
	r.samples["dst"] = s
	r.tick(t, "dst", 5)
	if h.Streams != 0 {
		t.Fatal("destination below15% did not pause")
	}
	s = r.samples["srcA"]
	s.DiskFreeBytes = 1
	s.ReadP99Rolling1mMS = 100
	s.ReadRolling1mSamples = 100
	s.Read1mWindowSeconds = 60
	r.samples["srcA"] = s
	r.tick(t, "srcA", 5)
	if r.tune.hosts["srcA"].Streams == 0 {
		t.Fatal("destination-only guard paused donor")
	}
}

func TestMaxSpeedAbsentProofBoundsDisabledOnlyExplicitMode(t *testing.T) {
	j := &journal{absent: map[string]absentSourceSkip{"churn": {BatchMetrics: 128, Count: 128}}}
	if j.absentError() == nil {
		t.Fatal("default absence bound changed")
	}
	j.maxSpeed = true
	if err := j.absentError(); err != nil {
		t.Fatal("MAX absence count stopped copy", err)
	}
	j.maxSpeed = false
	if j.absentError() == nil {
		t.Fatal("disabling explicit mode retained absence bypass")
	}
}

func TestMaxSpeedDecisionRequiresExactRawSHAAndOwner(t *testing.T) {
	value := maximumSpeedDecisionFixtureValue()
	raw, _ := json.Marshal(value)
	path := filepath.Join(t.TempDir(), "decision.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if err := verifyMaxSpeedDecision(path, hex.EncodeToString(sum[:])); err != nil {
		t.Fatal(err)
	}
	if err := verifyMaxSpeedDecision(path, ""); err == nil {
		t.Fatal("missing SHA accepted")
	}
	if err := os.WriteFile(path, append(raw, ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if err := verifyMaxSpeedDecision(path, hex.EncodeToString(sum[:])); err == nil {
		t.Fatal("changed raw decision accepted")
	}
}

func TestMaxSpeedIntegrityClassificationNeverSkipsJournalFailures(t *testing.T) {
	for _, tc := range []struct {
		err              error
		retry, integrity bool
	}{
		{syscall.EIO, false, false}, {syscall.ENOSPC, false, false}, {errors.New("unexpected local controller failure"), false, false},
		{&os.PathError{Op: "sync", Path: "journal", Err: syscall.EIO}, false, false},
		{&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, true, false},
		{io.ErrUnexpectedEOF, true, false}, {&apiError{503, "overloaded"}, true, false}, {&apiError{401, "expired token"}, true, false},
		{&apiError{400, "unclassified bad request"}, false, false},
		{transferIntegrityError(&apiError{400, "bad metric"}), false, true},
		{transferIntegrityError(&apiError{409, "proof mismatch"}), false, true},
		{transferIntegrityError(&apiError{500, "http://destination:4242 /v1/transfer/pull: HTTP 500 source point verification infra.metric failed\n"}), false, true},
		{transferIntegrityError(&apiError{500, "staged point verification infra.metric failed"}), false, true},
		{transferIntegrityError(&apiError{500, "staged transfer key verification failed"}), false, true},
		{transferIntegrityError(&apiError{500, "staged transfer value verification failed"}), false, true},
		{transferIntegrityError(&apiError{500, "transfer checksum mismatch"}), false, true},
		{transferIntegrityError(&apiError{500, "verify infra.metric receipt differs"}), false, true},
		{transferIntegrityError(&apiError{500, "open file: input/output error"}), true, false},
		{transferIntegrityError(&apiError{500, "open file: source point verification infra.metric failed"}), true, false},
		{transferIntegrityError(&apiError{500, "source point verification infra.metric failed: read I/O error"}), true, false},
		{transferIntegrityError(&apiError{500, "staged transfer key verification failed\nclose file: I/O error"}), true, false},
		{copyIntegrityError(errors.New("proof digest mismatch")), false, true},
	} {
		retry, integrity := maximumSpeedErrorKind(tc.err)
		if retry != tc.retry || integrity != tc.integrity {
			t.Fatalf("%T %v: retry=%v integrity=%v", tc.err, tc.err, retry, integrity)
		}
	}
}

func maximumSpeedDecisionFixtureValue() map[string]any {
	return map[string]any{
		"decision": "resize-MAX-SPEED", "owner": "operator", "user_instruction": "explicit max speed",
		"hard_stops_only":   map[string]string{"write_errors": "auto-resume 2 min", "process": "process is up 2 min", "disk": "free < 15%", "read_p99": "survivor 1-min read p99 > max(25 ms, 2.5x the cluster's pre-resize 30-min p99) for 3 consecutive minutes; auto-resume below max(15 ms, 1.5x baseline) for 2 min", "integrity": "skip and record the name"},
		"throttle_not_stop": map[string]string{"read_p99": "survivor p99 > max(15 ms, 1.5x the cluster's pre-resize p99) -> halve; below max(8 ms, 1.0x baseline) for 2 min -> double", "cpu": "> 85%", "compaction_debt": "REMOVED 14:50: compaction debt alone is not a throttle signal. Throttle only on L0 sublevels >= half of the store's stop-writes or an active write stall.", "unlogged": "REMOVED 15:45: record it, do not throttle on it", "cache_headroom": "survivor cache_points > 12B -> halve; > 16B or input_log_oldest_unpersisted_age_seconds > 3000 -> pause; resume full speed below 10B and 2400 s"},
		"amendments": []map[string]string{
			{"at": "2026-11-06T14:50:00+01:00", "what": "survivor transfer limits + debt throttle removed", "detail": "Controllers: drop the 'debt >2x normal -> halve' rule; raise concurrency when MaxTransfers 16. HTTP 429 from a server = back off that server briefly, never stop."},
			{"at": "2026-11-06T15:20:00+01:00", "what": "read p99 thresholds relative to each cluster's pre-resize p99", "detail": "Baselines: high-latency pool ~22 ms; other pools 1-4 ms."},
			{"at": "2026-11-06T15:45:00+01:00", "what": "unlogged throttle replaced by cache-headroom guard", "detail": "max copy speed is priority. Guard on cache headroom and oldest unpersisted age, not on unlogged."},
		},
	}
}

func TestMaxSpeedAmendedStorageThrottleAndDynamicServerSlots(t *testing.T) {
	for _, tc := range []struct {
		stop, sublevels int64
		stall, throttle bool
	}{{32, 15, false, false}, {32, 16, false, true}, {31, 15, false, false}, {31, 16, false, true}, {32, 0, true, true}} {
		r := newMaxSpeedRig(t)
		h := r.tune.hosts["dst"]
		h.Streams = 8
		h.debtBaseline = &backlogBaseline{Max: 1}
		store := r.stores["dst"]
		store.CompactionDebt = 100 << 30
		store.L0StopWrites = tc.stop
		store.L0Sublevels = tc.sublevels
		store.WriteStallActive = tc.stall
		r.stores["dst"] = store
		r.tick(t, "dst", 5)
		if tc.throttle && h.Streams != 4 || !tc.throttle && h.Streams <= 8 || h.Streams == 0 {
			t.Fatal("incorrect amended storage throttle", tc, h.Streams)
		}
	}
	r := newMaxSpeedRig(t)
	h := r.tune.hosts["dst"]
	store := r.stores["dst"]
	store.MaxTransfers = 4
	r.stores["dst"] = store
	r.tick(t, "dst", 5)
	if h.Streams != 4 {
		t.Fatal("server transfer ceiling ignored", h.Streams)
	}
	store.MaxTransfers = 16
	r.stores["dst"] = store
	for range 4 {
		r.tick(t, "dst", 5)
	}
	if h.Streams != 16 {
		t.Fatal("new server slots were not adopted dynamically", h.Streams)
	}
}

func TestMaxSpeedDebtRemovalRequiresAmendedRawAuthority(t *testing.T) {
	for _, mode := range []string{"old debt rule", "missing amendments"} {
		value := maximumSpeedDecisionFixtureValue()
		if mode == "old debt rule" {
			value["throttle_not_stop"].(map[string]string)["compaction_debt"] = "> 2x"
		} else {
			delete(value, "amendments")
		}
		raw, _ := json.Marshal(value)
		path := filepath.Join(t.TempDir(), "decision.json")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if err := verifyMaxSpeedDecision(path, hex.EncodeToString(sum[:])); err == nil {
			t.Fatal("unamended authority enabled debt-free policy", mode)
		}
	}
}

func TestMaxSpeedUpstream429BacksOffOnlyNamedSource(t *testing.T) {
	body := struct {
		Source   string
		Names    []string
		MaxBytes int64
	}{"http://donor:4242", []string{"metric"}, 1 << 20}
	api := &apiError{429, "http://target:4242 /v1/transfer/pull: HTTP 429 source export: 429 Too Many Requests\n"}
	if got := maximumSpeedBackpressureHost("http://target:4242", "/v1/transfer/pull", body, api); got != body.Source {
		t.Fatal("upstream429 throttled destination", got)
	}
	api.Message = "http://target:4242 /v1/transfer/pull: HTTP 429 transfer slots busy"
	if got := maximumSpeedBackpressureHost("http://target:4242", "/v1/transfer/pull", body, api); got != "http://target:4242" {
		t.Fatal("direct429 throttled another source", got)
	}
}
