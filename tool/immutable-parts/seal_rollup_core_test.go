package main

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func coreRollupTestConfig() coreConfig {
	return coreConfig{Shards: 2, Producers: 1, MaxRows: 4096, PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "ordered-rollup-test"}
}
func coreRollupBatches(t *testing.T, order uint64, value float64) []coreBatch {
	t.Helper()
	in := atomicTestInputs(order, value)[0]
	policy, err := json.Marshal(in.Config)
	if err != nil {
		t.Fatal(err)
	}
	var rows []row
	for _, p := range in.Batches[0].Points {
		rows = append(rows, row{ID: in.ID, Time: p.Time, Bits: p.Bits, Seq: p.Sequence})
	}
	return []coreBatch{{ID: in.ID, Order: order, Clock: in.Batches[0].Now, Policy: policy, Points: rows}}
}
func TestCoreRollupHook(t *testing.T) {
	root := t.TempDir()
	cfg := coreRollupTestConfig()
	e, states, err := openCoreRollups(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for n := uint64(1); n <= 2; n++ {
		if err = e.submitBatches(0, coreRollupBatches(t, n, float64(2+n*5-5)), n, true); err != nil {
			t.Fatal(err)
		}
	}
	s := states[coreShard(42, cfg.Shards)]
	if s.state[42].Frontier.Order != 2 || s.state[42].Slots[[2]int64{1, 480}].Bits != math.Float64bits(10) {
		t.Fatal("hook correction not published")
	}
	if got := e.durableFrontiers(); got[0][0] != 2 || got[1][0] != 2 {
		t.Fatal("frontier not atomic", got)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	e, states, err = openCoreRollups(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s = states[coreShard(42, cfg.Shards)]
	if s.state[42].Frontier.Order != 2 || s.state[42].Slots[[2]int64{1, 480}].Bits != math.Float64bits(10) {
		t.Fatal("restart lost rollup")
	}
	if err = e.submitBatches(0, coreRollupBatches(t, 3, 9), 3, true); err != nil {
		t.Fatal(err)
	}
	if s.state[42].Slots[[2]int64{1, 480}].Bits != math.Float64bits(12) {
		t.Fatal("restart correction lost prior bucket context")
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
}
func TestCoreRollupHookCrash(t *testing.T) {
	if root := os.Getenv("CORE_ROLLUP_CRASH_ROOT"); root != "" {
		cfg := coreRollupTestConfig()
		e, _, err := openCoreRollups(root, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err = e.submitBatches(0, coreRollupBatches(t, 1, 2), 1, true); err != nil {
			t.Fatal(err)
		}
		kill := func() {
			if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
				os.Exit(99)
			}
			for {
				time.Sleep(time.Second)
			}
		}
		owner := e.owners[coreShard(42, cfg.Shards)]
		prior := owner.plan
		switch os.Getenv("CORE_ROLLUP_CRASH_STAGE") {
		case "before":
			owner.plan = func(in coreSealInput) (coreSealPlan, error) { kill(); return prior(in) }
		case "after-ingest":
			owner.plan = func(in coreSealInput) (coreSealPlan, error) {
				p, err := prior(in)
				if err == nil {
					p.Commit = kill
				}
				return p, err
			}
		}
		if err = e.submitBatches(0, coreRollupBatches(t, 2, 7), 2, true); err != nil {
			t.Fatal(err)
		}
		kill()
		return
	}
	for _, stage := range []string{"before", "after-ingest", "after-publish"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCoreRollupHookCrash$")
			cmd.Env = append(os.Environ(), "CORE_ROLLUP_CRASH_ROOT="+root, "CORE_ROLLUP_CRASH_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatalf("expected SIGKILL: %v %s", err, out)
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("unexpected crash: %v %s", err, out)
			}
			cfg := coreRollupTestConfig()
			e, states, err := openCoreRollups(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			s := states[coreShard(42, cfg.Shards)]
			want := uint64(2)
			value := float64(10)
			if stage == "before" {
				want = 1
				value = 5
			}
			if s.state[42].Frontier.Order != want || s.state[42].Slots[[2]int64{1, 480}].Bits != math.Float64bits(value) {
				t.Fatal("torn raw/archive/frontier crash recovery", s.state[42])
			}
			// A crash may leave different owner frontiers. Replay starts at their min,
			// and submitBatches deduplicates each owner by its durable source cursor.
			if err = e.submitBatches(0, coreRollupBatches(t, 2, 7), 2, true); err != nil {
				t.Fatal(err)
			}
			if s.state[42].Frontier.Order != 2 || s.state[42].Slots[[2]int64{1, 480}].Bits != math.Float64bits(10) {
				t.Fatal("replay changed correction")
			}
			if err = e.close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCoreRollupPolicyFailure(t *testing.T) {
	cfg := coreRollupTestConfig()
	e, states, err := openCoreRollups(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	batches := coreRollupBatches(t, 1, 2)
	var policy sealrollup.Config
	if err := json.Unmarshal(batches[0].Policy, &policy); err != nil {
		t.Fatal(err)
	}
	policy.Method = 99
	batches[0].Policy, err = json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.submitBatches(0, batches, 1, true); err == nil {
		t.Fatal("unsupported policy accepted")
	}
	owner := states[coreShard(42, cfg.Shards)]
	if owner.Errors.Load() != 1 || len(owner.state) != 0 {
		t.Fatal("failure counter or detached plan")
	}
	// Atomicity is per owner: an empty peer owner may safely acknowledge its
	// source cursor independently. Only the affected owner's cursor must stay.
	if e.durableFrontiers()[coreShard(42, cfg.Shards)][0] != 0 {
		t.Fatal("failed policy advanced affected owner frontier")
	}
	if err := e.close(); err == nil {
		t.Fatal("owner failure lost on close")
	}
}

func TestCoreRollupHeadPolicyAndReopen(t *testing.T) {
	if got := (coreConfig{}).headMaxAge(); got != time.Second {
		t.Fatal("default head age changed", got)
	}
	cfg := coreRollupTestConfig()
	root := t.TempDir()
	p := sealrollup.HeadPolicy{MaxAge: 30 * time.Millisecond, MaxPoints: uint64(cfg.MaxRows)}
	e, _, err := openCoreRollupsWithHeadPolicy(root, cfg, p)
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.HeadMaxAge != p.MaxAge || e.cfg.MaxRows != int(p.MaxPoints) {
		t.Fatal("policy not installed", e.cfg)
	}
	if err := e.submitBatches(0, coreRollupBatches(t, 1, 2), 1, false); err != nil {
		t.Fatal(err)
	}
	// No second packet or explicit barrier: the idle poller must seal by age.
	deadline := time.Now().Add(3 * time.Second)
	for e.durableFrontiers()[coreShard(42, cfg.Shards)][0] != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.durableFrontiers()[coreShard(42, cfg.Shards)][0] != 1 {
		t.Fatal("custom age never sealed an idle head")
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	// Scheduling changes on reopen must not look like a disk-format change.
	p.MaxAge = 2 * time.Second
	e, _, err = openCoreRollupsWithHeadPolicy(root, cfg, p)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.submitBatches(0, coreRollupBatches(t, 2, 7), 2, true); err != nil {
		t.Fatal(err)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	cfg.HeadMaxAge = -1
	if _, err := openCore(root, cfg); err == nil {
		t.Fatal("negative head age accepted")
	}
}

func TestCoreRollupUnsortedMetricKeys(t *testing.T) {
	cfg := coreRollupTestConfig()
	cfg.Shards = 1
	e, states, err := openCoreRollups(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var batches []coreBatch
	for _, id := range []uint64{900, 2, 42, 1} {
		for order := uint64(1); order <= 2; order++ {
			b := coreRollupBatches(t, order, float64(order))[0]
			b.ID = id
			for i := range b.Points {
				b.Points[i].ID = id
			}
			batches = append(batches, b)
		}
	}
	if err = e.submitBatches(0, batches, 1, true); err != nil {
		t.Fatal("ordered append rejected unsorted metric admission", err)
	}
	for _, id := range []uint64{900, 2, 42, 1} {
		if states[0].state[id].Frontier.Order != 2 {
			t.Fatal("metric frontier lost", id)
		}
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
}

// Opt-in isolated-process memory receipt for one second of 20M/s intake. Heads
// deliberately remain unsealed, so process exit releases them without a large
// disk benchmark. This is not a recovery or durability test. Run this test alone.
func TestCoreRollupHeldHeadMemory(t *testing.T) {
	s := os.Getenv("SEAL_HEAD_MEMORY_POINTS")
	if s == "" {
		t.Skip("isolated-process held-head memory measurement")
	}
	points, err := strconv.Atoi(s)
	if err != nil || points < 1 {
		t.Fatal("invalid head point count")
	}
	const density = 118
	metrics := (points + density - 1) / density
	cfg := coreConfig{Shards: runtime.GOMAXPROCS(0), Producers: 1, MaxRows: 2 * points / runtime.GOMAXPROCS(0), PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "held-head-memory"}
	e, _, err := openCoreRollupsWithHeadPolicy(t.TempDir(), cfg, sealrollup.HeadPolicy{MaxAge: 16 * time.Minute, MaxPoints: uint64(cfg.MaxRows)})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	rows := make([]row, points)
	batches := make([]coreBatch, metrics)
	policy, _ := json.Marshal(atomicTestConfig())
	for i := range batches {
		lo, hi := i*density, min((i+1)*density, points)
		for j := lo; j < hi; j++ {
			rows[j] = row{ID: uint64(i), Time: 1800000001, Bits: math.Float64bits(1), Seq: uint64(j + 1)}
		}
		batches[i] = coreBatch{ID: uint64(i), Order: 1, Clock: 1800000060, Policy: policy, Points: rows[lo:hi]}
	}
	if err = e.submitBatches(0, batches, 1, false); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for e.routers[0].slots[0].remaining.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.routers[0].slots[0].remaining.Load() != 0 {
		t.Fatal("head admission did not finish")
	}
	for _, f := range e.durableFrontiers() {
		if f[0] != 0 {
			t.Fatal("memory fixture unexpectedly sealed")
		}
	}
	rows, batches = nil, nil
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(e)
	t.Logf("held_points=%d density=%d owners=%d heap_delta_bytes=%d heap_bytes_per_point=%.6f total_heap=%d", points, density, cfg.Shards, after.HeapAlloc-before.HeapAlloc, float64(after.HeapAlloc-before.HeapAlloc)/float64(points), after.HeapAlloc)
}
