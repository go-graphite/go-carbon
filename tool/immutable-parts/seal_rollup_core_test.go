package main

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
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
