package downsize

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadyNoopPreservesFinishedVerdict(t *testing.T) {
	for _, except := range []bool{false, true} {
		t.Run(map[bool]string{false: "READY", true: "READY_EXCEPT"}[except], func(t *testing.T) {
			f, o, r := redoReadyFixture(t)
			if except {
				o.issues, _ = openSyncIssues(o.state)
				if err := o.issues.record([]syncResidual{{syncHole: syncHole{Name: "excluded"}, Reason: "unrecoverable fixture"}}, "verify", false); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			err := runSyncReady(ctx, &o, r, f, func() error { return runSync(ctx, o, f) })
			if err != nil && !(except && errors.Is(err, ErrSyncQuarantined)) {
				t.Fatal(err)
			}
			before := readReadyReport(t, o.state)
			data, _ := os.ReadFile(filepath.Join(o.state, "ready.json"))
			p := redoProgress(t, o.state)
			calls := f.snapshot()
			journal, _ := os.ReadFile(filepath.Join(o.state, "sync.jsonl"))
			for range 2 {
				if err := runSyncReadyPrepared(ctx, &o, r, f, func() error { t.Fatal("prepared data work"); return nil }, func() error { t.Fatal("replayed root"); return nil }); err != nil {
					t.Fatal(err)
				}
				got, _ := os.ReadFile(filepath.Join(o.state, "ready.json"))
				if string(got) != string(data) {
					t.Fatal("terminal report changed")
				}
				if !reflect.DeepEqual(calls, f.snapshot()) {
					t.Fatal("backend data work repeated")
				}
				next := redoProgress(t, o.state)
				if next.Phase != "ready" || next.Done != p.Done || next.PhaseRegressions != 0 {
					t.Fatalf("progress regressed: %+v -> %+v", p, next)
				}
				for _, n := range next.RedoNames {
					if n != 0 {
						t.Fatal(next.RedoNames)
					}
				}
			}
			after, _ := os.ReadFile(filepath.Join(o.state, "sync.jsonl"))
			if string(after) != string(journal) {
				t.Fatal("no-op changed journal")
			}
			t.Logf("%s: done=%d redo_names=0 phase_regressions=0", before.Verdict, p.Done)
		})
	}
}

func TestReadyCompletedVerifyResumesAtReady(t *testing.T) {
	f, o, r := redoReadyFixture(t)
	ctx := context.Background()
	r.carbonAPI = ""
	if err := runSyncReady(ctx, &o, r, f, func() error { return runSync(ctx, o, f) }); !errors.Is(err, ErrSyncNotReady) {
		t.Fatal(err)
	}
	r.carbonAPI = "http://read.example"
	if err := runSyncReady(ctx, &o, r, f, func() error { t.Fatal("replayed completed verification"); return nil }); err != nil {
		t.Fatal(err)
	}
	if readReadyReport(t, o.state).Verdict != "READY" {
		t.Fatal("did not finish READY")
	}
}

func TestReadyChangedNameDoesNotReopenOtherNames(t *testing.T) {
	f, o, r := redoReadyFixture(t)
	ctx := context.Background()
	if err := runSyncReady(ctx, &o, r, f, func() error { return runSync(ctx, o, f) }); err != nil {
		t.Fatal(err)
	}
	before := f.snapshot()
	delete(f.store(syncT).points["metric.001"][3600], syncTestNow-5*3600)
	if err := runSyncReady(ctx, &o, r, f, func() error { return runSync(ctx, o, f) }); err != nil {
		t.Fatal(err)
	}
	for call, n := range f.snapshot() {
		if n > before[call] && call.Name != "metric.001" {
			t.Fatalf("unchanged name reopened: %+v", call)
		}
	}
	p := redoProgress(t, o.state)
	if p.PhaseRegressions != 0 {
		t.Fatal(p)
	}
}

func TestReadyRestartAttestationIsThenNoop(t *testing.T) {
	f, o, r := redoReadyFixture(t)
	ctx := context.Background()
	if err := runSyncReady(ctx, &o, r, f, func() error { return runSync(ctx, o, f) }); err != nil {
		t.Fatal(err)
	}
	before := f.snapshot()
	f.ready.starts[syncT] += 7200
	f.epoch = "new-process"
	var clock atomic.Int64
	clock.Store(syncTestNow + 7200)
	o.now = func() time.Time { return time.Unix(clock.Add(1), 0) }
	if err := runSyncReady(ctx, &o, r, f, func() error { t.Fatal("restarted old history"); return nil }); err != nil {
		t.Fatal(err)
	}
	for call, n := range f.snapshot() {
		if n > before[call] && call.From < syncTestNow-1 {
			t.Fatalf("replayed original history: %+v", call)
		}
	}
	settled := f.snapshot()
	if err := runSyncReady(ctx, &o, r, f, func() error { t.Fatal("replayed finished recovery"); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(settled, f.snapshot()) {
		t.Fatal("data work after finished restart")
	}
}

func TestReadyNoopFailsClosedWithoutFrontier(t *testing.T) {
	f, o, r := redoReadyFixture(t)
	ctx := context.Background()
	if err := runSyncReady(ctx, &o, r, f, func() error { return runSync(ctx, o, f) }); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(o.state, "verification-scope.json")); err != nil {
		t.Fatal(err)
	}
	before := f.snapshot()
	err := runSyncReady(ctx, &o, r, f, func() error { t.Fatal("silently replayed lost proof"); return nil })
	if err == nil || !strings.Contains(err.Error(), "verification-scope.json") {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, f.snapshot()) {
		t.Fatal("data work after lost frontier")
	}
}

func TestReadyNoopRepeatedBlock(t *testing.T) {
	for _, except := range []bool{false, true} {
		t.Run(fmt.Sprint(except), func(t *testing.T) {
			f, o, r := redoReadyFixture(t)
			block := filepath.Join(t.TempDir(), "block.json")
			data := fmt.Sprintf(`{"target":%q,"donors":[%q,%q],"peers":[%q],"state":%q,"period":{"from":%d,"until":%d},"carbonapi_url":%q,"gap_ledger_complete":true}`, syncT, syncD1, syncD2, syncP, o.state, r.period[0].From, r.period[0].Until, r.carbonAPI)
			if err := os.WriteFile(block, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"--block", block, "--streams", "2", "--include-short-retention", "--settle=0s"}
			c, err := parseSyncCommand(args)
			if err != nil {
				t.Fatal(err)
			}
			c.sync.now = o.now
			if err = c.prepareStateIntent(); err != nil {
				t.Fatal(err)
			}
			if err = c.saveConfig(); err != nil {
				t.Fatal(err)
			}
			if except {
				c.sync.issues, _ = openSyncIssues(o.state)
				if err = c.sync.issues.record([]syncResidual{{syncHole: syncHole{Name: "excluded"}, Reason: "known exception"}}, "verify", false); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			err = runSyncReady(ctx, &c.sync, c.ready, f, func() error { return c.run(ctx, f) })
			if err != nil && !(except && errors.Is(err, ErrSyncQuarantined)) {
				t.Fatal(err)
			}
			before := f.snapshot()
			again, err := parseSyncCommand(args)
			if err != nil {
				t.Fatal(err)
			}
			again.sync.now = o.now
			if err = again.prepareStateIntent(); err != nil {
				t.Fatal(err)
			}
			if err = again.saveConfig(); err != nil {
				t.Fatal(err)
			}
			if err = runSyncReady(ctx, &again.sync, again.ready, f, func() error { t.Fatal("block replayed data work"); return nil }); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, f.snapshot()) {
				t.Fatal("block repeated backend work")
			}
		})
	}
}

func TestReadyNoopUnavailableFenceDoesNotReplay(t *testing.T) {
	f, o, r := redoReadyFixture(t)
	ctx := context.Background()
	if err := runSyncReady(ctx, &o, r, f, func() error { return runSync(ctx, o, f) }); err != nil {
		t.Fatal(err)
	}
	f.unknown["metric.001"] = true
	before := f.snapshot()
	err := runSyncReady(ctx, &o, r, f, func() error { t.Fatal("replayed data for unknown fence"); return nil })
	if err == nil || !strings.Contains(err.Error(), "metric.001") {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, f.snapshot()) {
		t.Fatal("data work for unknown fence")
	}
}
