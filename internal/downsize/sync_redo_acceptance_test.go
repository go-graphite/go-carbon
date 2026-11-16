package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type redoCall struct {
	Op, Store, Name string
	From, Until     int64
}

// Counts actual successful backend work per metric and range, independently of
// storectl's counters. The failure hook runs before work reaches the backend.
type redoHarness struct {
	*proofFixture
	callsMu        sync.Mutex
	calls          map[redoCall]int
	kill           func(string, []string) bool
	cutAfterDigest bool
	cancel         context.CancelFunc
}

func (f *redoHarness) before(op string, names []string) error {
	if f.kill != nil && f.kill(op, names) {
		f.cancel()
		return context.Canceled
	}
	return nil
}
func (f *redoHarness) count(op, store string, names []string, from, until int64) {
	f.callsMu.Lock()
	defer f.callsMu.Unlock()
	for _, n := range names {
		f.calls[redoCall{op, store, n, from, until}]++
	}
}
func (f *redoHarness) List(ctx context.Context, store, prefix, after string, limit int) ([]string, error) {
	if err := f.before("list", nil); err != nil {
		return nil, err
	}
	return f.fakeSync.List(ctx, store, prefix, after, limit)
}
func (f *redoHarness) Info(ctx context.Context, store string, names []string) (map[string][]syncArchive, error) {
	if err := f.before("info", names); err != nil {
		return nil, err
	}
	return f.fakeSync.Info(ctx, store, names)
}
func (f *redoHarness) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	if err := f.before("digest", names); err != nil {
		return nil, err
	}
	got, err := f.proofFixture.Digest(ctx, store, names, from, until)
	if err == nil {
		f.count("digest", store, names, from, until)
		if f.cutAfterDigest {
			f.cancel()
			return nil, context.Canceled
		}
	}
	return got, err
}
func (f *redoHarness) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	if err := f.before("fetch", names); err != nil {
		return nil, err
	}
	got, err := f.fakeSync.Fetch(ctx, store, names, from, until)
	if err == nil {
		f.count("fetch", store, names, from, until)
	}
	return got, err
}
func (f *redoHarness) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	if err := f.before("import", names); err != nil {
		return syncCopyResult{}, err
	}
	got, err := f.fakeSync.Copy(ctx, source, target, names, journal)
	if err == nil {
		f.count("import", source, names, 0, 0)
	}
	return got, err
}
func (f *redoHarness) snapshot() map[redoCall]int {
	f.callsMu.Lock()
	defer f.callsMu.Unlock()
	out := map[redoCall]int{}
	for k, v := range f.calls {
		out[k] = v
	}
	return out
}
func newRedoHarness(t *testing.T) (*redoHarness, syncOptions) {
	t.Helper()
	p := &proofFixture{fakeSync: newFakeSync(), epoch: "stable", unknown: map[string]bool{}, seen: map[string]bool{}}
	p.listPageSize = 3
	for _, host := range []string{syncT, syncD1} {
		for i := range 6 {
			p.put(host, fmt.Sprintf("metric.%02d", i), []syncArchive{{3600, 240}})
		}
	}
	delete(p.store(syncT).archives, "metric.05")
	delete(p.store(syncT).points, "metric.05")
	f := &redoHarness{proofFixture: p, calls: map[redoCall]int{}}
	o := syncOptions{to: syncT, from: []string{syncD1}, state: t.TempDir(), batch: 2, streams: 1, maxRounds: 2, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	return f, o
}
func redoProgress(t *testing.T, state string) progressSnapshot {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(state, "progress.json"))
	if err != nil {
		t.Fatal(err)
	}
	var p progressSnapshot
	if err = json.Unmarshal(data, &p); err != nil {
		t.Fatal(err)
	}
	return p
}
func assertNoRedoCalls(t *testing.T, before, after map[redoCall]int) {
	t.Helper()
	for k, n := range before {
		if after[k] != n {
			t.Fatalf("completed work repeated: %+v before=%d after=%d", k, n, after[k])
		}
	}
}
func TestRedoAcceptanceDurablePhaseBoundaries(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"before_discovery", "discovery_committed", "copy_committed", "verify_batch_committed"} {
		t.Run(stage, func(t *testing.T) {
			f, o := newRedoHarness(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.cancel = cancel
			f.kill = func(op string, names []string) bool {
				switch stage {
				case "before_discovery":
					return op == "list"
				case "discovery_committed":
					return op == "import"
				case "copy_committed":
					return op == "digest"
				case "verify_batch_committed":
					return op == "digest" && len(names) > 0 && names[0] >= "metric.02"
				}
				return false
			}
			if err := runSync(ctx, o, f); !errors.Is(err, context.Canceled) {
				t.Fatalf("kill not injected: %v", err)
			}
			before := f.snapshot()
			saved := redoProgress(t, o.state)
			f.kill = nil
			if err := runSync(context.Background(), o, f); err != nil {
				t.Fatal(err)
			}
			after := redoProgress(t, o.state)
			assertNoRedoCalls(t, before, f.snapshot())
			if after.Done < saved.Done {
				t.Fatalf("durable done moved backward: %d -> %d", saved.Done, after.Done)
			}
			for phase, n := range after.RedoNames {
				if n != 0 {
					t.Fatalf("clean resume %s redo=%d", phase, n)
				}
			}
		})
	}
}
func TestRedoAcceptanceClientVersionAndBatchBudget(t *testing.T) {
	t.Parallel()
	f, o := newRedoHarness(t)
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	before := f.snapshot()
	saved := redoProgress(t, o.state)
	identity, _ := syncIdentity(o)
	// The executable version is deliberately absent from sync identity. Budget
	// changes exercise the actual backend rather than comparing counters alone.
	oldVersion, newVersion := "aaaaaaaaaaaa", "bbbbbbbbbbbb"
	if progressVersion(oldVersion) == progressVersion(newVersion) {
		t.Fatal("version fixture is not distinct")
	}
	o.batch, o.streams = 3, 2
	if got, _ := syncIdentity(o); got != identity {
		t.Fatal("operational budget changed identity")
	}
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, f.snapshot()) {
		t.Fatal("clean completed restart made backend calls")
	}
	if p := redoProgress(t, o.state); p.Done < saved.Done {
		t.Fatal("done regressed")
	}
}
func TestRedoAcceptanceCorruptJournalFailsClosed(t *testing.T) {
	t.Parallel()
	f, o := newRedoHarness(t)
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(o.state, "sync.jsonl")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteString("corrupt complete row\n"); err != nil {
		t.Fatal(err)
	}
	file.Close()
	before := f.snapshot()
	if err := runSync(context.Background(), o, f); err == nil {
		t.Fatal("corrupt state silently started fresh")
	}
	if !reflect.DeepEqual(before, f.snapshot()) {
		t.Fatal("corrupt state reached backend")
	}
}

// Each pending contract contains the real assertion. Set this variable to one
// subtest name to evaluate a worker's fix before removing that contract's Skip.
func TestRedoAcceptancePendingContracts(t *testing.T) {
	for _, tc := range []struct{ name, owner, reason string }{
		{"kill_mid_batch", "sync-ledger2", "successful subcalls before a batch receipt need durable operation-level resume"},
		{"store_restart_only_gap", "restart-gap", "durable restart lineage and dirty candidate proof unavailable; epoch invalidates clean history"},
		{"verify_complete_ready_failed", "sync-ledger2 / ready-blockers", "a later target-only write must preserve completed historical proof"},
		{"ready_terminal_no_backend_work", "sync-ledger2", "READY restart still repeats final policy/parity/readback"},
		{"all_mutation_state_missing", "sync-ledger2", "absence of journal and all evidence requires an external durable run manifest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if os.Getenv("STORECTL_REDO_CONTRACT") != tc.name {
				t.Skipf("owner %s: %s", tc.owner, tc.reason)
			}
			switch tc.name {
			case "kill_mid_batch":
				f, o := newRedoHarness(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				f.cancel = cancel
				f.cutAfterDigest = true
				if err := runSync(ctx, o, f); err == nil {
					t.Fatal("kill not injected")
				}
				before := f.snapshot()
				f.cutAfterDigest = false
				if err := runSync(context.Background(), o, f); err != nil {
					t.Fatal(err)
				}
				assertNoRedoCalls(t, before, f.snapshot())
			case "all_mutation_state_missing":
				f, o := newRedoHarness(t)
				if err := runSync(context.Background(), o, f); err != nil {
					t.Fatal(err)
				}
				entries, err := os.ReadDir(o.state)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if err := os.RemoveAll(filepath.Join(o.state, entry.Name())); err != nil {
						t.Fatal(err)
					}
				}
				before := f.snapshot()
				if err := runSync(context.Background(), o, f); err == nil {
					t.Fatal("all prior state lost but fresh run silently accepted")
				}
				assertNoRedoCalls(t, before, f.snapshot())
			default:
				f, o, r := redoReadyFixture(t)
				if tc.name == "verify_complete_ready_failed" {
					r.carbonAPI = ""
				}
				err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) })
				if tc.name == "verify_complete_ready_failed" {
					if !errors.Is(err, ErrSyncNotReady) {
						t.Fatal(err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				before := f.snapshot()
				oldReport := readReadyReport(t, o.state)
				if tc.name == "verify_complete_ready_failed" {
					r.carbonAPI = "http://read.example"
					f.store(syncT).points["metric.002"][3600][syncTestNow+3600] = 1
				}
				if tc.name == "store_restart_only_gap" {
					f.ready.starts[syncT] += 7200
					f.epoch = "restarted"
					o.now = func() time.Time { return time.Unix(syncTestNow+7200, 0) }
				}
				if err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) }); err != nil {
					t.Fatal(err)
				}
				assertNoRedoCalls(t, before, f.snapshot())
				if tc.name == "ready_terminal_no_backend_work" && !reflect.DeepEqual(oldReport, readReadyReport(t, o.state)) {
					t.Fatal("terminal verdict was recomputed")
				}
				if tc.name == "store_restart_only_gap" {
					for call, n := range f.snapshot() {
						if n > before[call] && (call.From < syncTestNow || call.Until > syncTestNow+7200) {
							t.Fatalf("work escaped known gap [%d,%d): %+v", syncTestNow, syncTestNow+7200, call)
						}
					}
				}
			}
		})
	}
}

type redoReadyHarness struct {
	*redoHarness
	ready *readyFixture
}

func (f *redoReadyHarness) Process(ctx context.Context, store string, port int) (float64, error) {
	return f.ready.Process(ctx, store, port)
}
func (f *redoReadyHarness) Policies(ctx context.Context, store string, names []string) (map[string]readyPolicy, error) {
	return f.ready.Policies(ctx, store, names)
}
func (f *redoReadyHarness) Readback(ctx context.Context, _ string, name string, from, until int64) (syncSeries, error) {
	got, err := f.Fetch(ctx, syncT, []string{name}, from, until)
	return got[name], err
}
func redoReadyFixture(t *testing.T) (*redoReadyHarness, syncOptions, syncReadyOptions) {
	t.Helper()
	ready, o, r := newReadyFixture(t, 100)
	proof := &proofFixture{fakeSync: ready.fakeSync, epoch: "stable", unknown: map[string]bool{}, seen: map[string]bool{}}
	return &redoReadyHarness{redoHarness: &redoHarness{proofFixture: proof, calls: map[redoCall]int{}}, ready: ready}, o, r
}
