package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type recoveryDigestWatch struct {
	*readyFixture
	mu     sync.Mutex
	armed  bool
	cancel context.CancelFunc
	calls  int
	names  []string
}

func (b *recoveryDigestWatch) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	b.mu.Lock()
	// The separate 24h presence probe is not a window-verification receipt.
	if b.armed && store == syncT && until-from < 86400 {
		b.calls++
		if b.cancel != nil && b.calls == 2 {
			b.cancel()
			b.mu.Unlock()
			return nil, ctx.Err()
		}
		b.names = append(b.names, names...)
	}
	b.mu.Unlock()
	return b.fakeSync.Digest(ctx, store, names, from, until)
}

func TestReadyInterruptedRecoveryResumesVerifiedPrefix(t *testing.T) {
	t.Parallel()
	for _, restarted := range []bool{false, true} {
		t.Run(map[bool]string{false: "same processes", true: "another restart"}[restarted], func(t *testing.T) {
			f, o, r := newReadyFixture(t, 100)
			o.batch, o.streams, o.verifyStreams = 10, 1, 1
			clock := syncTestNow - 7200
			wall := time.Now()
			o.now = func() time.Time { return time.Unix(clock, 0).Add(time.Since(wall)) }
			r.period[0].Until = syncTestNow - 10800
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b := &recoveryDigestWatch{readyFixture: f, cancel: cancel}
			err := runSyncReady(ctx, &o, r, b, func() error {
				if err := runSync(ctx, o, b); err != nil {
					return err
				}
				clock = syncTestNow
				f.starts[syncT]++
				b.armed = true
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("interruption: %v", err)
			}
			checkpoint := filepath.Join(o.state, "ready-recovery.json")
			cp, err := readReadyRecovery(checkpoint, o, r.scope)
			if err != nil || cp == nil {
				t.Fatalf("checkpoint: %+v %v", cp, err)
			}
			states, _ := filepath.Glob(filepath.Join(o.state, "ready-recovery-*"))
			if len(states) != 1 {
				t.Fatalf("recovery states: %v", states)
			}
			journal, err := os.ReadFile(filepath.Join(states[0], "sync.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			if len(b.names) != 10 {
				t.Fatalf("completed prefix: %v", b.names)
			}
			prefix := append([]string(nil), b.names...)
			b.names, b.calls, b.cancel = nil, 0, nil
			// Resume later: this must keep the original window and its completed batch.
			clock += 60
			if restarted {
				clock += 3600
				f.starts[syncT]++
			}
			if err = runSyncReady(context.Background(), &o, r, b, func() error { return runSync(context.Background(), o, b) }); err != nil {
				t.Fatal(err)
			}
			wantNames, wantStates := 90, 1
			if restarted {
				wantNames, wantStates = 190, 2
			}
			if len(b.names) != wantNames {
				t.Fatalf("repeated completed digest work: got %d names, want %d; prefix=%v", len(b.names), wantNames, prefix)
			}
			for _, n := range b.names[:90] {
				for _, done := range prefix {
					if n == done {
						t.Fatalf("reverified %s", n)
					}
				}
			}
			statesAfter, _ := filepath.Glob(filepath.Join(o.state, "ready-recovery-*"))
			if len(statesAfter) != wantStates || !reflect.DeepEqual(states, statesAfter[:1]) {
				t.Fatalf("new recovery on resume: %v -> %v", states, statesAfter)
			}
			after, err := os.ReadFile(filepath.Join(states[0], "sync.jsonl"))
			if err != nil || len(after) <= len(journal) {
				t.Fatalf("journal did not continue: %v", err)
			}
			report := readReadyReport(t, o.state)
			firstRestart := report.Restarts[:min(1, len(report.Restarts))]
			if report.Verdict != "READY" || len(report.Restarts) != wantStates || firstRestart.String() != cp.Restarts.String() {
				t.Fatalf("lost recovery proof: %+v", report)
			}
			if _, err = os.Stat(checkpoint); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("completed intent remains: %v", err)
			}
		})
	}
}

func TestReadyLaterRestartOnlyChecksNewWindow(t *testing.T) {
	t.Parallel()
	f, o, r := newReadyFixture(t, 100)
	clock := syncTestNow - 14400
	wall := time.Now()
	o.now = func() time.Time { return time.Unix(clock, 0).Add(time.Since(wall)) }
	r.period[0].Until = syncTestNow - 18000
	calls := 0
	f.process = func(host string) {
		if host != syncT {
			return
		}
		calls++
		if calls == 3 {
			clock = syncTestNow
			f.starts[syncT]++
		}
	}
	if err := runSyncReady(context.Background(), &o, r, f, func() error {
		if err := runSync(context.Background(), o, f); err != nil {
			return err
		}
		clock = syncTestNow - 7200
		f.starts[syncT]++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	report := readReadyReport(t, o.state)
	if report.Verdict != "READY" || len(report.Restarts) != 2 {
		t.Fatalf("restart coverage: %+v", report)
	}
	paths, _ := filepath.Glob(filepath.Join(o.state, "ready-recovery-*", "sync.jsonl"))
	if len(paths) != 2 {
		t.Fatalf("recovery states: %v", paths)
	}
	for i, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		var header syncRow
		err = json.NewDecoder(file).Decode(&header)
		file.Close()
		if err != nil {
			t.Fatal(err)
		}
		var scope map[string]any
		if err = json.Unmarshal([]byte(header.Identity), &scope); err != nil {
			t.Fatal(err)
		}
		want := syncRanges{report.Restarts[i]}
		if scope["windows"] != want.String() {
			t.Fatalf("recovery %d repeated another gap: %s", i, header.Identity)
		}
	}
}

func TestReadyRecoveryCheckpointRejectsChangedScope(t *testing.T) {
	t.Parallel()
	_, o, r := newReadyFixture(t, 100)
	identity, err := syncIdentity(o)
	if err != nil {
		t.Fatal(err)
	}
	processes := map[string]readyProcess{}
	for _, store := range append(append([]string(nil), o.from...), o.to) {
		processes[store] = readyProcess{Start: float64(syncTestNow - 100), Observed: syncTestNow - 10}
	}
	valid := readyRecovery{Identity: identity, Scope: r.scope, State: "ready-recovery-1", Windows: syncRanges{{"gap", 1, 2}}, Processes: processes}
	path := filepath.Join(o.state, "ready-recovery.json")
	for _, tc := range []struct {
		name   string
		change func(*readyRecovery)
	}{
		{"valid", func(*readyRecovery) {}},
		{"outside state", func(cp *readyRecovery) { cp.State = "../elsewhere" }},
		{"identity", func(cp *readyRecovery) { cp.Identity = "another sync" }},
		{"ready scope", func(cp *readyRecovery) { cp.Scope = "another scope" }},
		{"process boundary", func(cp *readyRecovery) { cp.Processes = nil }},
		{"window", func(cp *readyRecovery) { cp.Windows = syncRanges{{"gap", 2, 1}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cp := valid
			tc.change(&cp)
			if err := atomicStateJSON(path, cp); err != nil {
				t.Fatal(err)
			}
			_, err := readReadyRecovery(path, o, r.scope)
			if (err == nil) != (tc.name == "valid") {
				t.Fatalf("checkpoint validation: %v", err)
			}
		})
	}
}
