package downsize

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

type blockedReadyBackend struct {
	*readyFixture
	digests atomic.Int64
	fetches atomic.Int64
	imports atomic.Int64
}

func (b *blockedReadyBackend) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	b.digests.Add(1)
	return b.readyFixture.Digest(ctx, store, names, from, until)
}
func (b *blockedReadyBackend) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	b.fetches.Add(1)
	return b.readyFixture.Fetch(ctx, store, names, from, until)
}
func (b *blockedReadyBackend) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	b.imports.Add(1)
	return b.readyFixture.Copy(ctx, source, target, names, journal)
}

func TestReadyCompletedObservationMissingInputsDoesNoWork(t *testing.T) {
	t.Parallel()
	for _, recovery := range []bool{false, true} {
		t.Run(map[bool]string{false: "root", true: "recovery"}[recovery], func(t *testing.T) {
			f, o, r := newReadyFixture(t, 100)
			b := &blockedReadyBackend{readyFixture: f}
			ctx := context.Background()
			if recovery {
				// Finish recovery, then fail a final READY gate. This leaves the
				// real recovery intent and completed per-name receipts in place.
				f.policy = func(_ string, _ string, p *readyPolicy) { p.XFilesFactor = .25 }
				f.read = func(s syncSeries) syncSeries { s.Step++; return s }
			} else {
				r.gapsComplete = false
			}
			err := runSyncReady(ctx, &o, r, b, func() error {
				if err := runSync(ctx, o, b); err != nil {
					return err
				}
				if recovery {
					f.starts[syncT]++
				}
				return nil
			})
			if !errors.Is(err, ErrSyncNotReady) {
				t.Fatalf("first verdict: %v", err)
			}
			before := readReadyReport(t, o.state)
			if recovery && !strings.Contains(before.ProofState, "ready-recovery-") {
				t.Fatalf("no recovery: %+v", before)
			}
			var checkpoint []byte
			if recovery {
				// Adopt a completed pre-upgrade recovery even when a newer partial
				// recovery has already replaced the active intent (the live failure).
				if err := os.Remove(filepath.Join(o.state, "ready-completed.json")); err != nil {
					t.Fatal(err)
				}
				cp, err := readReadyRecovery(filepath.Join(o.state, "ready-recovery.json"), o, r.scope)
				if err != nil || cp == nil {
					t.Fatalf("checkpoint: %v", err)
				}
				cp.State += "-pending"
				if err := atomicStateJSON(filepath.Join(o.state, "ready-recovery.json"), cp); err != nil {
					t.Fatal(err)
				}
				checkpoint, _ = os.ReadFile(filepath.Join(o.state, "ready-recovery.json"))
			}
			b.digests.Store(0)
			b.fetches.Store(0)
			b.imports.Store(0)
			f.read, f.policy = nil, nil
			r.gapsComplete = false
			// A newly observed restart must remain pending, not trigger another
			// full catalog pass while operator prerequisites still block READY.
			f.starts[syncT]++
			for range 2 {
				err = runSyncReady(ctx, &o, r, b, func() error { t.Fatal("replayed initial work"); return nil })
				if !errors.Is(err, ErrSyncNotReady) {
					t.Fatal(err)
				}
				after := readReadyReport(t, o.state)
				if after.Verdict != "NOT_READY" || after.ProofState != before.ProofState || len(after.Reasons) != 1 || !strings.Contains(after.Reasons[0], "gap-ledger-complete") {
					t.Fatalf("lost completion/reasons: %+v", after)
				}
			}
			if b.digests.Load() != 0 || b.fetches.Load() != 0 || b.imports.Load() != 0 {
				t.Fatalf("repeated backend calls: digest=%d fetch=%d import=%d", b.digests.Load(), b.fetches.Load(), b.imports.Load())
			}
			if recovery {
				after, _ := os.ReadFile(filepath.Join(o.state, "ready-recovery.json"))
				if string(after) != string(checkpoint) {
					t.Fatal("pending recovery intent changed")
				}
			}
		})
	}
}

func TestReadyObservationNeverBypassesMutationChecks(t *testing.T) {
	t.Parallel()
	f, o, r := newReadyFixture(t, 100)
	ctx := context.Background()
	r.gapsComplete = false
	if err := runSyncReady(ctx, &o, r, f, func() error { return runSync(ctx, o, f) }); !errors.Is(err, ErrSyncNotReady) {
		t.Fatal(err)
	}
	delete(f.store(syncT).points["metric.001"][3600], syncTestNow-5*3600)
	r.gapsComplete = true
	initial := false
	if err := runSyncReady(ctx, &o, r, f, func() error { initial = true; return runSync(ctx, o, f) }); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.store(syncT).points["metric.001"][3600][syncTestNow-5*3600]; !initial || !ok {
		t.Fatal("completed observation bypassed mutation/repair")
	}
}
