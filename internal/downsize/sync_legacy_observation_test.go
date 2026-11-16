package downsize

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

type legacyExpiryFixture struct {
	*redoHarness
	expired bool
}

func (f *legacyExpiryFixture) WindowProofs(ctx context.Context, host string, names []string, from, until int64) ([]string, error) {
	return (&boundedProofFixture{proofFixture: f.proofFixture, incarnation: "durable"}).WindowProofs(ctx, host, names, from, until)
}

func (f *legacyExpiryFixture) ExpirationEvidence(_ context.Context, host string, names []string, _ bool) (map[string]buckyd.ExpirationEvidence, error) {
	out := map[string]buckyd.ExpirationEvidence{}
	if f.expired && host == syncT {
		for _, n := range names {
			if n == "metric.00" {
				out[n] = buckyd.ExpirationEvidence{Receipt: &chunkstore.ExpirationReceipt{DeletedAt: time.Now().Unix() + 1}}
			}
		}
	}
	return out, nil
}

// The first invocation writes actual legacy rows with no frontier or seals.
// Resume has modern endpoints; only recorded destructive events reopen data.
func TestSyncLegacyObservationSelectiveAdoption(t *testing.T) {
	for _, change := range []string{"none", "target_write", "expiry", "delete", "donor", "gap", "gap_checkpoint", "outside_gap", "older_event"} {
		t.Run(change, func(t *testing.T) {
			f, o := newRedoHarness(t)
			o.noCopy = true
			f.unsupported = true
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.cancel = cancel
			f.kill = func(op string, names []string) bool {
				return op == "digest" && len(names) > 0 && names[0] >= "metric.02"
			}
			b := &legacyExpiryFixture{redoHarness: f}
			if err := runSync(ctx, o, b); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			identity, _ := syncIdentity(o)
			j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
			if err != nil {
				t.Fatal(err)
			}
			rows := j.find(func(r syncRow) bool { return r.Kind == "verified" })
			if len(rows) != 1 || rows[0].ObservationProof != "" {
				t.Fatalf("not legacy: %+v", rows)
			}
			inv := syncInvalidation{Names: []string{"metric.00"}}
			switch change {
			case "target_write":
				f.store(syncT).points["metric.00"][3600][syncTestNow-10800] = 42
			case "expiry":
				b.expired = true
			case "delete":
				inv.Reason = "target_delete"
			case "donor":
				inv.Reason = "donor_change"
			case "gap", "gap_checkpoint", "outside_gap":
				inv.Reason = "store_restart_gap"
				inv.Names = nil
				inv.Windows = syncRanges{{Name: "restart", From: syncTestNow - 7200, Until: syncTestNow - 3600}}
				if change == "outside_gap" {
					inv.Windows = syncRanges{{Name: "outside", From: syncTestNow + 3600, Until: syncTestNow + 7200}}
				}
			case "older_event":
				inv.Reason = "target_delete"
			}
			if change == "gap_checkpoint" {
				if err := atomicStateJSON(filepath.Join(o.state, "ready-recovery.json"), readyRecovery{Identity: identity, RestartWindow: true, Windows: inv.Windows, Restarts: inv.Windows}); err != nil {
					t.Fatal(err)
				}
				inv.Reason = ""
			}
			if inv.Reason != "" {
				if err := j.append(syncRow{Kind: "verification-invalidated", Invalidation: &inv}); err != nil {
					t.Fatal(err)
				}
			}
			if change == "older_event" {
				if err := j.append(rows[0]); err != nil {
					t.Fatal(err)
				}
			}
			j.close()
			before := f.snapshot()
			f.kill = nil
			f.unsupported = false
			if err := runSync(context.Background(), o, b); err != nil && !errors.Is(err, ErrSyncResidual) {
				t.Fatal(err)
			}
			p := redoProgress(t, o.state)
			want := int64(0)
			if change == "expiry" || change == "delete" || change == "donor" {
				want = 1
			}
			if change == "gap" || change == "gap_checkpoint" {
				want = 2
			}
			if p.LegacyReverified != want || p.LegacyAdopted != 2-want {
				t.Fatalf("adoption counters: %+v", p)
			}
			after := f.snapshot()
			for k, n := range after {
				if k.Name >= "metric.02" || n <= before[k] {
					continue
				}
				if want == 0 {
					t.Fatalf("reopened %+v", k)
				}
				if change != "gap" && change != "gap_checkpoint" && k.Name != "metric.00" {
					t.Fatalf("unaffected name reopened: %+v", k)
				}
				if (change == "gap" || change == "gap_checkpoint") && (k.From < syncTestNow-7200 || k.Until > syncTestNow-3600) {
					t.Fatalf("read escaped gap: %+v", k)
				}
			}
			if change == "gap" || change == "gap_checkpoint" {
				if err := runSync(context.Background(), o, b); err != nil && !errors.Is(err, ErrSyncResidual) {
					t.Fatal(err)
				}
				assertNoRedoCalls(t, after, f.snapshot())
				if got := redoProgress(t, o.state).LegacyReverified; got != want {
					t.Fatalf("restart event applied twice: %d", got)
				}
			}
		})
	}
}

func TestSyncAdoptedLegacyWritesAndDonorChanges(t *testing.T) {
	f, o := newRedoHarness(t)
	o.noCopy = true
	f.unsupported = true
	b := &legacyExpiryFixture{redoHarness: f}
	f.kill = func(op string, names []string) bool {
		return op == "digest" && len(names) > 0 && names[0] >= "metric.02"
	}
	run := func() {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.cancel = cancel
		if err := runSync(ctx, o, b); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	run()
	f.unsupported = false
	before := f.snapshot()
	run()
	assertNoRedoCalls(t, before, f.snapshot())
	f.store(syncT).points["metric.00"][3600][syncTestNow-10800] = 42
	before = f.snapshot()
	run()
	assertNoRedoCalls(t, before, f.snapshot())
	if p := redoProgress(t, o.state); p.LegacyAdopted != 2 || p.LegacyReverified != 0 {
		t.Fatalf("recounted adoption: %+v", p)
	}
	f.store(syncD1).points["metric.00"][3600][syncTestNow-10800] = 43
	before = f.snapshot()
	run()
	repeated := false
	for k, n := range f.snapshot() {
		if k.Name == "metric.00" && n > before[k] {
			repeated = true
		}
		if k.Name == "metric.01" && n > before[k] {
			t.Fatalf("donor change reopened neighbor: %+v", k)
		}
	}
	if !repeated {
		t.Fatal("donor change was ignored")
	}
	if p := redoProgress(t, o.state); p.LegacyAdopted != 2 || p.LegacyReverified != 1 {
		t.Fatalf("missing donor accounting: %+v", p)
	}
}
