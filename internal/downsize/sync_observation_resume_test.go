package downsize

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// A comparison which found holes is completed work too. A client exit before
// later batches finish must retain both the observation and the repair intent.
func TestSyncInterruptedObservationWithHolesResumes(t *testing.T) {
	t.Parallel()
	f, o := newRedoHarness(t)
	o.noCopy = true
	delete(f.store(syncT).points["metric.00"][3600], syncTestNow-7200)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.cancel = cancel
	f.kill = func(op string, names []string) bool {
		return op == "digest" && len(names) > 0 && names[0] >= "metric.02"
	}
	if err := runSync(ctx, o, f); !errors.Is(err, context.Canceled) {
		t.Fatalf("kill: %v", err)
	}
	before := f.snapshot()
	if len(before) == 0 {
		t.Fatal("no completed backend work before interruption")
	}
	saved := redoProgress(t, o.state)
	f.kill = nil
	if err := runSync(context.Background(), o, f); !errors.Is(err, ErrSyncResidual) {
		t.Fatalf("lost holes: %v", err)
	}
	assertNoRedoCalls(t, before, f.snapshot())
	if got := redoProgress(t, o.state); got.Done < saved.Done {
		t.Fatalf("done regressed: %d -> %d", saved.Done, got.Done)
	}
}

// Modern seals retain their mutation rules; unsealed legacy observations are
// adopted when no destructive invalidation has been recorded.
func TestSyncObservationResumeInvalidation(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"none", "epoch", "target_write", "donor_write", "unknown", "legacy"} {
		t.Run(change, func(t *testing.T) {
			f, o := newRedoHarness(t)
			o.noCopy = true
			delete(f.store(syncT).points["metric.00"][3600], syncTestNow-7200)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.cancel = cancel
			f.kill = func(op string, names []string) bool {
				return op == "digest" && len(names) > 0 && names[0] >= "metric.02"
			}
			if err := runSync(ctx, o, f); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			before := f.snapshot()
			f.kill = nil
			switch change {
			case "epoch":
				f.epoch = "new process"
			case "target_write":
				f.store(syncT).points["metric.00"][3600][syncTestNow-10800] = 42
			case "donor_write":
				f.store(syncD1).points["metric.00"][3600][syncTestNow-10800] = 42
			case "unknown":
				f.unknown["metric.00"] = true
			case "legacy":
				identity, _ := syncIdentity(o)
				j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
				if err != nil {
					t.Fatal(err)
				}
				rows := j.find(func(r syncRow) bool { return r.Kind == "verified" })
				for _, r := range rows {
					r.MutationObserved = false
					r.ObservationProof = ""
					if err := j.append(r); err != nil {
						t.Fatal(err)
					}
				}
				j.close()
			}
			if err := runSync(context.Background(), o, f); !errors.Is(err, ErrSyncResidual) {
				t.Fatalf("lost residual: %v", err)
			}
			after := f.snapshot()
			repeated := false
			for k, n := range before {
				if k.Name == "metric.00" && after[k] > n {
					repeated = true
				}
			}
			if repeated != (change != "none" && change != "legacy") {
				t.Fatalf("change=%s rechecked prior hole=%v", change, repeated)
			}
		})
	}
}

func TestObservationProofBindsMembership(t *testing.T) {
	t.Parallel()
	stamps := []syncProof{{1}, {2}}
	p := observationProof([]string{"a", "b"}, []int64{0, 1}, stamps)
	if p == "" {
		t.Fatal("missing known proof")
	}
	for _, other := range []string{
		observationProof([]string{"a", "c"}, []int64{0, 1}, stamps),
		observationProof([]string{"a", "b"}, []int64{0, 2}, stamps),
		observationProof([]string{"a", "b"}, []int64{0, 1}, []syncProof{{2}, {1}}),
		observationProof([]string{"a", "b"}, []int64{0, 1}, []syncProof{{1}, {}}),
	} {
		if other == p {
			t.Fatal("changed membership reused observation")
		}
	}
	if observationProof([]string{"a"}, nil, stamps) != "" {
		t.Fatal("incomplete IDs accepted")
	}
}
