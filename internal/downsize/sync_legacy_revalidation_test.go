package downsize

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

type legacyRevalidationFixture struct {
	*redoHarness
	bounded *boundedProofFixture
}

func (f *legacyRevalidationFixture) WindowProofs(ctx context.Context, host string, names []string, from, until int64) ([]string, error) {
	return f.bounded.WindowProofs(ctx, host, names, from, until)
}

// A pre-v2 observation has no digest baseline. Revalidate it once using the
// existing digest-first comparison, then seal holes as well as clean names.
func TestSyncLegacyRevalidationSealsOnceAcrossStoreRestart(t *testing.T) {
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
	identity, _ := syncIdentity(o)
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range j.find(func(r syncRow) bool { return r.Kind == "verified" }) {
		row.ObservationProof = "" // exact legacy on-disk shape, including holes
		if err := j.append(row); err != nil {
			t.Fatal(err)
		}
	}
	j.close()
	f.kill = nil
	b := &legacyRevalidationFixture{f, &boundedProofFixture{proofFixture: f.proofFixture, incarnation: "durable"}}
	if err := runSync(context.Background(), o, b); !errors.Is(err, ErrSyncResidual) {
		t.Fatal(err)
	}
	if got := redoProgress(t, o.state).LegacyRevalidated; got != 2 {
		t.Fatalf("migration names=%d, want 2", got)
	}
	before := f.snapshot()
	f.epoch = "clean-successor"
	if err := runSync(context.Background(), o, b); !errors.Is(err, ErrSyncResidual) {
		t.Fatal(err)
	}
	assertNoRedoCalls(t, before, f.snapshot())
	if got := redoProgress(t, o.state).LegacyRevalidated; got != 2 {
		t.Fatalf("migration counted twice: %d", got)
	}
	j, err = openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	for _, row := range j.find(func(r syncRow) bool { return r.Kind == "verified" && r.LegacyRevalidated > 0 }) {
		if !strings.HasPrefix(row.ObservationProof, "v2:") || len(row.Holes) == 0 {
			t.Fatalf("migration lost v2 seal or hole observation: %+v", row)
		}
	}
}

func TestLegacyRevalidationProgressIsDurableAndSeparate(t *testing.T) {
	p := progressSnapshot{}
	p.apply(syncRow{Kind: "verified", Names: 100, LegacyRevalidated: 30})
	p.apply(syncRow{Kind: "verified", Names: 100, Reused: 100})
	if p.LegacyRevalidated != 30 || len(p.RedoNames) != 0 {
		t.Fatalf("migration/replay/redo conflated: %+v", p)
	}
}

// One unknown holder must not reopen an unrelated completed hole observation.
func TestSyncMixedKnownAndUnknownHoleProofs(t *testing.T) {
	f, o := newRedoHarness(t)
	o.noCopy = true
	f.unknown["metric.01"] = true
	delete(f.store(syncT).points["metric.00"][3600], syncTestNow-7200)
	b := &legacyRevalidationFixture{f, &boundedProofFixture{proofFixture: f.proofFixture, incarnation: "durable"}}
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
	before := f.snapshot()
	f.epoch = "restarted"
	run()
	after := f.snapshot()
	unknownRepeated := false
	for call, n := range before {
		if call.Name == "metric.00" && after[call] != n {
			t.Fatalf("known hole repeated: %+v", call)
		}
		if call.Name == "metric.01" && after[call] > n {
			unknownRepeated = true
		}
	}
	if !unknownRepeated {
		t.Fatal("unknown holder was trusted")
	}
	f.store(syncD1).points["metric.00"][3600][syncTestNow-10800] = 42
	before = after
	run()
	changedRepeated := false
	for call, n := range before {
		if call.Name == "metric.00" && f.snapshot()[call] > n {
			changedRepeated = true
		}
	}
	if !changedRepeated {
		t.Fatal("mutated hole proof reused")
	}
}
