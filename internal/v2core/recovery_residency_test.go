package v2core

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Recovery needs source archives for later rollups, while history readers need
// every archive. Neither path may change the physical-slot winners.
func TestReducerRecoveryKeepsOnlySourceArchives(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	t.Setenv("CORE_MERGE_WORKERS", "0")
	cfg, policies := checkpointTestSetup(t)
	e, err := Open(t.TempDir(), cfg, Hooks{Now: func() time.Time { return time.Unix(1794701400, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	checkpointFixture(t, e, policies, 0, 32)
	snapshot := e.DBs()[0].NewSnapshot()
	defer snapshot.Close()
	fold := func(reducerOnly bool) map[partsformat.ID]*rollupScanEntry {
		iter, err := newArchiveIter(context.Background(), snapshot, rollupScanBounds())
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		series := map[partsformat.ID]*rollupScanEntry{}
		if err := foldRollupKeys(iter, &PolicyCatalog{}, series, reducerOnly); err != nil {
			t.Fatal(err)
		}
		return series
	}
	full, reduced := fold(false), fold(true)
	if len(full) != len(policies) || len(reduced) != len(full) {
		t.Fatalf("lost names: full=%d reduced=%d", len(full), len(reduced))
	}
	allState, reducedState := newCoreRollups(), newCoreRollups()
	terminal, sources := 0, 0
	for name, entry := range full {
		policy, meta, records, err := entry.finish(name)
		if err != nil {
			t.Fatal(err)
		}
		_, gotMeta, got, err := reduced[name].finish(name)
		if err != nil {
			t.Fatal(err)
		}
		want := slices.Clone(records)
		want = slices.DeleteFunc(want, func(r sealrollup.Record) bool {
			if r.Archive == len(policy.Config.Archives)-1 {
				terminal++
				return true
			}
			sources++
			return false
		})
		if !slices.Equal(got, want) || compactMetadata(meta) != compactMetadata(gotMeta) {
			t.Fatalf("%s: source samples or metadata changed", name)
		}
		if err := allState.installRecovered(policy, meta, records); err != nil {
			t.Fatal(err)
		}
		if err := reducedState.installRecovered(policy, gotMeta, got); err != nil {
			t.Fatal(err)
		}
	}
	if terminal == 0 || sources == 0 {
		t.Fatal("fixture must exercise both terminal history and rollup sources")
	}
	if rollupStateDigest(allState) != rollupStateDigest(reducedState) {
		t.Fatal("reducer state differs from complete history")
	}
}
