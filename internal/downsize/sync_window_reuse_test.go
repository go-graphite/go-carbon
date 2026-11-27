package downsize

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestSyncIdenticalWindowsReusePhysicalComparison(t *testing.T) {
	t.Parallel()
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "digest", true: "legacy"}[legacy], func(t *testing.T) {
			f := newFakeSync()
			schema := []syncArchive{{Step: 60, Points: 240}}
			gap := syncTestNow - 120
			f.put(syncD1, "m", schema)
			f.put(syncT, "m", schema, gap)
			if legacy {
				f.noDigest[syncT] = true
				f.noDigest[syncD1] = true
			}
			windows := syncRanges{{"period", gap - 600, syncTestNow}, {"gap", gap - 600, syncTestNow}, {"clipped", gap - 600, syncTestNow + 3600}, {"other", gap - 300, syncTestNow}}
			s := &syncer{o: syncOptions{to: syncT, from: []string{syncD1}, windows: windows}, b: f}
			s.masks.Store("m", uint64(1))
			names := []string{"m"}
			schemas := map[string][]syncArchive{"m": schema}
			var want syncRow
			for _, w := range windows {
				w.Until = min(w.Until, syncTestNow)
				if err := s.checkWindow(context.Background(), &want, names, schemas, w); err != nil {
					t.Fatal(err)
				}
			}
			referenceFetches, referenceDigests := f.fetches.Load(), f.digested.Load()
			f.fetches.Store(0)
			f.digested.Store(0)
			var got syncRow
			if err := s.verifyWindows(context.Background(), &got, names, schemas, syncTestNow); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("verdicts differ: got %+v want %+v", got, want)
			}
			if len(got.Holes) != len(windows) {
				t.Fatalf("lost named verdicts: %+v", got.Holes)
			}
			if f.fetches.Load()*2 != referenceFetches || !legacy && f.digested.Load()*2 != referenceDigests {
				t.Fatalf("physical requests not halved: fetch %d/%d digest %d/%d", f.fetches.Load(), referenceFetches, f.digested.Load(), referenceDigests)
			}
			before := f.fetches.Load()
			var next syncRow
			if err := s.verifyWindows(context.Background(), &next, names, schemas, syncTestNow); err != nil {
				t.Fatal(err)
			}
			if f.fetches.Load() != 2*before {
				t.Fatal("cached across name batches")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := s.verifyWindows(ctx, &next, names, schemas, syncTestNow); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
		})
	}
}
