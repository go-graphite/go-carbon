package chunkstore

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func TestFetchWithPrefixNativeOracle(t *testing.T) {
	ctx := context.Background()
	db, now := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, MetadataCacheSize: 1 << 20})
	defer closeTestStore(t, db)
	m := createTestMetric(t, db, "prefix.native")
	for i := range 10 {
		if err := db.UpdateMany(ctx, m.Name, []Point{{Timestamp: int64(now - 600 + i*60), Value: float64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	full, err := db.Fetch(ctx, m.Name, now-600, now)
	if err != nil {
		t.Fatal(err)
	}
	prefix := *full
	prefix.Values = slices.Clone(full.Values[:5])
	prefix.UntilTime = prefix.FromTime + 5*prefix.Step
	epoch := db.ReadCacheEpoch()
	compare := func(from, until int, p *Series, token uint64, wantReuse bool) {
		t.Helper()
		want, err := db.Fetch(ctx, m.Name, from, until)
		if err != nil {
			t.Fatal(err)
		}
		got, reused, err := db.FetchWithPrefix(ctx, m.Name, from, until, p, token)
		if err != nil || (reused > 0) != wantReuse {
			t.Fatalf("reuse=%d want=%v err=%v", reused, wantReuse, err)
		}
		if got == nil || want == nil {
			if got != want {
				t.Fatal("nil mismatch")
			}
			return
		}
		if got.FromTime != want.FromTime || got.UntilTime != want.UntilTime || got.Step != want.Step || len(got.Values) != len(want.Values) {
			t.Fatal("grid differs")
		}
		for i, v := range got.Values {
			if math.Float64bits(v) != math.Float64bits(want.Values[i]) {
				t.Fatalf("value %d differs", i)
			}
		}
		if reused > 0 {
			saved := p.Values[0]
			got.Values[0] = 999
			if math.Float64bits(p.Values[0]) != math.Float64bits(saved) {
				t.Fatal("returned array aliases retained prefix")
			}
		}
	}
	compare(now-600, now, &prefix, epoch, true)
	compare(now-600, prefix.UntilTime-prefix.Step, &prefix, epoch, true)
	compare(now-600, now, &prefix, epoch+1, false)
	compare(now-60, now, &prefix, epoch, false)
	compare(now-600, now+60, &prefix, epoch, false)
	// A late correction invalidates historical values as well as recent ones.
	if err := db.UpdateMany(ctx, m.Name, []Point{{Timestamp: int64(now - 540), Value: 777}}); err != nil {
		t.Fatal(err)
	}
	compare(now-600, now, &prefix, epoch, false)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, n, err := db.FetchWithPrefix(cancelled, m.Name, now-600, now, &prefix, epoch); !errors.Is(err, context.Canceled) || n != 0 {
		t.Fatalf("cancelled: reused=%d err=%v", n, err)
	}
}
