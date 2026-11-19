package v2read

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// TestTimeSkipKeepsTouchedCoarseBucket seals a point after the render window
// but inside the coarse bucket a pending point touches. The range read skips
// parts by time coverage, yet that part must still be read: the reducer
// rebuilds the open coarse bucket from its finer window.
func TestTimeSkipKeepsTouchedCoarseBucket(t *testing.T) {
	archives := []sealrollup.Archive{{Step: 10, Count: 2160}, {Step: 60, Count: 10080}} // 10s:6h,1m:7d
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID("skip.m"), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: archives}})
	if err != nil {
		t.Fatal(err)
	}
	reader, _ := New(1, 1<<30, 64<<20)
	engine, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishBatchesWithInventory: reader.PublishWithInventory, Now: func() time.Time { return time.Unix(1, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	const minute = int64(1794700800) // bucket M of the 60 s archive
	append1 := func(order uint64, clock, at int64, v float64, seal bool) {
		previous := reader.owners[0].Load()
		batch := v2core.Batch{ID: 1, Policy: policy, Order: order, Clock: clock, Points: []v2core.Row{{ID: 1, Time: at, Bits: math.Float64bits(v), Seq: 1}}}
		if err := engine.AppendBatches(0, []v2core.Batch{batch}, order, false); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { return reader.owners[0].Load() != previous })
		if seal {
			previous = reader.owners[0].Load()
			if err := engine.AppendBatches(0, nil, order, true); err != nil {
				t.Fatal(err)
			}
			await(t, func() bool { return reader.owners[0].Load() != previous })
		}
	}
	append1(1, minute-2*86400+5, minute-2*86400, 100, true) // an old part no window touches
	append1(2, minute+51, minute+50, 5, true)               // sealed after the render window, inside bucket M
	append1(3, minute+55, minute+10, 7, false)              // pending, inside the render window and bucket M
	now := minute + 55
	from, until := now-7*3600, minute+15 // longer than 6 h: renders the 60 s archive
	if got := fetchBoth(t, reader, []string{"skip.m"}, from, until, now); got != 1 {
		t.Fatalf("compared %d", got)
	}
	reader.fullHistory = false
	resetRangeCaches(reader)
	var bucket float64
	err = reader.FetchMany(context.Background(), []string{"skip.m"}, from, until, now, func(_ string, s *Series) error {
		if s == nil || s.Step != 60 || minute < s.From || minute >= s.Until {
			t.Fatalf("series %+v does not hold bucket %d", s, minute)
		}
		bucket = s.Values[(minute-s.From)/s.Step]
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if bucket != 12 {
		t.Fatalf("bucket %d = %g, want 12 (sealed 5 + pending 7)", minute, bucket)
	}
}

// TestTimeSkipKeepsOldOnlyMetadata renders a name whose newest metadata and
// data live only in a part far outside the render window. The skip applies to
// slot reads only; metadata lookup still sees every part, so the name renders.
func TestTimeSkipKeepsOldOnlyMetadata(t *testing.T) {
	archives := []sealrollup.Archive{{Step: 10, Count: 2160}, {Step: 60, Count: 10080}}
	encode := func(name string) []byte {
		b, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: archives}})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	reader, _ := New(1, 1<<30, 64<<20)
	engine, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishBatchesWithInventory: reader.PublishWithInventory, Now: func() time.Time { return time.Unix(1, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	const now = int64(1794700800 + 55)
	old := now - 2*86400 - 3*60 // aliases no 1 h window slot of either archive
	seal := func(order uint64, b v2core.Batch) {
		previous := reader.owners[0].Load()
		if err := engine.AppendBatches(0, []v2core.Batch{b}, order, true); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { return reader.owners[0].Load() != previous })
	}
	seal(1, v2core.Batch{ID: 1, Policy: encode("skip.old"), Order: 1, Clock: old + 1, Points: []v2core.Row{{ID: 1, Time: old, Bits: math.Float64bits(100), Seq: 1}}})
	seal(2, v2core.Batch{ID: 2, Policy: encode("skip.new"), Order: 2, Clock: now, Points: []v2core.Row{{ID: 2, Time: now - 30, Bits: math.Float64bits(1), Seq: 1}}})
	names := []string{"skip.old", "skip.new"}
	for _, w := range [][2]int64{{now - 3600, now}, {now - 3*86400, now}} {
		fetchBoth(t, reader, names, w[0], w[1], now)
	}
	reader.fullHistory = false
	resetRangeCaches(reader)
	var sum float64
	var seen bool
	err = reader.FetchMany(context.Background(), []string{"skip.old"}, now-3*86400, now, now, func(_ string, s *Series) error {
		seen = s != nil
		for i, v := range s.Values {
			if s.Occupied[i] {
				sum += v
			}
		}
		return nil
	})
	if err != nil || !seen || sum != 100 {
		t.Fatalf("skip.old over 3 d: seen=%t sum=%g err=%v, want its sealed 100", seen, sum, err)
	}
}
