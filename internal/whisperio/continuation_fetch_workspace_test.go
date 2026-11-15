package whisperio

import (
	"context"
	"math"
	"slices"
	"sync"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Compare the complete native decoder against its allocating oracle. Keep an
// earlier returned Series alive across later reads to catch borrowed output.
func TestContinuationFetchWorkspaceOracle(t *testing.T) {
	db, codec, now := rangeFetchFixture(t)
	ctx := context.Background()
	var retained *store.Series
	var retainedBits []uint64
	for _, late := range []bool{false, true} {
		if late {
			if err := db.UpdateMany(ctx, "range.metric", []store.Point{{Timestamp: int64(now - 3600), Value: math.Copysign(0, -1)}, {Timestamp: int64(now - 7200), Value: math.Float64frombits(0x7ff8000000000042)}}); err != nil {
				t.Fatal(err)
			}
		}
		for _, ranged := range []bool{false, true} {
			codec.RangeReadsEnabled = func() bool { return ranged }
			for _, hours := range []int{1, 24, 168, 720} {
				codec.keepFetchBlockAllocations = true
				want, err := db.Fetch(ctx, "range.metric", now-hours*3600, now)
				if err != nil {
					t.Fatal(err)
				}
				codec.keepFetchBlockAllocations = false
				got, err := db.Fetch(ctx, "range.metric", now-hours*3600, now)
				if err != nil {
					t.Fatal(err)
				}
				if got.FromTime != want.FromTime || got.UntilTime != want.UntilTime || got.Step != want.Step || len(got.Values) != len(want.Values) {
					t.Fatal("shape differs")
				}
				for i, v := range want.Values {
					if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
						t.Fatalf("late=%v ranged=%v hours=%d slot=%d", late, ranged, hours, i)
					}
				}
				if retained != nil {
					bits := make([]uint64, len(retained.Values))
					for i, v := range retained.Values {
						bits[i] = math.Float64bits(v)
					}
					if !slices.Equal(bits, retainedBits) {
						t.Fatal("returned Series aliases scratch")
					}
				}
				retained = got
				retainedBits = make([]uint64, len(got.Values))
				for i, v := range got.Values {
					retainedBits[i] = math.Float64bits(v)
				}
			}
		}
	}
	codec.RangeReadsEnabled = nil
	for _, allocating := range []bool{true, false} {
		codec.keepFetchBlockAllocations = allocating
		allocs := testing.AllocsPerRun(20, func() {
			if _, err := db.Fetch(ctx, "range.metric", now-3600, now); err != nil {
				t.Fatal(err)
			}
		})
		t.Logf("allocating=%v allocs/op=%.0f", allocating, allocs)
	}
	codec.keepFetchBlockAllocations = false
	want, err := db.Fetch(ctx, "range.metric", now-3600, now)
	if err != nil {
		t.Fatal(err)
	}
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 16 {
				got, err := db.Fetch(ctx, "range.metric", now-3600, now)
				if err != nil {
					t.Error(err)
					return
				}
				if len(got.Values) != len(want.Values) {
					t.Error("parallel shape differs")
					return
				}
				for i, v := range want.Values {
					if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
						t.Errorf("parallel slot %d differs", i)
						return
					}
				}
			}
		})
	}
	readers.Wait()
}

func BenchmarkContinuationFetchWorkspace(b *testing.B) {
	db, codec, now := rangeFetchFixture(b)
	for _, allocating := range []bool{true, false} {
		name := "scratch"
		if allocating {
			name = "allocating"
		}
		b.Run(name, func(b *testing.B) {
			codec.keepFetchBlockAllocations = allocating
			b.ReportAllocs()
			for b.Loop() {
				if _, err := db.Fetch(context.Background(), "range.metric", now-3600, now); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
