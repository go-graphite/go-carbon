package partsops

import (
	"math"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func TestRetentionDifferentialAgainstWhisperPolicies(t *testing.T) {
	// Production-shaped single through four archive policies, including the
	// longest daily archive and the usual two-week catch-all.
	for _, schema := range []string{"1s:1d", "1s:1d,1m:30d,1h:1y,1d:10y", "1m:30d,1h:1y,1d:10y", "1m:14d,30m:2y", "1s:2d,1m:28d,1h:2y"} {
		rr, err := whisper.ParseRetentionDefs(schema)
		if err != nil {
			t.Fatal(err)
		}
		var policy []store.Retention
		var want int64
		for _, r := range rr {
			policy = append(policy, store.Retention{Step: r.SecondsPerPoint(), Count: r.NumberOfPoints()})
			want = max(want, int64(r.MaxRetention()))
		}
		got, err := LongestRetention(policy)
		if err != nil || got != want {
			t.Fatal(schema, got, want, err)
		}
		for _, end := range []int64{-10000, 0, 10000} {
			for _, delta := range []int64{-1, 0, 1} {
				p := Partition{ID: "one", Start: end - 60, End: end, LongestSeconds: got, Sealed: true}
				drop, err := ExpiredPartitions([]Partition{p}, time.Unix(end+want+delta, 0))
				if err != nil {
					t.Fatal(err)
				}
				if (len(drop) == 1) != (delta >= 0) {
					t.Fatalf("%s delta %d: %v", schema, delta, drop)
				}
			}
		}
	}
}

func TestRetentionPartitionOutsideClassicReadWindow(t *testing.T) {
	for _, schema := range []string{"1s:1m", "1s:1m,1m:1h", "1s:1m,1m:1h,1h:1d", "1s:1m,1m:1h,1h:1d,1d:7d"} {
		t.Run(schema, func(t *testing.T) {
			const base int64 = 1700000000
			now := time.Unix(base+120, 0)
			w, err := whisper.CreateWithOptions(filepath.Join(t.TempDir(), "classic.wsp"), whisper.MustParseRetentionDefs(schema), whisper.Average, .5, &whisper.Options{InMemory: true, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(base + 119), Value: math.Copysign(0, -1)}, {Time: int(base + 118), Value: 17}}); err != nil {
				t.Fatal(err)
			}
			p := Partition{ID: "sealed", Start: base, End: base + 120, LongestSeconds: int64(w.MaxRetention()), Sealed: true}
			now = time.Unix(p.End+p.LongestSeconds-1, 0)
			if drops, err := ExpiredPartitions([]Partition{p}, now); err != nil || len(drops) != 0 {
				t.Fatal("partition deleted before complete horizon", drops, err)
			}
			now = now.Add(time.Second)
			classic, err := w.Fetch(int(p.Start), int(p.End-1))
			if err != nil {
				t.Fatal(err)
			}
			drops, err := ExpiredPartitions([]Partition{p}, now)
			if err != nil || len(drops) != 1 || classic != nil {
				t.Fatal("retired partition overlaps classic read window", drops, classic, err)
			}
		})
	}
}

func TestRetentionNeverDeletesOpenOrOverflows(t *testing.T) {
	for _, p := range []Partition{{ID: "open", Start: 0, End: 10, LongestSeconds: 1}, {ID: "future", Start: math.MaxInt64 - 10, End: math.MaxInt64, LongestSeconds: 100, Sealed: true}} {
		drop, err := ExpiredPartitions([]Partition{p}, time.Unix(math.MaxInt64, 0))
		if err != nil || len(drop) != 0 {
			t.Fatal(drop, err)
		}
	}
	if _, err := LongestRetention([]store.Retention{{Step: math.MaxInt, Count: 2}}); err == nil {
		t.Fatal("overflow accepted")
	}
	if _, err := ExpiredPartitions([]Partition{{ID: "bad", Start: 10, End: 1, LongestSeconds: 1}}, time.Now()); err == nil {
		t.Fatal("invalid manifest accepted")
	}
}
