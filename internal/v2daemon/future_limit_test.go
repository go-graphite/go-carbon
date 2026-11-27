package v2daemon

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/points"
)

// The future limit refuses a point stamped more than the limit after its
// batch clock, before admission, and keeps everything up to the limit: a kept
// future point is stored, survives a restart, and renders once its time is
// inside the window. Limit 0 keeps every point.
func TestFutureLimitAtIntake(t *testing.T) {
	const clock, limit = int64(1_800_000_000), int64(48 * 3600)
	stream := []struct {
		name string
		ts   int64
	}{
		{"future.now", clock},
		{"future.below", clock + limit - 1},
		{"future.edge", clock + limit},
		{"future.beyond", clock + limit + 1},
		{"future.garbage", 4026531600}, // year 2097
	}
	resolve := func(name string) ([]byte, error) {
		return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 600}, {Step: 60, Count: 600}}}})
	}
	run := func(future int64, text bool) (map[string]int64, []*FutureTimestampError, map[string]float64) {
		t.Helper()
		root := t.TempDir()
		r, err := v2read.New(1, 16<<20, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		r.Identities = &v2core.PolicyCatalog{}
		cfg := Config{Layer: 3, Producers: 1, Shards: 1, MaxCoreRows: 4096, GroupInterval: time.Hour, Hooks: v2core.Hooks{Identities: r.Identities, PublishBatches: r.Publish}}
		s, err := Open(root, cfg)
		if err != nil {
			r.Close()
			t.Fatal(err)
		}
		var refused []*FutureTimestampError
		var refusedNames []string
		o := &applicationOwner{p: s.Producers[0], resolve: resolve, future: future, reject: func(name string, err error) {
			var f *FutureTimestampError
			if !errors.As(err, &f) {
				t.Errorf("%s: unexpected refusal %v", name, err)
				return
			}
			refused, refusedNames = append(refused, f), append(refusedNames, name)
		}}
		var names []string
		var data []points.Point
		for i, x := range stream {
			names = append(names, x.name)
			data = append(data, points.Point{Timestamp: x.ts, Value: float64(i + 1)})
		}
		if text {
			var buf []byte
			var ends []int
			for _, n := range names {
				buf = append(buf, n...)
				ends = append(ends, len(buf))
			}
			err = o.AppendBytes(buf, ends, nil, data, nil, clock)
		} else {
			err = o.Append(names, nil, data, nil, clock)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = o.Drain(); err != nil {
			t.Fatal(err)
		}
		for i, f := range refused {
			if f.Clock != clock || stream[3+i].name != refusedNames[i] || stream[3+i].ts != f.Timestamp {
				t.Fatalf("refusal %d: %s %+v", i, refusedNames[i], *f)
			}
		}
		if err = s.Core.AppendBatches(0, nil, s.Producers[0].cursor, true); err != nil {
			t.Fatal(err)
		}
		// A kept future point renders once the read clock reaches it.
		reads := map[string]float64{}
		for _, x := range stream {
			if err = r.FetchMany(context.Background(), []string{x.name}, x.ts-10, x.ts+1, x.ts+1, func(_ string, series *v2read.Series) error {
				if i := (x.ts - series.From) / series.Step; i >= 0 && int(i) < len(series.Values) && !math.IsNaN(series.Values[i]) {
					reads[x.name] = series.Values[i]
				}
				return nil
			}); err != nil && !errors.Is(err, v2read.ErrNotFound) {
				t.Fatal(x.name, err)
			}
		}
		r.Close()
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		// Every accepted point is still stored after a restart.
		cfg.Hooks = v2core.Hooks{Identities: &v2core.PolicyCatalog{}}
		if s, err = Open(root, cfg); err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		stored := map[string]int64{}
		for _, db := range s.Core.DBs() {
			snap := db.NewSnapshot()
			err = v2core.ScanRollups(snap, func(p v2core.RollupPolicy, _ v2core.RollupMetadata, records []sealrollup.Record) error {
				for _, rec := range records {
					if rec.Archive == 0 {
						stored[string(p.Name)] = rec.Sample.Time
					}
				}
				return nil
			})
			snap.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		return stored, refused, reads
	}
	for _, text := range []bool{false, true} {
		stored, refused, reads := run(limit, text)
		if len(refused) != 2 || len(stored) != 3 {
			t.Fatalf("text=%t: refused %d stored %v, want 2 refused and 3 stored", text, len(refused), stored)
		}
		for i, x := range stream[:3] {
			if stored[x.name] != x.ts || reads[x.name] != float64(i+1) {
				t.Fatalf("text=%t: %s stored at %d read %v, want %d and %d", text, x.name, stored[x.name], reads[x.name], x.ts, i+1)
			}
		}
		stored, refused, reads = run(0, text)
		if len(refused) != 0 || len(stored) != len(stream) {
			t.Fatalf("text=%t limit 0: refused %d stored %d, want every point", text, len(refused), len(stored))
		}
		for i, x := range stream {
			if stored[x.name] != x.ts || reads[x.name] != float64(i+1) {
				t.Fatalf("text=%t limit 0: %s stored at %d read %v", text, x.name, stored[x.name], reads[x.name])
			}
		}
	}
}
