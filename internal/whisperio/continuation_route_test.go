package whisperio

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Both stores start from the same clean seal, so the proof tokens (including
// lineage) can be compared exactly. The uncached store is the baseline oracle.
func TestRouteBaselineBitExact(t *testing.T) {
	for _, layout := range []string{"classic", "referenced", "compressed"} {
		t.Run(layout, func(t *testing.T) {
			ctx := context.Background()
			now := int64(1700000000)
			clock := func() time.Time { return time.Unix(now, 0) }
			kv, m := mutationClassicFixture(t, layout == "referenced", []store.Retention{{Step: 1, Count: 512}, {Step: 8, Count: 256}}, [][]store.Point{{{Timestamp: now - 200, Value: 1}, {Timestamp: now - 80, Value: 2}}, {{Timestamp: now - 1024, Value: 3}}}, clock)
			root := t.TempDir()
			seedDir := filepath.Join(root, "seed")
			opts := store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, Now: clock, SyncInterval: time.Hour, MetadataCacheSize: 64 << 20, CacheSize: 1 << 20, MemTableSize: 1 << 20}
			seed, err := store.Open(seedDir, opts)
			if err != nil {
				t.Fatal(err)
			}
			if layout == "compressed" {
				if _, err = seed.Create(ctx, m.MetricConfig); err != nil {
					t.Fatal(err)
				}
				if err = seed.UpdateMany(ctx, m.Name, []store.Point{{Timestamp: now - 200, Value: 1}, {Timestamp: now - 80, Value: 2}}); err != nil {
					t.Fatal(err)
				}
			} else if _, err = seed.ImportTransfer(ctx, []store.TransferMetric{classicTransfer(kv, store.Snapshot{Metadata: m})}); err != nil {
				t.Fatal(err)
			}
			scopes := [][2]int64{{now - 100, now - 50}, {now - 257, now + 1}, {now - 1023, now - 511}}
			for _, scope := range scopes {
				if _, _, err = seed.MutationProof(ctx, m.Name, scope[0], scope[1]); err != nil {
					t.Fatal(err)
				}
			}
			if err = seed.Close(); err != nil {
				t.Fatal(err)
			}
			var dbs [2]*store.Store
			for i := range dbs {
				dir := filepath.Join(root, fmt.Sprint(i))
				if err = os.CopyFS(dir, os.DirFS(seedDir)); err != nil {
					t.Fatal(err)
				}
				dbs[i], err = store.Open(dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer dbs[i].Close()
			}
			defer dbs[0].DisableMetadataCacheForTest()()
			trace := [][]store.Point{
				{{Timestamp: now, Value: 3}},
				{{Timestamp: now - 80, Value: 99}},
				{{Timestamp: now - 79, Value: math.Copysign(0, -1)}, {Timestamp: now - 78, Value: math.Float64frombits(1)}},
				{{Timestamp: now - 80, Value: math.Float64frombits(0x7ff8000000001234)}},
				{{Timestamp: now + 8, Value: 7}},
				{{Timestamp: now + 512, Value: 11}},
			}
			for seq, input := range trace {
				if seq == 3 {
					dbs[1].ForgetMetadataCacheForTest()
				}
				for _, db := range dbs {
					if seq == len(trace)-1 {
						err = db.UpdateBatch(ctx, []store.MetricUpdate{{Name: m.Name, Points: input}, {Name: m.Name, Points: input}}, nil)
					} else {
						err = db.UpdateMany(ctx, m.Name, input)
					}
					if err != nil {
						t.Fatal(err)
					}
					if err = db.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				a, err := dbs[0].ExportTransfer(ctx, []string{m.Name})
				if err != nil {
					t.Fatal(err)
				}
				b, err := dbs[1].ExportTransfer(ctx, []string{m.Name})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(a[0].Keys, b[0].Keys) {
					t.Fatalf("step %d: stored continuation bytes differ", seq)
				}
				sa, err := dbs[0].Snapshot(ctx, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				sb, err := dbs[1].Snapshot(ctx, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				if err = store.EqualSnapshots(sa, sb); err != nil {
					t.Fatalf("step %d: %v", seq, err)
				}
				for _, scope := range scopes {
					pa, ra, ea := dbs[0].MutationProof(ctx, m.Name, scope[0], scope[1])
					pb, rb, eb := dbs[1].MutationProof(ctx, m.Name, scope[0], scope[1])
					if ea != nil || eb != nil || pa != pb || ra != rb {
						t.Fatalf("step %d scope %v: proofs differ: %s/%s %s/%s %v/%v", seq, scope, pa, ra, pb, rb, ea, eb)
					}
				}
				now = max(now, input[0].Timestamp+1)
			}
		})
	}
}
