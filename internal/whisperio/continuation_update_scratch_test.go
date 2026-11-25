package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

var errScratchRollback = errors.New("injected failure after encoding")

type scratchRollbackCodec struct {
	ContinuationCodec
	fail bool
}

func (c *scratchRollbackCodec) Update(ctx context.Context, kv store.ContinuationIO, m store.Metadata, input []store.Point, retention int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	if err := c.ContinuationCodec.Update(ctx, kv, m, input, retention, clock, maintenance); err != nil {
		return err
	}
	if c.fail {
		return errScratchRollback
	}
	return nil
}

// Compare the previous allocation path with reuse, including encoded keys,
// physical float bits, digest/proof fences, failed publication and reopening.
// The same durable seed keeps lineage and revision tokens comparable too.
func TestContinuationUpdateScratchBitExact(t *testing.T) {
	for _, layout := range []string{"classic", "referenced", "compressed", "paged"} {
		t.Run(layout, func(t *testing.T) {
			ctx := context.Background()
			now := int64(1700000000)
			clock := func() time.Time { return time.Unix(now, 0) }
			kv, m := mutationClassicFixture(t, layout == "referenced", []store.Retention{{Step: 1, Count: 512}, {Step: 8, Count: 256}}, [][]store.Point{{{Timestamp: now - 200, Value: 1}, {Timestamp: now - 80, Value: 2}}, {{Timestamp: now - 1024, Value: 3}}}, clock)
			root := t.TempDir()
			seedDir := filepath.Join(root, "seed")
			opts := store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, TimeBuckets: layout == "paged", Now: clock, SyncInterval: time.Hour, MetadataCacheSize: 8 << 20, CacheSize: 1 << 20, MemTableSize: 1 << 20}
			seed, err := store.Open(seedDir, opts)
			if err != nil {
				t.Fatal(err)
			}
			if layout == "compressed" || layout == "paged" {
				if _, err = seed.Create(ctx, m.MetricConfig); err != nil {
					t.Fatal(err)
				}
				if err = seed.UpdateMany(ctx, m.Name, []store.Point{{Timestamp: now - 200, Value: 1}, {Timestamp: now - 80, Value: 2}}); err != nil {
					t.Fatal(err)
				}
			} else if _, err = seed.ImportTransfer(ctx, []store.TransferMetric{classicTransfer(kv, store.Snapshot{Metadata: m})}); err != nil {
				t.Fatal(err)
			}
			scopes := [][2]int64{{now - 101, now - 49}, {now - 257, now + 1}, {now - 1023, now - 511}}
			for _, scope := range scopes {
				if _, _, err = seed.MutationProof(ctx, m.Name, scope[0], scope[1]); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = seed.SumDigest(ctx, m.Name, int(now-256), int(now)); err != nil {
				t.Fatal(err)
			}
			if err = seed.Close(); err != nil {
				t.Fatal(err)
			}
			var dbs [2]*store.Store
			codecs := [2]*scratchRollbackCodec{{ContinuationCodec: ContinuationCodec{keepUpdateAllocations: true}}, {}}
			dirs := [2]string{filepath.Join(root, "old"), filepath.Join(root, "reuse")}
			open := func(i int) {
				t.Helper()
				opts.Continuation = codecs[i]
				dbs[i], err = store.Open(dirs[i], opts)
				if err != nil {
					t.Fatal(err)
				}
				dbs[i].StopBackground()
			}
			for i := range dbs {
				if err = os.CopyFS(dirs[i], os.DirFS(seedDir)); err != nil {
					t.Fatal(err)
				}
				open(i)
			}
			t.Cleanup(func() {
				for _, db := range dbs {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			export := func(i int) []store.TransferMetric {
				t.Helper()
				got, err := dbs[i].ExportTransfer(ctx, []string{m.Name})
				if err != nil {
					t.Fatal(err)
				}
				return got
			}
			check := func() {
				t.Helper()
				a, b := export(0), export(1)
				if !reflect.DeepEqual(a[0].Keys, b[0].Keys) {
					t.Fatal("stored bytes differ")
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
					t.Fatal(err)
				}
				for _, scope := range scopes {
					pa, ra, ea := dbs[0].MutationProof(ctx, m.Name, scope[0], scope[1])
					pb, rb, eb := dbs[1].MutationProof(ctx, m.Name, scope[0], scope[1])
					if ea != nil || eb != nil || pa != pb || ra != rb {
						t.Fatalf("proof differs: %s/%s %s/%s %v/%v", pa, ra, pb, rb, ea, eb)
					}
				}
				da, err := dbs[0].SumDigest(ctx, m.Name, int(now-256), int(now))
				if err != nil {
					t.Fatal(err)
				}
				db, err := dbs[1].SumDigest(ctx, m.Name, int(now-256), int(now))
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(da, db) {
					t.Fatal("sum digests differ")
				}
			}
			trace := [][]store.Point{
				{{Timestamp: now, Value: 3}},
				{{Timestamp: now - 80, Value: 99}, {Timestamp: now - 80, Value: 17}, {Timestamp: now - 81, Value: 9}},
				{{Timestamp: now - 79, Value: math.Copysign(0, -1)}, {Timestamp: now - 78, Value: math.Float64frombits(1)}},
				{{Timestamp: now - 80, Value: math.Float64frombits(0x7ff8000000001234)}, {Timestamp: now - 77, Value: math.Inf(-1)}},
				{{Timestamp: now + 8, Value: 7}},
				{{Timestamp: now + 512, Value: 11}}, // Future circular owner.
				{{Timestamp: now + 8, Value: 19}},   // Older epoch after the future owner.
			}
			for seq, input := range trace {
				t.Run(fmt.Sprint(seq), func(t *testing.T) {
					for _, db := range dbs {
						if err := db.UpdateBatch(ctx, []store.MetricUpdate{{Name: m.Name, Points: input}, {Name: m.Name, Points: input}}, nil); err != nil {
							t.Fatal(err)
						}
					}
					check()
					// Fail after writes and proof refinement, before the batch can publish.
					before := [2][]store.TransferMetric{export(0), export(1)}
					for i, db := range dbs {
						codecs[i].fail = true
						err := db.UpdateMany(ctx, m.Name, []store.Point{{Timestamp: now - 20, Value: float64(seq) + .125}})
						codecs[i].fail = false
						if !errors.Is(err, errScratchRollback) {
							t.Fatalf("expected rollback, got %v", err)
						}
						if !reflect.DeepEqual(before[i], export(i)) {
							t.Fatal("failed attempt published bytes or revision")
						}
					}
					check()
					if seq%2 == 1 {
						for i := range dbs {
							if err := dbs[i].Close(); err != nil {
								t.Fatal(err)
							}
							open(i)
						}
						check()
					}
				})
				now = max(now, input[0].Timestamp+1)
			}
		})
	}
}

func TestContinuationUpdateScratchReanchorAttempts(t *testing.T) {
	seed, snapshot := mixedClassicFixture(t)
	old, reused := cloneMutationMap(seed), cloneMutationMap(seed)
	codecs := []*ContinuationCodec{{keepUpdateAllocations: true}, {}}
	before := ClassicReanchors()
	for _, input := range [][]store.Point{
		{{Timestamp: 999810, Value: 7}},
		{{Timestamp: 999980, Value: 9}}, // Abandon an attempt and repair its anchor.
		{{Timestamp: 999870, Value: math.Copysign(0, -1)}},
	} {
		for i, kv := range []mapContinuationIO{old, reused} {
			if err := codecs[i].Update(t.Context(), kv, snapshot.Metadata, input, -1, classicTestClock, nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := sameKV(old, reused); err != nil {
			t.Fatal(err)
		}
	}
	if ClassicReanchors().ReanchoredArchives-before.ReanchoredArchives != 2 {
		t.Fatal("fixture did not exercise one failed attempt on each path")
	}
}

func TestContinuationUpdateScratchConcurrentSameNameBatches(t *testing.T) {
	const now = int64(1700000000)
	clock := func() time.Time { return time.Unix(now, 0) }
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, Now: clock, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := store.MetricConfig{Name: "scratch.concurrent", Retentions: []store.Retention{{Step: 1, Count: 4096}, {Step: 8, Count: 2048}}, AggregationMethod: store.Sum}
	if _, err := db.Create(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.MutationProof(t.Context(), cfg.Name, now-1024, now+1); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			for round := range 16 {
				ts := now - 1024 + int64(worker*32+round)
				updates := []store.MetricUpdate{{Name: cfg.Name, Points: []store.Point{{Timestamp: ts, Value: float64(worker*32 + round)}}}, {Name: cfg.Name, Points: []store.Point{{Timestamp: ts, Value: float64(worker*32 + round)}}}}
				if err := db.UpdateBatch(t.Context(), updates, nil); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	got, err := db.Snapshot(t.Context(), cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Archives[0].Points) != 128 {
		t.Fatalf("lost concurrent points: %d", len(got.Archives[0].Points))
	}
	for _, p := range got.Archives[0].Points {
		if p.Value != float64(p.Timestamp-(now-1024)) {
			t.Fatalf("changed point: %+v", p)
		}
	}
}

func TestContinuationManifestReuseOwnsMutations(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprint(compressed), func(t *testing.T) {
			seed := ownedHeaderFixture(compressed, 2)
			seed.pages[1] = true
			encoded, err := encodeContinuationManifest(seed)
			if err != nil {
				t.Fatal(err)
			}
			cache := new(continuationUpdateState)
			a, err := cache.decode(encoded)
			if err != nil {
				t.Fatal(err)
			}
			b, err := cache.decode(bytes.Clone(encoded))
			if err != nil {
				t.Fatal(err)
			}
			if &a.layout.Archives[0] != &b.layout.Archives[0] {
				t.Fatal("validated layout not reused")
			}
			a.header[0] ^= 0xff
			a.allocatePage(2)
			if !bytes.Equal(b.header, seed.header) || b.pages[2] {
				t.Fatal("mutable state escaped its view")
			}
			c, err := cache.decode(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(c.header, seed.header) || c.pages[2] {
				t.Fatal("cached immutable manifest changed")
			}
			if _, err := cache.decode(append(bytes.Clone(encoded), 0)); err == nil {
				t.Fatal("changed manifest bypassed validation")
			}
		})
	}
}

func TestContinuationScratchBoundAndReset(t *testing.T) {
	s := new(continuationScratch)
	first := s.block()
	first.Points[0] = store.Point{Timestamp: 1, Value: 42}
	for range len(s.blocks) + 16 {
		s.block()
	}
	if s.used != len(s.blocks) {
		t.Fatal("scratch retention is unbounded")
	}
	s.used = 0
	if next := s.block(); next != first || *next != (store.PointBlock{}) {
		t.Fatal("reused scratch retained points")
	}
}
