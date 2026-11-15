package downsize

import (
	"bytes"
	"context"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"math"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSyncClosedSparseBoundaryRepair(t *testing.T) {
	for _, tc := range []struct{ existing, later bool }{{false, true}, {true, true}, {false, false}, {true, false}} {
		existing := tc.existing
		t.Run(map[bool]string{false: "hole", true: "target-wins"}[existing]+map[bool]string{false: "/trailing", true: "/historical"}[tc.later], func(t *testing.T) {
			ctx := context.Background()
			open := func() *store.Store {
				db, e := store.Open("/db", store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(vfs.NewMem()))
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() { _ = db.Close() })
				return db
			}
			source, target := open(), open()
			seed := func(db *store.Store, snap store.Snapshot) {
				snap.Metadata.Continuation = true
				mem := policyMemory{}
				if e := (&whisperio.ContinuationCodec{}).RestoreArchives(ctx, mem, snap, time.Now); e != nil {
					t.Fatal(e)
				}
				record := store.TransferMetric{Config: snap.Metadata.MetricConfig, Version: store.TransferVersion{Name: snap.Metadata.Name, ID: 1, Generation: 1, Revision: 1}, Continuation: true}
				if e := mem.Visit(nil, func(k, v []byte) error {
					record.Keys = append(record.Keys, store.RelayoutKV{Key: bytes.Clone(k), Value: bytes.Clone(v)})
					return nil
				}); e != nil {
					t.Fatal(e)
				}
				if _, e := db.ImportTransfer(ctx, []store.TransferMetric{record}); e != nil {
					t.Fatal(e)
				}
			}
			base := testMetricNow/60*60 - 240
			ret := []store.Retention{{Step: 10, Count: 128}, {Step: 60, Count: 128}}
			snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "sparse", AggregationMethod: store.Average, Retentions: ret, XFilesFactor: 0}}, Archives: []store.Archive{
				{Retention: ret[0], Points: []store.Point{{Timestamp: base + 10, Value: 1}, {Timestamp: base + 70, Value: 2}}},
				{Retention: ret[1], Points: []store.Point{{Timestamp: base, Value: math.Copysign(0, -1)}, {Timestamp: base + 60, Value: 7}}},
			}}
			if !tc.later {
				snap.Archives[1].Points = snap.Archives[1].Points[:1]
			}
			seed(source, snap)
			want := snap
			want.Archives = []store.Archive{
				{Retention: ret[0], Points: []store.Point{{Timestamp: base - 10, Value: 9}, {Timestamp: base + 10, Value: 42}}},
				{Retention: ret[1], Points: []store.Point{{Timestamp: base - 60, Value: 11}}},
			}
			if existing {
				want.Archives[1].Points = append(want.Archives[1].Points, store.Point{Timestamp: base, Value: 43})
			}
			seed(target, want)
			donor := httptest.NewServer(nativeControllerService(t, source))
			defer donor.Close()
			survivor := httptest.NewServer(nativeControllerService(t, target))
			defer survivor.Close()
			path := filepath.Join(t.TempDir(), "copy")
			for range 2 {
				res, err := newSyncLiveBackend(syncLiveOptions{}).CopyWindow(ctx, donor.URL, survivor.URL, []string{"sparse"}, path, syncRanges{{From: base, Until: base + 20}})
				if !existing && !tc.later {
					if err != nil || res.Copied != 0 || len(res.Failed) != 1 {
						t.Fatalf("unproven trailing bucket accepted: %+v %v", res, err)
					}
					continue
				}
				if err != nil || res.Copied != 1 || len(res.Failed) != 0 {
					t.Fatalf("closed sparse boundary: %+v %v", res, err)
				}
			}
			if !existing && tc.later {
				want.Archives[1].Points = append(want.Archives[1].Points, store.Point{Timestamp: base, Value: math.Copysign(0, -1)})
			}
			got, err := target.Snapshot(ctx, "sparse")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal("repair changed points outside exact coarse hole or target values:", err)
			}
		})
	}
}
