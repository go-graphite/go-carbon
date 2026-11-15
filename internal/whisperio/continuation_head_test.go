package whisperio

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// A warm capsule must actually resume; output parity alone can pass while every
// candidate is silently evicted or reopened. The dropped candidate below models
// failure after preparation and before the enclosing database commit.
func TestOwnerHeadsResumeAbortAndEviction(t *testing.T) {
	for _, density := range []int{14, 60, 480} {
		t.Run(fmt.Sprint(density), func(t *testing.T) {
			now := time.Unix(1700000000, 0)
			clock := func() time.Time { return now }
			pool := NewOwnerHeads(1, 2<<20)
			codec := &ContinuationCodec{OwnerHeads: pool, ReuseBlockKeys: true, DirectLateWrites: true}
			oracle := &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
			kv := &ownerBenchIO{mapContinuationIO: mapContinuationIO{}, scope: new(byte)}
			ref := &ownerBenchIO{mapContinuationIO: mapContinuationIO{}, scope: new(byte)}
			m := store.Metadata{MetricConfig: store.MetricConfig{Name: "resume", Retentions: []store.Retention{{Step: 1, Count: 14400}, {Step: 60, Count: 10080}}, AggregationMethod: store.Last}, ID: 1, Generation: 1, Revision: 1, Continuation: true}
			for _, target := range []store.ContinuationIO{kv, ref} {
				if err := oracle.Create(t.Context(), target, m, clock); err != nil {
					t.Fatal(err)
				}
			}
			rng := rand.New(rand.NewSource(8144))
			update := func(round int, commit bool) {
				t.Helper()
				now = now.Add(time.Duration(density) * time.Second)
				input := ownerFixturePoints(rng, now.Unix(), density, round)
				tx := &ownerBenchIO{mapContinuationIO: mapContinuationIO{}, scope: kv.scope}
				// The real store's indexed batch is represented by a private full map.
				for k, v := range kv.mapContinuationIO {
					tx.mapContinuationIO[k] = append([]byte(nil), v...)
				}
				if err := codec.Update(t.Context(), tx, m, input, -1, clock, &store.ContinuationMaintenance{Allow: func() bool { return true }}); err != nil {
					t.Fatal(err)
				}
				if commit {
					if err := oracle.Update(t.Context(), ref, m, input, -1, clock, &store.ContinuationMaintenance{Allow: func() bool { return true }}); err != nil {
						t.Fatal(err)
					}
					m.Revision++
					if tx.commit == nil {
						t.Fatal("missing commit publication")
					}
					tx.commit(m)
					kv.mapContinuationIO = tx.mapContinuationIO
				}
				a, err := oracle.Snapshot(t.Context(), ref, m, clock)
				if err != nil {
					t.Fatal(err)
				}
				b, err := oracle.Snapshot(t.Context(), kv, m, clock)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.EqualSnapshots(a, b); err != nil {
					t.Fatal(err)
				}
				if !equalContinuationMaps(ref.mapContinuationIO, kv.mapContinuationIO) {
					t.Fatal("encoded bytes differ")
				}
			}
			update(0, true)
			opens := pool.Opens()
			update(1, true)
			if pool.Opens() != opens {
				t.Fatal("warm writer reopened")
			}
			update(2, false)
			if pool.Opens() != opens {
				t.Fatal("failed candidate did not resume")
			}
			o := pool.owner(m.Name)
			o.Lock()
			_, published := o.entries[m.Name]
			o.Unlock()
			if published {
				t.Fatal("aborted capsule remained published")
			}
			update(3, true)
			if pool.Opens() != opens+1 {
				t.Fatal("abort did not force cold recovery")
			}
			pool.budget = 512 // Forced budget eviction must return to the normal path.
			update(4, true)
			before := pool.Opens()
			update(5, true)
			if pool.Opens() != before+1 {
				t.Fatal("budget eviction did not reopen")
			}
			pool.budget = 2 << 20
			for round := 6; round < 70; round++ {
				update(round, true)
			}
		})
	}
}

func equalContinuationMaps(a, b mapContinuationIO) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if string(v) != string(b[k]) {
			return false
		}
	}
	return true
}

// Imported classic and compressed files retain their original physical slots.
// Classic files deliberately use the ordinary open path; no unsafe state from
// a repair/rewrite may enter the resumable compressed capsule.
func TestOwnerHeadsImportedLayouts(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, buckets := range []bool{false, true} {
			t.Run(fmt.Sprintf("compressed=%t/buckets=%t", compressed, buckets), func(t *testing.T) {
				now := time.Unix(1700000000, 0)
				clock := func() time.Time { return now }
				source := t.TempDir()
				path := source + "/imported.wsp"
				w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:2h,10s:1d,60s:1w"), whisper.Last, .5, &whisper.Options{Compressed: compressed, OutOfOrder: compressed, Now: clock})
				if err != nil {
					t.Fatal(err)
				}
				if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(now.Unix() - 60), Value: 1}, {Time: int(now.Unix() - 10), Value: 2}}); err != nil {
					t.Fatal(err)
				}
				if err = w.Close(); err != nil {
					t.Fatal(err)
				}
				cs := []*ContinuationCodec{{ReuseBlockKeys: true, DirectLateWrites: true}, {OwnerHeads: NewOwnerHeads(1, 2<<20), ReuseBlockKeys: true, DirectLateWrites: true}}
				dbs := make([]*store.Store, 2)
				published := make([]store.Metadata, 2)
				for i, c := range cs {
					opts := coldOptions(c)
					opts.Now = clock
					opts.TimeBuckets = buckets
					opts.IncrementalSource = source
					dbs[i], err = openMemoryTestStore(t, t.TempDir(), opts)
					if err != nil {
						t.Fatal(err)
					}
					defer dbs[i].Close()
					dbs[i].StopBackground()
					capture, e := ReadSnapshot(context.Background(), "imported", path)
					if e != nil {
						t.Fatal(e)
					}
					staged, e := dbs[i].StageFileSnapshot(t.Context(), capture)
					if e != nil {
						t.Fatal(e)
					}
					if published[i], e = dbs[i].PublishFileOwnership(t.Context(), capture, staged); e != nil {
						t.Fatal(e)
					}
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				dir, err := os.Open(source)
				if err != nil {
					t.Fatal(err)
				}
				if err = dir.Sync(); err != nil {
					t.Fatal(err)
				}
				if err = dir.Close(); err != nil {
					t.Fatal(err)
				}
				for i, db := range dbs {
					if err = db.CompleteFileCleanup(t.Context(), published[i]); err != nil {
						t.Fatal(err)
					}
				}
				rng := rand.New(rand.NewSource(8144))
				for round := 0; round < 12; round++ {
					now = now.Add(61 * time.Second)
					input := ownerFixturePoints(rng, now.Unix(), 60, round)
					for _, db := range dbs {
						if err = db.UpdateMany(t.Context(), "imported", input); err != nil {
							t.Fatal(err)
						}
					}
					compareOwnerStores(t, dbs[0], dbs[1], "imported", now)
				}
				if cs[1].OwnerHeads.Opens() == 0 {
					t.Fatal("adapter not exercised")
				}
			})
		}
	}
}
