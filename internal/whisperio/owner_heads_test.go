package whisperio

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Gate fixed before running: bit-identical ordered points, retentions, metadata
// and reads after EVERY write, plus seal/reopen, migration, churn and overlap.
// This is deterministic synthetic mechanism coverage, not a production trace.
func TestOwnerHeadsDifferential(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		for _, method := range []store.AggregationMethod{store.Average, store.Sum, store.Last, store.Max, store.Min, store.First} {
			t.Run(fmt.Sprintf("buckets=%t/method=%d", buckets, method), func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()
				now := time.Unix(1700000000, 0)
				clock := func() time.Time { return now }
				codecs := []*ContinuationCodec{{ReuseBlockKeys: true, DirectLateWrites: true}, {ReuseBlockKeys: true, DirectLateWrites: true, OwnerHeads: NewOwnerHeads(4, 256<<10)}}
				dirs := []string{filepath.Join(t.TempDir(), "oracle"), filepath.Join(t.TempDir(), "heads")}
				dbs := make([]*store.Store, 2)
				opts := func(i int) store.Options {
					o := coldOptions(codecs[i])
					o.Now = clock
					o.TimeBuckets = buckets
					return o
				}
				for i := range dbs {
					var err error
					dbs[i], err = openMemoryTestStore(t, dirs[i], opts(i))
					if err != nil {
						t.Fatal(err)
					}
					dbs[i].StopBackground()
				}
				defer func() {
					for _, db := range dbs {
						if db != nil {
							db.Close()
						}
					}
				}()
				names := make([]string, 6)
				for n := range names {
					names[n] = fmt.Sprintf("fixture.%d;density=%d", n, []int{14, 60, 480}[n%3])
					cfg := store.MetricConfig{Name: names[n], Retentions: []store.Retention{{Step: 1, Count: 7200}, {Step: 10, Count: 8640}, {Step: 60, Count: 10080}}, AggregationMethod: method, XFilesFactor: []float32{0, .5, 1}[n%3]}
					for _, db := range dbs {
						if _, err := db.Create(ctx, cfg); err != nil {
							t.Fatal(err)
						}
					}
				}
				rng := rand.New(rand.NewSource(8144))
				check := func(name string) { t.Helper(); compareOwnerStores(t, dbs[0], dbs[1], name, now) }
				for round := 0; round < 24; round++ {
					if round == 20 { // Expire every retention and reuse archive rings.
						now = now.Add(8 * 24 * time.Hour)
					}
					now = now.Add(61 * time.Second)
					for n, name := range names {
						// Half the names are sparse, at each density stratum.
						if n >= 3 && round%5 != 0 {
							continue
						}
						count := []int{14, 60, 480}[n%3]
						input := ownerFixturePoints(rng, now.Unix(), count, round)
						for i, db := range dbs {
							var err error
							if round%3 == 0 {
								err = db.UpdateBatch(ctx, []store.MetricUpdate{{Name: name, Points: input[:count/2]}, {Name: name, Points: input[count/2:]}}, nil)
							} else {
								err = db.UpdateMany(ctx, name, input)
							}
							if err != nil {
								t.Fatalf("round %d metric %d side %d: %v", round, n, i, err)
							}
						}
						check(name)
					}
					if round == 7 { // Forced ownership migration discards old owner state.
						codecs[1].OwnerHeads = NewOwnerHeads(7, 128<<10)
					}
					if round == 11 { // Overlapping archive import must fence every retained group.
						records, err := dbs[0].ExportTransfer(ctx, []string{names[3]})
						if err != nil {
							t.Fatal(err)
						}
						records[0].Config.Name, records[0].Version.Name = names[0], names[0]
						for _, db := range dbs {
							if _, err := db.ImportTransfer(ctx, records); err != nil {
								t.Fatal(err)
							}
						}
						check(names[0])
					}
					if round == 19 {
						cfg := store.MetricConfig{Name: names[3], Retentions: []store.Retention{{Step: 1, Count: 7200}, {Step: 10, Count: 8640}, {Step: 60, Count: 10080}}, AggregationMethod: method, XFilesFactor: 0}
						for _, db := range dbs {
							if err := db.Delete(ctx, names[3]); err != nil {
								t.Fatal(err)
							}
							if _, err := db.Create(ctx, cfg); err != nil {
								t.Fatal(err)
							}
						}
						check(names[3])
					}
					if round == 15 { // Seal/compact then reopen: no hot state is recovery authority.
						for i, db := range dbs {
							if err := db.Compact(); err != nil {
								t.Fatal(err)
							}
							if err := db.Close(); err != nil {
								t.Fatal(err)
							}
							dbs[i] = nil
							if i == 1 {
								codecs[i].OwnerHeads = NewOwnerHeads(3, 64<<10)
							}
							var err error
							dbs[i], err = openMemoryTestStore(t, dirs[i], opts(i))
							if err != nil {
								t.Fatal(err)
							}
							dbs[i].StopBackground()
						}
						for _, name := range names {
							check(name)
						}
					}
				}
				hits, reads, decodes := codecs[1].OwnerHeads.Counts()
				t.Logf("post-reopen mechanism: hits=%d backing_reads=%d actual_decodes=%d", hits, reads, decodes)
				if hits == 0 {
					t.Fatal("retention never avoided a group read/decode")
				}
				p := codecs[1].OwnerHeads
				for i := range p.owners {
					o := &p.owners[i]
					o.Lock()
					if o.bytes > p.budget {
						t.Errorf("owner exceeded retained budget: %d > %d", o.bytes, p.budget)
					}
					o.Unlock()
				}
			})
		}
	}
}

func ownerFixturePoints(rng *rand.Rand, now int64, count, round int) []store.Point {
	out := make([]store.Point, count)
	special := []uint64{0, 1, 0x8000000000000000, 0x8000000000000001, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7ff0000000000001, 0xfff8000000001234}
	for j := range out {
		value := float64(j + round)
		if j%3 == 1 {
			value = math.Float64frombits(rng.Uint64())
		}
		if j%17 == 0 {
			value = math.Float64frombits(special[(j/17+round)%len(special)])
		}
		stamp := now - int64(count-j)
		switch {
		case j%47 == 0:
			stamp = now + int64(j%7+1) // future slots
		case j%29 == 0:
			stamp = now - 8000 - int64(j) // lower archive / late
		case j%19 == 0:
			stamp = now - 17 // duplicate correction, deterministic winner
		}
		out[j] = store.Point{Timestamp: stamp, Value: value}
	}
	return out
}

func compareOwnerStores(t testing.TB, a, b *store.Store, name string, now time.Time) {
	t.Helper()
	ctx := context.Background()
	x, err := a.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	y, err := b.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(x, y); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if !reflect.DeepEqual(x.Metadata, y.Metadata) {
		t.Fatalf("metadata differs: %+v / %+v", x.Metadata, y.Metadata)
	}
	leftKeys, err := a.ExportTransfer(ctx, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	rightKeys, err := b.ExportTransfer(ctx, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(leftKeys[0].Keys, rightKeys[0].Keys) {
		t.Fatal("encoded output keys/bytes differ")
	}

	// EqualSnapshots checks exact bits and order. Check every archive read boundary too.
	for _, r := range x.Metadata.Retentions {
		from, until := int(now.Unix())-r.MaxRetention()+1, int(now.Unix())+1
		left, e := a.Fetch(ctx, name, from, until)
		if e != nil {
			t.Fatal(e)
		}
		right, e := b.Fetch(ctx, name, from, until)
		if e != nil {
			t.Fatal(e)
		}
		if left.FromTime != right.FromTime || left.UntilTime != right.UntilTime || left.Step != right.Step || len(left.Values) != len(right.Values) {
			t.Fatal("read shape differs")
		}
		for i, v := range left.Values {
			if math.Float64bits(v) != math.Float64bits(right.Values[i]) {
				t.Fatalf("read bit mismatch %s step %d slot %d", name, r.Step, i)
			}
		}
	}
}

func BenchmarkOwnerHeads(b *testing.B) {
	for _, density := range []int{14, 60, 480} {
		for _, mode := range []string{"current", "owners-only", "retained"} {
			b.Run(fmt.Sprintf("points=%d/%s", density, mode), func(b *testing.B) {
				now := time.Unix(1700000000, 0)
				clock := func() time.Time { return now }
				pool := NewOwnerHeads(1, 0)
				if mode == "current" {
					pool = nil
				}
				if mode == "retained" {
					pool = NewOwnerHeads(1, 256<<10)
				}
				codec := &ContinuationCodec{OwnerHeads: pool, ReuseBlockKeys: true, DirectLateWrites: true}
				kv := &ownerBenchIO{mapContinuationIO: mapContinuationIO{}, scope: new(byte)}
				m := store.Metadata{MetricConfig: store.MetricConfig{Name: "bench", Retentions: []store.Retention{{Step: 1, Count: 14400}, {Step: 60, Count: 10080}}, AggregationMethod: store.Average, XFilesFactor: .5}, ID: 1, Generation: 1, Revision: 1, Continuation: true}
				if err := codec.Create(b.Context(), kv, m, clock); err != nil {
					b.Fatal(err)
				}
				rng := rand.New(rand.NewSource(8144))
				fixture := make([][]store.Point, b.N)
				for i := range fixture {
					fixture[i] = make([]store.Point, density)
					for j := range fixture[i] {
						fixture[i][j] = store.Point{Timestamp: now.Unix() + int64(i*density+j+1), Value: float64(rng.Intn(10000)) + .123456789}
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					now = time.Unix(fixture[i][density-1].Timestamp+1, 0)
					if err := codec.Update(b.Context(), kv, m, fixture[i], -1, clock, nil); err != nil {
						b.Fatal(err)
					}
					m.Revision++
					if kv.commit != nil {
						kv.commit(m)
						kv.commit = nil
					}
				}
				b.StopTimer()
				if pool != nil {
					hits, reads, decodes := pool.Counts()
					b.ReportMetric(float64(hits)/float64(b.N), "group-hits/write")
					b.ReportMetric(float64(reads)/float64(b.N), "group-reads/write")
					b.ReportMetric(float64(decodes)/float64(b.N), "decodes/write")
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*density), "ns/point")
			})
		}
	}
}

type ownerBenchIO struct {
	scope *byte
	mapContinuationIO
	commit func(store.Metadata)
}

func (o *ownerBenchIO) AfterContinuationCommit(fn func(store.Metadata)) { o.commit = fn }
func (*ownerBenchIO) TimeBuckets() bool                                 { return true }

func (o *ownerBenchIO) ContinuationCacheScope() *byte { return o.scope }
