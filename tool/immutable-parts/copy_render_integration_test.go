//go:build partsread_integration

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
	sr "github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
	"math"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestCopyRenderDifferential(t *testing.T) {
	ctx := context.Background()
	var clock int64 = 1794700800
	stores := make([]*chunkstore.Store, 2)
	for k := range stores {
		opt := chunkstore.Options{Now: func() time.Time { return time.Unix(clock, 0) }, ExactBlocks: true}
		if k == 1 {
			opt.Continuation = &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
			opt.TimeBuckets = true
		}
		db, err := chunkstore.Open("oracle", opt.WithFS(vfs.NewMem()))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		stores[k] = db
	}
	queries, slots := 0, 0
	for si, def := range copyRenderSchemas {
		for method := sr.Average; method <= sr.First; method++ {
			t.Run(fmt.Sprintf("schema%d-method%d", si, method), func(t *testing.T) {
				clock = 1794700800
				name := fmt.Sprintf("s%d.m%d", si, method)
				rets := whisper.MustParseRetentionDefs(def)
				c := sr.Config{Method: method, XFilesFactor: .5}
				cfg := chunkstore.MetricConfig{Name: name, AggregationMethod: chunkstore.AggregationMethod(method), XFilesFactor: .5}
				var archives []pr.Archive
				for _, r := range rets {
					a := pr.Archive{Step: int64(r.SecondsPerPoint()), Points: int64(r.NumberOfPoints())}
					archives = append(archives, a)
					c.Archives = append(c.Archives, sr.Archive{Step: a.Step, Count: a.Points})
					cfg.Retentions = append(cfg.Retentions, chunkstore.Retention{Step: int(a.Step), Count: int(a.Points)})
				}
				for _, db := range stores {
					if _, err := db.Create(ctx, cfg); err != nil {
						t.Fatal(err)
					}
				}
				w, err := whisper.CreateWithOptions(filepath.Join(t.TempDir(), "oracle.wsp"), rets, whisper.AggregationMethod(method), .5, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				step := archives[0].Step
				start := (clock - 240*step) / step * step
				var dense, sparse, late, edge []sr.Sample
				for i := int64(0); i < 180; i++ {
					dense = append(dense, sr.Sample{Time: start + i*step, Bits: math.Float64bits(float64(i%19) - 9), Sequence: uint64(i + 1)})
				}
				for i := int64(180); i < 220; i += 3 {
					sparse = append(sparse, sr.Sample{Time: start + i*step, Bits: math.Float64bits(float64(i)), Sequence: uint64(i + 1)})
				}
				for i, b := range []uint64{0x8000000000000000, 0x7ff8000000000042, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x3ff0000000000000} {
					late = append(late, sr.Sample{Time: start + int64(i+1)*step, Bits: b, Sequence: uint64(1000 + i)})
				}
				late = append(late, sr.Sample{Time: start + 3*step, Bits: math.Float64bits(-13), Sequence: 2000})
				for _, a := range archives {
					for _, d := range []int64{-1, 0, 1} {
						edge = append(edge, sr.Sample{Time: clock - a.Step*a.Points + d, Bits: math.Float64bits(float64(d + 4)), Sequence: uint64(3000 + len(edge))})
					}
				}
				state := map[[2]int64]sr.Sample{}
				var allRecords []pr.Record
				for bi, input := range [][]sr.Sample{dense, sparse, late, edge} {
					ds, err := sr.Reduce(c, func(a int, slot int64) (sr.Sample, bool) { p, ok := state[[2]int64{int64(a), slot}]; return p, ok }, []sr.Batch{{Order: uint64(bi + 1), Now: clock, Points: input}})
					if err != nil {
						t.Fatal(err)
					}
					var records []pr.Record
					for _, d := range ds {
						state[[2]int64{int64(d.Archive), d.Slot}] = d.Sample
						records = append(records, pr.Record{ID: 7, Step: archives[d.Archive].Step, Sample: pr.Sample{Time: d.Sample.Time, Bits: d.Sample.Bits, Sequence: d.Order}})
					}
					allRecords = append(allRecords, records...)
					var ps []chunkstore.Point
					var ptrs []*whisper.TimeSeriesPoint
					for _, p := range input {
						v := math.Float64frombits(p.Bits)
						ps = append(ps, chunkstore.Point{Timestamp: p.Time, Value: v})
						ptrs = append(ptrs, &whisper.TimeSeriesPoint{Time: int(p.Time), Value: v})
					}
					if err = w.UpdateMany(ptrs); err != nil {
						t.Fatal(err)
					}
					for _, db := range stores {
						if err = db.UpdateMany(ctx, name, ps); err != nil {
							t.Fatal(err)
						}
					}
				}
				cat := pr.CatalogFuncs{LookupFunc: func(n string) (pr.Metric, bool) { return pr.Metric{ID: 7, Archives: archives}, n == name }}
				var pub pr.Publication
				pub.Publish(pr.View{Catalog: cat, Blocks: []pr.Source{copyRenderRoundtrip(t, name, allRecords)}})
				defer pub.Close()
				for _, width := range []int64{3600, 86400, 7 * 86400, 30 * 86400} {
					h, err := pub.Pin(pr.NewBudget(256<<20), 128<<20)
					if err != nil {
						t.Fatal(err)
					}
					defer h.Close()
					got, err := h.Render(ctx, name, clock-width, clock, clock)
					if err != nil {
						t.Fatal(err)
					}
					want, err := w.Fetch(int(clock-width), int(clock))
					if err != nil {
						t.Fatal(err)
					}
					if (got == nil) != (want == nil) {
						t.Fatal("nil mismatch")
					}
					if got == nil {
						continue
					}
					if got.From != int64(want.FromTime()) || got.Until != int64(want.UntilTime()) || got.Step != int64(want.Step()) || len(got.Values) != len(want.Values()) {
						t.Fatalf("classic shape width%d got%d/%d/%d/%d want%d/%d/%d/%d", width, got.From, got.Until, got.Step, len(got.Values), want.FromTime(), want.UntilTime(), want.Step(), len(want.Values()))
					}
					for i, v := range want.Values() {
						if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
							t.Fatalf("classic width%d slot%d got%x want%x", width, i, math.Float64bits(got.Values[i]), math.Float64bits(v))
						}
					}
					for mode, db := range stores {
						current, e := db.Fetch(ctx, name, int(clock-width), int(clock))
						if e != nil {
							t.Fatal(e)
						}
						if current == nil || int64(current.FromTime) != got.From || int64(current.UntilTime) != got.Until || int64(current.Step) != got.Step || len(current.Values) != len(got.Values) {
							t.Fatalf("current mode%d shape mismatch width%d", mode, width)
						}
						for i, v := range current.Values {
							if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
								t.Fatalf("current mode%d width%d slot%d got%x want%x", mode, width, i, math.Float64bits(got.Values[i]), math.Float64bits(v))
							}
						}
					}
					slots += len(got.Values)
					queries++
					h.Close()
				}
			})
		}
	}
	t.Logf("queries=%d slots=%d schemas=%d methods=6 oracles=classic,native,continuation", queries, slots, len(copyRenderSchemas))
}

var copyRenderSchemas = []string{
	"1s:1d", "10s:3d", "1s:2d", "1m:35d", "5m:90d", "15m:180d", "30m:1y", "1h:2y", "1d:10y",
	"1s:1d,1m:30d,1h:1y,1d:10y", "1m:30d,1h:1y,1d:10y", "1m:14d,30m:2y", "1s:3d,1m:14d,30m:2y",
	"10s:2d,1m:14d,30m:2y", "10m:1y,1d:10y", "1s:2d,1m:28d,1h:2y", "1s:2d,1m:62d,10m:5y",
	"10m:2d,1h:9d,1d:3y", "10s:4d,1m:31d,30m:4y", "1m:7d,5m:60d,1h:2y", "1m:14d,5m:60d,1h:2y",
	"1m:62d,30m:2y", "1h:90d,1d:2y", "1s:7d,1m:14d,30m:2y", "1s:1d,1m:30d,1h:2y",
	"1s:3d,1m:14d,30m:2y", "5m:60d,30m:2y", "10m:7d,1h:5y,1d:10y", "1m:14d,1h:5y",
	"10s:15d,1m:100d,15m:2y", "1m:200d,1h:5y", "1m:400d,1d:10y", "1m:400d,1h:5y",
	"1m:100d,30m:2y", "5m:400d,1h:2y", "1m:30d,15m:400d,1h:5y", "5m:400d,1h:5y",
	"5s:15d,1m:100d,15m:5y", "2s:7d,1m:100d,15m:2y", "1s:40d,1m:100d,15m:2y",
	"5m:14d,1d:10y", "1h:90d,1d:10y",
	"10m:30d",
	"12h:10y",
	"15m:1y",
	"15m:840d",
	"1d:2y",
	"1d:3y",
	"1d:5y",
	"1d:90d",
	"1h:10y",
	"1h:15y",
	"1h:1y",
	"1h:30d",
	"1h:5y",
	"1m:14d",
	"1m:14d,5m:60d,60m:2y",
	"1m:25y",
	"1m:2d,1h:1y,1d:10y",
	"1m:30d",
	"1m:7d,5m:60d,60m:2y",
	"1m:90d",
	"1s:7d",
	"1w:10y",
	"2h:10y",
	"30m:30d",
	"30m:3y",
	"5m:1y",
	"5m:30d",
	"5m:420d",
}

// copyRenderRoundtrip inserts the actual SST/TCP/ingest boundary into the read
// differential: every slot then rendered came back from the receiver Pebble.
func copyRenderRoundtrip(t *testing.T, name string, records []pr.Record) pr.Source {
	t.Helper()
	root := t.TempDir()
	db := copyTestDB(t, filepath.Join(root, "target"))
	defer db.Close()
	id := partsformat.ID(name)
	vs := partsformat.Route(id)
	part := partsformat.PartID{1}
	type pair struct{ k, v []byte }
	var rows []pair
	for i, r := range records {
		k, e := partsformat.Encode(partsformat.Key{VShard: vs, Partition: 0, Metric: id, Part: part, Archive: uint32(r.Step), BlockStart: r.Sample.Time, Ordinal: uint32(i)})
		if e != nil {
			t.Fatal(e)
		}
		v := make([]byte, 40)
		binary.LittleEndian.PutUint64(v, r.ID)
		binary.LittleEndian.PutUint64(v[8:], uint64(r.Step))
		binary.LittleEndian.PutUint64(v[16:], uint64(r.Sample.Time))
		binary.LittleEndian.PutUint64(v[24:], r.Sample.Bits)
		binary.LittleEndian.PutUint64(v[32:], r.Sample.Sequence)
		rows = append(rows, pair{k, v})
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].k, rows[j].k) < 0 })
	path := filepath.Join(root, "part.sst")
	f, e := vfs.Default.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{TableFormat: sstable.TableFormatPebblev4})
	for _, r := range rows {
		if e = w.Set(r.k, r.v); e != nil {
			t.Fatal(e)
		}
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	fd, e := copyDescribe(path, part)
	if e != nil {
		t.Fatal(e)
	}
	source := []copyPartition{{Manifest: partsformat.Manifest{VShard: vs, Partition: 0, Parts: []partsformat.Part{fd.Part}}, Files: []copyFile{fd}}}
	addr, stop := copyTestServer(t, source)
	defer stop()
	if _, e = copyTransfer(context.Background(), addr, source, []int{0}, 1, []*pebble.DB{db}, root); e != nil {
		t.Fatal(e)
	}
	it, e := db.NewIter(nil)
	if e != nil {
		t.Fatal(e)
	}
	defer it.Close()
	var received []pr.Record
	for ok := it.First(); ok; ok = it.Next() {
		v, e := it.ValueAndErr()
		if e != nil {
			t.Fatal(e)
		}
		if len(v) != 40 {
			t.Fatal("invalid record size")
		}
		received = append(received, pr.Record{ID: binary.LittleEndian.Uint64(v), Step: int64(binary.LittleEndian.Uint64(v[8:])), Sample: pr.Sample{Time: int64(binary.LittleEndian.Uint64(v[16:])), Bits: binary.LittleEndian.Uint64(v[24:]), Sequence: binary.LittleEndian.Uint64(v[32:])}})
	}
	if e = it.Error(); e != nil {
		t.Fatal(e)
	}
	return pr.Freeze(received)
}
