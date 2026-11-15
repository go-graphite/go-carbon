//go:build sealrollup_integration

package partspublish

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	pf "github.com/go-graphite/go-carbon/internal/partsformat"
	. "github.com/go-graphite/go-carbon/internal/partsread"
	sr "github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPublisherRenderDifferential(t *testing.T) {
	var fixture []oracleMetric
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
	for si, def := range combinedSchemas {
		for method := sr.Average; method <= sr.First; method++ {
			t.Run(fmt.Sprintf("schema%d-method%d", si, method), func(t *testing.T) {
				clock = 1794700800
				name := fmt.Sprintf("s%d.m%d", si, method)
				var fixturePart pf.PartID
				if _, err := rand.Read(fixturePart[:]); err != nil {
					t.Fatal(err)
				}
				rets := whisper.MustParseRetentionDefs(def)
				c := sr.Config{Method: method, XFilesFactor: .5}
				cfg := chunkstore.MetricConfig{Name: name, AggregationMethod: chunkstore.AggregationMethod(method), XFilesFactor: .5}
				var archives []Archive
				for _, r := range rets {
					a := Archive{int64(r.SecondsPerPoint()), int64(r.NumberOfPoints())}
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
				var manifestData []BlockData
				for bi, input := range [][]sr.Sample{dense, sparse, late, edge} {
					ds, err := sr.Reduce(c, func(a int, slot int64) (sr.Sample, bool) { p, ok := state[[2]int64{int64(a), slot}]; return p, ok }, []sr.Batch{{Order: uint64(bi + 1), Now: clock, Points: input}})
					if err != nil {
						t.Fatal(err)
					}
					var records []Record
					for _, d := range ds {
						state[[2]int64{int64(d.Archive), d.Slot}] = d.Sample
						records = append(records, Record{7, archives[d.Archive].Step, Sample{d.Sample.Time, d.Sample.Bits, d.Order}})
					}
					var blocks []SlotBlock
					for ri, r := range records {
						for ai, a := range archives {
							if a.Step != r.Step {
								continue
							}
							slot := (r.Sample.Time / a.Step) % a.Points
							if slot < 0 {
								slot += a.Points
							}
							key, e := pf.Encode(pf.Key{Group: "test", VShard: pf.Route(pf.ID(name)), Metric: pf.ID(name), Archive: uint32(ai), Part: fixturePart, Ordinal: uint32(bi*10000 + ri)})
							if e != nil {
								t.Fatal(e)
							}
							blocks = append(blocks, SlotBlock{ID: r.ID, Archive: a, Slots: SlotRange{slot, slot + 1}, Key: key})
						}
					}
					for i, b := range blocks {
						manifestData = append(manifestData, BlockData{SlotBlock: b, Samples: []Sample{records[i].Sample}})
					}
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
				// Publish all retained batch versions once. This keeps the oracle
				// focused on the real disk codec without thousands of extra fsyncs.
				metric := oracleMetric{Metric: testMetric{Name: name, Group: "test", Archives: archives}, Blocks: manifestData}
				catBytes, _ := json.Marshal([]testMetric{metric.Metric})
				scope, _ := pf.PartitionPrefixForGroup("test", pf.Route(pf.ID(name)), 0)
				var pub interface {
					Pin(*Budget, int64) (*Handle, error)
				}
				if os.Getenv("PARTS_PUBLISH_CRASHES") != "" {
					// Build the golden oracle without repeating durable publication
					// per metric. The matrix publishes all actual files, then checks
					// this oracle after every crash using the real recovered source.
					var records []Record
					for _, b := range manifestData {
						for _, sample := range b.Samples {
							records = append(records, Record{ID: 7, Step: b.Archive.Step, Sample: sample})
						}
					}
					cat, _, _, err := testDecoder(7)(catBytes)
					if err != nil {
						t.Fatal(err)
					}
					memory := &Publication{}
					memory.Publish(View{Catalog: cat, Parts: []Source{Freeze(records)}})
					defer memory.Close()
					pub = memory
				} else {
					p := testPublisher(t, t.TempDir(), 7, nil)
					commitRequest(t, p, Request{Envelope: Envelope{Version: 1, Generation: 4, Catalog: catBytes, Frontier: []uint64{4}, Parts: []Ref{{Name: "sealed", Generation: 4, Version: 1, Codec: CodecV1, Scope: scope}}}, Added: map[string][]BlockData{"sealed": manifestData}})
					pub = p
				}
				for _, width := range []int64{3600, 86400, 7 * 86400, 30 * 86400} {
					h, err := pub.Pin(NewBudget(256<<20), 128<<20)
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
					metric.Queries = append(metric.Queries, oracleQuery{From: clock - width, Until: clock, Now: clock, Digest: seriesDigest(got)})
					h.Close()
				}
				fixture = append(fixture, metric)
			})
		}
	}
	if os.Getenv("PARTS_PUBLISH_CRASHES") != "" {
		runOracleCrashMatrix(t, fixture)
	}
	t.Logf("queries=%d slots=%d schemas=%d methods=6 oracles=classic,native,continuation", queries, slots, len(combinedSchemas))
}

var combinedSchemas = []string{
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

type oracleQuery struct {
	From, Until, Now int64
	Digest           [32]byte
}
type oracleMetric struct {
	Metric  testMetric
	Blocks  []BlockData
	Queries []oracleQuery
}

func seriesDigest(s *Series) [32]byte {
	h := sha256.New()
	var buf [8]byte
	if s == nil {
		return sha256.Sum256(nil)
	}
	for _, n := range []int64{s.From, s.Until, s.Step, int64(len(s.Values))} {
		binary.LittleEndian.PutUint64(buf[:], uint64(n))
		h.Write(buf[:])
	}
	for _, v := range s.Values {
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
		h.Write(buf[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
