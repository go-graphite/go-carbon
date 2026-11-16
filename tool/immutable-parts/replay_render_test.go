//go:build sealrollup_integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partlog"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
	sr "github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func TestReplayedRenderDifferential(t *testing.T) {
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
				root := t.TempDir()
				journal, err := partlog.Open(filepath.Join(root, "log"), partlog.Options{Streams: 1})
				if err != nil {
					t.Fatal(err)
				}
				policy, err := json.Marshal(replayRenderPolicy{Name: name, Config: c})
				if err != nil {
					t.Fatal(err)
				}
				for bi, input := range [][]sr.Sample{dense, sparse, late, edge} {
					batch := coreBatch{ID: 7, Order: uint64(bi + 1), Clock: clock, Policy: policy}
					for _, p := range input {
						batch.Points = append(batch.Points, row{ID: 7, Time: p.Time, Bits: p.Bits, Seq: p.Sequence})
					}
					payload, err := encodeLogBatches([]coreBatch{batch})
					if err != nil {
						t.Fatal(err)
					}
					if _, err = journal.Append(0, clock, payload); err != nil {
						t.Fatal(err)
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
				if err = journal.Close(); err != nil {
					t.Fatal(err)
				}
				replayCfg := coreConfig{AppendParts: true, Shards: 2, Producers: 1, MaxRows: 1 << 20, PartitionSeconds: 3600}
				replay, err := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), replayCfg, partlog.Options{})
				if err != nil {
					t.Fatal(err)
				}
				if err = replay.replay(); err != nil {
					_ = replay.close()
					t.Fatal(err)
				}
				if err = replay.close(); err != nil {
					t.Fatal(err)
				}
				records, cat, err := replayRenderRecords(root, replayCfg.Shards)
				if err != nil {
					t.Fatal(err)
				}
				var manifestData []pr.BlockData
				for ri, r := range records {
					for _, a := range archives {
						if a.Step != r.Step {
							continue
						}
						slot := (r.Sample.Time / a.Step) % a.Points
						manifestData = append(manifestData, pr.BlockData{SlotBlock: pr.SlotBlock{ID: r.ID, Archive: a, Slots: pr.SlotRange{From: slot, Until: slot + 1}, Key: []byte(fmt.Sprintf("replayed/%s/%d", name, ri))}, Samples: []pr.Sample{r.Sample}})
					}
				}
				part, err := pr.PublishBlockPart(t.TempDir(), "sealed", 4, manifestData)
				if err != nil {
					t.Fatal(err)
				}
				index, err := pr.OpenBlockPart(part)
				if err != nil {
					t.Fatal(err)
				}
				defer index.Close()
				var pub pr.Publication
				pub.Publish(pr.View{Catalog: cat, Parts: []pr.Source{index}})
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
	if queries != 1680 {
		t.Fatalf("queries=%d, want 1680", queries)
	}
	t.Logf("replayed numeric store + journal dictionary: queries=%d slots=%d schemas=%d methods=6 oracles=classic,native,continuation", queries, slots, len(combinedSchemas))
}

func TestReplayRenderRejectsMissingDictionary(t *testing.T) {
	root := t.TempDir()
	if err := logBacklogBenchmark(root, "write", 100003, 101, 1, 2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := replayRenderRecords(root, 2); err == nil {
		t.Fatal("numeric-only journal accepted without a name/policy dictionary")
	}
}

func TestReplayRenderRejectsPersistedBitChange(t *testing.T) {
	root := t.TempDir()
	journal, err := partlog.Open(filepath.Join(root, "log"), partlog.Options{Streams: 1})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := json.Marshal(replayRenderPolicy{Name: "test.metric", Config: sr.Config{Archives: []sr.Archive{{Step: 1, Count: 86400}}, Method: sr.Average, XFilesFactor: .5}})
	if err != nil {
		t.Fatal(err)
	}
	batch := coreBatch{ID: 7, Order: 1, Clock: 1800000000, Policy: policy, Points: []row{{ID: 7, Time: 1800000000, Bits: 0x8000000000000000, Seq: 1}}}
	payload, err := encodeLogBatches([]coreBatch{batch})
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := journal.Append(0, batch.Clock, payload)
	if err != nil {
		t.Fatal(err)
	}
	cfg := coreConfig{AppendParts: true, JournalIdentity: journal.Identity(), Shards: 1, Producers: 1, MaxRows: 1 << 20, PartitionSeconds: 3600}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	engine, err := openCore(filepath.Join(root, "parts"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	changed := batch.Points[0]
	changed.Bits = 0
	if err = engine.submit(0, []row{changed}, cursor, true); err != nil {
		_ = engine.close()
		t.Fatal(err)
	}
	if err = engine.close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = replayRenderRecords(root, 1); err == nil {
		t.Fatal("accepted changed persisted signed-zero bits")
	}
}
