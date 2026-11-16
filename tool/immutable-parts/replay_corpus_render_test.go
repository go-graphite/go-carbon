//go:build sealrollup_integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partlog"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
	sr "github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

// This queries rows from the actual full-scale store, not a separate compact
// fixture. All rows are checked by the independent raw oracle; this samples one
// metric for each of 70 schema x 6 method combinations across the ID space.
func verifyCorpusRender(t *testing.T, root string, owners int) {
	t.Helper()
	c, err := readReplayCorpus(root)
	if err != nil {
		t.Fatal(err)
	}
	if c.Metrics < 420 {
		t.Fatal("render corpus requires at least420 metrics")
	}
	var live []uint64
	b, err := os.ReadFile(filepath.Join(root, "live-counts.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &live); err != nil || len(live) != c.Producers {
		t.Fatal("invalid live frontier", err)
	}
	scratch, err := os.MkdirTemp(root, "render-oracle-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(scratch)
	// Compact blocks contain many metrics. Scan each owner once rather than
	// rescanning its complete history independently for every sampled ID.
	samples := map[uint64][]row{}
	for i := 0; i < 420; i++ {
		id := uint64(i) + (uint64(i)*c.Metrics/420/420)*420
		samples[id] = nil
	}
	type sampleResult struct {
		Rows map[uint64][]row
		Err  error
	}
	results := make(chan sampleResult, owners)
	for owner := range owners {
		go func() {
			r := sampleResult{Rows: map[uint64][]row{}}
			defer func() { results <- r }()
			db, e := pebble.Open(filepath.Join(root, "parts", fmt.Sprintf("owner-%03d", owner)), &pebble.Options{ReadOnly: true})
			if e != nil {
				r.Err = e
				return
			}
			defer db.Close()
			it, e := db.NewIter(&pebble.IterOptions{LowerBound: []byte{1}, UpperBound: []byte{3}})
			if e != nil {
				r.Err = e
				return
			}
			defer it.Close()
			var decoder v2core.RawDecoder
			visit := func(p row) error {
				if _, ok := samples[p.ID]; ok {
					r.Rows[p.ID] = append(r.Rows[p.ID], p)
				}
				return nil
			}
			for it.First(); it.Valid(); it.Next() {
				if e = decoder.Visit(it.Key(), it.Value(), visit); e != nil {
					r.Err = e
					return
				}
			}
			r.Err = it.Error()
		}()
	}
	// Readers share the immutable selection set; merge only after joining all.
	selected := make([]sampleResult, 0, owners)
	for range owners {
		r := <-results
		if r.Err != nil {
			t.Error(r.Err)
		}
		selected = append(selected, r)
	}
	if t.Failed() {
		t.FailNow()
	}
	for _, r := range selected {
		for id, rows := range r.Rows {
			samples[id] = append(samples[id], rows...)
		}
	}
	ctx := context.Background()
	queries, slots, checked := 0, 0, 0
	for i := 0; i < 420; i++ {
		id := uint64(i) + (uint64(i)*c.Metrics/420/420)*420
		if id >= c.Metrics {
			t.Fatal("sample ID outside catalog")
		}
		rows := samples[id]
		var e error
		sort.Slice(rows, func(a, b int) bool { return rows[a].Seq < rows[b].Seq })
		var expected []row
		for seq := id; seq < c.Historical; seq += c.Metrics {
			expected = append(expected, row{ID: id, Time: 1800000000 + int64(seq/6000000), Bits: math.Float64bits(float64(seq % 100003)), Seq: seq})
		}
		seq := c.Historical + (id+c.Metrics-c.Historical%c.Metrics)%c.Metrics
		for ; ; seq += c.Metrics {
			ix := seq - c.Historical
			p := ix % uint64(c.Producers)
			if ix/uint64(c.Producers) >= live[p] {
				break
			}
			expected = append(expected, row{ID: id, Time: 1800000000 + int64(seq/20000000), Bits: math.Float64bits(float64(seq % 100003)), Seq: seq})
		}
		if len(rows) != len(expected) {
			t.Fatalf("metric%d rows%d want%d", id, len(rows), len(expected))
		}
		for n := range rows {
			if rows[n] != expected[n] {
				t.Fatalf("metric%d row%d differs", id, n)
			}
		}
		checked += len(rows)
		name := fmt.Sprintf(c.NameFormat, id)
		method := sr.Method(id%6 + 1)
		rets := whisper.MustParseRetentionDefs(c.Schemas[(id/6)%uint64(len(c.Schemas))])
		config := sr.Config{Method: method, XFilesFactor: .5}
		nativeCfg := chunkstore.MetricConfig{Name: name, AggregationMethod: chunkstore.AggregationMethod(method), XFilesFactor: .5}
		metric := pr.Metric{ID: id, XFilesFactor: .5}
		for _, r := range rets {
			step, count := int64(r.SecondsPerPoint()), int64(r.NumberOfPoints())
			config.Archives = append(config.Archives, sr.Archive{Step: step, Count: count})
			metric.Archives = append(metric.Archives, pr.Archive{Step: step, Points: count})
			nativeCfg.Retentions = append(nativeCfg.Retentions, chunkstore.Retention{Step: int(step), Count: int(count)})
		}
		clock := int64(1800000000)
		w, e := whisper.CreateWithOptions(filepath.Join(scratch, "oracle.wsp"), rets, whisper.AggregationMethod(method), .5, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
		if e != nil {
			t.Fatal(e)
		}
		stores := make([]*chunkstore.Store, 2)
		for k := range stores {
			opt := chunkstore.Options{Now: func() time.Time { return time.Unix(clock, 0) }, ExactBlocks: true}
			if k == 1 {
				opt.Continuation = &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
				opt.TimeBuckets = true
			}
			stores[k], e = chunkstore.Open("oracle", opt.WithFS(vfs.NewMem()))
			if e != nil {
				t.Fatal(e)
			}
			if _, e = stores[k].Create(ctx, nativeCfg); e != nil {
				t.Fatal(e)
			}
		}
		state := map[[2]int64]sr.Sample{}
		for _, r := range rows {
			clock = r.Time
			point := sr.Sample{Time: r.Time, Bits: r.Bits, Sequence: r.Seq}
			deltas, e := sr.Reduce(config, func(a int, s int64) (sr.Sample, bool) { p, ok := state[[2]int64{int64(a), s}]; return p, ok }, []sr.Batch{{Order: r.Seq + 1, Now: clock, Points: []sr.Sample{point}}})
			if e != nil {
				t.Fatal(e)
			}
			for _, d := range deltas {
				state[[2]int64{int64(d.Archive), d.Slot}] = d.Sample
			}
			ptr := &whisper.TimeSeriesPoint{Time: int(r.Time), Value: math.Float64frombits(r.Bits)}
			if e = w.UpdateMany([]*whisper.TimeSeriesPoint{ptr}); e != nil {
				t.Fatal(e)
			}
			for _, db := range stores {
				if e = db.UpdateMany(ctx, name, []chunkstore.Point{{Timestamp: r.Time, Value: ptr.Value}}); e != nil {
					t.Fatal(e)
				}
			}
		}
		var data []pr.BlockData
		for key, s := range state {
			a := metric.Archives[key[0]]
			slot := key[1]
			data = append(data, pr.BlockData{SlotBlock: pr.SlotBlock{ID: id, Archive: a, Slots: pr.SlotRange{From: slot, Until: slot + 1}, Key: []byte(fmt.Sprintf("a%d/s%d", key[0], slot))}, Samples: []pr.Sample{{Time: s.Time, Bits: s.Bits, Sequence: 1}}})
		}
		part, e := pr.PublishBlockPart(scratch, "sealed", uint64(i+1), data)
		if e != nil {
			t.Fatal(e)
		}
		index, e := pr.OpenBlockPart(part)
		if e != nil {
			t.Fatal(e)
		}
		var pub pr.Publication
		pub.Publish(pr.View{Catalog: pr.CatalogFuncs{LookupFunc: func(n string) (pr.Metric, bool) { return metric, n == name }}, Parts: []pr.Source{index}})
		for _, width := range []int64{3600, 86400, 7 * 86400, 30 * 86400} {
			h, e := pub.Pin(pr.NewBudget(256<<20), 128<<20)
			if e != nil {
				t.Fatal(e)
			}
			got, e := h.Render(ctx, name, clock-width, clock, clock)
			if e != nil {
				t.Fatal(e)
			}
			want, e := w.Fetch(int(clock-width), int(clock))
			if e != nil {
				t.Fatal(e)
			}
			if got == nil || want == nil {
				t.Fatalf("empty render metric%d", id)
			}
			if got.From != int64(want.FromTime()) || got.Until != int64(want.UntilTime()) || got.Step != int64(want.Step()) || len(got.Values) != len(want.Values()) {
				t.Fatalf("classic shape metric%d width%d", id, width)
			}
			for n, v := range want.Values() {
				if math.Float64bits(v) != math.Float64bits(got.Values[n]) {
					t.Fatalf("classic metric%d width%d slot%d got%x want%x", id, width, n, math.Float64bits(got.Values[n]), math.Float64bits(v))
				}
			}
			for mode, db := range stores {
				native, e := db.Fetch(ctx, name, int(clock-width), int(clock))
				if e != nil {
					t.Fatal(e)
				}
				if native == nil || int64(native.FromTime) != got.From || int64(native.UntilTime) != got.Until || int64(native.Step) != got.Step || len(native.Values) != len(got.Values) {
					t.Fatalf("native mode%d metric%d shape", mode, id)
				}
				for n, v := range native.Values {
					if math.Float64bits(v) != math.Float64bits(got.Values[n]) {
						t.Fatalf("native mode%d metric%d width%d slot%d", mode, id, width, n)
					}
				}
			}
			h.Close()
			queries++
			slots += len(got.Values)
		}
		pub.Close()
		_ = w.Close()
		_ = os.Remove(filepath.Join(scratch, "oracle.wsp"))
		for _, db := range stores {
			_ = db.Close()
		}
		// Bound temporary published block files between metrics.
		_ = os.RemoveAll(part)
	}
	t.Logf("full_corpus_render_exact queries=%d slots=%d sampled_metrics=420 sampled_rows=%d raw_all_rows=separate_oracle", queries, slots, checked)
}

func TestFullScaleRenderedCorpus(t *testing.T) {
	root := os.Getenv("PARTLOG_RENDER_ROOT")
	if root == "" {
		t.Skip("full corpus oracle")
	}
	owners := 48
	if v := os.Getenv("PARTLOG_BENCH_OWNERS"); v != "" {
		if _, err := fmt.Sscan(v, &owners); err != nil {
			t.Fatal(err)
		}
	}
	verifyCorpusRender(t, root, owners)
}

func TestCorpusRenderIntegration(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PARTLOG_BENCH_METADATA", "1")
	// Ten updates per metric cover replacement order without repeating the
	// same timestamp a hundred times in this small integration fixture.
	const total = 4200
	if err := logBacklogBenchmark(root, "write", total, 420, 3, 2); err != nil {
		t.Fatal(err)
	}
	l, err := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), coreConfig{AppendParts: true, Shards: 2, Producers: 3, MaxRows: 1 << 20, PartitionSeconds: 3600}, partlog.Options{Streams: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err = l.replay(); err != nil {
		t.Fatal(err)
	}
	if err = l.close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "live-counts.json"), []byte("[0,0,0]"), 0600); err != nil {
		t.Fatal(err)
	}
	verifyCorpusRender(t, root, 2)
}
