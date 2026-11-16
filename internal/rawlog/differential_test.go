package rawlog_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/rawlog"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/go-graphite/go-carbon/tags"
)

type bitsPoint struct {
	Stamp int64
	Bits  uint64
}
type observations map[string][]bitsPoint

func note(dst observations, p *points.Points) {
	for _, v := range p.Data {
		dst[p.Metric] = append(dst[p.Metric], bitsPoint{v.Timestamp, math.Float64bits(v.Value)})
	}
}
func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func replayFile(t testing.TB, path string, apply func(uint64, *points.Points) error) rawlog.ReplayStats {
	t.Helper()
	f, err := os.Open(path)
	must(t, err)
	defer f.Close()
	stats, err := rawlog.Replay(f, rawlog.ReplayOptions{Workers: 4}, apply)
	must(t, err)
	return stats
}

// The oracle traverses the current stream -> legacy varint -> FrameDecoder ->
// SegmentWriter -> SegmentReplay path, not a second raw-log implementation.
func TestDifferentialJournalCacheAndPebble(t *testing.T) {
	const stamp int64 = 1_800_000_000
	for _, flags := range []byte{0, codec.FlagZstd} {
		t.Run(fmt.Sprint(flags), func(t *testing.T) {
			dir := t.TempDir()
			w, err := rawlog.Create(dir)
			must(t, err)
			journal, err := os.CreateTemp(t.TempDir(), "journal")
			must(t, err)
			defer journal.Close()
			old, err := inputlog.NewSegmentWriter(journal, 1)
			must(t, err)
			defer old.Close()
			var encs [3]*codec.Encoder
			var decs [3]*codec.Decoder
			for i := range encs {
				encs[i], err = codec.NewEncoder(codec.Options{Flags: flags})
				must(t, err)
				defer encs[i].Close()
				decs[i], err = codec.NewDecoder(codec.Options{Flags: flags})
				must(t, err)
				defer decs[i].Close()
				p, e := codec.AppendPreamble(nil, flags)
				must(t, e)
				_, err = w.Begin(uint64(i), p)
				must(t, err)
			}
			source := make(observations)
			suffix := make(observations)
			var suffixStart bool
			values := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000001234, 0x7ff0000000000001, 0x3ff0000000000000}
			for round := 0; round < 20; round++ {
				if round == 10 {
					must(t, w.Rotate())
					suffixStart = true
				}
				for conn := range encs {
					enc := encs[conn]
					if round == 7 {
						must(t, enc.Reset())
					}
					for i, bits := range values {
						ts := stamp + int64(round%4)
						must(t, enc.Add([]byte("shared"), math.Float64frombits(bits), ts))
						name := []string{"tagged;b=2;a=1", "tagged;a=1;b=2"}[(i+round)%2]
						must(t, enc.Add([]byte(name), float64(round*100+conn*10+i), ts))
					}
					// Odd/extreme timestamp arithmetic and connection-unique metric names.
					must(t, enc.Add([]byte(fmt.Sprintf("conn.%d", conn)), math.SmallestNonzeroFloat64, math.MinInt64+int64(round)))
					wire, e := enc.Flush(nil)
					must(t, e)
					reader, e := codec.NewWireReader(codec.Options{Flags: flags})
					must(t, e)
					rd := bytes.NewReader(wire)
					for rd.Len() > 0 {
						frame, _, e := reader.Read(rd)
						must(t, e)
						var legacy []byte
						_, e = decs[conn].Decode(frame, func(_ uint32, n []byte, v float64, ts int64, _ *any) {
							p := points.OnePoint(string(n), v, ts)
							note(source, p)
							if suffixStart {
								note(suffix, p)
							}
							legacy = p.AppendBinary(legacy)
						})
						must(t, e)
						var records []*points.Points
						var decoder points.FrameDecoder
						must(t, decoder.Read(legacy, func(n []byte, data []points.Point) {
							records = append(records, &points.Points{Metric: string(n), Data: append([]points.Point(nil), data...)})
						}))
						_, e = old.Append(records, nil)
						must(t, e)
						before := w.Stats().Durable
						seq, e := w.Stage(uint64(conn), frame, int64(round+1))
						must(t, e)
						if w.Stats().Durable != before || seq <= before {
							t.Fatal("Stage falsely acknowledged durability")
						}
					}
				}
			}
			must(t, w.Close())
			oldPoints := make(observations)
			rawPoints := make(observations)
			var oldRows, rawRows []*points.Points
			r, err := inputlog.NewSegmentReplay(journal)
			must(t, err)
			defer r.Close()
			st, err := journal.Stat()
			must(t, err)
			for {
				rows, _, _, e := r.Next(st.Size())
				if e == io.EOF {
					break
				}
				must(t, e)
				for _, p := range rows {
					note(oldPoints, p)
					oldRows = append(oldRows, p.Copy())
				}
			}
			files, err := filepath.Glob(filepath.Join(dir, "*.raw"))
			must(t, err)
			sort.Strings(files)
			for _, f := range files {
				replayFile(t, f, func(_ uint64, p *points.Points) error { note(rawPoints, p); rawRows = append(rawRows, p); return nil })
			}
			if !reflect.DeepEqual(source, oldPoints) {
				t.Fatal("current journal changed per-name IEEE/order semantics")
			}
			if !reflect.DeepEqual(oldPoints, rawPoints) {
				t.Fatal("raw log changed per-name IEEE/order semantics")
			}
			onlySuffix := make(observations)
			replayFile(t, files[1], func(_ uint64, p *points.Points) error { note(onlySuffix, p); return nil })
			if !reflect.DeepEqual(suffix, onlySuffix) {
				t.Fatal("independent segment snapshot replay differs")
			}
			caches := [2]*cache.Cache{cache.New(), cache.New()}
			stores := [2]*chunkstore.Store{}
			for i := range stores {
				stores[i], err = chunkstore.Open(t.TempDir(), chunkstore.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, TimeBuckets: true, CacheSize: 8 << 20, MemTableSize: 4 << 20, Now: func() time.Time { return time.Unix(stamp+100, 0) }})
				must(t, err)
				defer stores[i].Close()
			}
			names := map[string]bool{}
			for side, rows := range [][]*points.Points{oldRows, rawRows} {
				caches[side].SetTagsEnabled(true)
				made := map[string]bool{}
				for _, p := range rows {
					caches[side].Add(p.Copy())
					name, e := tags.Normalize(p.Metric)
					must(t, e)
					names[name] = true
					if !made[name] {
						_, e = stores[side].Create(context.Background(), chunkstore.MetricConfig{Name: name, Retentions: []chunkstore.Retention{{Step: 1, Count: 1000}}, AggregationMethod: chunkstore.Last, XFilesFactor: 0})
						must(t, e)
						made[name] = true
					}
					must(t, stores[side].UpdateMany(context.Background(), name, p.Data))
				}
				must(t, stores[side].Sync())
			}
			for name := range names {
				a, b := make(observations), make(observations)
				note(a, &points.Points{Metric: name, Data: caches[0].Get(name)})
				note(b, &points.Points{Metric: name, Data: caches[1].Get(name)})
				if !reflect.DeepEqual(a, b) {
					t.Fatalf("cache differs for %s", name)
				}
				aSeries, e := stores[0].Fetch(context.Background(), name, int(stamp-10), int(stamp+10))
				must(t, e)
				bSeries, e := stores[1].Fetch(context.Background(), name, int(stamp-10), int(stamp+10))
				must(t, e)
				if aSeries.Step != bSeries.Step || len(aSeries.Values) != len(bSeries.Values) {
					t.Fatal("store series shape differs")
				}
				for j, v := range aSeries.Values {
					if math.Float64bits(v) != math.Float64bits(bSeries.Values[j]) {
						t.Fatalf("store differs for %s at %d", name, j)
					}
				}
			}
		})
	}
}
