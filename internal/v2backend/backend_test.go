package v2backend

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

func testOptions(dir string, m daemonstore.Metric) daemonstore.Options {
	return daemonstore.Options{DataDir: dir, Shards: 1, Owners: 1, PartInterval: time.Hour, Policy: func(string) (daemonstore.Metric, error) { return m, nil }}
}
func simplePolicy() daemonstore.Metric {
	return daemonstore.Metric{Archives: []daemonstore.Archive{{Step: 1, Points: 3600}}, AggregationMethod: "average", XFilesFactor: .5}
}
func requireClose(t *testing.T, s *Store) {
	t.Helper()
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
}

func TestPortableBackendOracle1680(t *testing.T) {
	ctx := context.Background()
	clock := int64(1794700800)
	policies := make(map[string]daemonstore.Metric)
	opts := testOptions(t.TempDir(), simplePolicy())
	opts.Policy = func(n string) (daemonstore.Metric, error) { return policies[n], nil }
	s, e := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
	if e != nil {
		t.Fatal(e)
	}
	defer requireClose(t, s)
	queries := 0
	slots := 0
	for si, def := range oracleSchemas {
		for method := 1; method <= 6; method++ {
			name := fmt.Sprintf("schema%d.method%d", si, method)
			rets := whisper.MustParseRetentionDefs(def)
			m := daemonstore.Metric{AggregationMethod: store.AggregationMethod(method).String(), XFilesFactor: .5}
			for _, r := range rets {
				m.Archives = append(m.Archives, daemonstore.Archive{Step: r.SecondsPerPoint(), Points: r.NumberOfPoints()})
			}
			policies[name] = m
			w, e := whisper.CreateWithOptions(filepath.Join(t.TempDir(), "oracle.wsp"), rets, whisper.AggregationMethod(method), .5, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
			if e != nil {
				t.Fatal(e)
			}
			step := int64(m.Archives[0].Step)
			start := (clock - 240*step) / step * step
			var dense, sparse, late, edge []points.Point
			for i := int64(0); i < 180; i++ {
				dense = append(dense, points.Point{Timestamp: start + i*step, Value: float64(i%19) - 9})
			}
			for i := int64(180); i < 220; i += 3 {
				sparse = append(sparse, points.Point{Timestamp: start + i*step, Value: float64(i)})
			}
			for i, b := range []uint64{0x8000000000000000, 0x7ff8000000000042, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x3ff0000000000000} {
				late = append(late, points.Point{Timestamp: start + int64(i+1)*step, Value: math.Float64frombits(b)})
			}
			late = append(late, points.Point{Timestamp: start + 3*step, Value: -13})
			for _, a := range m.Archives {
				for _, d := range []int64{-1, 0, 1} {
					edge = append(edge, points.Point{Timestamp: clock - int64(a.Step)*int64(a.Points) + d, Value: float64(d + 4)})
				}
			}
			for _, batch := range [][]points.Point{dense, sparse, late, edge} {
				ps := make([]*whisper.TimeSeriesPoint, len(batch))
				for i, p := range batch {
					ps[i] = &whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: p.Value}
				}
				if e = w.UpdateMany(ps); e != nil {
					t.Fatal(e)
				}
				if e = s.Append(&points.Points{Metric: name, Data: batch}); e != nil {
					t.Fatalf("%s %s: %v", name, def, e)
				}
			}
			if e = s.Flush(); e != nil {
				t.Fatal(e)
			}
			for _, width := range []int64{3600, 86400, 7 * 86400, 30 * 86400} {
				got, e := s.Fetch(ctx, name, int(clock-width), int(clock))
				if e != nil {
					t.Fatal(e)
				}
				want, e := w.Fetch(int(clock-width), int(clock))
				if e != nil {
					t.Fatal(e)
				}
				if (got == nil) != (want == nil) {
					t.Fatal("nil shape differs")
				}
				if got == nil {
					continue
				}
				if got.From != want.FromTime() || got.Until != want.UntilTime() || got.Step != want.Step() || len(got.Values) != len(want.Values()) {
					t.Fatalf("%s width%d grid differs", name, width)
				}
				for i, v := range want.Values() {
					if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
						t.Fatalf("%s %s width%d slot%d bits%x != %x", name, def, width, i, math.Float64bits(got.Values[i]), math.Float64bits(v))
					}
				}
				queries++
				slots += len(got.Values)
				got.Release()
			}
			if e = w.Close(); e != nil {
				t.Fatal(e)
			}
		}
	}
	if queries != 1680 {
		t.Fatalf("queries=%d", queries)
	}
	t.Logf("queries=%d bit-exact slots=%d real GCP3 SST backend", queries, slots)
}

func TestBackendRecoveryOwnsInputAndResponseLifetime(t *testing.T) {
	clock := int64(1794700800)
	opts := testOptions(t.TempDir(), simplePolicy())
	s, e := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
	if e != nil {
		t.Fatal(e)
	}
	input := []points.Point{{Timestamp: clock - 10, Value: math.Float64frombits(0x8000000000000000)}, {Timestamp: clock - 9, Value: math.Float64frombits(0x7ff8000000000042)}}
	if e = s.Append(&points.Points{Metric: "portable.\xff.name", Data: input}); e != nil {
		t.Fatal(e)
	}
	input[0].Value = 42
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	requireClose(t, s)
	s, e = openAt(opts, func() time.Time { return time.Unix(clock, 0) })
	if e != nil {
		t.Fatal(e)
	}
	ctx, done := daemonstore.ResponseScope(context.Background())
	got, e := s.Fetch(ctx, "portable.\xff.name", int(clock-11), int(clock-8))
	if e != nil {
		t.Fatal(e)
	}
	if math.Float64bits(got.Values[0]) != 0x8000000000000000 || math.Float64bits(got.Values[1]) != 0x7ff8000000000042 {
		t.Fatalf("lost input bits: %x %x", math.Float64bits(got.Values[0]), math.Float64bits(got.Values[1]))
	}
	if s.Stats().SnapshotPins != 1 {
		t.Fatal("response not pinned")
	}
	done()
	if s.Stats().SnapshotPins != 0 {
		t.Fatal("response not released")
	}
	if s.Stats().IngestedPoints != 2 {
		t.Fatal("replay duplicated input")
	}
	requireClose(t, s)
}

func TestBackendSIGKILL(t *testing.T) {
	if dir := os.Getenv("V2_BACKEND_CRASH_DIR"); dir != "" {
		s, e := openAt(testOptions(dir, simplePolicy()), func() time.Time { return time.Unix(1794700800, 0) })
		if e != nil {
			panic(e)
		}
		for i := 0; i < 200; i++ {
			if e = s.Append(&points.Points{Metric: "crash.metric", Data: []points.Point{{Timestamp: 1794700000 + int64(i), Value: math.Float64frombits(uint64(i) + 0x3ff0000000000000)}}}); e != nil {
				panic(e)
			}
			if i == 99 {
				if e = s.Flush(); e != nil {
					panic(e)
				}
			}
		}
		if e = s.log.Sync(); e != nil {
			panic(e)
		}
		if e = os.WriteFile(filepath.Join(dir, "ready"), []byte("ready"), 0600); e != nil {
			panic(e)
		}
		select {}
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestBackendSIGKILL$")
	cmd.Env = append(os.Environ(), "V2_BACKEND_CRASH_DIR="+dir)
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, e := os.Stat(filepath.Join(dir, "ready")); e == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("crash helper not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if e := cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	_ = cmd.Wait()
	s, e := openAt(testOptions(dir, simplePolicy()), func() time.Time { return time.Unix(1794700800, 0) })
	if e != nil {
		t.Fatal(e)
	}
	defer requireClose(t, s)
	got, e := s.Fetch(context.Background(), "crash.metric", 1794699999, 1794700200)
	if e != nil {
		t.Fatal(e)
	}
	defer got.Release()
	for i := 0; i < 200; i++ {
		if math.Float64bits(got.Values[i]) != uint64(i)+0x3ff0000000000000 {
			t.Fatalf("replay bits at %d", i)
		}
	}
	if s.Stats().IngestedPoints != 200 {
		t.Fatalf("replay count=%d", s.Stats().IngestedPoints)
	}
}

func TestBackendMixedLegacyPhysicalOverwrite(t *testing.T) {
	clock := int64(1794700800)
	legacyDir := filepath.Join(t.TempDir(), "legacy")
	db, e := store.Open(legacyDir, store.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(clock, 0) }})
	if e != nil {
		t.Fatal(e)
	}
	_, e = db.Create(context.Background(), store.MetricConfig{Name: "legacy.metric", AggregationMethod: store.Average, XFilesFactor: .5, Retentions: []store.Retention{{Step: 1, Count: 3600}}})
	if e != nil {
		t.Fatal(e)
	}
	if e = db.UpdateMany(context.Background(), "legacy.metric", []points.Point{{Timestamp: clock - 10, Value: 8}, {Timestamp: clock - 9, Value: 9}}); e != nil {
		t.Fatal(e)
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	opts := testOptions(t.TempDir(), simplePolicy())
	opts.LegacyStoreDir = legacyDir
	opts.LegacyBackend = "pebble-chunk"
	s, e := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
	if e != nil {
		t.Fatal(e)
	}
	defer requireClose(t, s)
	if e = s.Append(&points.Points{Metric: "legacy.metric", Data: []points.Point{{Timestamp: clock - 10, Value: math.Float64frombits(0x7ff8000000000042)}}}); e != nil {
		t.Fatal(e)
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	r, e := s.Fetch(context.Background(), "legacy.metric", int(clock-11), int(clock-8))
	if e != nil {
		t.Fatal(e)
	}
	defer r.Release()
	if !reflect.DeepEqual([]uint64{math.Float64bits(r.Values[0]), math.Float64bits(r.Values[1])}, []uint64{0x7ff8000000000042, math.Float64bits(9)}) {
		t.Fatal("mixed legacy overwrite differs", r.Values)
	}
}
