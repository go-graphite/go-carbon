package v2backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

func TestSealPreparationAbortPreservesPublishedState(t *testing.T) {
	clock := int64(1794700800)
	policy := daemonstore.Metric{Archives: []daemonstore.Archive{{Step: 1, Points: 60}, {Step: 10, Points: 60}}, AggregationMethod: "average", XFilesFactor: .5}
	s, err := openAt(testOptions(t.TempDir(), policy), func() time.Time { return time.Unix(clock, 0) })
	if err != nil {
		t.Fatal(err)
	}
	defer requireClose(t, s)
	if err = s.Append(&points.Points{Metric: "test.value", Data: []points.Point{{Timestamp: clock - 1, Value: 7}}}); err != nil {
		t.Fatal(err)
	}
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	o := s.owners[0]
	base, meta, metric := o.readBase.Load(), o.meta, o.metrics["test.value"]
	slots := make([]map[int64]uint64, len(metric.slots))
	for i, ring := range metric.slots {
		slots[i] = make(map[int64]uint64)
		for slot, p := range ring {
			slots[i][slot] = p.Bits
		}
	}
	f, err := s.makeFrame("test.value", []points.Point{{Timestamp: clock - 1, Value: math.Float64frombits(0x8000000000000000)}}, clock)
	if err != nil {
		t.Fatal(err)
	}
	f.cursor, f.order = meta.Cursor+1, meta.Order+1
	tx, err := o.prepareSeal([]frame{f})
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.files) < 3 {
		t.Fatal("missing portable data/catalog/checkpoint files")
	}
	for i, rows := range tx.files {
		for j := 1; j < len(rows); j++ {
			if bytes.Compare(rows[j-1].key, rows[j].key) >= 0 {
				t.Fatal("unordered seal file")
			}
		}
		if i > 0 && bytes.Compare(tx.files[i-1][len(tx.files[i-1])-1].key, rows[0].key) >= 0 {
			t.Fatal("overlapping seal files")
		}
	}
	if _, err = o.prepareSeal([]frame{f}); err == nil {
		t.Fatal("allowed two unresolved plans to share scratch")
	}
	tx.abort()
	tx.commit() // An aborted transaction can never advance the receipt.
	tx.abort()
	if o.sealPending || o.readBase.Load() != base || o.metrics["test.value"] != metric || !reflect.DeepEqual(o.meta, meta) {
		t.Fatal("aborted seal changed published state")
	}
	for i, ring := range metric.slots {
		if len(ring) != len(slots[i]) {
			t.Fatal("aborted seal changed reducer slots")
		}
		for slot, p := range ring {
			if p.Bits != slots[i][slot] {
				t.Fatal("aborted seal changed reducer bits")
			}
		}
	}
	got, err := s.Fetch(context.Background(), "test.value", int(clock-2), int(clock+1))
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()
	if got.Values[0] != 7 {
		t.Fatalf("aborted seal leaked into reads: %v", got.Values)
	}
	// Errors during preparation also return the owner's scratch reservation.
	f.cursor = meta.Cursor
	if _, err = o.prepareSeal([]frame{f}); err == nil || o.sealPending {
		t.Fatal("failed plan retained scratch", err)
	}
}

func TestCoreBackendOracle3360(t *testing.T) {
	t.Run("sealed", func(t *testing.T) { testPortableBackendOracle1680(t, true, "core") })
	t.Run("live", func(t *testing.T) { testPortableBackendOracle1680(t, false, "core") })
}
func TestCoreCatalogBackendSIGKILL(t *testing.T) {
	t.Setenv("V2_BACKEND_CRASH_CODEC", "pdc4")
	t.Setenv("V2_BACKEND_CRASH_ENGINE", "core")
	TestBackendSIGKILL(t)
}

func TestCoreBackendSIGKILL(t *testing.T) {
	t.Setenv("V2_BACKEND_CRASH_ENGINE", "core")
	TestBackendSIGKILL(t)
}

func TestCoreDecoupledGeometry(t *testing.T) {
	clock := int64(1794700800)
	opts := testOptions(t.TempDir(), simplePolicy())
	opts.WriteEngine, opts.Owners, opts.Shards = "core", 7, 3
	s, err := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
	if err != nil {
		t.Fatal(err)
	}
	const names = 128
	for round := range 3 {
		var ns [][]byte
		var ps []points.Point
		for i := range names {
			ns = append(ns, []byte(fmt.Sprintf("geometry.series%d", i)))
			ps = append(ps, points.Point{Timestamp: clock - 10 + int64(round), Value: float64(i+round) + .125})
		}
		if err = s.AppendBatch(ns, ps); err != nil {
			t.Fatal(err)
		}
		if err = s.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	requireClose(t, s)
	s, err = openAt(opts, func() time.Time { return time.Unix(clock, 0) })
	if err != nil {
		t.Fatal(err)
	}
	defer requireClose(t, s)
	if s.Stats().IngestedPoints != 3*names {
		t.Fatal("replayed committed points", s.Stats())
	}
	for i := range names {
		r, err := s.Fetch(context.Background(), fmt.Sprintf("geometry.series%d", i), int(clock-11), int(clock-8))
		if err != nil {
			t.Fatal(err)
		}
		for round, v := range r.Values {
			if math.Float64bits(v) != math.Float64bits(float64(i+round)+.125) {
				t.Fatalf("series%d round%d got%v", i, round, r.Values)
			}
		}
		r.Release()
	}
}

func TestCorePortableRoutingAndRestartFence(t *testing.T) {
	clock := int64(1794700800)
	opts := testOptions(t.TempDir(), simplePolicy())
	opts.WriteEngine, opts.Owners, opts.Shards = "core", 3, 3
	s, err := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
	if err != nil {
		t.Fatal(err)
	}
	names := [][]byte{[]byte("one.metric"), []byte("two.metric"), []byte("three.metric"), []byte("one.metric")}
	bits := []uint64{0x3ff0000000000000, 0x8000000000000000, 0x7ff8000000000042, 1}
	data := make([]points.Point, len(bits))
	for i, b := range bits {
		data[i] = points.Point{Timestamp: clock - 10 + int64(i), Value: math.Float64frombits(b)}
	}
	if err = s.AppendBatch(names, data); err != nil {
		t.Fatal(err)
	}
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, db := range s.dbs {
		it, err := db.NewIter(nil)
		if err != nil {
			t.Fatal(err)
		}
		for it.First(); it.Valid(); it.Next() {
			if it.Key()[0] < 3 {
				t.Fatal("numeric raw data escaped the replacement planner")
			}
		}
		if err = it.Close(); err != nil {
			t.Fatal(err)
		}
	}
	requireClose(t, s)
	wrong := opts
	wrong.WriteEngine = "standalone"
	if other, err := openAt(wrong, time.Now); err == nil {
		_ = other.Close()
		t.Fatal("silently opened a core directory with the standalone writer")
	}
	marker := filepath.Join(opts.DataDir, "V2FORMAT")
	current, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, bytes.Replace(current, []byte("core-v2"), []byte("core-v1"), 1), 0600); err != nil {
		t.Fatal(err)
	}
	if other, err := openAt(opts, time.Now); err == nil {
		_ = other.Close()
		t.Fatal("accepted an older core format without migration")
	}
	if err := os.WriteFile(marker, current, 0600); err != nil {
		t.Fatal(err)
	}
	s, err = openAt(opts, func() time.Time { return time.Unix(clock, 0) })
	if err != nil {
		t.Fatal(err)
	}
	defer requireClose(t, s)
	for i, name := range names {
		got, err := s.Fetch(context.Background(), string(name), int(data[i].Timestamp-1), int(data[i].Timestamp))
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Values) != 1 || math.Float64bits(got.Values[0]) != bits[i] {
			t.Fatalf("portable restart changed %q: %v", name, got.Values)
		}
		got.Release()
	}
	if s.Stats().IngestedPoints != uint64(len(data)) {
		t.Fatal("replayed an already committed envelope")
	}
}

// TestCoreFixtureThroughput120 is the preidentified engine control for the TCP
// fixture: the same cardinality, per-name value bits and advancing wall clock.
// It excludes TCP parsing, journalling and the portable replacement planner;
// those differences must accompany any reported daemon/core ratio.
func TestCoreFixtureThroughput120(t *testing.T) {
	if os.Getenv("V2_CORE_FIXTURE_BENCH") != "1" {
		t.Skip("native opt-in throughput control")
	}
	readInt := func(name string, fallback int) int {
		if value := os.Getenv(name); value != "" {
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 {
				t.Fatalf("invalid %s", name)
			}
			return n
		}
		return fallback
	}
	runtime.GOMAXPROCS(readInt("V2_BACKEND_CPUS", 96))
	producers, dbs := readInt("V2_CORE_PRODUCERS", 48), readInt("V2_BACKEND_DBS", 12)
	cardinality := readInt("V2_BACKEND_CARDINALITY", 1048576)
	duration := 120 * time.Second
	if value := os.Getenv("V2_BACKEND_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		if err != nil || duration <= 0 {
			t.Fatal("invalid duration", err)
		}
	}
	e, err := v2core.Open(t.TempDir(), v2core.Config{JournalIdentity: "matched-name-value-clock-fixture-v1", Shards: dbs, Producers: producers, Owners: dbs, MaxRows: 2 << 20, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1"}, v2core.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = e.Close()
		}
	}()
	var profile *os.File
	if dir := os.Getenv("V2_BACKEND_PROFILES"); dir != "" {
		runtime.SetMutexProfileFraction(10)
		runtime.SetBlockProfileRate(1000000)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		profile, err = os.Create(filepath.Join(dir, "cpu.pprof"))
		if err != nil {
			t.Fatal(err)
		}
		if err = pprof.StartCPUProfile(profile); err != nil {
			t.Fatal(err)
		}
		defer func() { pprof.StopCPUProfile(); _ = profile.Close() }()
	}
	var u0, u1 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u0)
	start := time.Now()
	baseline := v2core.Sample(e, "baseline", start, 0)
	deadline := start.Add(duration)
	var offered atomic.Uint64
	var wg sync.WaitGroup
	for producer := range producers {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
			rows := make([]v2core.Row, 32768)
			next, cursor := producer, uint64(0)
			for time.Now().Before(deadline) {
				stamp := time.Now().Unix() - 1
				for i := range rows {
					id := next
					rows[i] = v2core.Row{ID: uint64(id), Time: stamp, Bits: math.Float64bits(float64(id%100003) + .125), Seq: cursor + uint64(i) + 1}
					next += producers
					if next >= cardinality {
						next = producer
					}
				}
				cursor += uint64(len(rows))
				if err := e.Append(producer, rows, cursor, false); err != nil {
					t.Error(err)
					return
				}
				offered.Add(uint64(len(rows)))
			}
			if cursor > 0 {
				if err := e.Append(producer, nil, cursor, true); err != nil {
					t.Error(err)
				}
			}
		}(producer)
	}
	wg.Wait()
	final := v2core.Sample(e, "matched_core_final", start, offered.Load())
	final["baseline"], final["cardinality"], final["producers"] = baseline, cardinality, producers
	if got := final["committed"].(uint64); got != offered.Load() {
		t.Fatalf("core drain: committed=%d offered=%d", got, offered.Load())
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u1)
	seconds := func(x syscall.Timeval) float64 { return float64(x.Sec) + float64(x.Usec)/1e6 }
	elapsed := time.Since(start).Seconds()
	cpu := seconds(u1.Utime) + seconds(u1.Stime) - seconds(u0.Utime) - seconds(u0.Stime)
	final["complete_seconds"], final["complete_points_per_second"] = elapsed, float64(offered.Load())/elapsed
	final["complete_cpu_ns_per_point"], final["maxrss_native_units"] = cpu*1e9/float64(offered.Load()), u1.Maxrss
	if err := json.NewEncoder(os.Stdout).Encode(final); err != nil {
		t.Fatal(err)
	}
}

func TestCoreCloseSealsAcceptedSuffix(t *testing.T) {
	opts := testOptions(t.TempDir(), simplePolicy())
	opts.WriteEngine = "core"
	s, err := openAt(opts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(&points.Points{Metric: "close.suffix", Data: []points.Point{{Timestamp: time.Now().Unix() - 1, Value: 7}}}); err != nil {
		t.Fatal(err)
	}
	requireClose(t, s) // Closing admission must not cancel its own durable flush.
	s, err = openAt(opts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer requireClose(t, s)
	if s.Stats().IngestedPoints != 1 {
		t.Fatal("lost or duplicated accepted suffix")
	}
}
