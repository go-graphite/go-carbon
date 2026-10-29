package main

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/go-graphite/go-carbon/carbon"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
)

// The store-merge tests run the tool against a real go-carbon App with the
// shared Pebble store in its production shape (incremental migration, exact
// encoding, continuation metrics, v6 or v7 time buckets) and its plaintext
// receiver, then check what the persister merged.

const (
	mergeMetric = "backfill.merge.metric"
	mergeStep   = 10
)

type mergeCase struct {
	timeBuckets bool // v7 layout
	outOfOrder  bool // persister folds the out-of-order sidecar on write
}

func startStoreApp(t *testing.T, c mergeCase) *carbon.App {
	t.Helper()
	root := t.TempDir()
	path := carbon.TestConfig(root)
	cfg, err := carbon.ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Common.MetricInterval = &carbon.Duration{Duration: time.Hour} // keep persister counters for the test
	cfg.Udp.Enabled, cfg.Pickle.Enabled, cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false, false, false
	cfg.Pprof.Enabled, cfg.Prometheus.Enabled, cfg.Buckyd.Enabled = false, false, false
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Whisper.StorageBackend = "pebble-chunk"
	cfg.Whisper.StoreDir = filepath.Join(root, "shared")
	cfg.Whisper.DataDir = filepath.Join(root, "whisper")
	cfg.Whisper.StoreCacheSize, cfg.Whisper.StoreMemTableSize = 1<<20, 1<<20
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreIncrementalMigration = true
	cfg.Whisper.StoreTimeBuckets = c.timeBuckets
	cfg.Whisper.FLock, cfg.Whisper.Compressed = true, true
	cfg.Whisper.StoreMigrationWorkers = 0
	cfg.Whisper.StoreMigrationBytesInFlight = 8 << 20
	cfg.Whisper.StoreMigrationControlFile = filepath.Join(root, "migration-control.json")
	if c.outOfOrder {
		cfg.Whisper.OutOfOrder = true
		cfg.Whisper.OutOfOrderCompactRate = 1000
		cfg.Whisper.OutOfOrderCompactMinPoints = 1
		cfg.Whisper.OutOfOrderCompactMaxPointAge = carbon.Duration{Duration: time.Second}
		cfg.Whisper.OutOfOrderCompactRetentionMargin = carbon.Duration{Duration: time.Second}
	}
	cfg.Carbonserver.Enabled, cfg.Carbonserver.Listen = true, "127.0.0.1:0"
	cfg.Carbonserver.TrieIndex, cfg.Carbonserver.TrigramIndex = true, false
	// The realtime index races /metrics/list/ on the trie's file count
	// (carbonserver trie.go insertMutablePath vs allMetricsMutable), which
	// the race detector reports; the scan publishes the metric instead.
	cfg.Carbonserver.ConcurrentIndex, cfg.Carbonserver.RealtimeIndex = false, 0
	cfg.Carbonserver.ScanWorkers = 1
	cfg.Carbonserver.FileListCache = filepath.Join(root, "files.flc")
	cfg.Carbonserver.FileListCacheVersion = 2
	cfg.Carbonserver.ScanFrequency = &carbon.Duration{Duration: 100 * time.Millisecond}
	cfg.Carbonserver.QueryCacheEnabled, cfg.Carbonserver.FindCacheEnabled = false, false
	if err := os.MkdirAll(cfg.Whisper.DataDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[default]\npattern = .*\nretentions = 10s:1d,1m:30d\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	lease := fmt.Sprintf(`{"version":1,"generation":1,"workers":0,"expires_at":%d}`, time.Now().Add(time.Hour).Unix())
	if err := os.WriteFile(cfg.Whisper.StoreMigrationControlFile, []byte(lease), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := toml.NewEncoder(f).Encode(cfg); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	app := carbon.New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Stop)
	return app
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// mergeValue gives every slot a distinct value with a non-trivial mantissa,
// so a changed bit is visible.
func mergeValue(ts int64) float64 { return math.Sqrt(float64(ts%100003)) * 1.000001 }

func storeSeries(t *testing.T, app *carbon.App, from, until int64) *store.Series {
	t.Helper()
	s, err := app.MetricStore.Fetch(context.Background(), mergeMetric, int(from), int(until))
	if err != nil || s == nil {
		t.Fatalf("store fetch: %v", err)
	}
	return s
}

func seriesAt(s *store.Series, ts int64) float64 {
	i := (int(ts) - s.FromTime) / s.Step
	if (int(ts)-s.FromTime)%s.Step != 0 || i < 0 || i >= len(s.Values) {
		return math.NaN()
	}
	return s.Values[i]
}

func TestStoreMergesBackfill(t *testing.T) {
	t.Parallel()
	for _, c := range []mergeCase{{false, false}, {true, false}, {true, true}, {false, true}} {
		for _, newerCached := range []bool{false, true} {
			name := fmt.Sprintf("v7=%v/fold=%v/newer-cached=%v", c.timeBuckets, c.outOfOrder, newerCached)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				checkStoreMerge(t, c, newerCached)
			})
		}
	}
}

func checkStoreMerge(t *testing.T, c mergeCase, newerCached bool) {
	app := startStoreApp(t, c)
	local := "http://" + app.Carbonserver.HTTPListener().Addr().String()
	var ingest string
	for _, r := range app.Receivers {
		if r.Name == "tcp" {
			ingest = r.Receiver.(interface{ Addr() net.Addr }).Addr().String()
		}
	}
	if ingest == "" {
		t.Fatal("TCP receiver not started")
	}
	now := time.Now().Unix()
	T := now - now%60 - 5*60
	holeFrom, holeUntil := T-20*60, T-4*60
	inHole := func(ts int64) bool { return ts >= holeFrom && ts <= holeUntil }

	// Base data [T-60m, T] minus the hole, persisted before the backfill.
	base := &points.Points{Metric: mergeMetric}
	for ts := T - 60*60; ts <= T; ts += mergeStep {
		if !inHole(ts) {
			base.Add(mergeValue(ts), ts)
		}
	}
	app.Cache.Add(base)
	waitFor(t, "base data persisted", func() bool {
		if !app.Cache.IsEmpty() {
			return false
		}
		s, err := app.MetricStore.Fetch(context.Background(), mergeMetric, int(T-61*60), int(T))
		return err == nil && s != nil && !math.IsNaN(seriesAt(s, T))
	})
	before := storeSeries(t, app, T-61*60, T)
	marker, err := os.ReadFile(filepath.Join(app.Config.Whisper.StoreDir, "CHUNKSTORE"))
	wantMarker := map[bool]string{false: "go-carbon-chunks-v6\n", true: "go-carbon-chunks-v7\n"}[c.timeBuckets]
	if err != nil || string(marker) != wantMarker {
		t.Fatalf("store format %q %v, want %q", marker, err, wantMarker)
	}
	if md, err := app.MetricStore.Metadata(context.Background(), mergeMetric); err != nil || !md.Continuation {
		t.Fatalf("metric is not a continuation (cwhisper) metric: %+v %v", md, err)
	}
	// Sanity: before the fill the boundary minute averages the five points it
	// has and the minutes inside the hole are empty, so a stale aggregate
	// would be visible below.
	coarseBefore := storeSeries(t, app, now-2*86400, T)
	var five float64
	for ts := holeUntil + mergeStep; ts < holeUntil+60; ts += mergeStep {
		five += mergeValue(ts)
	}
	if got := seriesAt(coarseBefore, holeUntil); math.Abs(got-five/5) > 1e-9*five || !math.IsNaN(seriesAt(coarseBefore, holeFrom)) {
		t.Fatalf("coarse archive before the fill: boundary %v (want %v), hole minute %v", got, five/5, seriesAt(coarseBefore, holeFrom))
	}

	newest := T
	if newerCached {
		// Newer points stay in the cache, unpersisted, while the fill arrives.
		if err := app.Cache.SetWriteoutBatching(1_000_000, time.Hour); err != nil {
			t.Fatal(err)
		}
		newer := &points.Points{Metric: mergeMetric}
		for ts := T + mergeStep; ts <= T+2*60; ts += mergeStep {
			newer.Add(mergeValue(ts), ts)
		}
		newest = T + 2*60
		app.Cache.Add(newer)
	}

	// The replica has every slot.
	replica := newFake(t)
	replica.step = mergeStep
	for ts := T - 60*60; ts <= newest; ts += mergeStep {
		replica.set(mergeMetric, ts, mergeValue(ts))
	}
	opts := func(dry bool) *options {
		return &options{
			local: local, ingest: ingest,
			replicas: urlList{replica.srv.URL},
			from:     strconv.FormatInt(T-30*60, 10), until: strconv.FormatInt(T, 10),
			lookback: 30 * time.Minute, batch: 100, workers: 1, replicaRPS: 100,
			timeout: 10 * time.Second, retries: 2, backoff: 10 * time.Millisecond, maxBack: 100 * time.Millisecond,
			flushPt: 1000, writeRetries: 3, dryRun: dry,
		}
	}
	waitFor(t, "metric indexed", func() bool {
		names, err := newClient(local, opts(true), nil).list(context.Background())
		return err == nil && len(names) == 1
	})
	r, err := run(context.Background(), opts(false), time.Unix(now, 0))
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	holePoints := (holeUntil-holeFrom)/mergeStep + 1
	if r.Metrics["missing_slots"] != holePoints || r.Metrics["fillable_points"] != holePoints || r.Write["points_sent"] != holePoints {
		t.Fatalf("backfill report %+v %+v, want %d points", r.Metrics, r.Write, holePoints)
	}

	// Reads merge the cache: the hole is closed before persistence too.
	waitFor(t, "filled points readable", func() bool {
		r, err := run(context.Background(), opts(true), time.Unix(now, 0))
		return err == nil && r.Metrics["missing_slots"] == 0
	})
	if newerCached {
		if app.Cache.IsEmpty() {
			t.Fatal("newer points were persisted before the fill")
		}
		if err := app.Cache.SetWriteoutBatching(0, 0); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "fill persisted", func() bool {
		if !app.Cache.IsEmpty() {
			return false
		}
		s, err := app.MetricStore.Fetch(context.Background(), mergeMetric, int(T-61*60), int(newest))
		return err == nil && s != nil && !math.IsNaN(seriesAt(s, holeFrom)) && !math.IsNaN(seriesAt(s, holeUntil)) && !math.IsNaN(seriesAt(s, newest))
	})

	stats := map[string]float64{}
	app.Persister.Stat(func(name string, v float64) { stats[name] += v })
	if stats["oooDiscardedPoints"] != 0 || (stats["oooCompactions"] > 0) != c.outOfOrder {
		t.Fatalf("out-of-order stats %v (fold=%v)", stats, c.outOfOrder)
	}

	// First archive: the hole holds the replica values, every other slot is
	// bit-identical to before the backfill.
	after := storeSeries(t, app, T-61*60, newest)
	if after.Step != mergeStep {
		t.Fatalf("first archive step %d", after.Step)
	}
	for ts := T - 60*60; ts <= newest; ts += mergeStep {
		got := seriesAt(after, ts)
		want := mergeValue(ts)
		if !inHole(ts) && ts <= T {
			want = seriesAt(before, ts)
		}
		if math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("slot %d (hole=%v): got %v want %v", ts-T, inHole(ts), got, want)
		}
	}

	// Lower archive: every minute is the average of its six 10s points, as
	// read (store fetch older than the first archive selects the 1m archive)
	// and as the transfer snapshot exports it.
	expect := func(minute int64) float64 {
		var sum float64
		for ts := minute; ts < minute+60; ts += mergeStep {
			sum += seriesAt(after, ts)
		}
		return sum / 6
	}
	coarse := storeSeries(t, app, now-2*86400, newest)
	if coarse.Step != 60 {
		t.Fatalf("coarse archive step %d", coarse.Step)
	}
	snap, err := app.MetricStore.Snapshot(context.Background(), mergeMetric)
	if err != nil {
		t.Fatal(err)
	}
	snapMinute := map[int64]float64{}
	for _, p := range snap.Archives[1].Points {
		snapMinute[p.Timestamp] = p.Value
	}
	var stale []string
	for minute := T - 60*60; minute+60 <= newest; minute += 60 {
		want := expect(minute)
		read := seriesAt(coarse, minute)
		exported := snapMinute[minute]
		if math.Abs(read-want) > 1e-9*math.Abs(want) || math.Abs(exported-want) > 1e-9*math.Abs(want) {
			stale = append(stale, fmt.Sprintf("minute %+dm: read %v snapshot %v want %v", (minute-T)/60, read, exported, want))
		}
	}
	if len(stale) > 0 {
		t.Errorf("lower archive disagrees with the merged points:\n%s", joinLines(stale))
	}

	// Nothing is left to fill.
	r, err = run(context.Background(), opts(true), time.Unix(now, 0))
	if err != nil || r.Metrics["missing_slots"] != 0 || r.Metrics["fillable_points"] != 0 {
		t.Fatalf("second dry run %+v %v", r.Metrics, err)
	}
}

func joinLines(s []string) string {
	out := ""
	for _, l := range s {
		out += "  " + l + "\n"
	}
	return out
}
