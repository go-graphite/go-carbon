package whisperio

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

const coldBase = int64(1700000040)

func coldOptions(codec store.ContinuationCodec) store.Options {
	return store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec, Now: func() time.Time { return time.Unix(coldBase+2000*60, 0) }, CacheSize: 2 << 20, MemTableSize: 1 << 20, MetadataCacheSize: 1 << 20}
}
func coldFixture(t *testing.T, db *store.Store) store.Snapshot {
	t.Helper()
	ctx := context.Background()
	cfg := store.MetricConfig{Name: "cold.metric", Retentions: []store.Retention{{Step: 60, Count: 10080}, {Step: 3600, Count: 8760}}, AggregationMethod: store.Average}
	if _, err := db.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	points := make([]store.Point, 1800)
	for i := range points {
		points[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: 1000 + math.Sin(float64(i))*.0123456789012345}
	}
	if err := db.UpdateMany(ctx, cfg.Name, points); err != nil {
		t.Fatal(err)
	}
	want, err := db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	return want
}
func coldSweep(t *testing.T, db *store.Store, mode string) {
	t.Helper()
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: mode, WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40}); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 1000; n++ {
		done, err := db.ColdPackStep(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return
		}
	}
	t.Fatal("bounded sweep failed to finish")
}
func coldEqual(t *testing.T, want, got store.Snapshot, decimal bool) {
	t.Helper()
	if !decimal {
		if err := store.EqualSnapshots(want, got); err != nil {
			t.Fatal(err)
		}
		return
	}
	if len(want.Archives) != len(got.Archives) {
		t.Fatal("archives differ")
	}
	for a, w := range want.Archives {
		g := got.Archives[a]
		if w.Retention != g.Retention || len(w.Points) != len(g.Points) {
			t.Fatal("archive layout differs")
		}
		for i, p := range w.Points {
			q := g.Points[i]
			if p.Timestamp != q.Timestamp || math.Abs((q.Value-p.Value)/p.Value) > 5.1e-12 {
				t.Fatalf("oracle mismatch archive=%d point=%d want=%+v got=%+v", a, i, p, q)
			}
		}
	}
}

func TestColdPackOracleCorrectionsTransferDowngrade(t *testing.T) {
	for _, mode := range []string{"exact", "decimal"} {
		for _, direct := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/sidecar", true: "/direct"}[direct], func(t *testing.T) {
				ctx := context.Background()
				options := coldOptions(&ContinuationCodec{DirectLateWrites: direct})
				dir := t.TempDir()
				db, err := store.Open(dir, options)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if db != nil {
						db.Close()
					}
				}()
				db.StopBackground()
				want := coldFixture(t, db)
				// Default-off and hot history are preserved.
				if db.TransferFormat() != 7 {
					t.Fatal("default changed format")
				}
				if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: mode, WriteWindow: 7 * 24 * time.Hour}); err != nil {
					t.Fatal(err)
				}
				for n := 0; n < 100; n++ {
					done, e := db.ColdPackStep(ctx)
					if e != nil {
						t.Fatal(e)
					}
					if done {
						break
					}
				}
				if db.ColdPackStats().Blocks != 0 {
					t.Fatal("hot blocks packed")
				}
				// Change mode to reset the cursor before the explicit cold sweep.
				if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "off"}); err != nil {
					t.Fatal(err)
				}
				rangeDeletes := db.Stats().ContinuationRangeDeletes
				coldSweep(t, db, mode)
				if db.Stats().ContinuationRangeDeletes != rangeDeletes {
					t.Fatal("maintenance introduced range tombstones")
				}
				if db.ColdPackStats().Blocks == 0 {
					t.Fatal("no blocks sealed")
				}
				got, err := db.Snapshot(ctx, want.Metadata.Name)
				if err != nil {
					t.Fatal(err)
				}
				coldEqual(t, want, got, mode == "decimal")
				packed := got
				records, err := db.ExportTransfer(ctx, []string{want.Metadata.Name})
				if err != nil {
					t.Fatal(err)
				}
				target, err := store.Open(filepath.Join(t.TempDir(), "target"), options)
				if err != nil {
					t.Fatal(err)
				}
				defer target.Close()
				target.StopBackground()
				if _, err := target.ImportTransfer(ctx, records); err == nil {
					t.Fatal("v7 accepted sealed import")
				}
				if err := target.ConfigureColdPack(store.ColdPackOptions{Mode: "exact"}); err != nil {
					t.Fatal(err)
				}
				if _, err := target.ImportTransfer(ctx, records); err != nil {
					t.Fatal(err)
				}
				copy, err := target.Snapshot(ctx, want.Metadata.Name)
				if err != nil {
					t.Fatal(err)
				}
				coldEqual(t, packed, copy, false)
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db = nil
				db, err = store.Open(dir, options)
				if err != nil {
					t.Fatal(err)
				}
				db.StopBackground()
				got, err = db.Snapshot(ctx, want.Metadata.Name)
				if err != nil {
					t.Fatal(err)
				}
				coldEqual(t, packed, got, false)
				// Independent exact ordinary writer sees the same corrections and rollups.
				control, err := store.Open(filepath.Join(t.TempDir(), "control"), options)
				if err != nil {
					t.Fatal(err)
				}
				defer control.Close()
				control.StopBackground()
				coldFixture(t, control)
				updates := []store.Point{{Timestamp: coldBase + 300*60, Value: math.Pi}, {Timestamp: coldBase + 1800*60, Value: 3}}
				for _, s := range []*store.Store{db, control} {
					if err := s.UpdateMany(ctx, want.Metadata.Name, updates); err != nil {
						t.Fatal(err)
					}
				}
				want, err = control.Snapshot(ctx, want.Metadata.Name)
				if err != nil {
					t.Fatal(err)
				}
				got, err = db.Snapshot(ctx, want.Metadata.Name)
				if err != nil {
					t.Fatal(err)
				}
				coldEqual(t, want, got, mode == "decimal")
				if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "unpack"}); err != nil {
					t.Fatal(err)
				}
				if err := db.DowngradeColdPack(ctx); err == nil {
					t.Fatal("downgrade accepted before unpack sweep")
				}
				coldSweep(t, db, "unpack")
				if err := db.DowngradeColdPack(ctx); err != nil {
					t.Fatal(err)
				}
				if db.TransferFormat() != 7 {
					t.Fatal("downgrade fence not cleared")
				}
				marker, err := os.ReadFile(filepath.Join(dir, "CHUNKSTORE"))
				if err != nil || string(marker) != "go-carbon-chunks-v7\n" {
					t.Fatalf("marker %q %v", marker, err)
				}
				got, err = db.Snapshot(ctx, want.Metadata.Name)
				if err != nil {
					t.Fatal(err)
				}
				coldEqual(t, want, got, mode == "decimal")
			})
		}
	}
}

type coldBarrierCodec struct {
	ContinuationCodec
	planned chan struct{}
	resume  chan struct{}
	fail    bool
	crash   bool
	once    sync.Once
}

func (c *coldBarrierCodec) Repack(ctx context.Context, io store.ContinuationIO, r store.ColdPackRequest) (store.RepackStats, uint64, bool, error) {
	st, next, done, err := c.ContinuationCodec.Repack(ctx, io, r)
	if st.Blocks > 0 && err == nil {
		c.once.Do(func() {
			if c.planned != nil {
				close(c.planned)
				<-c.resume
			}
		})
		if c.crash {
			os.Exit(74)
		}
		if c.fail {
			return st, next, done, errors.New("injected plan failure")
		}
	}
	return st, next, done, err
}
func TestColdPackConcurrentRevisionFence(t *testing.T) {
	codec := &coldBarrierCodec{planned: make(chan struct{}), resume: make(chan struct{})}
	db, err := store.Open(t.TempDir(), coldOptions(codec))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	coldFixture(t, db)
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", WriteWindow: time.Second}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, e := db.ColdPackStep(context.Background()); result <- e }()
	select {
	case <-codec.planned:
	case <-time.After(5 * time.Second):
		t.Fatal("planner blocked")
	}
	// Reads and ingestion complete while repack's replacement is held outside lock.
	if _, err := db.Snapshot(context.Background(), "cold.metric"); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(context.Background(), "cold.metric", []store.Point{{Timestamp: coldBase + 400*60, Value: 7}}); err != nil {
		t.Fatal(err)
	}
	want, err := db.Snapshot(context.Background(), "cold.metric")
	if err != nil {
		t.Fatal(err)
	}
	close(codec.resume)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if db.ColdPackStats().Conflicts != 1 {
		t.Fatal("stale plan was not fenced")
	}
	got, err := db.Snapshot(context.Background(), "cold.metric")
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
	coldSweep(t, db, "exact")
	got, err = db.Snapshot(context.Background(), "cold.metric")
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
}
func TestColdPackPlanFailureIsAtomic(t *testing.T) {
	codec := &coldBarrierCodec{fail: true}
	db, err := store.Open(t.TempDir(), coldOptions(codec))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	want := coldFixture(t, db)
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", WriteWindow: time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ColdPackStep(context.Background()); err == nil {
		t.Fatal("failure hidden")
	}
	if st := db.ColdPackStats(); st.Errors != 1 || st.Error == "" {
		t.Fatal("failure metric absent")
	}
	got, err := db.Snapshot(context.Background(), "cold.metric")
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
}
func TestColdPackCrashRestart(t *testing.T) {
	if phase := os.Getenv("GO_CARBON_COLD_CRASH"); phase != "" {
		codec := &coldBarrierCodec{crash: phase == "prepared"}
		db, err := store.Open(os.Getenv("GO_CARBON_COLD_DIR"), coldOptions(codec))
		if err != nil {
			t.Fatal(err)
		}
		db.StopBackground()
		if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", WriteWindow: time.Second}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ColdPackStep(context.Background()); err != nil {
			t.Fatal(err)
		}
		os.Exit(74)
	}
	for _, phase := range []string{"prepared", "published"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			opts := coldOptions(&ContinuationCodec{})
			db, err := store.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			db.StopBackground()
			want := coldFixture(t, db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestColdPackCrashRestart$")
			cmd.Env = append(os.Environ(), "GO_CARBON_COLD_CRASH="+phase, "GO_CARBON_COLD_DIR="+dir)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 74 {
				t.Fatalf("child failed: %v %s", err, out)
			}
			db, err = store.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.StopBackground()
			got, err := db.Snapshot(context.Background(), "cold.metric")
			if err != nil {
				t.Fatal(err)
			}
			coldEqual(t, want, got, false)
			coldSweep(t, db, "exact")
			got, err = db.Snapshot(context.Background(), "cold.metric")
			if err != nil {
				t.Fatal(err)
			}
			coldEqual(t, want, got, false)
		})
	}
}

func TestColdPackConcurrentIngestionAndReads(t *testing.T) {
	ctx := context.Background()
	opts := coldOptions(&ContinuationCodec{DirectLateWrites: true})
	db, err := store.Open(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	coldFixture(t, db)
	control, err := store.Open(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	control.StopBackground()
	coldFixture(t, control)
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", WriteWindow: time.Second}); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 3)
	go func() {
		for i := 0; i < 48; i++ {
			p := []store.Point{{Timestamp: coldBase + int64((i*31%1800)*60), Value: float64(i)}, {Timestamp: coldBase + int64((1800+i)*60), Value: float64(i + 1)}}
			for _, s := range []*store.Store{db, control} {
				if e := s.UpdateMany(ctx, "cold.metric", p); e != nil {
					results <- e
					return
				}
			}
		}
		results <- nil
	}()
	go func() {
		for i := 0; i < 96; i++ {
			if _, e := db.Snapshot(ctx, "cold.metric"); e != nil {
				results <- e
				return
			}
		}
		results <- nil
	}()
	go func() {
		for i := 0; i < 96; i++ {
			if _, e := db.ColdPackStep(ctx); e != nil {
				results <- e
				return
			}
		}
		results <- nil
	}()
	for i := 0; i < 3; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	want, err := control.Snapshot(ctx, "cold.metric")
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Snapshot(ctx, "cold.metric")
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
}

func TestColdPackLargeNativeRange(t *testing.T) {
	ctx := context.Background()
	opts := coldOptions(&ContinuationCodec{})
	opts.Now = func() time.Time { return time.Unix(coldBase+30000, 0) }
	db, err := store.Open(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	cfg := store.MetricConfig{Name: "cold.constant", Retentions: []store.Retention{{Step: 1, Count: 86400}}, AggregationMethod: store.Sum}
	if _, err := db.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	points := make([]store.Point, 24000)
	for i := range points {
		points[i] = store.Point{Timestamp: coldBase + int64(i), Value: 1}
	}
	if err := db.UpdateMany(ctx, cfg.Name, points); err != nil {
		t.Fatal(err)
	}
	want, err := db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	coldSweep(t, db, "exact")
	st := db.ColdPackStats()
	if st.Points < 20000 || st.Skipped != 0 {
		t.Fatalf("dense native range skipped: %+v", st)
	}
	got, err := db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
	coldSweep(t, db, "unpack")
	if err := db.DowngradeColdPack(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
}
