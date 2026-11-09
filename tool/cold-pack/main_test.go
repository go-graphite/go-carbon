package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestColdPackOfflineUnpackDowngrade(t *testing.T) {
	if dir := os.Getenv("COLD_PACK_DOWNGRADE_TEST_STORE"); dir != "" {
		// Isolate the real CLI's global flags and signal handling in a child.
		flag.CommandLine = flag.NewFlagSet("cold-pack", flag.ExitOnError)
		os.Args = []string{"cold-pack", "-store", dir, "-mode", os.Getenv("COLD_PACK_DOWNGRADE_TEST_MODE"), "-downgrade",
			"-write-window", "1s", "-bytes-per-second", "1099511627776", "-cpu-fraction", "1", "-min-free-bytes", "1"}
		if err := run(); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, mode := range []string{"unpack", "float-downgrade"} {
		t.Run(mode, func(t *testing.T) { testColdPackOfflineDowngrade(t, mode) })
	}
}

func testColdPackOfflineDowngrade(t *testing.T, mode string) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "store")
	opts := store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{},
		CacheSize: 2 << 20, MemTableSize: 1 << 20}
	db, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	db.StopBackground()
	cfg := store.MetricConfig{Name: "offline.rollback", Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}
	if _, err := db.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(-48*time.Hour).Unix() / 60 * 60
	points := make([]store.Point, 1800)
	rng := rand.New(rand.NewPCG(42, 17))
	for i := range points {
		points[i] = store.Point{Timestamp: base + int64(i*60), Value: float64(rng.IntN(16)) / 3}
	}
	if err := db.UpdateMany(ctx, cfg.Name, points); err != nil {
		t.Fatal(err)
	}
	want, err := db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", PackTails: true, FloatCodec: mode == "float-downgrade", WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40}); err != nil {
		t.Fatal(err)
	}
	if err := db.RunColdPack(ctx); err != nil {
		t.Fatal(err)
	}
	packedFormat, markerFormat := 9, 7
	if mode == "float-downgrade" {
		packedFormat, markerFormat = 11, 9
	}
	if db.TransferFormat() != packedFormat || db.ColdPackStats().Blocks == 0 {
		t.Fatal("fixture did not create packed v9 blocks")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	childCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestColdPackOfflineUnpackDowngrade$")
	cmd.Env = append(os.Environ(), "COLD_PACK_DOWNGRADE_TEST_STORE="+dir, "COLD_PACK_DOWNGRADE_TEST_MODE="+mode)
	output, err := cmd.CombinedOutput()
	t.Logf("child cold-pack output (including stats):\n%s", output)
	if err != nil {
		t.Fatalf("unpack -downgrade: %v\n%s", err, output)
	}
	marker, err := os.ReadFile(filepath.Join(dir, "CHUNKSTORE"))
	if err != nil || string(marker) != fmt.Sprintf("go-carbon-chunks-v%d\n", markerFormat) {
		t.Fatalf("downgrade marker: %q %v", marker, err)
	}
	db, err = store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	db.StopBackground()
	got, err := db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal("rollback changed values", err)
	}
}

// Fail at the codec boundary so the real store owns pass completion, checkpoint
// state and the downgrade fence throughout these retry tests.
type retryColdCodec struct {
	*whisperio.ContinuationCodec
	failures int
	err      error
	cancel   context.CancelFunc
	revise   func() error
	conflict func(context.Context) error
}

func (c *retryColdCodec) Repack(ctx context.Context, io store.ContinuationIO, r store.ColdPackRequest) (store.RepackStats, uint64, bool, error) {
	if c.cancel != nil {
		c.cancel()
		return store.RepackStats{}, 0, false, ctx.Err()
	}
	if c.failures > 0 {
		if c.revise != nil {
			c.failures--
			return store.RepackStats{Blocks: 1}, 0, true, c.revise()
		}
		c.failures--
		if c.conflict != nil {
			return store.RepackStats{Blocks: 1}, 0, true, c.conflict(ctx)
		}
		return store.RepackStats{Skipped: 1}, 0, true, c.err
	}
	return c.ContinuationCodec.Repack(ctx, io, r)
}

func TestColdPackDowngradeRetries(t *testing.T) {
	for _, mode := range []string{"unpack", "float-downgrade"} {
		t.Run(mode, func(t *testing.T) { testColdPackDowngradeRetries(t, mode) })
	}
}

func testColdPackDowngradeRetries(t *testing.T, mode string) {
	for _, tc := range []struct {
		name           string
		failures       int
		metricErr      bool
		conflict       bool
		metricConflict bool
		downgrade      bool
		cancel         bool
		passes         uint64
		format         int
	}{
		{name: "revision-conflict", failures: 1, conflict: true, downgrade: true, passes: 2, format: 7},
		{name: "two-conflicted-passes", failures: 2, metricConflict: true, downgrade: true, passes: 3, format: 7},
		{name: "persistent-conflict", failures: 3, metricConflict: true, downgrade: true, passes: 3, format: 9},
		{name: "two-skipped-passes", failures: 2, downgrade: true, passes: 3, format: 7},
		{name: "failed-metric", failures: 1, metricErr: true, downgrade: true, passes: 2, format: 7},
		{name: "persistent-skip", failures: 3, downgrade: true, passes: 3, format: 9},
		{name: "persistent-error", failures: 3, metricErr: true, downgrade: true, passes: 3, format: 9},
		{name: "single-pass-without-downgrade", failures: 1, metricErr: true, passes: 1, format: 9},
		{name: "cancelled", downgrade: true, cancel: true, format: 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			codec := &retryColdCodec{ContinuationCodec: &whisperio.ContinuationCodec{}}
			if tc.metricErr {
				codec.err = errors.New("injected metric failure")
			}

			db, err := store.Open(filepath.Join(t.TempDir(), "store"), store.Options{
				ExactBlocks: true, TimeBuckets: true, Continuation: codec, CacheSize: 2 << 20, MemTableSize: 1 << 20,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.StopBackground()
			if _, err := db.Create(ctx, store.MetricConfig{Name: "retry", Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}); err != nil {
				t.Fatal(err)
			}
			wantFormat, packedFormat := tc.format, 9
			if mode == "float-downgrade" {
				wantFormat += 2
				packedFormat = 11
			}
			points := make([]store.Point, 1800)
			rng := rand.New(rand.NewPCG(42, 17))
			base := time.Now().Add(-48*time.Hour).Unix() / 60 * 60
			for i := range points {
				points[i] = store.Point{Timestamp: base + int64(i*60), Value: float64(rng.IntN(16)) / 3}
			}
			if err := db.UpdateMany(ctx, "retry", points); err != nil {
				t.Fatal(err)
			}
			if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", FloatCodec: mode == "float-downgrade",
				WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40, MinFreeBytes: 1}); err != nil {
				t.Fatal(err)
			}
			prepareRetryFixture(t, ctx, db, "exact", packedFormat)
			if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: mode, CPUFraction: 1, BytesPerSecond: 1 << 40, MinFreeBytes: 1}); err != nil {
				t.Fatal(err)
			}
			// Use the valid state after physical unpacking but before lowering
			// the reader fence. The retry subject then exercises real Store pass,
			// checkpoint and fence state without depending on an uncontended
			// physical publication finishing within2ms on a shared CI runner.
			// TestColdPackOfflineUnpackDowngrade covers the actual rewrite and
			// exact snapshots through the CLI, including close/reopen.
			prepareRetryFixture(t, ctx, db, mode, packedFormat)
			// Inject dirty sweeps only after constructing the packed fixture.
			codec.failures = tc.failures
			if tc.conflict {
				codec.revise = func() error {
					return db.UpdateMany(ctx, "retry", []store.Point{{Timestamp: time.Now().Unix()/60*60 - 60, Value: 777}})
				}
			}
			if tc.metricConflict {
				codec.conflict = func(ctx context.Context) error {
					return db.UpdateMany(ctx, "retry", []store.Point{{Timestamp: time.Now().Unix() - 60, Value: float64(codec.failures)}})
				}
			}
			if tc.cancel {
				codec.cancel = cancel
			}
			if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: mode, CPUFraction: 1, BytesPerSecond: 1 << 40, MinFreeBytes: 1}); err != nil {
				t.Fatal(err)
			}
			before := db.ColdPackStats()
			err = runColdPack(ctx, db, tc.downgrade)
			stats := db.ColdPackStats()
			if (err == nil) != (tc.format == 7) || db.TransferFormat() != wantFormat || stats.Passes-before.Passes != tc.passes {
				t.Fatalf("run: %v; format=%d; stats=%+v", err, db.TransferFormat(), stats)
			}
			if tc.downgrade && tc.format == 9 && !tc.cancel && !tc.metricErr && !errors.Is(err, store.ErrColdPackIncomplete) {
				t.Fatalf("lost downgrade refusal: %v", err)
			}
			if tc.conflict {
				if stats.Conflicts <= before.Conflicts || !strings.Contains(stats.LastIncomplete, "metric changed during planning") {
					t.Fatalf("lost conflict reason: %+v", stats)
				}
				if refusal := coldPackFailure(store.ErrColdPackIncomplete, 3, stats); !strings.Contains(refusal.Error(), "metric changed during planning") {
					t.Fatalf("refusal omitted conflict reason: %v", refusal)
				}
			}
			if tc.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			incomplete := "skipped 1 ranges"
			if tc.conflict {
				incomplete = "metric changed during planning"
			}
			if tc.metricConflict {
				incomplete = "metric changed during planning"
				if stats.Conflicts-before.Conflicts != uint64(tc.failures) {
					t.Errorf("missing conflicts: %+v", stats)
				}
			}
			if tc.failures > 0 && !tc.metricErr && !strings.Contains(stats.LastIncomplete, incomplete) {
				t.Errorf("missing incomplete reason: %+v", stats)
			}
			if (stats.Error != "") != tc.metricErr {
				t.Errorf("failure field includes a benign incomplete pass: %+v", stats)
			}
			if err != nil {
				if tc.metricErr && !strings.Contains(err.Error(), "injected metric failure") || !tc.metricErr && !tc.cancel && !strings.Contains(err.Error(), incomplete) {
					t.Errorf("missing failure/incomplete reason: %v", err)
				}
				for _, field := range []string{"errors=", "conflicts=", "skipped=", "last_error=", "last_incomplete=", "cursor=", "block_cursor="} {
					if !strings.Contains(err.Error(), field) {
						t.Fatalf("missing diagnostic %s: %v", field, err)
					}
				}
			}
		})
	}
}

func prepareRetryFixture(t *testing.T, ctx context.Context, db *store.Store, mode string, format int) {
	t.Helper()
	for attempt := 0; attempt < 3; attempt++ {
		before := db.ColdPackStats()
		if err := db.RunColdPack(ctx); err != nil {
			t.Fatalf("prepare %s fixture: %v", mode, err)
		}
		after := db.ColdPackStats()
		if db.TransferFormat() == format && after.Conflicts == before.Conflicts && after.Skipped == before.Skipped {
			return
		}
		if after.Errors != before.Errors || !strings.Contains(after.LastIncomplete, "publication validation exceeded 2ms") {
			t.Fatalf("unexpected %s fixture failure: %+v", mode, after)
		}
	}
	t.Fatalf("%s fixture never reached a clean physical pass: %+v", mode, db.ColdPackStats())
}

func TestColdPackFailureReportsBothReasons(t *testing.T) {
	stats := store.ColdPackStats{RepackStats: store.RepackStats{Skipped: 3}, Errors: 1, Conflicts: 2,
		Error: "broken manifest", LastIncomplete: "metric stripe busy"}
	err := coldPackFailure(store.ErrColdPackIncomplete, 3, stats)
	if !errors.Is(err, store.ErrColdPackIncomplete) {
		t.Fatalf("lost downgrade refusal: %v", err)
	}
	for _, want := range []string{`last_error="broken manifest"`, `last_incomplete="metric stripe busy"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s: %v", want, err)
		}
	}
}
