package main

import (
	"context"
	"errors"
	"flag"
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
		os.Args = []string{"cold-pack", "-store", dir, "-mode", "unpack", "-downgrade",
			"-write-window", "1s", "-bytes-per-second", "1099511627776", "-cpu-fraction", "1", "-min-free-bytes", "1"}
		if err := run(); err != nil {
			t.Fatal(err)
		}
		return
	}
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
	for i := range points {
		points[i] = store.Point{Timestamp: base + int64(i*60), Value: float64(i % 17)}
	}
	if err := db.UpdateMany(ctx, cfg.Name, points); err != nil {
		t.Fatal(err)
	}
	want, err := db.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40}); err != nil {
		t.Fatal(err)
	}
	if err := db.RunColdPack(ctx); err != nil {
		t.Fatal(err)
	}
	if db.TransferFormat() != 9 || db.ColdPackStats().Blocks == 0 {
		t.Fatal("fixture did not create packed v9 blocks")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	childCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestColdPackOfflineUnpackDowngrade$")
	cmd.Env = append(os.Environ(), "COLD_PACK_DOWNGRADE_TEST_STORE="+dir)
	output, err := cmd.CombinedOutput()
	t.Logf("child cold-pack output (including stats):\n%s", output)
	if err != nil {
		t.Fatalf("unpack -downgrade: %v\n%s", err, output)
	}
	marker, err := os.ReadFile(filepath.Join(dir, "CHUNKSTORE"))
	if err != nil || string(marker) != "go-carbon-chunks-v7\n" {
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
}

func (c *retryColdCodec) Repack(ctx context.Context, io store.ContinuationIO, r store.ColdPackRequest) (store.RepackStats, uint64, bool, error) {
	if c.cancel != nil {
		c.cancel()
		return store.RepackStats{}, 0, false, ctx.Err()
	}
	if c.failures > 0 {
		c.failures--
		return store.RepackStats{Skipped: 1}, 0, true, c.err
	}
	return c.ContinuationCodec.Repack(ctx, io, r)
}

func TestColdPackDowngradeRetries(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failures  int
		metricErr bool
		downgrade bool
		cancel    bool
		passes    uint64
		format    int
	}{
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
			codec := &retryColdCodec{ContinuationCodec: &whisperio.ContinuationCodec{}, failures: tc.failures}
			if tc.metricErr {
				codec.err = errors.New("injected metric failure")
			}
			if tc.cancel {
				codec.cancel = cancel
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
			// Raise the fence before unpacking, without running an exact sweep.
			if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact"}); err != nil {
				t.Fatal(err)
			}
			if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "unpack", CPUFraction: 1, BytesPerSecond: 1 << 40, MinFreeBytes: 1}); err != nil {
				t.Fatal(err)
			}
			err = runColdPack(ctx, db, tc.downgrade)
			stats := db.ColdPackStats()
			if (err == nil) != (tc.format == 7) || db.TransferFormat() != tc.format || stats.Passes != tc.passes {
				t.Fatalf("run: %v; format=%d; stats=%+v", err, db.TransferFormat(), stats)
			}
			if tc.downgrade && tc.format == 9 && !tc.cancel && !tc.metricErr && !errors.Is(err, store.ErrColdPackIncomplete) {
				t.Fatalf("lost downgrade refusal: %v", err)
			}
			if tc.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if err != nil {
				for _, field := range []string{"errors=", "conflicts=", "skipped=", "last_error=", "cursor=", "block_cursor="} {
					if !strings.Contains(err.Error(), field) {
						t.Fatalf("missing diagnostic %s: %v", field, err)
					}
				}
			}
		})
	}
}
