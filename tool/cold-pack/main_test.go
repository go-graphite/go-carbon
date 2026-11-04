package main

import (
	"context"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
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
	if output, err := cmd.CombinedOutput(); err != nil {
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
