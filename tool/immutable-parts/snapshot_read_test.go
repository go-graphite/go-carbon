package main

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

func TestSnapshotCoreFrozenHeadAndSealedPart(t *testing.T) {
	dir := t.TempDir()
	cfg := coreConfig{Shards: 1, Producers: 1, MaxRows: 64, PartitionSeconds: 86400}
	var frozen, sealed bool
	e, err := openCoreWithHook(filepath.Join(dir, "db"), cfg, func(shard int, snap *pebble.Snapshot, head []row) error {
		defer snap.Close()
		if len(head) > 0 {
			frozen = true
			if head[0].Bits != math.Float64bits(42) {
				t.Error("head bits changed")
			}
		}
		src := snapshotCoreSource{snap: snap, partitions: []int64{corePartition(1800000000, 86400)}}
		return src.Scan(context.Background(), 7, pr.Archive{Step: 1, Points: 86400}, 1799999999, 1800000001, func(p pr.Sample) error {
			if p.Bits != math.Float64bits(42) {
				t.Error("sealed bits changed")
			}
			sealed = true
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.submit(0, []row{{7, 1800000000, math.Float64bits(42), 1}}, 1, false); err != nil {
		t.Fatal(err)
	}
	if err = e.submit(0, nil, 2, true); err != nil {
		t.Fatal(err)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	if !frozen || !sealed {
		t.Fatal("missing frozen or sealed visibility", frozen, sealed)
	}
}

// This opt-in test binary uses the same core ingest workload with and without
// render readers. Its explicit environment prevents accidental load in CI.
func TestSnapshotReadBenchmark(t *testing.T) {
	out := os.Getenv("PARTS_READ_BENCH_DIR")
	if out == "" {
		t.Skip("set PARTS_READ_BENCH_DIR to an empty private directory")
	}
	seconds := 20
	readers := 8
	if v := os.Getenv("PARTS_READ_BENCH_SECONDS"); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil || n < 4 {
			t.Fatal("invalid seconds")
		}
		seconds = n
	}
	if v := os.Getenv("PARTS_READ_BENCH_READERS"); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil || n < 0 {
			t.Fatal("invalid readers")
		}
		readers = n
	}
	cfg := coreConfig{Shards: 16, Producers: 4, MaxRows: 262144, PartitionSeconds: 86400}
	if err := runSnapshotReadBenchmark(out, cfg, time.Duration(seconds)*time.Second, 25_000_000, 1<<20, 3*time.Second, readers); err != nil {
		t.Fatal(err)
	}
}
