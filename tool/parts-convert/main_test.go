//go:build linux || darwin

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestSyntheticRunsRealIngestAndVerification(t *testing.T) {
	c := config{dir: filepath.Join(t.TempDir(), "new"), workers: 2, shards: 2, metrics: 2, points: 128, archives: 3, chunkMiB: 1}
	r, err := run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if r.Points != 4*(128+32+8) || r.SSTBytes == 0 || r.VerifyWorkerSeconds <= 0 || r.Seconds <= 0 {
		t.Fatalf("incomplete measured result: %+v", r)
	}
	if _, err := run(context.Background(), c); err == nil {
		t.Fatal("opened an existing output directory")
	}
}

func TestCompareActualColdPackSameStoredSlots(t *testing.T) {
	c := config{dir: filepath.Join(t.TempDir(), "comparison"), workers: 1, shards: 1, metrics: 2, points: 512, archives: 1, chunkMiB: 1}
	r, err := compare(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if !r.BitExact || r.Points == 0 || r.LegacySSTBytes == 0 || r.ConvertedSSTBytes == 0 || r.ColdPackBlocks == 0 {
		t.Fatalf("incomplete comparison: %+v", r)
	}
}

func TestCompareRealConvertedSameSlots(t *testing.T) {
	root := t.TempDir()
	c := config{dir: filepath.Join(root, "synthetic"), workers: 1, shards: 1, metrics: 2, points: 512, archives: 1, chunkMiB: 1}
	original, err := compare(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "target")
	if err = os.Mkdir(target, 0750); err != nil {
		t.Fatal(err)
	}
	if _, err = durableIdentity(filepath.Join(target, "CONVERSION-TARGET")); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(filepath.Join(c.dir, "converted"), filepath.Join(target, "parts")); err != nil {
		t.Fatal(err)
	}
	c.dir = filepath.Join(root, "real")
	c.realConverted = target
	real, err := compare(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(c.dir, "converted")); !os.IsNotExist(err) {
		t.Fatalf("real comparison rebuilt an existing target: %v", err)
	}
	if real.RebuiltConverted || real.ConvertedSSTBytes != 0 || real.ActualConvertedSSTBytes == 0 || real.ActualRatio <= 0 {
		t.Fatalf("real target density confused with rebuild: %+v", real)
	}
	if !real.RealStoredSlots || !real.BitExact || real.Points != original.Points || real.Metrics != original.Metrics || real.ColdPackBlocks == 0 {
		t.Fatalf("real slots comparison: %+v original %+v", real, original)
	}
}
