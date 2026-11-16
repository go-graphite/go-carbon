//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

func packingFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "CONVERSION-TARGET"), []byte("0123456789abcdef0123456789abcdef\n"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open(filepath.Join(dir, "parts"), packingOptions(false))
	if err != nil {
		t.Fatal(err)
	}
	name := partsformat.ID("packing.metric")
	u := partsconvert.Unit{Group: "namespace", VShard: partsformat.Route(name)}
	for partition := range 3 {
		m := partsconvert.StoredMetric{Name: name, Group: "namespace", Method: 1,
			Archives: []partsconvert.StoredArchive{{Step: 60, Slots: 120, Points: []partsconvert.StoredPoint{
				{Timestamp: int64(partition) * 86400, ValueBits: math.Float64bits(math.Copysign(0, -1))},
				{Timestamp: int64(partition)*86400 + 60, ValueBits: 0x7ff8000000000042},
			}}}}
		source := partsconvert.ArchiveSource{SnapshotID: "fixture", PartitionSeconds: 86400, BlockPoints: 4096,
			ScanMetrics: func(_ context.Context, _ partsconvert.Unit, yield func(partsconvert.StoredMetric) error) error {
				return yield(m)
			}}
		err = source.Scan(context.Background(), u, func(r partsconvert.Record) error {
			for part := range 16 {
				r.Key.Part[0] = byte(part)
				key, err := partsformat.Encode(r.Key)
				if err != nil {
					return err
				}
				if err = db.Set(key, r.Value, pebble.Sync); err != nil {
					return err
				}
				if err = db.Flush(); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestPackingPreservesLogicalReceiptsAndBoundaries(t *testing.T) {
	dir := packingFixture(t)
	r, err := packCopy(context.Background(), "", dir, 0, 16<<20, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !r.BitExact || r.Before.Records != 48 || r.Before.Points != 96 || r.After.SSTs != 1 || r.Before.SSTs != 48 || r.After.Boundaries != 3 {
		t.Fatalf("packing: %+v", r)
	}
	var index packingIndex
	data, e := os.ReadFile(filepath.Join(dir, "parts", "PARTITION-INDEX.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(data, &index); e != nil {
		t.Fatal(e)
	}
	if index.Version != 1 || len(index.Ranges) != 3 {
		t.Fatalf("index %+v", index)
	}
	for _, span := range index.Ranges {
		if span.Table < 0 || span.Table >= len(index.Tables) {
			t.Fatal("invalid table", span)
		}
		table := index.Tables[span.Table]
		if _, e = os.Stat(filepath.Join(dir, "parts", table.SST)); e != nil {
			t.Fatal(e)
		}
		if bytes.Compare(table.Largest, span.Prefix) < 0 || bytes.Compare(table.Smallest, partsformat.PrefixEnd(span.Prefix)) >= 0 {
			t.Fatal("wrong partition range", span)
		}
	}
	if r.DiskCapQualified {
		t.Fatal("offline packing is not a whole-conversion disk qualification")
	}
	if r.After.SSTBytes >= r.Before.SSTBytes {
		t.Fatalf("packing did not reduce SST bytes: %+v", r)
	}
	if r.After.SSTBytes != r.Before.PlannedSSTBytes {
		t.Fatalf("dry encoding differs from actual SST bytes: %+v", r)
	}
	if _, err := os.Stat(filepath.Join(dir, "packing-old")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("old generation retained", err)
	}
	// The same logical digest is also stable after close/reopen and a repeated
	// pack. No receipt's per-Part prefix or value needs to be rewritten.
	r2, err := packCopy(context.Background(), "", dir, 0, 16<<20, true, nil)
	if err != nil || !samePacking(r.Before, r2.After) {
		t.Fatalf("repeat: %+v %v", r2, err)
	}
}

func TestPackingVerifiedCrashRecovery(t *testing.T) {
	for _, boundary := range []string{"packing-verified", "packing-old-renamed", "packing-new-renamed", "packing-installed"} {
		t.Run(boundary, func(t *testing.T) {
			dir := packingFixture(t)
			interrupted := errors.New("injected crash")
			r, err := packCopy(context.Background(), "", dir, 0, 16<<20, true, func(stage string) error {
				if stage == boundary {
					return interrupted
				}
				return nil
			})
			if !errors.Is(err, interrupted) {
				t.Fatal(err)
			}
			if err = recoverPacking(context.Background(), dir); err != nil {
				t.Fatal(err)
			}
			got, err := packCopy(context.Background(), "", dir, 0, 16<<20, false, nil)
			if err != nil || !samePacking(r.Before, got.Before) || got.Before.SSTs != 1 {
				t.Fatalf("recovery: %+v %v", got, err)
			}
		})
	}
}

func TestPackingRefusesChangedVerifiedReplacement(t *testing.T) {
	dir := packingFixture(t)
	_, err := packCopy(context.Background(), "", dir, 0, 16<<20, true, func(string) error { return errors.New("stop") })
	if err == nil {
		t.Fatal("missing interruption")
	}
	db, err := pebble.Open(filepath.Join(dir, "packing-next"), packingOptions(false))
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Set([]byte("bad-key"), []byte("bad-value"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = recoverPacking(context.Background(), dir); err == nil {
		t.Fatal("changed replacement accepted")
	}
	if _, err = os.Stat(filepath.Join(dir, "parts")); err != nil {
		t.Fatal("source disappeared", err)
	}
}

func TestPackingRefusesScratchBeforeOutput(t *testing.T) {
	dir := packingFixture(t)
	source := t.TempDir()
	current, err := allocatedBytes(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = packCopy(context.Background(), source, dir, current, 16<<20, true, nil); err == nil {
		t.Fatal("scratch budget accepted")
	}
	for _, name := range []string{"PACKING.json", "packing-next", "packing-old"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("mutated before scratch admission", name, err)
		}
	}
}

func TestPackingRejectsConcurrentConverter(t *testing.T) {
	dir := packingFixture(t)
	unlock, err := packingLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if _, err := packCopy(context.Background(), "", dir, 0, 16<<20, true, nil); err == nil {
		t.Fatal("concurrent target owner accepted")
	}
}

func TestPackingRefusesChangedVerifiedIndex(t *testing.T) {
	dir := packingFixture(t)
	_, err := packCopy(context.Background(), "", dir, 0, 16<<20, true, func(string) error { return errors.New("stop") })
	if err == nil {
		t.Fatal("missing interruption")
	}
	if err = os.WriteFile(filepath.Join(dir, "packing-next", "PARTITION-INDEX.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = recoverPacking(context.Background(), dir); err == nil {
		t.Fatal("changed index accepted")
	}
	if _, err = os.Stat(filepath.Join(dir, "parts")); err != nil {
		t.Fatal("source removed", err)
	}
}

func TestPackingKeepsPlacementGroupAndVShardOwnership(t *testing.T) {
	dir := packingFixture(t)
	db, err := pebble.Open(filepath.Join(dir, "parts"), packingOptions(false))
	if err != nil {
		t.Fatal(err)
	}
	it, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !it.First() {
		t.Fatal("empty fixture")
	}
	value := bytes.Clone(it.Value())
	if err = it.Close(); err != nil {
		t.Fatal(err)
	}
	for _, group := range []partsformat.Group{"first", "second"} {
		for _, name := range []partsformat.ID{"extra.a", "extra.b"} {
			for partition := range 3 {
				key, e := partsformat.Encode(partsformat.Key{Group: group, VShard: partsformat.Route(name), Partition: int64(partition), Metric: name})
				if e != nil {
					t.Fatal(e)
				}
				if err = db.Set(key, value, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := packCopy(context.Background(), "", dir, 0, 16<<20, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.After.SSTs != 5 || r.After.Boundaries != 15 || !r.BitExact {
		t.Fatalf("ownership packing %+v", r)
	}
	var index packingIndex
	data, err := os.ReadFile(filepath.Join(dir, "parts", "PARTITION-INDEX.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &index); err != nil {
		t.Fatal(err)
	}
	if len(index.Tables) != 5 || len(index.Ranges) != 15 {
		t.Fatalf("index tables=%d ranges=%d", len(index.Tables), len(index.Ranges))
	}
}
