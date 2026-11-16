//go:build linux || darwin

package main

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

func TestRealComparisonDuplicatePartsPreserveBitsAndRejectConflict(t *testing.T) {
	dir := packingFixture(t)
	images, err := storedComparisonInput(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(images) != 1 {
		t.Fatalf("ownership groups: %d", len(images))
	}
	var unit partsconvert.Unit
	var image partsconvert.StoredMetric
	for u, ms := range images {
		if len(ms) != 1 {
			t.Fatalf("metrics: %d", len(ms))
		}
		unit, image = u, ms[0]
	}
	points := image.Archives[0].Points
	if len(points) != 6 {
		t.Fatalf("duplicated occupied slots: %d", len(points))
	}
	for i, p := range points {
		want := uint64(0x7ff8000000000042)
		if i%2 == 0 {
			want = math.Float64bits(math.Copysign(0, -1))
		}
		if p.ValueBits != want {
			t.Fatalf("slot%d bits%016x want%016x", i, p.ValueBits, want)
		}
	}
	image.Archives[0].Points = append([]partsconvert.StoredPoint(nil), points[:1]...)
	image.Archives[0].Points[0].ValueBits ^= 1
	db, err := pebble.Open(filepath.Join(dir, "parts"), packingOptions(false))
	if err != nil {
		t.Fatal(err)
	}
	source := partsconvert.ArchiveSource{PartitionSeconds: 86400, BlockPoints: 4096, ScanMetrics: func(_ context.Context, _ partsconvert.Unit, y func(partsconvert.StoredMetric) error) error {
		return y(image)
	}}
	err = source.Scan(context.Background(), unit, func(r partsconvert.Record) error {
		r.Key.Part[0] = 200
		key, e := partsformat.Encode(r.Key)
		if e != nil {
			return e
		}
		return db.Set(key, r.Value, pebble.Sync)
	})
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if _, err = storedComparisonInput(context.Background(), dir); err == nil {
		t.Fatal("accepted disagreeing bits from another part")
	}
}
