package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestColdPackExhaustive(t *testing.T) {
	ctx := context.Background()
	var results [][]store.RelayoutKV
	for _, exhaustive := range []bool{false, true} {
		db, err := store.Open(t.TempDir(), coldOptions(&ContinuationCodec{}))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		db.StopBackground()
		cfg := store.MetricConfig{Name: "selector", Retentions: []store.Retention{{Step: 60, Count: 10080}}, AggregationMethod: store.Average}
		if _, err = db.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewPCG(3, 9))
		points := make([]store.Point, 1800)
		for i := range points {
			points[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: float64(rng.IntN(16)) / 3}
			if i < 64 {
				points[i].Value = math.Float64frombits(0x3ff0000000000000 | rng.Uint64()>>12)
			}
		}
		if err = db.UpdateMany(ctx, cfg.Name, points); err != nil {
			t.Fatal(err)
		}
		want, err := db.Snapshot(ctx, cfg.Name)
		if err != nil {
			t.Fatal(err)
		}
		if err = db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", FloatCodec: true, FloatExhaustive: exhaustive, WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40}); err != nil {
			t.Fatal(err)
		}
		if err = db.RunColdPack(ctx); err != nil {
			t.Fatal(err)
		}
		got, err := db.Snapshot(ctx, cfg.Name)
		if err != nil {
			t.Fatal(err)
		}
		coldEqual(t, want, got, false)
		records, err := db.ExportTransfer(ctx, []string{cfg.Name})
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, records[0].Keys)
		var sealed int
		for _, kv := range records[0].Keys {
			if len(kv.Key) != 25 || kv.Key[14] != 'c' || len(kv.Value) < 16 || kv.Value[0] < 3 {
				continue
			}
			p, err := store.DecodeSealedPoints(kv.Value)
			if err != nil {
				t.Fatal(err)
			}
			sealed++
			if !exhaustive {
				continue
			}
			expected, err := store.EncodeSealedPointsFloatMin(p, int(binary.LittleEndian.Uint32(kv.Value[8:12])))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(expected, kv.Value) {
				t.Fatal("pack did not use exhaustive selection")
			}
		}
		if sealed == 0 {
			t.Fatal("fixture did not pack")
		}
	}
	oldBytes, newBytes := 0, 0
	for _, kv := range results[0] {
		oldBytes += len(kv.Value)
	}
	for _, kv := range results[1] {
		newBytes += len(kv.Value)
	}
	if newBytes >= oldBytes {
		t.Fatalf("exhaustive fixture did not improve: %d -> %d", oldBytes, newBytes)
	}
}
