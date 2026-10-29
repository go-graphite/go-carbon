package whisperio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// TestContinuationBackfillIntoSealedOlderBlock fills two gaps of a continuation
// metric late, as tool/replica-backfill does after a crash: one inside a
// sealed block written hours earlier (in v7, an older time bucket), one in
// the tail block. Block buckets follow the store clock, so this runs at store
// level with a controlled clock; the App-level test in tool/replica-backfill
// covers the ingest path.
func TestContinuationBackfillIntoSealedOlderBlock(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		for _, fold := range []bool{false, true} {
			t.Run(fmt.Sprintf("v7=%v/fold=%v", buckets, fold), func(t *testing.T) { checkContinuationBackfill(t, buckets, fold) })
		}
	}
}

func checkContinuationBackfill(t *testing.T, buckets, fold bool) {
	ctx := context.Background()
	const name = "backfill.sealed"
	end := int64(1792432800) // hour aligned
	start := end - 240*60
	now := time.Unix(start, 0)
	clock := func() time.Time { return now }
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := openTestStore(filepath.Join(root, "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{},
		IncrementalSource: source, TimeBuckets: buckets, Now: clock, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// 1s:6h holds 7,200 points per block: the base data below fills one block
	// at about end-104m, so hole A lies in a sealed block created in an
	// earlier hour than the tail block holding hole B.
	if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 6 * 3600}, {Step: 60, Count: 30 * 1440}},
		AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
		t.Fatal(err)
	}
	holeA := [2]int64{end - 200*60, end - 184*60}
	holeB := [2]int64{end - 20*60, end - 4*60}
	inHole := func(ts int64) bool { return ts >= holeA[0] && ts <= holeA[1] || ts >= holeB[0] && ts <= holeB[1] }
	value := func(ts int64) float64 { return math.Sqrt(float64(ts%100003)) * 1.000001 }

	// A cwhisper file receiving the same writes shows the physical layout,
	// which VerifyContinuationSource proves the store shares.
	oraclePath := filepath.Join(root, "oracle.wsp")
	oracleOptions := &whisper.Options{Compressed: true, OutOfOrder: true, Now: clock}
	oracle, err := whisper.CreateWithOptions(oraclePath, whisper.MustParseRetentionDefs("1s:6h,1m:30d"), whisper.Average, .5, oracleOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.Close(); err != nil {
		t.Fatal(err)
	}

	// Base data arrives in order, ten minutes at a time, as the clock passes.
	for chunk := start; chunk <= end; chunk += 600 {
		var batch []store.Point
		var wb []*whisper.TimeSeriesPoint
		for ts := chunk; ts < chunk+600 && ts <= end; ts++ {
			if !inHole(ts) {
				batch = append(batch, store.Point{Timestamp: ts, Value: value(ts)})
				wb = append(wb, &whisper.TimeSeriesPoint{Time: int(ts), Value: value(ts)})
			}
		}
		now = time.Unix(min(chunk+600, end)+2, 0)
		if err := db.UpdateMany(ctx, name, batch); err != nil {
			t.Fatal(err)
		}
		w, err := whisper.OpenWithOptions(oraclePath, oracleOptions)
		if err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(w.UpdateMany(wb), w.Close()); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.VerifyContinuationSource(ctx, name, oraclePath); err != nil {
		t.Fatalf("store layout differs from the cwhisper oracle: %v", err)
	}
	w, err := whisper.OpenWithOptions(oraclePath, oracleOptions)
	if err != nil {
		t.Fatal(err)
	}
	layout, err := w.ContinuationLayout()
	if err := errors.Join(err, w.Close()); err != nil {
		t.Fatal(err)
	}
	var blocks []whisper.ContinuationBlock
	for _, b := range layout.Blocks {
		if b.Archive == 0 && b.Count > 0 {
			blocks = append(blocks, b)
		}
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].First < blocks[j].First })
	aInSealed := len(blocks) == 2 && int64(blocks[0].First) < holeA[0] && int64(blocks[0].Last) > holeA[1]
	if !aInSealed || int64(blocks[1].First) > holeB[0] {
		t.Fatalf("premise: want hole A inside the sealed first block and hole B in the tail block, got %+v", blocks)
	}
	// A block's bucket is the store clock's hour when it got its first point:
	// the chunk end after that point.
	createdHour := func(first int) int64 { return (int64(first) - (int64(first)-start)%600 + 600 + 2) / 3600 }
	if createdHour(blocks[0].First) == createdHour(blocks[1].First) {
		t.Fatalf("premise: both blocks were created in the same hour %+v", blocks)
	}
	before, err := db.Fetch(ctx, name, int(start-1), int(end))
	if err != nil {
		t.Fatal(err)
	}

	// The fill arrives two hours after hole A's block was sealed.
	now = time.Unix(end+300, 0)
	var fill []store.Point
	for ts := start; ts <= end; ts++ {
		if inHole(ts) {
			fill = append(fill, store.Point{Timestamp: ts, Value: value(ts)})
		}
	}
	if fold {
		folded := false
		err = db.UpdateManyMaintained(ctx, name, fill, store.ContinuationMaintenance{MinPoints: 1, MaxPointAge: time.Second, RetentionMargin: time.Second,
			Allow: func() bool { return true }, Result: func(merged bool, _ error) { folded = folded || merged }})
		if err == nil && !folded {
			t.Fatal("out-of-order sidecar was not folded")
		}
	} else {
		err = db.UpdateMany(ctx, name, fill)
	}
	if err != nil {
		t.Fatal(err)
	}

	after, err := db.Fetch(ctx, name, int(start-1), int(end))
	if err != nil || after.Step != 1 {
		t.Fatalf("fetch %+v %v", after, err)
	}
	at := func(s *store.Series, ts int64) float64 { return s.Values[(int(ts)-s.FromTime)/s.Step] }
	for ts := start; ts <= end; ts++ {
		want := at(before, ts)
		if inHole(ts) {
			if !math.IsNaN(want) {
				t.Fatalf("slot %d was not empty before the fill", ts)
			}
			want = value(ts)
		}
		if got := at(after, ts); math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("slot %+ds (hole=%v): got %v want %v", ts-end, inHole(ts), got, want)
		}
	}

	coarse, err := db.Fetch(ctx, name, int(end-7*3600), int(end))
	if err != nil || coarse.Step != 60 {
		t.Fatalf("coarse fetch %+v %v", coarse, err)
	}
	snap, err := db.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	exported := map[int64]float64{}
	for _, p := range snap.Archives[1].Points {
		exported[p.Timestamp] = p.Value
	}
	bad := 0
	for minute := start; minute+60 <= end; minute += 60 {
		var sum float64
		for ts := minute; ts < minute+60; ts++ {
			sum += value(ts)
		}
		want := sum / 60
		got, exp := at(coarse, minute), exported[minute]
		if math.Abs(got-want) > 1e-9*want || math.Abs(exp-want) > 1e-9*want {
			if bad++; bad <= 5 {
				t.Errorf("minute %+dm: read %v snapshot %v want %v", (minute-end)/60, got, exp, want)
			}
		}
	}
	if bad > 0 {
		t.Errorf("%d minutes of the coarse archive disagree with the merged points", bad)
	}
}
