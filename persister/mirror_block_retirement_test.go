package persister

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func TestMirrorRetiresCompressedSourceBlocks(t *testing.T) {
	for _, test := range []struct {
		name     string
		rollup   bool
		largeGap bool
	}{{"primary", false, false}, {"with_rollup", true, false}, {"large_sparse_gap", false, true}} {
		t.Run(test.name, func(t *testing.T) { testMirrorRetiresCompressedSourceBlocks(t, test.rollup, test.largeGap) })
	}
}

func testMirrorRetiresCompressedSourceBlocks(t *testing.T, rollup, largeGap bool) {
	root := t.TempDir()
	original := filepath.Join(root, "original")
	const metric = "retired.block"
	path := filepath.Join(original, "retired", "block.wsp")
	c := &fakeCache{}
	p := newOOOTestPersister(t, original, c)
	p.outOfOrder.ticker.Stop()
	p.outOfOrder.ticker = nil
	oldNow := whisper.Now
	now := time.Now().Unix()
	whisper.Now = func() time.Time { return time.Unix(now, 0) }
	defer func() { whisper.Now = oldNow }()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	retentions := []whisper.Retention{whisper.NewRetention(1, 4096)}
	method, xff := whisper.Last, float32(0)
	if rollup {
		retentions = append(retentions, whisper.NewRetention(64, 256))
		method, xff = whisper.Average, 0.5
	}
	w, err := whisper.CreateWithOptions(path, whisper.NewRetentionsNoPointer(retentions), method, xff, &whisper.Options{Compressed: true, FLock: true, OutOfOrder: true, PointsPerBlock: 64, PointSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	state := uint64(0x123456789abcdef)
	value := func() float64 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		return math.Float64frombits(0x3ff0000000000000 | (state & 0x000fffffffffffff))
	}

	start := now - 8192
	oldTimestamp := start + 1
	if largeGap {
		oldTimestamp -= 10 * 4096
		now = oldTimestamp
		if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(oldTimestamp), Value: value()}}); err != nil {
			t.Fatal(err)
		}
	}
	for batch := 0; batch < 8; batch++ {
		input := make([]*whisper.TimeSeriesPoint, 0, 513)
		for i := 0; i < 512; i++ {
			timestamp := start + int64(batch*512+i)*2
			input = append(input, &whisper.TimeSeriesPoint{Time: int(timestamp), Value: value()})
			if batch == 0 && i == 0 && !largeGap {
				input = append(input, &whisper.TimeSeriesPoint{Time: int(timestamp + 1), Value: value()})
			}
		}
		now = start + int64((batch+1)*512-1)*2
		if err = w.UpdateMany(input); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := openTestStore(t, filepath.Join(root, "exact"), store.Options{ExactBlocks: true, CacheSize: 64 << 20, MemTableSize: 8 << 20, Now: whisper.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	before, err := whisperio.ReadSnapshotIn(ctx, metric, path, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.CreateFromSnapshot(ctx, before); err != nil {
		t.Fatal(err)
	}
	p.SetMetricStore(db)
	p.SetMirrorFiles(true)
	p.SetMirrorState(&MirrorState{ScratchDir: root})

	foundOld := false
	for _, point := range before.Archives[0].Points {
		if point.Timestamp == oldTimestamp {
			foundOld = true
		}
	}
	if !foundOld {
		t.Fatal("fixture lost the sparse old slot before mirroring")
	}
	for batch := 0; batch < 32; batch++ {
		previous := now
		now += 1024
		for i := 0; i < 512; i++ {
			c.add(metric, previous+2+int64(i)*2, value())
		}
		p.store(metric)
	}
	if c.confirmedPoints() != 32*512 {
		t.Fatalf("unconfirmed points: %d", c.confirmedPoints())
	}
	want, err := whisperio.ReadSnapshotIn(ctx, metric, path, root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Snapshot(ctx, metric)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.EqualSnapshots(want, got); err != nil {
		t.Fatalf("compressed block retirement was not mirrored: %v", err)
	}
	if largeGap && got.Metadata.Generation == 1 {
		t.Fatal("large sparse gap did not use bounded full-source fallback")
	}
	if !largeGap && got.Metadata.Generation != 1 {
		t.Fatal("ordinary retirement unexpectedly replaced the whole source")
	}
}
