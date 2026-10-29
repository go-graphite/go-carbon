package persister

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func TestMirrorOOOCompactionWindows(t *testing.T) {
	t.Parallel()
	for _, expired := range []bool{false, true} {
		name := "live"
		if expired {
			name = "expired"
		}
		t.Run(name, func(t *testing.T) {
			if storageClockMatrix(t) {
				return
			}
			for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max} {
				for _, xff := range []float32{0, 0.5, 1} {
					t.Run(method.String()+"/"+fmtFloat(xff), func(t *testing.T) { testMirrorOOOCompactionWindows(t, method, xff, expired) })
				}
			}
		})
	}
}

func testMirrorOOOCompactionWindows(t *testing.T, method whisper.AggregationMethod, xff float32, expired bool) {
	root := t.TempDir()
	const metric = "compaction.windows"
	original := filepath.Join(root, "original")
	path := filepath.Join(original, "compaction", "windows.wsp")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(time.Now().Unix()/4096*4096, 0)
	previousNow := whisper.Now
	whisper.Now = func() time.Time { return now }
	defer func() { whisper.Now = previousNow }()
	retentions := whisper.MustParseRetentionDefs("1s:65536s,256s:262144s,4096s:1048576s")
	w, err := whisper.CreateWithOptions(path, retentions, method, xff, &whisper.Options{Compressed: true, FLock: true, OutOfOrder: true, PointsPerBlock: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(now.Unix()) - 1, Value: 7.25}}); err != nil {
		t.Fatal(err)
	}
	input := make([]*whisper.TimeSeriesPoint, 128)
	for i := range input {
		input[i] = &whisper.TimeSeriesPoint{Time: int(now.Unix()) - 30 - i*257, Value: float64(i%17) + 0.125}
	}
	if err = w.UpdateMany(input); err != nil {
		t.Fatal(err)
	}
	sidecar := w.OutOfOrderPath()
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(sidecar)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the actual production physical-byte threshold, not logical size.
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Blocks*512 < 65536 {
		t.Fatalf("OOO fixture below production threshold: %d", stat.Blocks*512)
	}
	ctx := context.Background()
	before, err := whisperio.ReadSnapshotIn(ctx, metric, path, root)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "exact")
	opts := store.Options{ExactBlocks: true, Now: whisper.Now, CacheSize: 8 << 20, MemTableSize: 1 << 20}
	db, err := openTestStore(t, dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err = db.CreateFromSnapshot(ctx, before); err != nil {
		t.Fatal(err)
	}
	cache := &fakeCache{}
	p := newOOOTestPersister(t, original, cache)
	p.schemas[0].Retentions = retentions
	p.aggregation.Default.aggregationMethod = method
	p.aggregation.Default.xFilesFactor = float64(xff)
	p.outOfOrder.ticker.Stop()
	p.EnableOutOfOrder(500, 65536)
	// Supply exactly one allowed compaction, independently of scheduler timing.
	p.outOfOrder.ticker = &helper.ThrottleTicker{C: make(chan bool, 1)}
	p.outOfOrder.ticker.C <- true
	p.SetFLock(true)
	p.SetMirrorFiles(true)
	p.SetMirrorState(&MirrorState{ScratchDir: root})
	wrapped := &boundaryMirrorStore{Store: db}
	p.SetMetricStore(wrapped)
	timestamp := now.Unix() - 30 - 17*257
	if expired {
		now = now.Add((1 << 21) * time.Second)
		timestamp = now.Unix() - 1
	}
	cache.add(metric, timestamp, math.Copysign(0, -1))
	p.store(metric)
	if p.oooCompactErrors != 0 {
		t.Fatalf("compactions=%d errors=%d", p.oooCompactions, p.oooCompactErrors)
	}
	expectedRepairs := 0
	if expired && p.oooCompactions == 0 {
		// UpdateMany folded the expired sidecar implicitly; this uncertain
		// rewrite must retain the authoritative full-repair fallback.
		expectedRepairs = 1
	} else if p.oooCompactions != 1 {
		t.Fatalf("expected an explicit source compaction, got %d", p.oooCompactions)
	}
	if wrapped.replacements != expectedRepairs || cache.confirmedPoints() != 1 || p.HasPendingMirrors() {
		t.Fatalf("full repairs=%d confirmed=%d pending=%t", wrapped.replacements, cache.confirmedPoints(), p.HasPendingMirrors())
	}
	want, err := whisperio.ReadSnapshotIn(ctx, metric, path, root)
	if err != nil {
		t.Fatal(err)
	}
	assertEqual := func() {
		t.Helper()
		got, err := db.Snapshot(ctx, metric)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.EqualSnapshots(want, got); err != nil {
			t.Fatal(err)
		}
	}
	assertEqual()
	if err = db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openTestStore(t, dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	assertEqual()
}
