package inputlog_test

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// Exercise the real cache generation bookkeeping, not a synthetic low water:
// recovered inserts happen after empty writer cycles have advanced every shard.
func TestRecoveryWriterTransfersRealCacheGenerationAuthority(t *testing.T) {
	dir := t.TempDir()
	opts := inputlog.Options{SegmentSize: 256, SyncInterval: time.Hour, MaxAge: 24 * time.Hour}
	core := cache.New()
	core.SetMaxSize(0)
	core.EnableInputLog()
	log, err := inputlog.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	log.Start(core, cache.ShardCount)
	const bits = 0x7ff8000000000042
	core.Add(points.OnePoint("history", math.Float64frombits(bits), 17))
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(files) != 1 {
		t.Fatal("seed journal", files, err)
	}
	old := files[0]
	log, err = inputlog.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	core = cache.New()
	core.SetMaxSize(0)
	core.EnableInputLog()
	log.StartRecovery(core, cache.ShardCount)
	for range 4 {
		if err := log.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	var stream cache.RecoveryStream
	source := stream.NextJournal()
	if _, err := log.Replay(func(p *points.Points) { core.AddRecoveredFrom(p, source) }); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		core.Add(points.OnePoint("live", float64(i), int64(i)))
		if err := log.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.CompleteRecovery(); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if err := log.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(old); err != nil {
		t.Fatal("current cache generation retired original journal", err)
	}
	batch, ok := core.PopNotConfirmed("history")
	if !ok || len(batch.Data) != 1 || math.Float64bits(batch.Data[0].Value) != bits {
		t.Fatal("historical receipt changed", batch)
	}
	if source.Pending() == 0 {
		t.Fatal("in-flight historical receipt lost source ownership")
	}
	// The in-flight batch still protects its original append after all readers
	// joined. The writer must also preserve that boundary through a restart.
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	log, err = inputlog.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	found := false
	if _, err := log.Replay(func(p *points.Points) {
		if p.Metric == "history" {
			if len(p.Data) != 1 || math.Float64bits(p.Data[0].Value) != bits {
				t.Error("restart changed historical bits")
			}
			found = true
		}
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("restart skipped applied but unpersisted original journal")
	}
}
