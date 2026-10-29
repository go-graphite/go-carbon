package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Opt-in local catalog fixture. Seeding is separate so every bounded probe
// measures the same million-name store without recreating or retaining it.
func TestMetricStoreCatalogScale(t *testing.T) {
	directory := os.Getenv("CREATE_MATCH_CATALOG_DIR")
	if directory == "" {
		t.Skip("set CREATE_MATCH_CATALOG_DIR for the local scale probe")
	}
	root := filepath.Join(directory, "originals")
	if err := os.MkdirAll(root, 0750); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(filepath.Join(directory, "store"), store.Options{ExactBlocks: true, IncrementalSource: root, SyncInterval: time.Hour, CacheSize: 4 << 20, MemTableSize: 16 << 20, MaxUnflushedMemTableBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	high, err := db.CatalogHighWatermark(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mode := os.Getenv("CREATE_MATCH_CATALOG_MODE")
	if mode == "seed" {
		n, _ := strconv.Atoi(os.Getenv("CREATE_MATCH_CATALOG_SEED"))
		if n == 0 {
			n = 250000
		}
		start := time.Now()
		for i := int(high); i < int(high)+n; i++ {
			name := fmt.Sprintf("general.flink.host%04d.task%03d.process.partition%03d.count", i/1000, (i/10)%100, i%10)
			if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("seeded %d catalog entries in %v", n, time.Since(start))
		return
	}
	expected, _ := strconv.Atoi(os.Getenv("CREATE_MATCH_CATALOG_COUNT"))
	if expected == 0 {
		expected = 1000000
	}
	l, _ := newScanTestListener(root, filepath.Join(t.TempDir(), "files.gz"), 1)
	var scanErrors bytes.Buffer
	l.logger = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&scanErrors), zap.ErrorLevel))
	l.SetMetricStore(db)
	l.SetEstimateSize(nil)
	l.SetIncrementalIndexScratch(filepath.Join(t.TempDir(), "scratch"))
	l.SetMetricStoreFileIndex(mode == "legacy")
	if mode == "incremental" {
		if err := l.RefreshMetricStoreIndex(); err != nil {
			t.Fatal(err)
		}
		// Any accidental source-union scan now fails immediately.
		poison := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(poison, nil, 0600); err != nil {
			t.Fatal(err)
		}
		l.SetIncrementalIndexScratch(poison)
	}
	profile := os.Getenv("CREATE_MATCH_SCALE_PROFILE")
	if profile != "" {
		f, err := os.Create(profile)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			t.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}
	usage, started := scanUsage(), time.Now()
	repeats := 1
	if mode == "incremental" {
		repeats = 100
	}
	initial := l.CurrentFileIndex()
	for range repeats {
		l.updateFileList(root, nil, nil)
	}
	if scanErrors.Len() != 0 {
		t.Fatalf("scan failed or touched poisoned source scanner: %s", scanErrors.String())
	}
	t.Logf("mode=%s metrics=%d scans=%d wall=%v %s", mode, expected, repeats, time.Since(started), usage.since())
	if l.CurrentFileIndex() == nil || l.metrics.MetricsKnown != uint64(expected) {
		t.Fatalf("wrong catalog size: got %d want %d", l.metrics.MetricsKnown, expected)
	}
	if mode == "incremental" {
		if l.CurrentFileIndex().trieIdx != initial.trieIdx {
			t.Fatal("idle scans rebuilt catalog")
		}
		if _, err := db.Create(ctx, store.MetricConfig{Name: "general.new.metric", Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Average}); err != nil {
			t.Fatal(err)
		}
		usage, started = scanUsage(), time.Now()
		l.updateFileList(root, nil, nil)
		if scanErrors.Len() != 0 {
			t.Fatalf("new metric touched source scanner: %s", scanErrors.String())
		}
		t.Logf("one new metric wall=%v %s", time.Since(started), usage.since())
		if l.CurrentFileIndex().trieIdx != initial.trieIdx || l.metrics.MetricsKnown != uint64(expected)+1 {
			t.Fatal("new metric forced full rebuild")
		}
		if err := db.Delete(ctx, "general.new.metric"); err != nil {
			t.Fatal(err)
		}
	}
}
