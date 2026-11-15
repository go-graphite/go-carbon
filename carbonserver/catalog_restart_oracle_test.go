package carbonserver

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

// This fixture can also run unchanged against the parent revision. It covers
// committed history, a tail not yet delivered to discovery, recovery-only
// names, corrections, signed zero and NaN payloads. Compare its normalized
// digest rather than generation IDs, first-seen times or temporary paths.
func TestCatalogRestartOracle(t *testing.T) {
	const now = 1700000000
	ctx := context.Background()
	directory, root, cache := t.TempDir(), t.TempDir(), filepath.Join(t.TempDir(), "files.gz")
	options := store.Options{ExactBlocks: true, MetadataCacheSize: 1 << 20, MetadataCacheCheckpoint: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour, Now: func() time.Time { return time.Unix(now, 0) }}
	db, err := store.Open(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	count := 1024
	if os.Getenv("CATALOG_ORACLE_OUTPUT") != "" {
		count = 7501
	}
	name := func(i int) string { return fmt.Sprintf("services.app%d.region%d.host%d.requests", i%37, i%3, i) }
	create := func(metric string, i int) {
		t.Helper()
		if _, err := db.Create(ctx, store.MetricConfig{Name: metric, Retentions: []store.Retention{{Step: 1, Count: 60}, {Step: 60, Count: 120}}, AggregationMethod: store.Sum}); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateMany(ctx, metric, []store.Point{{Timestamp: now - 3, Value: math.Float64frombits(0x8000000000000000)}, {Timestamp: now - 2, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: now - 1, Value: float64(i)}}); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateMany(ctx, metric, []store.Point{{Timestamp: now - 1, Value: float64(i) + 0.25}}); err != nil {
			t.Fatal(err)
		}
	}
	listener := func() *CarbonserverListener {
		l, _ := newScanTestListener(root, cache, 1)
		l.logger = zap.NewNop()
		l.SetMetricStore(db)
		l.SetScanFrequency(time.Hour)
		l.SetMaxGlobs(count * 2)
		l.SetEstimateSize(func(string) (int64, int64, int64) { return 16 + 24 + 180*12, 0, 180 })
		t.Cleanup(func() { _ = l.Stop() })
		return l
	}
	for i := range count {
		create(name(i), i)
	}
	first := listener()
	if err := first.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	for i := range 72 {
		create(fmt.Sprintf("late.host%d.requests", i), i)
	}
	if err := db.PrepareMetadataCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := first.CheckpointReadIndex(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = store.Open(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	next := listener()
	next.WarmupIndexWithPending(func(_ string, visit, _ func(string) error) error {
		for i := range 12 {
			if err := visit(fmt.Sprintf("pending.host%d.requests", i)); err != nil {
				return err
			}
		}
		return nil
	})
	next.WaitForWarmup()
	if err := next.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	digest := sha256.New()
	var buf [8]byte
	for i := range count {
		metric := name(i)
		fmt.Fprintln(digest, metric, next.MetricExists(metric))
		series, err := db.Fetch(ctx, metric, now-5, now)
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range series.Values {
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(value))
			digest.Write(buf[:])
		}
	}
	for _, pattern := range []string{"services.app1.*.*.requests", "late.*.requests", "pending.*.requests", "absent.*"} {
		names, leaves, _, _, err := next.CurrentFileIndex().trieIdx.query("/"+strings.ReplaceAll(pattern, ".", "/"), count*2, nil)
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]string, len(names))
		for i := range names {
			rows[i] = fmt.Sprint(names[i], ":", leaves[i])
		}
		sort.Strings(rows)
		for _, row := range rows {
			fmt.Fprintln(digest, row)
		}
	}
	usage, err := next.CurrentFileIndex().trieIdx.snapshotQuotaUsage("", next.CurrentFileIndex().trieIdx.snapshot.namespaceExists)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(digest, usage)
	next.SetQuotas([]*Quota{{Pattern: "/", Metrics: int64(count + 72 + 12)}})
	next.SetQuotaUsageReportFrequency(time.Hour)
	if err := next.applySharedStoreQuotas(next.CurrentFileIndex().trieIdx); err != nil {
		t.Fatal(err)
	}
	newThrottled := next.ShouldThrottleMetric(points.OnePoint("quota.new.metric", 1, now), false)
	existingThrottled := next.ShouldThrottleMetric(points.OnePoint(name(0), 1, now), false)
	if !newThrottled || existingThrottled {
		t.Fatalf("throttle new=%v existing=%v", newThrottled, existingThrottled)
	}
	fmt.Fprintln(digest, newThrottled, existingThrottled)
	result := struct {
		Names int
		Hash  string
		Usage QuotaUsage
	}{count + 72 + 12, hex.EncodeToString(digest.Sum(nil)), usage}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(raw))
	if path := os.Getenv("CATALOG_ORACLE_OUTPUT"); path != "" {
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
