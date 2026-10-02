package carbonserver

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestInitialFileListCachePublishesNotificationsAndQuotas(t *testing.T) {
	dir := t.TempDir()
	flcPath := filepath.Join(dir, "file-list-cache")
	flc, err := NewFileListCache(flcPath, FLCVersion2, 'w')
	if err != nil {
		t.Fatal(err)
	}
	if err := flc.Write(&FLCEntry{Path: "/namespace/existing.wsp", LogicalSize: 1024, PhysicalSize: 1024, DataPoints: 60}); err != nil {
		t.Fatal(err)
	}
	if err := flc.Close(); err != nil {
		t.Fatal(err)
	}
	listener := NewCarbonserverListener(nil)
	listener.SetWhisperData(dir)
	listener.SetTrieIndex(true)
	listener.SetConcurrentIndex(true)
	listener.SetFileListCache(flcPath)
	listener.SetQuotaUsageReportFrequency(time.Minute)
	listener.SetEstimateSize(func(string) (int64, int64, int64) { return 1024, 1024, 60 })
	listener.SetQuotas([]*Quota{{Pattern: "/", Metrics: 2}})
	ch := listener.SetRealtimeIndex(10)
	ch <- "namespace.pending"
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	if !listener.updateFileList(dir, nil, ticks) {
		t.Fatal("expected initial index from cache")
	}
	if len(ticks) != 0 {
		t.Fatal("startup quota refresh left a stale tick that would immediately repeat the full traversal")
	}
	result, err := listener.queryMetricsList("namespace.pending", 1, true, false)
	if err != nil || len(result.Metrics) != 1 {
		t.Fatalf("queued metric absent from published index: %v, %v", result, err)
	}
	if !listener.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) {
		t.Fatal("initial published index must enforce usage quota before the next minute tick")
	}
	if listener.ShouldThrottleMetric(points.OnePoint("namespace.existing", 1, 1), false) {
		t.Fatal("usage quota must not block an existing series")
	}
}

func TestInitialIndexQuotaBeforeFirstTick(t *testing.T) {
	dir := t.TempDir()
	listener := NewCarbonserverListener(nil)
	listener.SetWhisperData(dir)
	listener.SetTrieIndex(true)
	listener.SetConcurrentIndex(true)
	listener.SetRealtimeIndex(1)
	listener.SetQuotaUsageReportFrequency(time.Minute)
	listener.SetEstimateSize(func(string) (int64, int64, int64) { return 1024, 1024, 60 })
	listener.SetQuotas([]*Quota{{Pattern: "/", Metrics: 1}})
	listener.updateFileList(dir, map[string]struct{}{"/namespace/existing.wsp": {}}, nil)
	if listener.CurrentFileIndex() == nil {
		t.Fatal("index was not published")
	}
	if !listener.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) {
		t.Fatal("published index bypasses its configured quota")
	}
	if listener.ShouldThrottleMetric(points.OnePoint("namespace.existing", 1, 1), false) {
		t.Fatal("existing series rejected by creation quota")
	}
}

func BenchmarkInitialFileListCache(b *testing.B) {
	const count = 10000
	for _, quotas := range []bool{false, true} {
		b.Run(fmt.Sprintf("quotas=%t", quotas), func(b *testing.B) {
			dir := b.TempDir()
			flcPath := filepath.Join(dir, "file-list-cache")
			flc, err := NewFileListCache(flcPath, FLCVersion2, 'w')
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < count; i++ {
				if err := flc.Write(&FLCEntry{Path: fmt.Sprintf("/namespace/server%d/value.wsp", i), LogicalSize: 1024, PhysicalSize: 1024, DataPoints: 60}); err != nil {
					b.Fatal(err)
				}
			}
			if err := flc.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				listener := NewCarbonserverListener(nil)
				listener.SetWhisperData(dir)
				listener.SetTrieIndex(true)
				listener.SetConcurrentIndex(true)
				listener.SetFileListCache(flcPath)
				if quotas {
					listener.SetRealtimeIndex(1)
					listener.SetQuotaUsageReportFrequency(time.Minute)
					listener.SetEstimateSize(func(string) (int64, int64, int64) { return 1024, 1024, 60 })
					listener.SetQuotas([]*Quota{{Pattern: "/", Metrics: count}})
				}
				if !listener.updateFileList(dir, nil, nil) {
					b.Fatal("file list cache load failed")
				}
			}
		})
	}
}
