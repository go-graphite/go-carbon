package carbonserver

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
)

func TestRestartCaptureEnforcesCreationQuota(t *testing.T) {
	for _, mode := range []string{"tail", "capture", "grouped-history", "grouped-live", "diversion", "captured-diversion"} {
		t.Run(mode, func(t *testing.T) {
			l := NewCarbonserverListener(nil)
			l.SetWhisperData(t.TempDir())
			l.SetTrieIndex(true)
			l.SetConcurrentIndex(true)
			l.SetRealtimeIndex(10)
			l.SetQuotaUsageReportFrequency(time.Minute)
			l.SetEstimateSize(func(string) (int64, int64, int64) { return 1, 1, 1 })
			l.SetQuotas([]*Quota{{Pattern: "over", Metrics: 1}, {Pattern: "under", Metrics: 10}})
			l.updateFileList(l.whisperData, map[string]struct{}{"/over/existing.wsp": {}, "/over/second.wsp": {}, "/over/tagged;a=1;b=2.wsp": {}, "/under/existing.wsp": {}}, nil)
			defer l.Stop()
			c := cache.New()
			c.SetTagsEnabled(true)
			c.SetThrottleWithHash(l.ShouldThrottleMetricWithHash)
			c.SetCreationThrottle(l.ShouldThrottleCreation)
			var stream cache.RecoveryStream
			source := stream.NextCaptured()
			var diverted []string
			diversion := mode == "diversion" || mode == "captured-diversion"
			if diversion {
				// An accepted name may still be waiting for the realtime index.
				c.AddRecovered(points.OnePoint("over.cached", 1, 1))
				c.DivertToPointWriter(func(p *points.Points) error { diverted = append(diverted, p.Metric); return nil })
			}
			add := func(name string) {
				p := points.OnePoint(name, 1, 1)
				switch mode {
				case "tail":
					c.AddRecoveredFrom(p, source)
				case "capture", "captured-diversion":
					c.AddCapturedFrom(p, source)
				case "grouped-history", "grouped-live":
					c.AddRecoveryMetric(c.ResolveRecoveryMetric([]byte(name)), p, source, mode == "grouped-live")
				default:
					c.Add(p)
				}
			}
			add("over.new") // first input after restoring a complete over-quota index
			add("under.new")
			add("over.existing")
			add("over.tagged;b=2;a=1")
			if diversion {
				add("over.cached")
				for _, name := range diverted {
					if name == "over.new" {
						t.Fatal("over-quota new name entered restart WAL")
					}
				}
				if len(diverted) != 4 {
					t.Fatalf("diverted %v, want under.new, over.existing, over.tagged and over.cached", diverted)
				}
			} else {
				if len(c.Get("over.new")) != 0 {
					t.Fatal("first captured input bypassed creation quota")
				}
				for _, name := range []string{"under.new", "over.existing", "over.tagged;a=1;b=2"} {
					if len(c.Get(name)) != 1 {
						t.Fatalf("valid captured input %s was dropped", name)
					}
				}
				if source.Pending() != 3 {
					t.Fatalf("rejected capture pinned source: pending=%d, want 3", source.Pending())
				}
			}
			if got := c.AdmissionStats().Throttled; got != 1 {
				t.Fatalf("throttled points=%d, want 1", got)
			}
		})
	}
}

func TestNativeRestartCaptureQuotaAndThroughput(t *testing.T) {
	for _, mapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("mapped=%t", mapped), func(t *testing.T) {
			storePath := t.TempDir()
			options := store.Options{CacheSize: 1 << 20, MemTableSize: 1 << 20}
			db, err := store.Open(storePath, options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			for _, name := range []string{"over.existing", "over.second", "under.existing"} {
				if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average}); err != nil {
					t.Fatal(err)
				}
			}
			root, indexPath := t.TempDir(), filepath.Join(t.TempDir(), "files.gzip")
			listener := func() *CarbonserverListener {
				l := NewCarbonserverListener(nil)
				l.SetWhisperData(root)
				l.SetMetricStore(db)
				l.SetTrieIndex(true)
				l.SetConcurrentIndex(true)
				l.SetRealtimeIndex(10)
				l.SetScanFrequency(time.Hour)
				l.SetFileListCache(indexPath)
				l.SetFileListCacheVersion(2)
				l.SetQuotaUsageReportFrequency(time.Hour)
				l.SetEstimateSize(func(string) (int64, int64, int64) { return 1, 1, 1 })
				l.SetQuotas([]*Quota{{Pattern: "over", Metrics: 1, Throughput: 1}, {Pattern: "under", Metrics: 10, Throughput: 1}})
				return l
			}
			first := listener()
			if !mapped {
				first.SetFileListCacheVersion(1)
			}
			if err := first.RefreshMetricStoreIndex(); err != nil {
				t.Fatal(err)
			}
			first.SetFileListCacheVersion(2)
			if !first.PrepareReadCheckpoint() {
				t.Fatal("native catalog checkpoint preparation failed")
			}
			id, err := first.CheckpointReadIndex()
			if err != nil {
				t.Fatal(err)
			}
			if err := first.Stop(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = store.Open(storePath, options)
			if err != nil {
				t.Fatal(err)
			}
			next := listener()
			defer next.Stop()
			next.WarmupIndexWithPending(func(got string, _, _ func(string) error) error {
				if got != id {
					return fmt.Errorf("checkpoint ID %q, want %q", got, id)
				}
				return nil
			})
			next.WaitForWarmup()
			if !next.HasMappedIndex() {
				t.Fatal("saved native index was not restored")
			}
			if err := next.WaitForQuotaIndex(); err != nil {
				t.Fatal(err)
			}
			c := cache.New()
			c.SetThrottleWithHash(next.ShouldThrottleMetricWithHash)
			c.SetCreationThrottle(next.ShouldThrottleCreation)
			var stream cache.RecoveryStream
			raw := stream.NextCaptured()
			c.AddRecoveredFrom(points.OnePoint("over.new", 1, 1), raw)
			if len(c.Get("over.new")) != 0 || raw.Pending() != 0 {
				t.Fatal("first point after native restart bypassed quota")
			}
			c.AddCapturedFrom(points.OnePoint("under.new", 1, 1), raw)
			if len(c.Get("under.new")) != 1 {
				t.Fatal("under-quota capture was dropped")
			}
			// Accepted journal/checkpoint records remain durable even if limits
			// have since fallen below their usage. They consume no live window.
			c.AddRecoveredFrom(points.OnePoint("over.accepted", 1, 1), stream.NextJournal())
			c.AddRecoveredFrom(points.OnePoint("over.checkpoint", 1, 1), stream.Next())
			for _, name := range []string{"over.accepted", "over.checkpoint"} {
				if len(c.Get(name)) != 1 {
					t.Fatalf("accepted history %s was dropped", name)
				}
			}
			recorder := next.CurrentFileIndex().trieIdx.throughputs.load("under")
			if recorder.dpRecorder().total() != 0 {
				t.Fatal("capture replay consumed live throughput")
			}
			c.Add(points.OnePoint("under.existing", 2, 2))
			c.Add(points.OnePoint("under.existing", 3, 3))
			if len(c.Get("under.existing")) != 1 || recorder.dpRecorder().total() != 1 {
				t.Fatal("live throughput quota changed after capture replay")
			}
		})
	}
}

func TestQuotaIndexReadiness(t *testing.T) {
	synctest.Test(t, testQuotaIndexReadiness)
}

func testQuotaIndexReadiness(t *testing.T) {
	l := NewCarbonserverListener(nil)
	l.SetTrieIndex(true)
	l.SetConcurrentIndex(true)
	l.SetScanFrequency(time.Hour)
	l.SetQuotas([]*Quota{{Pattern: "/", Metrics: 10}})
	defer l.Stop()
	p := points.OnePoint("unknown.name", 1, 1)
	var state any
	if !l.ShouldThrottleMetric(p, false) || !l.ShouldThrottleMetricWithHash(p, false, 0, &state) || !l.ShouldThrottleCreation(p, false) {
		t.Fatal("unknown usage failed open")
	}
	done := make(chan error, 1)
	go func() { done <- l.WaitForQuotaIndex() }()
	synctest.Wait()
	select {
	case err := <-done:
		t.Fatalf("input opened before quota index: %v", err)
	default:
	}
	l.SetWhisperData(t.TempDir())
	l.SetEstimateSize(func(string) (int64, int64, int64) { return 1, 1, 1 })
	l.updateFileList(l.whisperData, map[string]struct{}{}, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("quota readiness did not open")
	}
	if l.ShouldThrottleCreation(p, false) {
		t.Fatal("known under-quota namespace rejected")
	}
	plain := NewCarbonserverListener(nil)
	defer plain.Stop()
	if err := plain.WaitForQuotaIndex(); err != nil || plain.ShouldThrottleCreation(p, false) {
		t.Fatal("unconfigured quotas gated input", err)
	}
	stopped := NewCarbonserverListener(nil)
	stopped.SetTrieIndex(true)
	stopped.SetScanFrequency(time.Hour)
	stopped.SetQuotas([]*Quota{{Pattern: "/", Metrics: 1}})
	_ = stopped.Stop()
	if err := stopped.WaitForQuotaIndex(); err == nil {
		t.Fatal("stopped index builder opened intake")
	}
	stopped.SetScanFrequency(0)
	if err := stopped.WaitForQuotaIndex(); err == nil {
		t.Fatal("disabled index builder opened intake")
	}
}
