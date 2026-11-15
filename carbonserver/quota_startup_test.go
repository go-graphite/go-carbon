package carbonserver

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
)

// Recover a validated discovery checkpoint while pending replay keeps scans gated.
func quotaRecoveryListener(t *testing.T, shared bool) *CarbonserverListener {
	t.Helper()
	cache, root, _ := snapshotFixture(t)
	first := overlayListener(t, cache, root)
	id, err := first.CheckpointReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	l := unstartedOverlayListener(t, cache, root)
	if shared {
		db, err := store.Open(t.TempDir(), store.Options{CacheSize: 1 << 20, MemTableSize: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		// Stop the listener before closing its backing store.
		t.Cleanup(func() { _ = l.Stop() })
		l.SetMetricStore(db)
		l.SetLegacyWhisperDisabled(true)
	}
	l.SetStartupScanGate(make(chan struct{}))
	l.WarmupIndexWithPending(func(got string, visit, _ func(string) error) error {
		if got != id {
			return fmt.Errorf("checkpoint ID %q, want %q", got, id)
		}
		return visit("pending.recovered")
	})
	waitWarmup(t, l)
	return l
}

func TestRecoveryFirstPublicationEnforcesQuotas(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared=%t", shared), func(t *testing.T) {
			l := quotaRecoveryListener(t, shared)
			if !l.MetricExists("pending.recovered") {
				t.Fatal("pending name missing from first publication")
			}
			idx := l.CurrentFileIndex()
			if idx == nil || idx.trieIdx.quotaNodes["/"] == nil {
				t.Fatal("first publication has no quota nodes")
			}
			if !l.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) {
				t.Fatal("first publication bypasses finite metric quota")
			}
			if l.ShouldThrottleMetric(points.OnePoint("pending.recovered", 1, 1), false) {
				t.Fatal("creation quota rejected recovered name")
			}
		})
	}
}

func TestRecoveryGateServicesQuotaReloadAndStats(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared=%t", shared), func(t *testing.T) {
			l := quotaRecoveryListener(t, shared)
			// Isolate updater servicing from initial-publication quota initialization.
			l.refreshQuotaAndUsage(nil)
			l.quotaAndUsageMetrics = make(chan []points.Points, 1)
			l.SetQuotaUsageReportFrequency(10 * time.Millisecond)
			reloaded := watchListenerLog(l, "quota rules reloaded")
			scanned := make(chan struct{}, 1)
			l.cacheGetRecentMetrics = func() []map[string]struct{} {
				select {
				case scanned <- struct{}{}:
				default:
				}
				return nil
			}
			gate := make(chan struct{})
			l.SetStartupScanGate(gate)
			scan, force := make(chan time.Time, 1), make(chan struct{}, 1)
			scan <- time.Now()
			force <- struct{}{}
			exit, done := make(chan struct{}), make(chan struct{})
			go func() { defer close(done); l.fileListUpdater(l.whisperData, scan, force, exit) }()
			defer func() {
				close(exit)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("updater did not stop")
				}
			}()
			for _, limit := range []int64{1000, 1} {
				if err := l.ReloadQuotas([]*Quota{{Pattern: "/", Metrics: limit}}); err != nil {
					t.Fatal(err)
				}
				waitListenerEvent(t, reloaded)
				if got := l.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false); got != (limit == 1) {
					t.Fatalf("limit %d: throttled=%t", limit, got)
				}
			}
			select {
			case <-l.quotaAndUsageMetrics:
				if atomic.LoadUint64(&l.metrics.MetricsKnown) == 0 {
					t.Fatal("quota stats did not count recovered names")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("quota statistics blocked by startup gate")
			}
			select {
			case <-scanned:
				t.Fatal("scan ran before pending recovery gate opened")
			default:
			}
			close(gate)
			select {
			case <-scanned:
			case <-time.After(5 * time.Second):
				t.Fatal("queued scan did not resume after recovery")
			}
		})
	}
}
