package carbonserver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func overlayListener(t *testing.T, cache, root string) *CarbonserverListener {
	t.Helper()
	l := NewCarbonserverListener(nil)
	l.SetWhisperData(root)
	l.SetFileListCache(cache)
	l.SetTrieIndex(true)
	l.SetConcurrentIndex(true)
	l.SetRealtimeIndex(100)
	l.SetScanFrequency(time.Hour)
	l.SetQuotaUsageReportFrequency(time.Minute)
	l.SetEstimateSize(func(string) (int64, int64, int64) { return 1024, 4096, 60 })
	l.SetQuotas([]*Quota{{Pattern: "/", Metrics: 1}})
	l.WarmupIndex()
	l.WaitForWarmup()
	t.Cleanup(func() { _ = l.Stop() })
	return l
}

func TestSnapshotOverlayCheckpoint(t *testing.T) {
	cache, root, _ := snapshotFixture(t)
	first := overlayListener(t, cache, root)
	first.insertRealtimeMetric(first.CurrentFileIndex().trieIdx, "new.persisted")
	first.newMetricsChan <- "new.queued"
	id, err := first.CheckpointReadIndex()
	if err != nil || id == "" {
		t.Fatal(id, err)
	}
	if len(first.newMetricsChan) != 0 {
		t.Fatal("queued notification not checkpointed")
	}
	next := overlayListener(t, cache, root)
	if id != next.RecoveryIndexID() {
		t.Fatal("checkpoint identity changed")
	}
	if got, want := next.CurrentFileIndex().trieIdx.allMetrics('.'), first.CurrentFileIndex().trieIdx.allMetrics('.'); !reflect.DeepEqual(got, want) {
		t.Fatalf("catalogue differs %v != %v", got, want)
	}
	if !next.ShouldThrottleMetric(points.OnePoint("another.metric", 1, 1), false) {
		t.Fatal("initial quota missing")
	}
	if err = next.PreparePendingReadIndex(func(visit func(string) error) error { return visit("wal.only") }); err != nil {
		t.Fatal(err)
	}
	if !next.MetricExists("wal.only") {
		t.Fatal("unindexed WAL name missing")
	}
	again, err := next.CheckpointReadIndex()
	if err != nil || again == id {
		t.Fatal("overlay identity did not change", err)
	}
	// The previous generation's in-flight queries remain valid after replacement.
	if !first.MetricExists("new.queued") {
		t.Fatal("old reader invalidated")
	}
	last := overlayListener(t, cache, root)
	if last.RecoveryIndexID() != again || !last.MetricExists("wal.only") {
		t.Fatal("second checkpoint incomplete")
	}
}

func TestSnapshotOverlayRejectsCorruptOrStaleCheckpoint(t *testing.T) {
	for _, kind := range []string{"checksum", "count", "base", "version", "path"} {
		t.Run(kind, func(t *testing.T) {
			cache, root, _ := snapshotFixture(t)
			first := overlayListener(t, cache, root)
			first.insertRealtimeMetric(first.CurrentFileIndex().trieIdx, "overlay.metric")
			if _, err := first.CheckpointReadIndex(); err != nil {
				t.Fatal(err)
			}
			m, err := readOverlayManifest(overlayManifestPath(cache))
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "checksum":
				if err = os.WriteFile(filepath.Join(filepath.Dir(cache), m.File.Name), []byte("broken"), 0600); err != nil {
					t.Fatal(err)
				}
			case "count":
				m.Records++
			case "base":
				m.Base = "other"
			case "version":
				m.Version++
			case "path":
				m.File.Name = "../escape.flc"
			}
			raw, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(overlayManifestPath(cache), raw, 0600); err != nil {
				t.Fatal(err)
			}
			next := overlayListener(t, cache, root)
			if next.RecoveryIndexID() != "" || next.MetricExists("overlay.metric") {
				t.Fatal("invalid overlay published")
			}
			if !next.HasMappedIndex() || !next.MetricExists("a.value") {
				t.Fatal("base fallback lost")
			}
		})
	}
}

func TestCompletedSnapshotInstallsWhenShutdownStarts(t *testing.T) {
	l := savedIndex(t, "existing.metric")
	l.updateFileList(l.whisperData, nil, nil)
	previous := l.CurrentFileIndex()
	u := newFileListUpdate(l, nil)
	if !u.loadFileListCache(false) || !u.scanFiles(l.whisperData, nil) {
		t.Fatal("scan failed")
	}
	// A notification handled during the first conversion must survive without
	// walking the entire old trie again.
	l.newMetricsChan <- "during.scan"
	u.drainRealtimeMetrics()
	u.pruneRealtimeMetrics()
	u.closeFileListCaches()
	if !u.snapshotReady {
		t.Fatal("complete snapshot missing")
	}
	// Shutdown arrives after durable publication but before the live swap.
	l.PauseIndexUpdates()
	u.replaceSnapshot()
	u.publish(l.whisperData, nil)
	if l.CurrentFileIndex() == previous || !l.HasMappedIndex() {
		t.Fatal("durable generation not installed")
	}
	if !l.MetricExists("during.scan") {
		t.Fatal("first conversion lost concurrent notification")
	}
	id, err := l.CheckpointReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	next := overlayListener(t, l.fileListCache, l.whisperData)
	if next.RecoveryIndexID() != id || !next.MetricExists("during.scan") {
		t.Fatal("checkpoint differs from durable base")
	}
}
