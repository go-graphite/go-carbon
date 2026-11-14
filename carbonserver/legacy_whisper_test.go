package carbonserver

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestLegacyWhisperCompletionRefusesFallbackAndFilesystemGlob(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "unexpected.wsp")
	w, err := whisper.Create(path, whisper.MustParseRetentionDefs("1s:1h"), whisper.Average, .5)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, IncrementalSource: root, Continuation: &whisperio.ContinuationCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l := NewCarbonserverListener(cache.New().Get)
	l.SetWhisperData(root)
	l.SetMetricStore(db)
	l.SetMetricStoreFileIndex(true)
	l.logger = zap.NewNop()
	now := int32(time.Now().Unix())
	if _, err = l.fetchFromDisk(context.Background(), "unexpected", now-30, now-1); err != nil {
		t.Fatalf("migrating fallback: %v", err)
	}
	if _, err = l.getMetricInfo("unexpected"); err != nil {
		t.Fatalf("migrating info fallback: %v", err)
	}
	if got := l.matchGlobFilesystem([]string{"*.wsp"}); len(got) != 1 {
		t.Fatalf("migrating glob: %v", got)
	}
	l.metrics.LockFiles = 2
	l.metrics.OOOFiles = 3
	l.metrics.OOOPhysicalBytes = 40
	l.SetLegacyWhisperDisabled(true)
	// Keep a real, readable source present: returning its points or metadata is forbidden.
	if _, err = l.fetchFromDisk(context.Background(), "unexpected", now-30, now-1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("completed fallback: %v", err)
	}
	if _, err = l.getMetricInfo("unexpected"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("completed info: %v", err)
	}
	if got := l.expandGlobFiles([]string{"*.wsp"}, true); len(got) != 0 {
		t.Fatalf("completed glob consulted source: %v", got)
	}
	if l.metrics.LockFiles != 0 || l.metrics.OOOFiles != 0 || l.metrics.OOOPhysicalBytes != 0 {
		t.Fatal("source gauges retained stale values")
	}
	u := newFileListUpdate(l, nil)
	if u.scanFiles(filepath.Join(root, "missing"), nil) || !u.scanCancelled {
		t.Fatal("completed source scan was started")
	}
}

func TestUnexpectedWhisperFileIsLoud(t *testing.T) {
	var logs bytes.Buffer
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.ErrorLevel)
	l := NewCarbonserverListener(nil)
	l.logger = zap.New(core)
	reg := prom.NewRegistry()
	l.InitPrometheus(reg)
	l.SetLegacyWhisperDisabled(true)
	path := filepath.Join(t.TempDir(), "unexpected.wsp")
	if err := os.WriteFile(path, []byte("never read"), 0600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	u := newFileListUpdate(l, nil)
	if err = u.walkFile(path, info, nil, nil); err != filepath.SkipAll {
		t.Fatalf("unexpected file accepted: %v", err)
	}
	if atomic.LoadUint64(&l.metrics.UnexpectedWhisperFiles) != 1 || strings.Count(logs.String(), `"level":"error"`) != 1 {
		t.Fatal("missing ERROR or counter")
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, family := range families {
		if family.GetName() == "unexpected_whisper_files_total" {
			found = true
			if family.Metric[0].GetCounter().GetValue() != 1 {
				t.Fatal("Prometheus counter did not increment")
			}
		}
	}
	if !found {
		t.Fatal("Prometheus counter not registered")
	}
}

func TestLegacyWhisperCompletionDrainsReaders(t *testing.T) {
	l := NewCarbonserverListener(nil)
	l.legacyWhisperMu.RLock()
	done := make(chan struct{})
	go func() { l.SetLegacyWhisperDisabled(true); close(done) }()
	deadline := time.Now().Add(time.Second)
	for !l.legacyWhisperDisabled.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !l.legacyWhisperDisabled.Load() {
		t.Fatal("new source operations were not stopped")
	}
	select {
	case <-done:
		t.Fatal("completion returned before source reader finished")
	default:
	}
	l.legacyWhisperMu.RUnlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("completion did not drain")
	}
}

// A checkpoint or new metric can still exist only in cache after all source
// files have been converted. A store miss must preserve that read path.
func TestLegacyWhisperCompletionPreservesCacheOnlyReads(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, IncrementalSource: root, Continuation: &whisperio.ContinuationCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := int32(time.Now().Unix())
	cached := []points.Point{{Timestamp: int64(now - 20), Value: .125}, {Timestamp: int64(now - 10), Value: 1e100}}
	oracle := newCacheOnlyListener(t, t.TempDir(), cached, whisper.Average, 0)
	want, err := oracle.fetchSingleMetric(context.Background(), cacheOnlyMetric, "", now-30, now-1)
	if err != nil {
		t.Fatal(err)
	}
	l := newCacheOnlyListener(t, root, cached, whisper.Average, 0)
	l.SetMetricStore(db)
	for _, complete := range []bool{false, true} {
		l.SetLegacyWhisperDisabled(complete)
		got, err := l.fetchSingleMetric(context.Background(), cacheOnlyMetric, "", now-30, now-1)
		if err != nil {
			t.Fatalf("complete=%v: %v", complete, err)
		}
		if n := sameSeries(t, "cache-only completion", got, want); n == 0 {
			t.Fatal("no points compared")
		}
	}
}
