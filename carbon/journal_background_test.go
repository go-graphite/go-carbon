package carbon

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

// The journal is larger than RAM admission and persistence is deliberately slow.
// Start must release read/background readiness without waiting for its drain.
func TestJournalOnlyStartDoesNotWaitForRecoveryCapacity(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := sharedAppConfig(t)
	cfg.Common.MaxCPU = 2
	cfg.Dump.Enabled, cfg.Dump.StoreInputHandoff = true, true
	cfg.Dump.Path = t.TempDir()
	cfg.InputLog.Enabled = true
	cfg.InputLog.Path = filepath.Join(t.TempDir(), "journal")
	cfg.InputLog.SyncInterval = Duration{time.Hour}
	cfg.InputLog.MaxAge = Duration{1000 * time.Hour}
	cfg.InputLog.SegmentSize = 16 << 20
	cfg.Cache.MaxSize = 1
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Whisper.Workers = 1
	cfg.Whisper.MaxUpdatesPerSecond = 1
	writer := New("")
	writer.Config, writer.Cache = cfg, cache.New()
	if err := writer.openInputLog(writer.Cache); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().Unix() - 10
	for i := range 40 {
		writer.Cache.Add(points.OnePoint(fmt.Sprintf("journal.background.%03d", i), float64(i), stamp))
	}
	if err := writer.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	writer.closeInputLog()
	writer.Cache.Stop()
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- app.Start() }()
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start waited for the journal to fit in cache")
	}
	defer app.Stop()
	if app.startupInput == nil || !app.startupInput.journalOnly {
		t.Fatal("journal-only fixture did not capture input")
	}
	if app.startupInput.historyReady.Load() {
		t.Fatal("fixture failed to retain a journal backlog")
	}
	select {
	case <-app.metadataPrewarmReady:
	default:
		t.Fatal("background readiness is still gated")
	}
	awaitCapture(t, func() bool { return app.Cache.Size() > 0 })
	awaitCapture(t, func() bool { return app.Persister.CommittedPoints() > 0 })
	if got := app.inputLog.Load().Metrics().ReplayedPoints; got >= 40 {
		t.Fatal("Start returned only after draining the fixture", got)
	}
	app.Stop()
	// A stopped partial replay retains the original journal authority. Opening
	// it again must still recover the complete original sequence bit for bit.
	log, err := inputlog.Open(cfg.InputLog.Path, inputlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	got := map[string][]points.Point{}
	var mu sync.Mutex
	stats, err := log.Replay(func(p *points.Points) {
		mu.Lock()
		got[p.Metric] = append(got[p.Metric], p.Data...)
		mu.Unlock()
	})
	if err != nil || stats.Points != 40 {
		t.Fatal("partial background replay lost journal authority", stats, err)
	}
	for i := range 40 {
		p := got[fmt.Sprintf("journal.background.%03d", i)]
		if len(p) != 1 || p[0].Timestamp != stamp || p[0].Value != float64(i) {
			t.Fatal("retained journal changed", i, p)
		}
	}
}
