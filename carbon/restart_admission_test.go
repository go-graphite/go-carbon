//go:build unix

package carbon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/receiver/tcp"
	whisper "github.com/go-graphite/go-whisper"
	"github.com/lomik/zapwriter"
)

func TestDumpStopUnsupportedSuccessorKeepsNativeWriterAndMigration(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := incrementalAppFixture(t)
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Dump.Enabled, cfg.Dump.HandoffSupervisor, cfg.Dump.StoreInputHandoff, cfg.Dump.RequireStartupCapture = true, true, true, true
	cfg.Dump.Path, cfg.Dump.Format = t.TempDir(), "compact"
	cfg.InputLog.Enabled, cfg.InputLog.Path = true, filepath.Join(t.TempDir(), "journal")
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	if !app.Carbonserver.HasMappedIndex() {
		app.Stop()
		app = startIncrementalApp(t, path)
	}
	awaitCapture(t, app.Carbonserver.HasMappedIndex)
	conn, err := net.Dial("tcp", app.Receivers[0].Receiver.(*tcp.TCP).Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	write := func(name string, value float64) {
		t.Helper()
		stamp := int(time.Now().Unix() - 2)
		if _, err := fmt.Fprintf(conn, "%s %g %d\n", name, value, stamp); err != nil {
			t.Fatal(err)
		}
		awaitCapture(t, func() bool {
			series, err := app.MetricStore.Fetch(context.Background(), name, stamp-1, stamp+1)
			if err != nil {
				return false
			}
			for i, point := range series.Values {
				if series.FromTime+i*series.Step == stamp && point == value {
					return true
				}
			}
			return false
		})
	}
	write("before.admission", 1.125)
	writer, migration := app.Persister, app.incrementalMigration.Load()
	released := false
	app.SetReadHandoff("replaced-by-resolver", func() { released = true })
	// This binary knows the older socket protocol but lacks admission/capture
	// configuration checks. The live service-manager choice must be checked.
	app.SetSuccessorResolver(func() string { return storeHandoffSuccessor(t, true) })
	previousCapture := app.startupCaptureState.Load()
	app.startupCaptureState.Store(&startupInput{})
	err = app.DumpStop()
	app.startupCaptureState.Store(previousCapture)
	if !errors.Is(err, ErrRestartRefused) || !strings.Contains(err.Error(), "unfinished") {
		t.Fatalf("pending capture did not refuse before stopping: %v", err)
	}
	app.SetSuccessorResolver(func() string { return "" })
	err = app.DumpStop()
	if !errors.Is(err, ErrRestartRefused) || !strings.Contains(err.Error(), "cannot determine") {
		t.Fatalf("unknown actual successor was not refused: %v", err)
	}
	app.SetSuccessorResolver(func() string { return storeHandoffSuccessor(t, true) })
	err = app.DumpStop()
	if !errors.Is(err, ErrRestartRefused) || !strings.Contains(err.Error(), "successor rejected") {
		t.Fatalf("actual unsupported successor was not refused: %v", err)
	}
	if released || app.Persister != writer || app.incrementalMigration.Load() != migration {
		t.Fatal("refusal changed incumbent ownership/writer/migration")
	}
	select {
	case <-migration.done:
		t.Fatal("refusal cancelled live migration")
	default:
	}
	files, err := os.ReadDir(cfg.Dump.Path)
	if err != nil || len(files) != 0 {
		t.Fatal("refusal began a dump", files, err)
	}
	write("after.admission", 2.125)
}

func startupAdmissionConfig(t *testing.T) *Config {
	t.Helper()
	c := handoffTestConfig(t, t.TempDir())
	c.Whisper.StorageBackend = "pebble-chunk"
	c.Dump.HandoffSupervisor = true
	c.Dump.StoreInputHandoff = true
	c.InputLog.Enabled = true
	return c
}

func TestStartupCaptureAdmissionRefusesFallbackConfigurations(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"dump disabled", func(c *Config) { c.Dump.Enabled = false }},
		{"supervisor disabled", func(c *Config) { c.Dump.HandoffSupervisor = false }},
		{"handoff disabled", func(c *Config) { c.Dump.StoreInputHandoff = false }},
		{"journal disabled", func(c *Config) { c.InputLog.Enabled = false }},
		{"file store", func(c *Config) { c.Whisper.StorageBackend = "whisper" }},
		{"mirrored store", func(c *Config) { c.Whisper.StoreMirrorFiles = true }},
		{"tags", func(c *Config) { c.Tags.Enabled = true }},
		{"buffered tcp", func(c *Config) { c.Tcp.BufferSize = 1 }},
		{"fixed compression", func(c *Config) { c.Tcp.Compression = "zstd" }},
		{"udp", func(c *Config) { c.Udp.Enabled = true }},
		{"pickle", func(c *Config) { c.Pickle.Enabled = true }},
		{"no read index", func(c *Config) { c.Carbonserver.Enabled = false }},
		{"heap-only index", func(c *Config) { c.Carbonserver.FileListCacheVersion = 1 }},
		{"relative dump", func(c *Config) { c.Dump.Path = "relative" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := startupAdmissionConfig(t)
			tc.change(c)
			if err := CheckStartupCaptureAdmission(c); err == nil {
				t.Fatal("configuration could stop with startup capture unavailable")
			}
		})
	}
}

func TestStartupCaptureAdmissionHeadroom(t *testing.T) {
	for _, p := range []startupCapturePolicy{
		{available: 64<<20 - 1, spoolBytes: 1 << 30},
		{available: 1 << 30, spoolBytes: 64<<20 - 1},
	} {
		if err := checkStartupCaptureHeadroom(p); err == nil {
			t.Fatal("accepted ordered-recovery fallback")
		}
	}
	if err := checkStartupCaptureHeadroom(startupCapturePolicy{available: 64 << 20, spoolBytes: 64 << 20}); err != nil {
		t.Fatal(err)
	}
}

// A refused planned stop must preserve the working receiver and writer, rather
// than merely returning an error after teardown or leaving a diversion behind.
func TestDumpStopRefusalLeavesIncumbentReceivingAndPersisting(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	root := t.TempDir()
	app := New(filepath.Join(root, "go-carbon.conf"))
	app.Config = handoffTestConfig(t, root)
	app.Config.Whisper.Compressed = false
	app.Config.Whisper.OutOfOrder = false
	cfg := startHandoffTestApp(t, app)
	defer app.Stop()
	conn, err := net.Dial("tcp", cfg.Tcp.Listen)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	write := func(name string, value int) {
		t.Helper()
		stamp := time.Now().Unix() - 2
		if _, err := fmt.Fprintf(conn, "%s %d %d\n", name, value, stamp); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(cfg.Whisper.DataDir, strings.ReplaceAll(name, ".", "/")+".wsp")
		awaitCapture(t, func() bool {
			w, err := whisper.Open(path)
			if err != nil {
				return false
			}
			points, err := w.ArchivePoints(0)
			_ = w.Close()
			if err != nil {
				return false
			}
			for _, p := range points {
				if p.Time == int(stamp) && p.Value == float64(value) {
					return true
				}
			}
			return false
		})
	}
	write("before.refusal", 1)
	writer := app.Persister
	app.Config.Dump.RequireStartupCapture = true
	err = app.DumpStop()
	if !errors.Is(err, ErrRestartRefused) {
		t.Fatalf("unsafe stop was not refused before teardown: %v", err)
	}
	if app.Persister != writer || len(app.Receivers) != 1 {
		t.Fatal("refusal changed live components")
	}
	write("after.refusal", 2)
	if app.Cache.Backlogged() {
		t.Fatal("refused restart left input backlogged")
	}
}
