package carbon

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/lomik/zapwriter"
)

func TestIndexWarmupOverlapsRestoreWithoutOpeningReceivers(t *testing.T) {
	defer zapwriter.Test()()
	root := t.TempDir()
	app := New(TestConfig(root))
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	cfg := app.Config
	cfg.Whisper.Compressed = true
	cfg.Whisper.Quotas = persister.WhisperQuotas{{Pattern: "/", Metrics: 1}}
	cfg.Dump.Enabled = true
	cfg.Dump.Path = root
	cfg.Dump.RestorePerSecond = 0
	cfg.Whisper.MaxUpdatesPerSecond = 1
	cfg.Udp.Enabled = false
	cfg.Pickle.Enabled = false
	cfg.Grpc.Enabled = false
	cfg.Carbonlink.Enabled = false
	cfg.Tcp.Enabled = true
	reserve := func() string {
		t.Helper()
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		return l.Addr().String()
	}
	cfg.Tcp.Listen = reserve()
	cfg.Carbonserver.Enabled = true
	cfg.Carbonserver.Listen = reserve()
	cfg.Carbonserver.TrieIndex = true
	cfg.Carbonserver.ConcurrentIndex = true
	cfg.Carbonserver.RealtimeIndex = 10
	cfg.Carbonserver.FileListCache = filepath.Join(t.TempDir(), "flc")
	flc, err := carbonserver.NewFileListCache(cfg.Carbonserver.FileListCache, carbonserver.FLCVersion2, 'w')
	if err != nil {
		t.Fatal(err)
	}
	if err := flc.Write(&carbonserver.FLCEntry{Path: "/existing/metric.wsp", LogicalSize: 1024, PhysicalSize: 1024, DataPoints: 60}); err != nil {
		t.Fatal(err)
	}
	if err := flc.Close(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	if err := os.WriteFile(filepath.Join(root, "input.1.1"), []byte(fmt.Sprintf("restore.one 1 %d\nrestore.two 2 %d\n", now, now)), 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- app.Start() }()
	// Logs are synchronized by zapwriter.Test; do not read App fields while Start
	// holds its mutex. Wait for the actual index publication during throttled disk drain.
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(zapwriter.TestString(), "file list updated") {
		select {
		case err := <-done:
			t.Fatalf("Start returned before index warmup during replay: %v\n%s", err, zapwriter.TestString())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("index did not warm during restore")
		}
		time.Sleep(time.Millisecond)
	}
	for _, addr := range []string{cfg.Tcp.Listen, cfg.Carbonserver.Listen} {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			t.Errorf("listener %s opened before restore drained", addr)
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("startup stalled")
	}
	defer app.Stop()
	for _, name := range []string{"one", "two"} {
		if _, err := os.Stat(filepath.Join(root, "restore", name+".wsp")); err != nil {
			t.Fatalf("live quota rejected restored file %s: %v", name, err)
		}
	}
	if !app.Cache.IsEmpty() {
		t.Fatal("receivers started before restore drained")
	}
	logs := zapwriter.TestString()
	if strings.Index(logs, "file list updated") > strings.Index(logs, "dump restored, starting receivers") {
		t.Fatal("index load was serialized after restore")
	}
	deadline = time.Now().Add(5 * time.Second)
	for !app.Carbonserver.MetricExists("restore.two") {
		if time.Now().After(deadline) {
			t.Fatal("post-restore scan missed restored metric")
		}
		time.Sleep(time.Millisecond)
	}
}
