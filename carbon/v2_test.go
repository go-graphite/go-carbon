package carbon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-whisper"
)

func v2TestApp(t *testing.T, root string) *App {
	t.Helper()
	app := New(testConfig(t, root))
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	c := app.Config
	c.V2 = v2Config{Enabled: true, IDKeys: c.V2.IDKeys, Path: filepath.Join(root, "v2"), Producers: 2, Shards: 2}
	c.Whisper.DataDir = filepath.Join(root, "empty")
	c.Whisper.StoreDir = ""
	if err := os.MkdirAll(c.Whisper.DataDir, 0700); err != nil {
		t.Fatal(err)
	}
	c.Udp.Enabled = false
	c.Pickle.Enabled = false
	c.Carbonlink.Enabled = false
	c.Grpc.Enabled = false
	c.Buckyd.Enabled = false
	c.Dump.Enabled = false
	c.InputLog.Enabled = false
	c.Tcp.Enabled = true
	c.Tcp.Listen = "127.0.0.1:0"
	c.Carbonserver.Enabled = true
	c.Carbonserver.Listen = "127.0.0.1:0"
	c.Carbonserver.Grpc.Enabled = false
	c.Carbonserver.TrieIndex = true
	c.Carbonserver.TrigramIndex = false
	c.Carbonserver.ScanFrequency = &Duration{time.Hour}
	return app
}

// setV2Mode selects a storage mode. Dual modes run the classic input log,
// which recovers the classic engine after a crash.
func setV2Mode(a *App, mode string) {
	a.Config.V2.Mode = mode
	a.Config.InputLog.Enabled = mode == v2Shadow || mode == v2ServingDual
	a.Config.InputLog.Path = filepath.Join(filepath.Dir(a.Config.V2.Path), "input-log")
	a.Config.InputLog.SyncInterval = Duration{10 * time.Millisecond}
}

func v2HTTP(t *testing.T, app *App, path string) []byte {
	t.Helper()
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + app.Carbonserver.HTTPListener().Addr().String() + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("%s: %d %s", path, resp.StatusCode, b)
	}
	return b
}

// waitV2CatalogName waits for the background maintenance owner to publish
// name: startup discovery is published off the request path,
// bounded by the same 2.2 s visibility contract as live names.
func waitV2CatalogName(t *testing.T, app *App, name string) {
	t.Helper()
	deadline := time.Now().Add(2200 * time.Millisecond)
	for !slices.Contains(slices.Collect(app.v2.catalog.Snapshot().Names()), name) {
		if time.Now().After(deadline) {
			t.Fatalf("catalog did not publish %q within 2.2s", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func TestV2TCPRenderFindInfoRestart(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	root := t.TempDir()
	app := v2TestApp(t, root)
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if app != nil {
			app.Stop()
		}
	}()
	// The policy uses sixty-second primary slots, matching TestConfig's classic
	// schema. Compare the actual HTTP JSON values to an independent Whisper file.
	now := time.Now().Unix()
	stamp := now - now%60 - 120
	classic, err := whisper.Create(filepath.Join(root, "oracle.wsp"), app.Config.Whisper.Schemas[0].Retentions, whisper.Average, 0.5)
	if err != nil {
		t.Fatal(err)
	}
	defer classic.Close()
	input := []*whisper.TimeSeriesPoint{{Time: int(stamp), Value: 3.25}, {Time: int(stamp + 60), Value: -7.5}}
	for _, p := range input {
		if err := classic.UpdateMany([]*whisper.TimeSeriesPoint{p}); err != nil {
			t.Fatal(err)
		}
	}
	// Keep a connection open while querying and stopping, as a relay does.
	conn, err := net.Dial("tcp", app.v2.server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, p := range input {
		if _, err := fmt.Fprintf(conn, "wire.metric %g %d\n", p.Value, p.Time); err != nil {
			t.Fatal(err)
		}
	}
	// Receiver buffers must publish at idle without requiring sender EOF.
	deadline := time.Now().Add(8 * time.Second)
	for {
		names, err := app.v2.reader.Names(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(names) > 0 && app.v2.catalog.Snapshot().Len() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle TCP did not publish names")
		}
		time.Sleep(20 * time.Millisecond)
	}
	check := func() {
		deadline := time.Now().Add(2200 * time.Millisecond)
		for !slices.Contains(slices.Collect(app.v2.catalog.Snapshot().Names()), "wire.metric") {
			if time.Now().After(deadline) {
				t.Fatal("recovered catalog not published within 2.2s")
			}
			time.Sleep(10 * time.Millisecond)
		}
		b := v2HTTP(t, app, "/metrics/find/?format=json&query=wire.*")
		if !strings.Contains(string(b), "wire.metric") {
			t.Fatalf("find: %s", b)
		}
		b = v2HTTP(t, app, "/info/?format=json&target=wire.metric")
		if !strings.Contains(strings.ToLower(string(b)), "average") {
			t.Fatalf("info: %s", b)
		}
		path := fmt.Sprintf("/render/?format=json&target=wire.metric&from=%d&until=%d", stamp-60, stamp+60)
		if _, err := app.v2.reader.Fetch(t.Context(), "wire.metric", int(stamp-60), int(stamp+60)); err != nil {
			t.Fatalf("direct reader: %v", err)
		}
		b = v2HTTP(t, app, path)
		var body struct {
			Metrics []struct {
				Values   []float64 `json:"values"`
				IsAbsent []bool    `json:"isAbsent"`
				Start    int       `json:"startTime"`
				Step     int       `json:"stepTime"`
			} `json:"metrics"`
		}

		if err := json.Unmarshal(b, &body); err != nil {
			t.Fatalf("render %s: %v", b, err)
		}
		expected, err := classic.Fetch(int(stamp-60), int(stamp+60))
		if err != nil {
			t.Fatal(err)
		}
		if len(body.Metrics) != 1 || len(body.Metrics[0].Values) != len(expected.Values()) {
			t.Fatalf("render shape %s expected %v", b, expected.Values())
		}
		for i, want := range expected.Values() {
			got := body.Metrics[0].Values[i]
			if math.IsNaN(want) {
				if !body.Metrics[0].IsAbsent[i] {
					t.Fatalf("slot %d expected null", i)
				}
			} else if body.Metrics[0].IsAbsent[i] || math.Float64bits(got) != math.Float64bits(want) {
				t.Fatalf("slot %d got %v want %g: %s", i, got, want, b)
			}
		}
	}
	check()
	// Close sender first for a clean drain; daemon shutdown of open connections
	// is separately bounded. Reopen the same journal/core, then continue intake.
	_ = conn.Close()
	app.Stop()
	app = nil
	app = v2TestApp(t, root)
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	check()
	conn2, err := net.Dial("tcp", app.v2.server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(conn2, "wire.metric 11 %d\n", stamp+60)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn2.Close()
	deadline = time.Now().Add(8 * time.Second)
	for app.v2.server.Stats().Accepted < 1 {
		if time.Now().After(deadline) {
			t.Fatal("restart intake failed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := classic.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(stamp + 60), Value: 11}}); err != nil {
		t.Fatal(err)
	}
	for {
		series, err := app.v2.reader.Fetch(t.Context(), "wire.metric", int(stamp-60), int(stamp+60))
		if err == nil && series != nil && len(series.Values()) == 2 && series.Values()[1] == 11 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("correction after restart not visible: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	check()
	// Verify the second epoch's correction survives another exact same-root open.
	app.Stop()
	app = nil
	app = v2TestApp(t, root)
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	check()
}
func TestV2LifecycleConfigurationAccepted(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, mode := range []string{"expiry", "quota"} {
		t.Run(mode, func(t *testing.T) {
			app := v2TestApp(t, t.TempDir())
			if mode == "expiry" {
				app.Config.Whisper.StoreExpiration = &Duration{time.Hour}
			} else {
				app.Config.Whisper.Quotas = persister.WhisperQuotas{{Pattern: "/", Metrics: 1, DroppingPolicy: "new"}}
			}
			if err := app.Start(); err != nil {
				app.Stop()
				t.Fatal(err)
			}
			defer app.Stop()
			if mode == "quota" && app.v2.quotas == nil {
				t.Fatal("quota admission missing")
			}
		})
	}
}

func TestV2RejectsCorruptClassicAndPhysicalQuota(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, mode := range []string{"classic", "physical-quota"} {
		t.Run(mode, func(t *testing.T) {
			app := v2TestApp(t, t.TempDir())
			if mode == "classic" {
				if err := os.WriteFile(filepath.Join(app.Config.Whisper.DataDir, "exists.wsp"), []byte("existing"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				app.Config.Whisper.Quotas = persister.WhisperQuotas{{Pattern: "/", PhysicalSize: 1, DroppingPolicy: "new"}}
			}
			if err := app.Start(); err == nil {
				app.Stop()
				t.Fatal("accepted corrupt history/unsupported physical quota")
			}
			app.Stop()
		})
	}
}

// The child is killed by the parent while a real connection is still sending.
// Only the explicitly observed durable prefix is required after recovery.
func TestV2CrashChild(t *testing.T) {
	root := os.Getenv("V2_WIRE_CRASH_ROOT")
	if root == "" {
		t.Skip("subprocess helper")
	}
	stamp, err := strconv.ParseInt(os.Getenv("V2_WIRE_CRASH_STAMP"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	app := v2TestApp(t, root)
	setV2Mode(app, os.Getenv("V2_WIRE_MODE"))
	if app.Config.V2.Mode == v2ServingDual {
		app.Config.Whisper.Schemas[0].Retentions = app.Config.Whisper.Schemas[0].Retentions[:1]
	}
	if app.Config.V2.Mode == v2Shadow {
		app.Config.Carbonserver.ScanFrequency = &Duration{20 * time.Millisecond}
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", app.v2.server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fmt.Fprintf(conn, "crash.durable 3.25 %d\n", stamp); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for {
		series, err := app.v2.reader.Fetch(t.Context(), "crash.durable", int(stamp-60), int(stamp))
		if err == nil && series != nil && len(series.Values()) == 1 && series.Values()[0] == 3.25 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("durable prefix not published: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if app.Config.InputLog.Enabled {
		// The mirror ran before v2 published; let the input log sync it.
		time.Sleep(200 * time.Millisecond)
	}
	if err := os.WriteFile(filepath.Join(root, "crash-ready"), []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; ; i++ {
		if _, err = fmt.Fprintf(conn, "crash.tail %d %d\n", i, stamp); err != nil {
			t.Fatal(err)
		}
	}
}
func TestV2RuntimeSIGKILLWithActiveTCP(t *testing.T) { testV2RuntimeSIGKILLWithActiveTCP(t, v2Only) }
func testV2RuntimeSIGKILLWithActiveTCP(t *testing.T, mode string) {
	if isolatedTestProcess(t) {
		return
	}
	root := t.TempDir()
	now := time.Now().Unix()
	stamp := now - now%60 - 120
	log, err := os.Create(filepath.Join(root, "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestV2CrashChild$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "V2_WIRE_MODE="+mode, "V2_WIRE_CRASH_ROOT="+root, "V2_WIRE_CRASH_STAMP="+strconv.FormatInt(stamp, 10))
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(root, "crash-ready")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(filepath.Join(root, "child.log"))
			t.Fatalf("child not ready: %s", b)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	exit := &exec.ExitError{}
	ok := errors.As(err, &exit)
	if !ok {
		t.Fatalf("child was not killed: %v", err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || status.Signal() != syscall.SIGKILL {
		t.Fatalf("wrong exit: %v", err)
	}
	app := v2TestApp(t, root)
	setV2Mode(app, mode)
	if mode == v2ServingDual {
		app.Config.Whisper.Schemas[0].Retentions = app.Config.Whisper.Schemas[0].Retentions[:1]
	}
	if mode == v2Shadow {
		app.Config.Carbonserver.ScanFrequency = &Duration{20 * time.Millisecond}
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	if mode != v2Shadow {
		waitV2CatalogName(t, app, "crash.durable")
	}
	b := v2HTTP(t, app, "/metrics/find/?format=json&query=crash.durable")
	if !strings.Contains(string(b), "crash.durable") {
		t.Fatalf("lost durable name: %s", b)
	}
	b = v2HTTP(t, app, fmt.Sprintf("/render/?format=json&target=crash.durable&from=%d&until=%d", stamp-60, stamp))
	if !strings.Contains(string(b), "3.25") {
		t.Fatalf("lost durable prefix: %s", b)
	}
}

func TestV2RejectsClassicOnlyFallbackWithRetainedHistory(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	root := t.TempDir()
	app := v2TestApp(t, root)
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	app.Stop()
	app = v2TestApp(t, root)
	app.Config.V2.Enabled = false
	if err := app.Start(); err == nil {
		app.Stop()
		t.Fatal("classic-only fallback hid retained v2 history")
	}
	app.Stop()
}

func TestV2RejectsPhysicalRootAliases(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	root := t.TempDir()
	app := v2TestApp(t, root)
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(app.Config.Whisper.DataDir, alias); err != nil {
		t.Fatal(err)
	}
	app.Config.V2.Path = filepath.Join(alias, "new-v2-root")
	if err := app.Start(); err == nil {
		app.Stop()
		t.Fatal("accepted physically overlapping roots")
	}
	app.Stop()
}
