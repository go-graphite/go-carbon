//go:build unix

package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	whisper "github.com/go-graphite/go-whisper"
)

const restartMainConfigEnv = "GO_CARBON_TEST_RESTART_MAIN_CONFIG"

// Run the actual signal loop, including its exit decision, in a private child.
func TestRestartMainProcess(_ *testing.T) {
	config := os.Getenv(restartMainConfigEnv)
	if config == "" {
		return
	}
	os.Args = []string{os.Args[0], "-config", config}
	if os.Getenv("GO_CARBON_TEST_CAPTURE_PROBE") == "1" {
		os.Args = append(os.Args, "-check-startup-capture")
	}
	if os.Getenv("GO_CARBON_TEST_CAPTURE_PROBE") == "generation" {
		os.Args = append(os.Args, "-startup-generation-protocol")
	}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	main()
}

func TestStartupGenerationCapabilityDoesNotOpenConfig(t *testing.T) {
	child := exec.Command(os.Args[0], "-test.run=^TestRestartMainProcess$")
	child.Env = append(os.Environ(), restartMainConfigEnv+"=/does-not-exist", "GO_CARBON_TEST_CAPTURE_PROBE=generation")
	out, err := child.CombinedOutput()
	if err != nil || strings.TrimSpace(string(out)) != handoff.StartupGenerationProtocol+"\nPASS" {
		t.Fatalf("successor generation capability opened configuration: %v: %s", err, out)
	}
}

func TestStartupCaptureProbeDoesNotOpenStorageOrListeners(t *testing.T) {
	root := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dump := filepath.Join(root, "dump")
	if err := os.Mkdir(dump, 0700); err != nil {
		t.Fatal(err)
	}
	schemas := filepath.Join(root, "schemas.conf")
	if err := os.WriteFile(schemas, []byte("[all]\npattern = .*\nretentions = 1s:1h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`[common]
user = ""
[whisper]
enabled = true
storage-backend = "pebble-chunk"
store-encoding = "exact"
store-dir = %q
schemas-file = %q
compressed = true
[tcp]
enabled = true
listen = %q
[udp]
enabled = false
[pickle]
enabled = false
[carbonserver]
enabled = true
listen = %q
trie-index = true
concurrent-index = true
file-list-cache = %q
file-list-cache-version = 2
[dump]
enabled = true
handoff-supervisor = true
store-input-handoff = true
require-startup-capture = true
path = %q
[input-log]
enabled = true
path = %q
`, filepath.Join(root, "store"), schemas, listener.Addr().String(), listener.Addr().String(), filepath.Join(root, "flc"), dump, filepath.Join(root, "journal"))
	path := filepath.Join(root, "carbon.conf")
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestRestartMainProcess$")
	child.Env = append(os.Environ(), restartMainConfigEnv+"="+path, "GO_CARBON_TEST_CAPTURE_PROBE=1")
	out, err := child.CombinedOutput()
	// The test wrapper prints PASS after the real main returns from probe mode.
	if err != nil || strings.TrimSpace(string(out)) != handoff.StartupCaptureProtocol+"\nPASS" {
		t.Fatalf("successor admission: %v: %s", err, out)
	}
	for _, name := range []string{"store", "journal", "flc"} {
		if _, err := os.Stat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Fatalf("read-only admission opened %s: %v", name, err)
		}
	}
	entries, err := os.ReadDir(dump)
	if err != nil || len(entries) != 0 {
		t.Fatal("admission changed dump directory", entries, err)
	}
}

func TestRefusedRestartDoesNotExitMain(t *testing.T) {
	root := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	for _, dir := range []string{"data", "dump"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "schemas.conf"), []byte("[all]\npattern = .*\nretentions = 1s:1h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`[common]
user = ""
max-cpu = 2
metric-endpoint = "udp://127.0.0.1:1"
[[logging]]
logger = ""
level = "info"
encoding = "json"
file = "stderr"
[whisper]
enabled = true
data-dir = %q
schemas-file = %q
[tcp]
enabled = true
listen = %q
[udp]
enabled = false
[pickle]
enabled = false
[grpc]
enabled = false
[carbonlink]
enabled = false
[carbonserver]
enabled = false
[pprof]
enabled = false
[dump]
enabled = true
path = %q
require-startup-capture = true
`, filepath.Join(root, "data"), filepath.Join(root, "schemas.conf"), address, filepath.Join(root, "dump"))
	configPath := filepath.Join(root, "carbon.conf")
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, "worker.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestRestartMainProcess$")
	child.Env = append(os.Environ(), restartMainConfigEnv+"="+configPath)
	child.Stdout, child.Stderr = log, log
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	wait := func(condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !condition() {
			if time.Now().After(deadline) {
				raw, _ := os.ReadFile(logPath)
				t.Fatalf("main did not keep receiving/persisting: %s", raw)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	var conn net.Conn
	wait(func() bool {
		var err error
		conn, err = net.DialTimeout("tcp", address, 100*time.Millisecond)
		return err == nil
	})
	defer conn.Close()
	write := func(name string, value int) {
		t.Helper()
		stamp := time.Now().Unix() - 2
		if _, err := fmt.Fprintf(conn, "%s %d %d\n", name, value, stamp); err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(root, "data", strings.ReplaceAll(name, ".", "/")+".wsp")
		wait(func() bool {
			w, err := whisper.Open(file)
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
	for attempt := 1; attempt <= 2; attempt++ {
		if err := child.Process.Signal(syscall.SIGUSR2); err != nil {
			t.Fatal(err)
		}
		wait(func() bool {
			raw, _ := os.ReadFile(logPath)
			return strings.Count(string(raw), "restart admission refused; incumbent remains active") >= attempt
		})
		write(fmt.Sprintf("after.refusal%d", attempt), attempt+1)
	}
}
