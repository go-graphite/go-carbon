//go:build unix

package carbon

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
)

// Run the predecessor's actual store handoff/cleanup path in a child, so a
// retained socket or a wait that prevents process exit fails a bounded test.
func TestStoreInputRetentionExpiresAndExits(t *testing.T) {
	const childEnv = "GO_CARBON_TEST_RETENTION_CHILD"
	if os.Getenv(childEnv) == "" {
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStoreInputRetentionExpiresAndExits$", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("predecessor failed to exit: %v\n%s", err, out)
		}
		t.Logf("predecessor exited after retention cleanup:\n%s", out)
		return
	}
	defer zapwriter.Test()()
	handoff.InputRetentionTimeout = 100 * time.Millisecond
	handoff.PeerExecutable = func(*net.UnixConn) (string, error) { return "", errors.ErrUnsupported }
	dir, err := os.MkdirTemp("/tmp", "gc-retain-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	app := New(filepath.Join(dir, "carbon.conf"))
	app.Config = NewConfig()
	app.Config.Dump.Path = dir
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	lf, err := ln.File()
	if err != nil {
		t.Fatal(err)
	}
	in := &handoff.Input{Receivers: []handoff.InputReceiver{{Name: "tcp", Listener: lf}}}
	defer in.Close()
	var clients []net.Conn
	const payload = "unread at retention expiry"
	for i := range 2 {
		client, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		clients = append(clients, client)
		conn, err := ln.AcceptTCP()
		if err != nil {
			t.Fatal(err)
		}
		f, err := conn.File()
		_ = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		leftover := make([]byte, 7)
		if i == 0 {
			leftover = make([]byte, 1<<20)
		}
		in.Receivers[0].Conns = append(in.Receivers[0].Conns, handoff.InputConn{File: f, Leftover: leftover})
		if _, err = io.WriteString(client, payload); err != nil {
			t.Fatal(err)
		}
		if runtime.GOOS == "linux" {
			deadline := time.Now().Add(time.Second)
			for {
				n, err := handoff.KernelUnread(f)
				if err != nil {
					t.Fatal(err)
				}
				if n == len(payload) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("kernel bytes = %d", n)
				}
				time.Sleep(time.Millisecond)
			}
		}
	}
	_ = ln.Close()
	files := []*os.File{lf, in.Receivers[0].Conns[0].File, in.Receivers[0].Conns[1].File}
	h := &inputHandover{logger: zapwriter.Logger("dump"), done: true, detached: in, closed: time.Now()}
	released := make(chan error, 1)
	app.readRelease = func() {
		go func() {
			uc, err := net.Dial("unix", readHandoffPath(app.Config))
			if err != nil {
				released <- err
				return
			}
			defer uc.Close()
			_ = uc.SetDeadline(time.Now().Add(time.Second))
			var header [5]byte
			if _, err = uc.Write([]byte{'R'}); err == nil {
				_, err = io.ReadFull(uc, header[:1])
			}
			if err == nil && header[0] != 'O' {
				err = errors.New("input not offered")
			}
			if err == nil {
				_, err = uc.Write([]byte{'I'})
			}
			if err == nil {
				_, err = io.ReadFull(uc, header[:])
			}
			// Interrupt the raw transfer before receiving any descriptors.
			released <- err
		}()
	}
	started := time.Now()
	app.handOffStoreInput(h.logger, h)
	h.stop("successor did not take input") // DumpStop's final cleanup
	if time.Since(started) > 2*time.Second {
		t.Fatal("predecessor shutdown exceeded short retention window")
	}
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("retained descriptor not closed: %v", err)
		}
	}
	for _, c := range clients {
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_, err := c.Read(make([]byte, 1))
		var ne net.Error
		if err == nil {
			t.Fatal("sender still connected")
		} else if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("sender did not observe close", err)
		}
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(handoff.InputCloseMetrics())
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		"input_handoff_closed_connections_total":           2,
		"input_handoff_closed_leftover_bytes_total":        (1 << 20) + 7,
		"input_handoff_closed_kernel_unread_bytes_total":   0,
		"input_handoff_closed_kernel_unread_unknown_total": 2,
	}
	if runtime.GOOS == "linux" {
		want["input_handoff_closed_kernel_unread_bytes_total"] = 2 * float64(len(payload))
		want["input_handoff_closed_kernel_unread_unknown_total"] = 0
	}
	for _, family := range families {
		for _, m := range family.Metric {
			for _, label := range m.Label {
				if label.GetName() == "reason" && label.GetValue() == "retention_timeout" {
					if expected, ok := want[family.GetName()]; !ok || m.GetCounter().GetValue() != expected {
						t.Fatalf("%s = %v, want %v", family.GetName(), m.GetCounter().GetValue(), expected)
					}
					delete(want, family.GetName())
				}
			}
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing counters: %v", want)
	}
	logs := zapwriter.TestString()
	if !strings.Contains(logs, "input retention expired; predecessor exiting") ||
		strings.Count(logs, "input handoff socket discarded") != 2 {
		t.Fatal("missing retention ERROR and per-socket accounting", logs)
	}
}
