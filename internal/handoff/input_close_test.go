//go:build unix

package handoff

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func queuedInput(t *testing.T) (*Input, []net.Conn) {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	lf, err := ln.File()
	if err != nil {
		t.Fatal(err)
	}
	in := &Input{Receivers: []InputReceiver{{Name: "tcp", Listener: lf}}}
	var clients []net.Conn
	for i, payload := range []string{"kernel one", "kernel two longer"} {
		client, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		conn, err := ln.AcceptTCP()
		if err != nil {
			t.Fatal(err)
		}
		f, err := conn.File()
		_ = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		in.Receivers[0].Conns = append(in.Receivers[0].Conns, InputConn{File: f, Leftover: make([]byte, i+3)})
		clients = append(clients, client)
		if _, err := io.WriteString(client, payload); err != nil {
			t.Fatal(err)
		}
		if runtime.GOOS == "linux" {
			until := time.Now().Add(time.Second)
			for {
				n, err := KernelUnread(f)
				if err != nil {
					t.Fatal(err)
				}
				if n == len(payload) {
					break
				}
				if time.Now().After(until) {
					t.Fatalf("kernel bytes = %d, want %d", n, len(payload))
				}
				time.Sleep(time.Millisecond)
			}
		}
	}
	return in, clients
}

func closeCounters(t *testing.T, reason string) map[string]float64 {
	t.Helper()
	r := prometheus.NewPedanticRegistry()
	r.MustRegister(InputCloseMetrics())
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, f := range families {
		for _, m := range f.Metric {
			for _, label := range m.Label {
				if label.GetName() == "reason" && label.GetValue() == reason {
					out[f.GetName()] = m.GetCounter().GetValue()
				}
			}
		}
	}
	return out
}

func TestDiscardInputCountsAndCloses(t *testing.T) {
	in, clients := queuedInput(t)
	const reason = "test_discard"
	before := closeCounters(t, reason)
	var logs bytes.Buffer
	logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.InfoLevel))
	files := []*os.File{in.Receivers[0].Listener, in.Receivers[0].Conns[0].File, in.Receivers[0].Conns[1].File}
	got := in.Discard(logger, reason)
	want := InputCloseStats{Connections: 2, LeftoverBytes: 7}
	if runtime.GOOS == "linux" {
		want.KernelUnreadBytes = uint64(len("kernel one") + len("kernel two longer"))
	} else {
		want.KernelUnreadUnknown = 2
	}
	if got != want {
		t.Fatalf("close totals = %+v, want %+v", got, want)
	}
	for _, f := range files {
		if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("file still open: %v", err)
		}
	}
	for _, c := range clients {
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_, err := c.Read(make([]byte, 1))
		var ne net.Error
		if err == nil {
			t.Fatal("sender socket still open")
		} else if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("sender socket did not observe close", err)
		}
	}
	if got := in.Discard(logger, reason); got != (InputCloseStats{}) {
		t.Fatal("duplicate cleanup counted again", got)
	}
	after := closeCounters(t, reason)
	for name, value := range map[string]uint64{
		"input_handoff_closed_connections_total":           want.Connections,
		"input_handoff_closed_kernel_unread_bytes_total":   want.KernelUnreadBytes,
		"input_handoff_closed_kernel_unread_unknown_total": want.KernelUnreadUnknown,
		"input_handoff_closed_leftover_bytes_total":        want.LeftoverBytes,
	} {
		if delta := after[name] - before[name]; delta != float64(value) {
			t.Fatalf("%s increased by %v, want %d", name, delta, value)
		}
	}
	entries := bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n"))
	if len(entries) != 3 {
		t.Fatalf("per-socket logs: %s", logs.String())
	}
	for i, e := range entries[:2] {
		var fields map[string]interface{}
		if err := json.Unmarshal(e, &fields); err != nil {
			t.Fatal(err)
		}
		wantUnread := -1
		if runtime.GOOS == "linux" {
			wantUnread = []int{len("kernel one"), len("kernel two longer")}[i]
		}
		if fields["level"] != "error" || fields["kernel_unread_bytes"] != float64(wantUnread) || fields["leftover_bytes"] != float64(i+3) {
			t.Fatalf("socket log: %s", e)
		}
	}
}

func TestReleaseCopiesDoesNotCountLoss(t *testing.T) {
	in, _ := queuedInput(t)
	const reason = "test_copy"
	before := closeCounters(t, reason)
	var logs bytes.Buffer
	logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.InfoLevel))
	in.close(logger, reason, false)
	if after := closeCounters(t, reason); len(after) != len(before) {
		t.Fatal("releasing copies counted as lost input", after)
	}
	if bytes.Count(logs.Bytes(), []byte("input handoff socket copy released")) != 2 {
		t.Fatal("copy-close logs missing unread-byte figures")
	}
}
