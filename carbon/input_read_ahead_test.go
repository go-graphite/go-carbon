package carbon

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/klauspost/compress/zstd"
)

// Hold recovery at a real tail lock while every protocol writes on inherited
// sockets. Then check every point and same-timestamp correction in cache order.
func TestInheritedInputReadsWhileTailLocked(t *testing.T) {
	app := New("")
	opts := tcp.NewOptions()
	opts.Listen = "127.0.0.1:0"
	ro, err := receiver.WithProtocol(opts, "tcp")
	if err != nil {
		t.Fatal(err)
	}
	warm := make(chan struct{}, 3)
	raw, err := receiver.New("tcp", ro, func(*points.Points) { warm <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	old := raw.(*tcp.TCP)
	defer old.Stop()
	z, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	e, err := codec.NewEncoder(codec.Options{Flags: codec.FlagZstd})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	clients := make([]net.Conn, 3)
	modes := []string{"plaintext", "zstd", "stream"}
	encode := func(mode, name string, first, count int) []byte {
		var b []byte
		for i := first; i < first+count; i++ {
			if mode == "stream" {
				if err := e.Add([]byte(name), float64(i), 1700000000); err != nil {
					t.Fatal(err)
				}
			} else {
				b = fmt.Appendf(b, "%s %d 1700000000\n", name, i)
			}
		}
		switch mode {
		case "zstd":
			return z.EncodeAll(b, nil)
		case "stream":
			b, err = e.Flush(nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return b
	}
	for i, mode := range modes {
		c, err := net.Dial("tcp", old.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = c
		defer c.Close()
		b := encode(mode, "gate."+mode, 0, 1)
		if mode == "stream" {
			pre, _ := codec.AppendPreamble(nil, codec.FlagZstd)
			b = append(pre, b...)
		}
		if _, err = c.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	for range clients {
		select {
		case <-warm:
		case <-time.After(3 * time.Second):
			t.Fatal("warmup did not arrive")
		}
	}
	moved, err := old.Detach()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	tail := filepath.Join(root, "input.1.2.tail.bin")
	var saved []byte
	for _, mode := range modes {
		saved = points.OnePoint("gate."+mode, 1, 1700000000).AppendBinary(saved)
	}
	if err = os.WriteFile(tail, saved, 0600); err != nil {
		t.Fatal(err)
	}
	unlock, err := handoff.LockExclusive(tail)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if unlock != nil {
			_ = unlock()
		}
	}()
	app.Config = NewConfig()
	app.Config.Udp.Enabled = false
	app.Config.Pickle.Enabled = false
	app.Config.Prometheus.Enabled = false
	app.Config.Tcp = opts
	opts.Listen = old.Addr().String()
	app.Config.Dump.Enabled = true
	app.Config.Dump.Path = root
	app.pendingDone = make(chan struct{})
	in := &handoff.Input{Receivers: []handoff.InputReceiver{{Name: "tcp", Listener: moved.Listener}}}
	for _, c := range moved.Conns {
		in.Receivers[0].Conns = append(in.Receivers[0].Conns, handoff.InputConn{File: c.File, Leftover: c.Leftover})
	}
	app.inheritedInput = in
	core := cache.New()
	core.SetMaxSize(1000000)
	done := make(chan error, 1)
	go func() { done <- app.startReceivers(core) }()
	// Output enough raw bytes to exercise read-ahead rather than just a line.
	// Writer deadlines also catch a receiver that fails to start before recovery.
	const count = 10000
	for i, mode := range modes {
		_ = clients[i].SetWriteDeadline(time.Now().Add(2 * time.Second))
		b := encode(mode, "gate."+mode, 2, count)
		if _, err = clients[i].Write(b); err != nil {
			t.Fatal(err)
		}
	}
	// A new connection accepted from the inherited listener shares the same gate.
	newClient, err := net.Dial("tcp", opts.Listen)
	if err != nil {
		t.Fatal(err)
	}
	defer newClient.Close()
	if _, err = newClient.Write([]byte("gate.new 2 1700000000\n")); err != nil {
		t.Fatal(err)
	}
	if core.Size() != 0 {
		t.Fatal("socket points reached cache before tail")
	}
	select {
	case err = <-done:
		t.Fatalf("startup bypassed locked tail: %v", err)
	default:
	}
	if err = unlock(); err != nil {
		t.Fatal(err)
	}
	unlock = nil
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("startup stuck")
	}
	defer func() {
		for _, r := range app.Receivers {
			r.Stop()
		}
	}()
	// EOF flushes a final plaintext batch; readers retain all queued bytes.
	for _, c := range clients {
		_ = c.(*net.TCPConn).CloseWrite()
	}
	_ = newClient.(*net.TCPConn).CloseWrite()
	deadline := time.Now().Add(3 * time.Second)
	for core.Size() != 3*(count+1)+1 {
		if time.Now().After(deadline) {
			t.Fatalf("point count %d", core.Size())
		}
		time.Sleep(time.Millisecond)
	}
	for _, mode := range modes {
		got := core.Get("gate." + mode)
		if len(got) != count+1 {
			t.Fatal(mode, len(got))
		}
		for i, p := range got {
			if p.Value != float64(i+1) || p.Timestamp != 1700000000 {
				t.Fatalf("%s point %d: %+v", mode, i, p)
			}
		}
	}
	if got := core.Get("gate.new"); len(got) != 1 || got[0].Value != 2 {
		t.Fatal(got)
	}
}
