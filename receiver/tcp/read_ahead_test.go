package tcp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/prometheus/client_golang/prometheus"
)

func TestHandoffReadAheadConfiguration(t *testing.T) {
	for _, size := range []int{0, handoffReadBlock, 3*handoffReadBlock + 23, 256 << 20, -1, handoffReadBlock - 1, 1<<30 + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			opts := NewOptions()
			opts.Listen = "127.0.0.1:0"
			opts.HandoffReadAheadBytes = size
			r, err := newTCP("tcp", opts, func(*points.Points) {})
			valid := size == 0 || size >= handoffReadBlock && size <= 1<<30
			if !valid {
				if err == nil {
					r.Stop()
					t.Fatal("unsafe startup buffer accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer r.Stop()
			want := size
			if want == 0 {
				want = handoffReadAheadBytes
			}
			if r.handoffReadAheadBytes != want {
				t.Fatalf("effective buffer %d, want %d", r.handoffReadAheadBytes, want)
			}
			reg := prometheus.NewRegistry()
			r.InitPrometheus(reg)
			families, err := reg.Gather()
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, f := range families {
				if f.GetName() == "tcp_handoff_read_ahead_limit_bytes" {
					found = true
					if got := f.Metric[0].GetGauge().GetValue(); got != float64(want) {
						t.Fatalf("exported buffer %v, want %d", got, want)
					}
				}
			}
			if !found {
				t.Fatal("effective buffer metric missing")
			}
		})
	}
}

func TestReadAheadEOFAndDetach(t *testing.T) {
	for _, detach := range []bool{false, true} {
		t.Run(map[bool]string{false: "eof", true: "detach"}[detach], func(t *testing.T) {
			server, client := net.Pipe()
			defer client.Close()
			r := newReadAheadConn(server)
			defer r.Close()
			want := bytes.Repeat([]byte("0123456789abcdef"), 10000)
			sent := make(chan error, 1)
			go func() {
				_, err := client.Write(want)
				sent <- err
				if !detach {
					_ = client.Close()
				}
			}()
			if err := <-sent; err != nil {
				t.Fatal(err)
			}
			var got []byte
			if detach {
				b := make([]byte, 17)
				if _, err := io.ReadFull(r, b); err != nil {
					t.Fatal(err)
				}
				got = r.pending(b)
			} else {
				var err error
				got, err = io.ReadAll(r)
				if err != nil {
					t.Fatal(err)
				}
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("read %d of %d bytes", len(got), len(want))
			}
		})
	}
}

func TestReadAheadByteBound(t *testing.T) {
	for _, limit := range []int{handoffReadAheadBytes, 3*handoffReadBlock + 23} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) { testReadAheadByteBound(t, limit) })
	}
}

func testReadAheadByteBound(t *testing.T, limit int) {
	t.Helper()
	server, client := net.Pipe()
	defer client.Close()
	r := newReadAheadConnLimit(server, limit)
	defer r.Close()
	want := bytes.Repeat([]byte("x"), limit+3*handoffReadBlock)
	sent := make(chan error, 1)
	go func() { _, err := client.Write(want); sent <- err }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		r.mu.Lock()
		full := r.size == limit
		r.mu.Unlock()
		if full {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("buffer did not fill")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-sent:
		t.Fatal("read-ahead exceeded its byte bound")
	default:
	}
	got := r.pending(nil)
	if len(got) > limit+handoffReadBlock || !bytes.Equal(got, want[:len(got)]) {
		t.Fatalf("invalid buffered prefix: %d bytes", len(got))
	}
	_ = client.Close()
	<-sent
}

func TestReadAheadReturnsToDirect(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	r := newReadAheadConn(server)
	defer r.Close()
	first := bytes.Repeat([]byte("a"), 100000)
	sent := make(chan error, 1)
	go func() { _, err := client.Write(first); sent <- err }()
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	r.finish()
	got := make([]byte, len(first))
	if _, err := io.ReadFull(r, got); err != nil || !bytes.Equal(got, first) {
		t.Fatal("buffered prefix changed", err)
	}
	select {
	case <-r.done:
	default:
		t.Fatal("pump still running after replay")
	}
	second := []byte("direct continuation")
	go func() { _, err := client.Write(second); sent <- err }()
	got = make([]byte, len(second))
	if _, err := io.ReadFull(r, got); err != nil || !bytes.Equal(got, second) {
		t.Fatal("direct continuation changed", err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if len(r.data) != 0 {
		t.Fatal("retained startup buffer")
	}
	if got := r.pending(nil); len(got) != 0 {
		t.Fatal("invented pending bytes")
	}
}

type terminalReadConn struct {
	net.Conn
	err error
}

func (c terminalReadConn) Read(b []byte) (int, error) { return copy(b, "prefix"), c.err }

func TestReadAheadPreservesTerminalFailure(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	failure := errors.New("transport failure")
	r := newReadAheadConn(terminalReadConn{Conn: server, err: failure})
	defer r.Close()
	<-r.done
	r.finish()
	got, err := io.ReadAll(r)
	if string(got) != "prefix" || !errors.Is(err, failure) {
		t.Fatalf("got %q, %v", got, err)
	}
}
