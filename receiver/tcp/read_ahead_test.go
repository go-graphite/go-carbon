package tcp

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

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
	server, client := net.Pipe()
	defer client.Close()
	r := newReadAheadConn(server)
	defer r.Close()
	want := bytes.Repeat([]byte("x"), handoffReadAheadBytes+3*handoffReadBlock)
	sent := make(chan error, 1)
	go func() { _, err := client.Write(want); sent <- err }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		r.mu.Lock()
		full := r.size == handoffReadAheadBytes
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
	if len(got) > handoffReadAheadBytes+handoffReadBlock || !bytes.Equal(got, want[:len(got)]) {
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
