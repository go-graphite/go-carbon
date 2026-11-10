package tcp

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// Stop each owner in a decoded frame, before it can consume its remaining
// plaintext. The second handoff must preserve an inherited tail larger than
// the line reader, as well as subsequent compressed frames on the socket.
func TestZstdRepeatedHandoffLeftover(t *testing.T) {
	const count = 12000
	var plain []byte
	for i := range count {
		plain = fmt.Appendf(plain, "client.metric %d 1700000000\n", i)
	}
	wire := encodeFrames(t, plain, len(plain)/3)
	var stores [3]sequenceStore
	blocked, release := make(chan struct{}, 2), make(chan struct{})
	store := func(i int) func(*points.Points) {
		return func(p *points.Points) {
			stores[i].add(p)
			if i < 2 && stores[i].count() == 1 {
				blocked <- struct{}{}
				<-release
			}
		}
	}
	opts := NewOptions()
	opts.Listen = "127.0.0.1:0"
	r, err := newTCP("tcp", opts, store(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { close(release); r.Stop() }()
	addr := r.Addr().String()
	client, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write(wire); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		select {
		case <-blocked:
		case <-time.After(3 * time.Second):
			t.Fatal("reader did not reach blocked sink")
		}
		r.handoff.mu.Lock()
		for c := range r.handoff.conns {
			c.interrupt()
		}
		r.handoff.mu.Unlock()
		release <- struct{}{}
		h, err := r.Detach()
		if err != nil {
			t.Fatal(err)
		}
		if r.errors != 0 || r.zstdStats.decodeErrors != 0 {
			t.Fatal("predecessor reported a connection error")
		}
		if len(h.Conns) != 1 || len(h.Conns[0].Leftover) <= plainReadBufferSize || !bytes.HasPrefix(h.Conns[0].Leftover, []byte{0x28, 0xb5, 0x2f, 0xfd}) {
			t.Fatal("missing large zstd leftover")
		}
		next, err := NewInherited("tcp", &Options{Listen: addr, Enabled: true}, store(i+1), h)
		if err != nil {
			t.Fatal(err)
		}
		r = next
		r.FinishInputReplay()
	}
	_ = client.Close()
	deadline := time.Now().Add(3 * time.Second)
	for stores[2].count() < count-2 && atomic.LoadUint32(&r.errors) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	r.Stop()
	var got []int
	for i := range stores {
		got = append(got, stores[i].seqs["client"]...)
	}
	if len(got) != count || r.errors != 0 || r.zstdStats.decodeErrors != 0 {
		t.Fatalf("got %d/%d records, read errors %d, decode errors %d", len(got), count, r.errors, r.zstdStats.decodeErrors)
	}
	for want, seq := range got {
		if seq != want {
			t.Fatalf("record %d: got %d", want, seq)
		}
	}
}

// TestZstdHandoffEveryByte interrupts an actual TCP reader after it consumed
// each possible frame prefix, including headers, blocks and checksums. The
// leftover case also covers a restart during replay of an inherited prefix.
func TestZstdHandoffEveryByte(t *testing.T) {
	plain := []byte("client.metric 0 1700000000\nclient.metric 1 1700000001\nclient.metric 2 1700000002\n")
	var streamed bytes.Buffer
	encoder, err := zstd.NewWriter(&streamed, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	split := bytes.IndexByte(plain, '\n') + 1
	if _, err := encoder.Write(plain[:split]); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(plain[split:]); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []struct {
		name string
		wire []byte
	}{{"independent", encodeFrames(t, plain, 40)}, {"streamed", streamed.Bytes()}} {
		wire := mode.wire
		for _, inherited := range []bool{false, true} {
			for cut := 0; cut <= len(wire); cut++ {
				t.Run(fmt.Sprintf("%s/leftover%t/cut%d", mode.name, inherited, cut), func(t *testing.T) {
					listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					client, err := net.Dial("tcp", listener.Addr().String())
					if err != nil {
						t.Fatal(err)
					}
					defer client.Close()
					raw, err := listener.AcceptTCP()
					if err != nil {
						t.Fatal(err)
					}
					observed := &zstdHandoffConn{observedStreamConn: observedStreamConn{TCPConn: raw, wake: make(chan struct{}, 1)}, reading: make(chan struct{}, 1)}
					var before, after sequenceStore
					old := &TCP{listener: listener, out: before.add, decompressor: newDecompressor(""), logger: zap.NewNop()}
					old.handoff.enabled = true
					old.handoff.acceptDone = make(chan struct{})
					close(old.handoff.acceptDone)
					old.Start()
					defer old.Stop()
					tracked := old.handoff.track(raw)
					var leftover []byte
					if inherited {
						leftover = wire[:cut]
					} else if _, err := client.Write(wire[:cut]); err != nil {
						t.Fatal(err)
					}
					old.Go(func(chan bool) { defer old.handoff.untrack(tracked); old.handlePlain(observed, leftover, tracked) })
					timeout := time.NewTimer(3 * time.Second)
					defer timeout.Stop()
					if inherited {
						select {
						case <-observed.reading:
						case <-timeout.C:
							t.Fatal("old handler did not consume inherited prefix")
						}
					} else {
						for observed.read.Load() < int64(cut) {
							select {
							case <-observed.wake:
							case <-timeout.C:
								t.Fatal("old handler did not consume socket prefix")
							}
						}
					}
					h, err := old.Detach()
					if err != nil {
						t.Fatal(err)
					}
					if len(h.Conns) != 1 {
						t.Fatalf("got %d inherited connections", len(h.Conns))
					}
					next, err := NewInherited("tcp", &Options{Enabled: true, Listen: listener.Addr().String()}, after.add, h)
					if err != nil {
						t.Fatal(err)
					}
					defer next.Stop()
					next.FinishInputReplay()
					if _, err := client.Write(wire[cut:]); err != nil {
						t.Fatal(err)
					}
					_ = client.Close()
					deadline := time.Now().Add(3 * time.Second)
					for before.count()+after.count() < 3 && atomic.LoadUint32(&next.errors) == 0 && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					next.Stop()
					got := append(append([]int(nil), before.seqs["client"]...), after.seqs["client"]...)
					if !reflect.DeepEqual(got, []int{0, 1, 2}) || old.errors != 0 || next.errors != 0 || old.zstdStats.decodeErrors != 0 || next.zstdStats.decodeErrors != 0 {
						t.Fatalf("records %v; read errors %d/%d; zstd errors %d/%d", got, old.errors, next.errors, old.zstdStats.decodeErrors, next.zstdStats.decodeErrors)
					}
				})
			}
		}
	}
}

type zstdHandoffConn struct {
	observedStreamConn
	reading chan struct{}
}

func (c *zstdHandoffConn) Read(b []byte) (int, error) {
	select {
	case c.reading <- struct{}{}:
	default:
	}
	return c.observedStreamConn.Read(b)
}
