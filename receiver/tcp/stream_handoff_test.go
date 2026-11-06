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
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"go.uber.org/zap"
)

// observedStreamConn lets the test interrupt after the old handler has actually
// read a chosen prefix, including a partial header or payload. The tracked
// connection remains the real TCP socket so its descriptor can be handed over.
type observedStreamConn struct {
	*net.TCPConn
	read atomic.Int64
	wake chan struct{}
}

func (c *observedStreamConn) Read(b []byte) (int, error) {
	n, err := c.TCPConn.Read(b)
	c.read.Add(int64(n))
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return n, err
}

func handoffWire(t *testing.T, flags byte) []byte {
	t.Helper()
	e, err := codec.NewEncoder(codec.Options{Flags: flags})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	b, _ := codec.AppendPreamble(nil, flags)
	for i := range 3 {
		if i == 1 {
			if err := e.Reset(); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.Add([]byte("client.metric"), float64(i), 1700000000+int64(i)); err != nil {
			t.Fatal(err)
		}
		b, err = e.Flush(b)
		if err != nil {
			t.Fatal(err)
		}
	}
	return b
}

func TestStreamHandoffEveryByte(t *testing.T) {
	testStreamHandoffEveryByte(t, false)
}
func TestStreamCaptureHandoffEveryByte(t *testing.T) { testStreamHandoffEveryByte(t, true) }
func testStreamHandoffEveryByte(t *testing.T, capture bool) {
	for _, flags := range []byte{0, codec.FlagZstd} {
		wire := handoffWire(t, flags)
		for cut := 1; cut <= len(wire); cut++ {
			t.Run(fmt.Sprintf("flags%d/cut%d", flags, cut), func(t *testing.T) {
				listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
				if err != nil {
					t.Fatal(err)
				}
				client, err := net.Dial("tcp", listener.Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				raw, err := listener.AcceptTCP()
				if err != nil {
					t.Fatal(err)
				}
				observed := &observedStreamConn{TCPConn: raw, wake: make(chan struct{}, 1)}
				var before, after sequenceStore
				old := &TCP{listener: listener, out: before.add, decompressor: newDecompressor(""), logger: zap.NewNop()}
				old.handoff.enabled = true
				old.handoff.acceptDone = make(chan struct{})
				close(old.handoff.acceptDone)
				old.Start()
				defer old.Stop()
				tracked := old.handoff.track(raw)
				old.Go(func(chan bool) { defer old.handoff.untrack(tracked); old.handlePlain(observed, nil, tracked) })
				_ = client.SetWriteDeadline(time.Now().Add(3 * time.Second))
				if _, err := client.Write(wire[:cut]); err != nil {
					t.Fatal(err)
				}
				timeout := time.NewTimer(3 * time.Second)
				defer timeout.Stop()
				for observed.read.Load() < int64(cut) {
					select {
					case <-observed.wake:
					case <-timeout.C:
						t.Fatal("old handler did not consume prefix")
					}
				}
				h, err := old.Detach()
				if err != nil {
					t.Fatal(err)
				}
				if len(h.Conns) != 1 {
					t.Fatalf("got %d inherited connections", len(h.Conns))
				}
				// The new owner uses only the ordinary receiver and wire decoder.
				next, err := inheritTestInput(capture, &Options{Enabled: true, Listen: listener.Addr().String()}, after.add, h)
				if err != nil {
					t.Fatal(err)
				}
				defer next.Stop()
				if _, err := client.Write(wire[cut:]); err != nil {
					t.Fatal(err)
				}
				_ = client.Close()
				deadline := time.Now().Add(3 * time.Second)
				for before.count()+after.count() < 3 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				before.mu.Lock()
				a := append([]int(nil), before.seqs["client"]...)
				before.mu.Unlock()
				after.mu.Lock()
				a = append(a, after.seqs["client"]...)
				after.mu.Unlock()
				if !reflect.DeepEqual(a, []int{0, 1, 2}) {
					t.Fatalf("lost, duplicated, or reordered points: %v", a)
				}
				if old.streamCounters[streamDecodeErrors].Load() != 0 || next.streamCounters[streamDecodeErrors].Load() != 0 {
					t.Fatal("handoff caused decode error")
				}
			})
		}
	}
}

func TestStreamRepeatedHandoffRetainsState(t *testing.T) {
	options := NewOptions()
	options.Listen = "127.0.0.1:0"
	got := make(chan *points.Points, 8)
	r, err := newTCP("tcp", options, func(p *points.Points) { got <- p })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { r.Stop() }()
	conn, err := net.Dial("tcp", r.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	e, err := codec.NewEncoder(codec.Options{Flags: codec.FlagZstd})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	pending, _ := codec.AppendPreamble(nil, codec.FlagZstd)
	addr := r.Addr().String()
	for i := range 5 {
		if err := e.Add([]byte("example.metric"), float64(i), 1700000000+int64(i)); err != nil {
			t.Fatal(err)
		}
		pending, err = e.Flush(pending)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(pending); err != nil {
			t.Fatal(err)
		}
		pending = pending[:0]
		select {
		case p := <-got:
			if p.Data[0].Value != float64(i) {
				t.Fatal("wrong point")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("missing point")
		}
		h, err := r.Detach()
		if err != nil {
			t.Fatal(err)
		}
		if len(h.Conns) != 1 || !bytes.HasPrefix(h.Conns[0].Leftover, []byte(codec.Magic)) {
			t.Fatal("missing snapshot preamble")
		}
		r, err = NewInherited("tcp", &Options{Enabled: true, Listen: addr}, func(p *points.Points) { got <- p }, h)
		if err != nil {
			t.Fatal(err)
		}
	}
}
