package v2recv

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2wire"
)

type ackTestSink struct {
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
	fail    bool
}

func (s *ackTestSink) AcceptPlaced(v2wire.Frame) error {
	s.calls.Add(1)
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	if s.fail {
		return errors.New("fsync failed")
	}
	return nil
}
func TestPlacementACKWaitsForDurabilityAndAuthenticates(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "durable", true: "failed"}[fail], func(t *testing.T) {
			sink := &ackTestSink{entered: make(chan struct{}), release: make(chan struct{}), fail: fail}
			key := bytes.Repeat([]byte{1}, 32)
			l, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			s, e := Start(l, nil, Config{Placement: sink, PlacementKey: key, PlacementProducer: "relay/inc-1"})
			if e != nil {
				t.Fatal(e)
			}
			c, e := net.Dial("tcp", l.Addr().String())
			if e != nil {
				t.Fatal(e)
			}
			f := v2wire.Frame{Kind: v2wire.Data, Stream: v2wire.Stream{Group: "g", Replica: "r", Store: "s", MapDigest: strings.Repeat("a", 64), Revision: 1, Epoch: 1, Producer: "relay/inc-1", Lane: "lane"}, Sequence: 1, VShard: 1, Name: "x", Timestamp: 100}
			if e = v2wire.Write(c, f, key); e != nil {
				t.Fatal(e)
			}
			select {
			case <-sink.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("sink not called")
			}
			_ = c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
			if _, e = v2wire.Read(c, key); e == nil {
				t.Fatal("ACK before durability")
			}
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
			close(sink.release)
			ack, e := v2wire.Read(c, key)
			if !fail && (e != nil || ack != f.Acknowledgement()) {
				t.Fatal(ack, e)
			}
			if fail && e == nil {
				t.Fatal("ACK after failed fsync")
			}
			c.Close()
			c, e = net.Dial("tcp", l.Addr().String())
			if e != nil {
				t.Fatal(e)
			}
			f.Stream.Producer = "other"
			_ = v2wire.Write(c, f, key)
			_ = c.SetReadDeadline(time.Now().Add(time.Second))
			if _, e = v2wire.Read(c, key); e == nil {
				t.Fatal("unauthorized producer ACKed")
			}
			c.Close()
			if sink.calls.Load() != 1 {
				t.Fatal("unauthorized producer reached sink")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if e = s.Wait(ctx); e != nil {
				t.Fatal(e)
			}
		})
	}
}
