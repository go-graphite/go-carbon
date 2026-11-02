package tcp

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

func TestStreamByteReservationReleased(t *testing.T) {
	options := NewOptions()
	options.Listen = "127.0.0.1:0"
	options.StreamDictionaryTotalBytes = int64(options.StreamDictionaryBytes)
	var received atomic.Uint32
	r, err := newTCP("tcp", options, func(*points.Points) { received.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	wait := func(predicate func() bool) {
		t.Helper()
		until := time.Now().Add(3 * time.Second)
		for !predicate() {
			if time.Now().After(until) {
				t.Fatal("stream reservation did not settle")
			}
			time.Sleep(time.Millisecond)
		}
	}
	preamble, err := codec.AppendPreamble(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	first, err := net.Dial("tcp", r.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := first.Write(preamble); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return r.streamDictionaryReserved.Load() == int64(options.StreamDictionaryBytes) })
	second, err := net.Dial("tcp", r.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write(preamble); err != nil {
		t.Fatal(err)
	}
	_ = second.SetReadDeadline(time.Now().Add(3 * time.Second))
	var b [1]byte
	if _, err := second.Read(b[:]); err == nil {
		t.Fatal("excess stream connection accepted")
	}
	_ = second.Close()
	if received.Load() != 0 {
		t.Fatal("budget rejection emitted points")
	}
	_ = first.Close()
	wait(func() bool { return r.streamDictionaryReserved.Load() == 0 })
	third, err := net.Dial("tcp", r.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	wire := streamWire(t, 0)
	if _, err := third.Write(wire); err != nil {
		t.Fatal(err)
	}
	_ = third.Close()
	wait(func() bool { return received.Load() == 4 && r.streamDictionaryReserved.Load() == 0 })
}
