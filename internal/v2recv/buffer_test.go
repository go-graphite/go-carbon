package v2recv_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
)

// A connection hands its points to the owners once it holds BufferBytes,
// summed over all owners, even when no owner batch is full and the stream
// never ends a read on a line boundary (the trailing line stays unterminated).
func TestConnectionBufferBoundedByBytes(t *testing.T) {
	const owners, lines, bufferBytes = 16, 4000, 4096
	list := make([]v2recv.Owner, owners)
	for i := range list {
		list[i] = &fakeOwner{}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s, err := v2recv.Start(l, list, v2recv.Config{BatchSize: 1024, BufferBytes: bufferBytes, SyncInterval: 5 * time.Millisecond, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	var payload bytes.Buffer
	for i := range lines {
		fmt.Fprintf(&payload, "bound.%d.%s 1 %d\n", i%101, strings.Repeat("x", i%17), 1700000000+i)
	}
	payload.WriteString("bound.tail 1")
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Write(payload.Bytes()); err != nil {
		t.Fatal(err)
	}
	// What a connection may still hold: less than BufferBytes at rowBytes (48) per point.
	const held = bufferBytes / 48
	for deadline := time.Now().Add(10 * time.Second); s.Stats().Accepted < lines-held; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("accepted %d of %d points; a connection holds at most %d", s.Stats().Accepted, lines, held)
		}
	}
	if _, err = io.WriteString(c, " 1700000000\n"); err != nil {
		t.Fatal(err)
	}
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.Accepted != lines+1 || st.Rejected+st.ParseErrors+st.TransportErrors != 0 {
		t.Fatal(st)
	}
}

type recordingBlockedOwner struct {
	entered, block chan struct{}
	once           sync.Once
	mu             sync.Mutex
	timestamps     []int64
}

func (o *recordingBlockedOwner) Append(_ []string, _ []uint64, data []points.Point, _ []uint64, _ int64) error {
	o.once.Do(func() { close(o.entered); <-o.block })
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, p := range data {
		o.timestamps = append(o.timestamps, p.Timestamp)
	}
	return nil
}
func (*recordingBlockedOwner) Seal() error  { return nil }
func (*recordingBlockedOwner) Drain() error { return nil }

// Backpressure: an owner that stops taking input stops the connection from
// reading its socket once its byte budget and the owner queue are full. No
// point is dropped and the order survives when the owner resumes.
func TestFullOwnerBlocksConnectionNeverDrops(t *testing.T) {
	const lines = 250000
	o := &recordingBlockedOwner{entered: make(chan struct{}), block: make(chan struct{})}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s, err := v2recv.Start(l, []v2recv.Owner{o}, v2recv.Config{BatchSize: 16, BufferBytes: 1024, SyncInterval: time.Hour, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	var payload bytes.Buffer
	for i := range lines {
		fmt.Fprintf(&payload, "blocked.%d %d %d\n", i%10, i, 1700000000+i)
	}
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() { _, e := c.Write(payload.Bytes()); written <- e }()
	<-o.entered
	// Two 64 KiB read buffers, the byte budget, one queued and one pending batch.
	const bound = 256 << 10
	var wire uint64
	for stable, deadline := 0, time.Now().Add(10*time.Second); stable < 3; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("connection kept reading behind a blocked owner: %d of %d bytes", s.Stats().WireBytes, payload.Len())
		}
		if w := s.Stats().WireBytes; w == wire {
			stable++
		} else {
			wire, stable = w, 0
		}
	}
	if st := s.Stats(); wire > bound || st.Accepted != 0 || st.Rejected != 0 {
		t.Fatalf("read %d bytes (bound %d) behind a blocked owner; stats %+v", wire, bound, st)
	}
	close(o.block)
	if err = <-written; err != nil {
		t.Fatal(err)
	}
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.Accepted != lines || st.Rejected+st.ParseErrors+st.TransportErrors+st.SinkErrors != 0 {
		t.Fatal(st)
	}
	if len(o.timestamps) != lines {
		t.Fatalf("owner saw %d of %d points", len(o.timestamps), lines)
	}
	for i, ts := range o.timestamps {
		if ts != int64(1700000000+i) {
			t.Fatalf("point %d has timestamp %d", i, ts)
		}
	}
}
