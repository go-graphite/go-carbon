package v2recv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

type pendingOwner struct {
	appended              chan struct{}
	draining              chan struct{}
	release               chan struct{}
	appendOnce, drainOnce sync.Once
}

func (o *pendingOwner) Append(_ []string, _ []points.Point, _ []uint64, _ int64) error {
	o.appendOnce.Do(func() { close(o.appended) })
	return nil
}
func (o *pendingOwner) Seal() error { return nil }
func (o *pendingOwner) Drain() error {
	select {
	case <-o.appended:
		o.drainOnce.Do(func() { close(o.draining); <-o.release })
	default:
	}
	return nil
}
func TestPendingHeldThroughDurableDrain(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	o := &pendingOwner{appended: make(chan struct{}), draining: make(chan struct{}), release: make(chan struct{})}
	s, err := Start(l, []Owner{o}, Config{BatchSize: 1, SyncInterval: 10 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release := func() { once.Do(func() { close(o.release) }) }
	defer func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Wait(ctx); err != nil {
			t.Error(err)
		}
	}()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fmt.Fprintln(c, "pending.metric 1 1700000000"); err != nil {
		t.Fatal(err)
	}
	c.Close()
	select {
	case <-o.draining:
	case <-time.After(time.Second):
		t.Fatal("no drain")
	}
	if !s.Pending("pending.metric") {
		t.Fatal("expiry reservation released before durable drain")
	}
	release()
	deadline := time.Now().Add(time.Second)
	for s.Pending("pending.metric") {
		if time.Now().After(deadline) {
			t.Fatal("durable drain retained expiry reservation")
		}
		time.Sleep(time.Millisecond)
	}
}
