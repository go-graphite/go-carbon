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

func TestPendingConcurrentRetirement(t *testing.T) {
	s := &Server{pending: make([]pendingShard, 4)}
	for i := range s.pending {
		s.pending[i].names = make(map[string]int64)
	}
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 1000 {
				name := fmt.Sprintf("pending.shared.%d", (j+worker)%17)
				owner := OwnerFor(name, len(s.pending))
				s.pendingAdd(owner, name, 1)
				if !s.Pending(name) {
					t.Error("lost live reservation")
				}
				s.pendingAdd(owner, name, -1)
			}
		}()
	}
	wg.Wait()
	for i := range s.pending {
		if len(s.pending[i].names) != 0 {
			t.Fatal("retained retired names")
		}
	}
}

type coalescingOwner struct {
	calls []int
	names []string
	data  []points.Point
}

func (o *coalescingOwner) Append(names []string, data []points.Point, _ []uint64, _ int64) error {
	o.calls = append(o.calls, len(data))
	o.names = append(o.names, names...)
	o.data = append(o.data, data...)
	return nil
}
func (o *coalescingOwner) Seal() error  { return nil }
func (o *coalescingOwner) Drain() error { return nil }
func TestOwnerCoalescesSocketFragments(t *testing.T) {
	s := &Server{cfg: Config{BatchSize: 8, SyncInterval: time.Hour}, pending: []pendingShard{{names: make(map[string]int64)}}}
	q := make(chan batch, 17)
	o := &coalescingOwner{}
	for i := range 17 {
		name := fmt.Sprintf("fragment.%d", i/3)
		s.pendingAdd(0, name, 1)
		q <- batch{names: []string{name}, data: []points.Point{{Timestamp: int64(i), Value: float64(i) + .125}}}
	}
	close(q)
	s.workers.Add(1)
	s.work(0, o, q, make(chan ownerCall))
	if fmt.Sprint(o.calls) != "[8 8 1]" {
		t.Fatal("socket fragments escaped coalescing", o.calls)
	}
	if s.Stats().Accepted != 17 {
		t.Fatal(s.Stats())
	}
	for i := range 17 {
		if o.names[i] != fmt.Sprintf("fragment.%d", i/3) || o.data[i].Timestamp != int64(i) || o.data[i].Value != float64(i)+.125 {
			t.Fatal("changed point order/bits", i)
		}
	}
	if len(s.pending[0].names) != 0 {
		t.Fatal("reservations retained after drain")
	}
}
