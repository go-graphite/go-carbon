package v2recv

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/points"
)

type coalescingOwner struct {
	calls  []int
	names  []string
	hashes []uint64
	data   []points.Point
}

func (o *coalescingOwner) Append(names []string, hashes []uint64, data []points.Point, _ []uint64, _ int64) error {
	o.calls = append(o.calls, len(data))
	o.names = append(o.names, names...)
	o.hashes = append(o.hashes, hashes...)
	o.data = append(o.data, data...)
	return nil
}
func (o *coalescingOwner) Seal() error  { return nil }
func (o *coalescingOwner) Drain() error { return nil }

// ownerOnly runs one owner's worker without a listener, for hand-off tests.
// Close s.closing and wait for s.workers to drain it.
func ownerOnly(o Owner, cfg Config) *Server {
	s := &Server{cfg: cfg, owners: []Owner{o}, inboxes: []*inbox{newInbox(o)}, controls: []chan ownerCall{make(chan ownerCall)}, done: make(chan struct{}), closing: make(chan struct{})}
	s.workers.Add(1)
	go s.work(0, o)
	return s
}

func TestOwnerCoalescesSocketFragments(t *testing.T) {
	o := &coalescingOwner{}
	s := ownerOnly(o, Config{BatchSize: 8, SyncInterval: time.Hour})
	for i := range 17 {
		name := fmt.Sprintf("fragment.%d", i/3)
		if err := s.handOff(0, &batch{names: []string{name}, hashes: []uint64{namefp.Route(name)}, data: []points.Point{{Timestamp: int64(i), Value: float64(i) + .125}}}); err != nil {
			t.Fatal(err)
		}
	}
	close(s.closing)
	s.workers.Wait()
	if fmt.Sprint(o.calls) != "[8 8 1]" {
		t.Fatal("socket fragments escaped coalescing", o.calls)
	}
	if s.Stats().Accepted != 17 {
		t.Fatal(s.Stats())
	}
	if len(o.hashes) != 17 {
		t.Fatal("hashes lane lost in coalescing", len(o.hashes))
	}
	for i := range 17 {
		if o.names[i] != fmt.Sprintf("fragment.%d", i/3) || o.data[i].Timestamp != int64(i) || o.data[i].Value != float64(i)+.125 {
			t.Fatal("changed point order/bits", i)
		}
		if o.hashes[i] != namefp.Route(o.names[i]) {
			t.Fatal("hash lane misaligned with names", i)
		}
	}
}

type fenceOwner struct {
	appended, durable atomic.Int64
	block, entered    chan struct{}
	once              sync.Once
}

func (o *fenceOwner) Append(names []string, _ []uint64, _ []points.Point, _ []uint64, _ int64) error {
	if o.block != nil {
		o.once.Do(func() { close(o.entered); <-o.block })
	}
	o.appended.Add(int64(len(names)))
	return nil
}
func (o *fenceOwner) Seal() error  { return nil }
func (o *fenceOwner) Drain() error { o.durable.Store(o.appended.Load()); return nil }

func TestPendingHeldThroughDurableDrain(t *testing.T) {
	o := &fenceOwner{}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Start(l, []Owner{o}, Config{BatchSize: 64, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(c, "pending.metric 1 1700000000")
	c.Close()
	// EOF proves the complete decoded prefix has entered the owner queue.
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		n := len(s.conns)
		s.mu.Unlock()
		if n == 0 && s.Stats().Connections > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("handler retained")
		}
		time.Sleep(time.Millisecond)
	}
	if err = s.Fence(func() error {
		if o.durable.Load() != 1 {
			t.Error("maintenance preceded durable drain", o.durable.Load())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = s.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// Repeated maintenance races actual concurrent socket intake of the same name.
// Each checkpoint must cover every application already appended by the owner.
func TestPendingConcurrentRetirement(t *testing.T) {
	o := &fenceOwner{}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Start(l, []Owner{o}, Config{BatchSize: 8, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, e := net.Dial("tcp", l.Addr().String())
			if e != nil {
				t.Error(e)
				return
			}
			defer c.Close()
			for range 100 {
				if _, e = fmt.Fprintln(c, "same.name 1 1700000000"); e != nil {
					t.Error(e)
					return
				}
			}
		}()
	}
	for range 20 {
		if err = s.Fence(func() error {
			if o.durable.Load() != o.appended.Load() {
				t.Error("expiry crossed non-durable input")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	// Wait closes the listener; a dialed but unaccepted connection would be dropped.
	for deadline := time.Now().Add(10 * time.Second); s.Stats().Connections < 8; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("accepted connections", s.Stats().Connections)
		}
	}
	if err = s.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if o.durable.Load() != 800 {
		t.Fatal("lost concurrent input", o.durable.Load())
	}
}

func TestPendingBatchesFenceDecodedQueueAndAbort(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(fmt.Sprint(abort), func(t *testing.T) {
			o := &fenceOwner{block: make(chan struct{}), entered: make(chan struct{})}
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			s, err := Start(l, []Owner{o}, Config{BatchSize: 1, SyncInterval: time.Hour, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				t.Fatal(err)
			}
			// A pipe makes the input-read boundary observable without socket buffering.
			input, output := net.Pipe()
			readDone := make(chan error, 1)
			go func() { readDone <- s.read(input) }()
			if _, err = io.WriteString(output, "same.name 1 1000\n"); err != nil {
				t.Fatal(err)
			}
			<-o.entered
			if _, err = io.WriteString(output, "same.name 2 1001\n"); err != nil {
				t.Fatal(err)
			}
			if _, err = io.WriteString(output, "same.name 3 1002\n"); err != nil {
				t.Fatal(err)
			}
			// Taking a fourth read requires flushing the third decoded point first.
			fourth := make(chan struct{})
			go func() { io.WriteString(output, "same.name 4 1003\n"); close(fourth) }()
			fenced := make(chan error, 1)
			go func() {
				fenced <- s.Fence(func() error {
					if o.durable.Load() < 2 {
						t.Error("queued input not drained")
					}
					return nil
				})
			}()
			select {
			case <-fenced:
				t.Fatal("maintenance passed blocked decoded/queued input")
			case <-time.After(10 * time.Millisecond):
			}
			if abort {
				s.Abort(fmt.Errorf("test abort"))
			}
			close(o.block)
			output.Close()
			<-fourth
			<-readDone
			input.Close()
			err = <-fenced
			if !abort && err != nil {
				t.Fatal(err)
			}
			if e := s.Wait(t.Context()); !abort && e != nil {
				t.Fatal(e)
			}
		})
	}
}

// An incomplete next line leaves an already decoded point in a local batch
// while the socket waits for more bytes. Expiry must flush it without EOF.
func TestPendingDecodedIdleFence(t *testing.T) {
	o := &fenceOwner{}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Start(l, []Owner{o}, Config{BatchSize: 64, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	input, output := net.Pipe()
	readDone := make(chan error, 1)
	go func() { readDone <- s.read(input) }()
	if _, err = io.WriteString(output, "same.name 1 1000\nnext.incomplete"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for o.durable.Load() == 0 {
		if err = s.Fence(func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("decoded point remained behind idle socket")
		}
		time.Sleep(time.Millisecond)
	}
	if o.durable.Load() != 1 {
		t.Fatal(o.durable.Load())
	}
	output.Close()
	<-readDone
	input.Close()
	if err = s.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}
