package tcp

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// sequenceStore records each client's point sequence numbers in arrival order.
type sequenceStore struct {
	mu   sync.Mutex
	seqs map[string][]int
	n    int
	wake chan struct{}
}

func (s *sequenceStore) add(p *points.Points) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seqs == nil {
		s.seqs = make(map[string][]int)
	}
	client := p.Metric[:strings.IndexByte(p.Metric, '.')]
	for _, d := range p.Data {
		s.seqs[client] = append(s.seqs[client], int(d.Value))
		s.n++
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *sequenceStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// TestDetachKeepsEveryPoint moves a receiver's connections to a new receiver
// while clients keep writing lines split at arbitrary byte boundaries. Each
// point must arrive exactly once and in order, across both receivers.
func TestDetachKeepsEveryPoint(t *testing.T)        { testDetachKeepsEveryPoint(t, false) }
func TestDetachBatchedKeepsEveryPoint(t *testing.T) { testDetachKeepsEveryPoint(t, true) }
func testDetachKeepsEveryPoint(t *testing.T, batched bool) {
	options := &Options{Listen: "127.0.0.1:0", Enabled: true}
	wake := make(chan struct{}, 1)
	before, after := sequenceStore{wake: wake}, sequenceStore{wake: wake}
	old, err := newTCP("tcp", options, before.add)
	if err != nil {
		t.Fatal(err)
	}
	if !old.Handoffable() {
		t.Fatal("plaintext receiver must be handoffable")
	}
	if batched {
		old.SetPointsOut(func(name []byte, data []points.Point) { before.add(&points.Points{Metric: string(name), Data: data}) })
	}
	defer old.Stop()
	addr := old.Addr().String()

	const clients, perClient = 24, 3000
	// One client's metric name is longer than the read buffer, so a detach
	// can split it across the partial line and the buffer.
	long := strings.Repeat("x", plainReadBufferSize+100)
	var wg sync.WaitGroup
	around, inherited := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-around:
		default:
			close(around)
		}
		select {
		case <-inherited:
		default:
			close(inherited)
		}
		wg.Wait()
	}()
	start := func(i int) {
		wg.Go(func() {
			switch i % 6 {
			case 3, 4:
				<-around
			case 5:
				<-inherited
			}
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			rng := rand.New(rand.NewSource(int64(i)))
			name := fmt.Sprintf("c%d.metric", i)
			if i == 0 {
				name = "c0." + long
			}
			var pending []byte
			var fragments [64][]byte
			for seq := range perClient {
				if seq == perClient/2 {
					<-inherited
				}
				pending = append(pending, name...)
				pending = append(pending, ' ')
				pending = strconv.AppendInt(pending, int64(seq), 10)
				pending = append(pending, " 1700000000\n"...)
				if rng.Intn(4) == 0 || seq == perClient-1 {
					for rest := pending; len(rest) > 0; {
						// TCP can coalesce writes; batch the same short fragments in writev.
						var n int
						for n < len(fragments) && len(rest) > 0 {
							size := min(len(rest), 1+rng.Intn(64))
							fragments[n] = rest[:size]
							n++
							rest = rest[size:]
						}
						batch := net.Buffers(fragments[:n])
						if _, err := batch.WriteTo(conn); err != nil {
							t.Error(err)
							return
						}
					}
					pending = pending[:0]
				}
			}
		})
	}
	for i := range clients {
		start(i)
	}

	wait := func(ready func() bool) {
		t.Helper()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		for !ready() {
			select {
			case <-wake:
			case <-timer.C:
				t.Fatal("timed out waiting for points")
			}
		}
	}
	wait(func() bool {
		before.mu.Lock()
		defer before.mu.Unlock()
		return len(before.seqs) == clients/2
	})
	close(around)
	h, err := old.Detach()
	if err != nil {
		t.Fatal(err)
	}
	next, err := NewInherited("tcp", &Options{Listen: addr, Enabled: true}, after.add, h)
	if err != nil {
		t.Fatal(err)
	}
	if batched {
		next.SetPointsOut(func(name []byte, data []points.Point) { after.add(&points.Points{Metric: string(name), Data: data}) })
	}
	defer next.Stop()
	close(inherited)
	if next.Addr().String() != addr {
		t.Fatalf("inherited %s, want %s", next.Addr(), addr)
	}
	wg.Wait()

	wait(func() bool { return before.count()+after.count() == clients*perClient })
	if before.count() == 0 || after.count() == 0 {
		t.Fatalf("detach did not split the stream: %d before, %d after", before.count(), after.count())
	}
	for i := range clients {
		client := fmt.Sprintf("c%d", i)
		seqs := append(append([]int(nil), before.seqs[client]...), after.seqs[client]...)
		if len(seqs) != perClient {
			t.Fatalf("%s: %d points, want %d", client, len(seqs), perClient)
		}
		for want, got := range seqs {
			if got != want {
				t.Fatalf("%s: point %d has sequence %d", client, want, got)
			}
		}
	}
}

func TestDetachRequiresPlaintext(t *testing.T) {
	for _, options := range []*Options{
		{Listen: "127.0.0.1:0", Enabled: true, Compression: "gzip"},
		{Listen: "127.0.0.1:0", Enabled: true, BufferSize: 10},
	} {
		r, err := newTCP("tcp", options, func(*points.Points) {})
		if err != nil {
			t.Fatal(err)
		}
		if r.Handoffable() {
			t.Fatalf("%+v must not be handoffable", options)
		}
		if _, err = r.Detach(); err == nil {
			t.Fatalf("%+v detached", options)
		}
		r.Stop()
	}
}
