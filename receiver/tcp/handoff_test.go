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
}

func (s *sequenceStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// TestDetachKeepsEveryPoint moves a receiver's connections to a new receiver
// while clients keep writing lines split at arbitrary byte boundaries. Each
// point must arrive exactly once and in order, across both receivers.
func TestDetachKeepsEveryPoint(t *testing.T) {
	options := &Options{Listen: "127.0.0.1:0", Enabled: true}
	var before, after sequenceStore
	old, err := newTCP("tcp", options, before.add)
	if err != nil {
		t.Fatal(err)
	}
	if !old.Handoffable() {
		t.Fatal("plaintext receiver must be handoffable")
	}
	addr := old.Addr().String()

	const clients, perClient = 24, 3000
	// One client's metric name is longer than the read buffer, so a detach
	// can split it across the partial line and the buffer.
	long := strings.Repeat("x", 6000)
	var wg sync.WaitGroup
	start := func(i int, delay time.Duration) {
		wg.Go(func() {
			time.Sleep(delay)
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
			for seq := range perClient {
				pending = append(pending, name+" "+strconv.Itoa(seq)+" 1700000000\n"...)
				for len(pending) > 0 && (rng.Intn(4) == 0 || seq == perClient-1) {
					n := min(len(pending), 1+rng.Intn(64))
					if _, err := conn.Write(pending[:n]); err != nil {
						t.Error(err)
						return
					}
					pending = pending[n:]
				}
				if rng.Intn(50) == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		})
	}
	for i := range clients {
		// Some clients connect around the detach.
		start(i, time.Duration(i%6)*20*time.Millisecond)
	}

	time.Sleep(60 * time.Millisecond)
	h, err := old.Detach()
	if err != nil {
		t.Fatal(err)
	}
	next, err := NewInherited("tcp", &Options{Listen: addr, Enabled: true}, after.add, h)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Stop()
	if next.Addr().String() != addr {
		t.Fatalf("inherited %s, want %s", next.Addr(), addr)
	}
	wg.Wait()

	deadline := time.Now().Add(10 * time.Second)
	for before.count()+after.count() < clients*perClient && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
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
