package v2daemon

import (
	"bytes"
	"context"
	"errors"
	"github.com/go-graphite/go-carbon/internal/v2placement"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/internal/v2wire"
	"math"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRelayDurableACKDisconnectRestartDedup(t *testing.T) {
	s, stamp, doc := placedFixture(t)
	cfg := s.cfg
	root := filepath.Dir(s.lock.Name())
	key := bytes.Repeat([]byte{0x4b}, 32)
	selector, v, e := doc.Routing.Select("metric.a")
	if e != nil {
		t.Fatal(e)
	}
	f := v2wire.Frame{Kind: v2wire.Data, Stream: v2wire.Stream{Group: selector.GroupID(), Replica: stamp.Replica, Store: "store-a/inc-1", MapDigest: doc.Digest(), Revision: 1, Epoch: 1, Producer: stamp.Lane.Producer, Lane: stamp.Lane.Stream}, Sequence: 1, VShard: v, Name: "metric.a", Bits: math.Float64bits(math.Copysign(0, -1)), Timestamp: 100}
	start := func(s *Store) *v2recv.Server {
		t.Helper()
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		server, e := s.ReceivePlacedTCP(l, v2recv.Config{PlacementKey: key, PlacementProducer: stamp.Lane.Producer}, nil)
		if e != nil {
			l.Close()
			t.Fatal(e)
		}
		return server
	}
	send := func(server *v2recv.Server, f v2wire.Frame, read bool) error {
		t.Helper()
		c, e := net.Dial("tcp", server.Addr().String())
		if e != nil {
			return e
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		if e = v2wire.Write(c, f, key); e != nil {
			return e
		}
		if !read {
			return nil
		}
		ack, e := v2wire.Read(c, key)
		if e == nil && ack != f.Acknowledgement() {
			return errors.New("wrong ACK")
		}
		return e
	}
	stop := func(server *v2recv.Server) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := server.Wait(ctx); e != nil {
			t.Fatal(e)
		}
	}
	server := start(s)
	// Lose the first response after the server durably accepts it. The relay
	// cannot infer acceptance from its Write and must replay the exact frame.
	if e = send(server, f, false); e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(5 * time.Second)
	for server.Stats().Accepted < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.Stats().Accepted != 1 {
		t.Fatal("record never accepted")
	}
	stop(server)
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(root, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	server = start(s)
	if e = send(server, f, true); e != nil {
		t.Fatal("durable duplicate was not acknowledged", e)
	}
	if s.Producers[0].cursor != 1 {
		t.Fatal("retry mutated journal", s.Producers[0].cursor)
	}
	second := f
	second.Sequence++
	second.Bits = math.Float64bits(2)
	if e = send(server, second, true); e != nil {
		t.Fatal(e)
	}
	conflict := second
	conflict.Bits++
	if e = send(server, conflict, true); e == nil {
		t.Fatal("conflicting retry ACKed")
	}
	stale := second
	stale.Sequence++
	stale.Stream.Epoch++
	if e = send(server, stale, true); e == nil {
		t.Fatal("stale epoch ACKed")
	}
	wrong := second
	wrong.Sequence++
	wrong.VShard = (wrong.VShard + 1) % 4096
	if e = send(server, wrong, true); e == nil {
		t.Fatal("wrong vshard ACKed")
	}
	stop(server)
	if s.Producers[0].cursor != 2 {
		t.Fatal("invalid frame mutated journal")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(root, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	server = start(s)
	if e = send(server, second, true); e != nil {
		t.Fatal("second recovered ACK", e)
	}
	stop(server)
	if s.Producers[0].cursor != 2 {
		t.Fatal("second restart duplicated point")
	}
}

func TestRelayIndependentProducerPositionsAndCredentials(t *testing.T) {
	cfg, doc := relayCrashConfig(t, t.TempDir())
	secondLane := v2placement.Lane{Producer: "relay-b/inc-1", Stream: "lane-0"}
	doc.Groups[0].Replicas[0].Producers = append(doc.Groups[0].Replicas[0].Producers, secondLane)
	a, e := v2placement.OpenAuthority(t.TempDir(), doc, doc.Groups[0].Selector.GroupID(), "site-a", "store-a/inc-1")
	if e != nil {
		t.Fatal(e)
	}
	cfg.Placement = a
	cfg.Producers = 2
	root := t.TempDir()
	s, e := Open(root, cfg)
	if e != nil {
		t.Fatal(e)
	}
	keyA := bytes.Repeat([]byte{1}, 32)
	keyB := bytes.Repeat([]byte{2}, 32)
	keys := map[string][]byte{"relay-a/inc-1": keyA, "relay-b/inc-1": keyB}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server, e := s.ReceivePlacedTCP(l, v2recv.Config{PlacementKeys: keys}, nil)
	if e != nil {
		t.Fatal(e)
	}
	sel, v, e := doc.Routing.Select("metric.a")
	if e != nil {
		t.Fatal(e)
	}
	send := func(producer string, key []byte) error {
		c, e := net.Dial("tcp", l.Addr().String())
		if e != nil {
			return e
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		f := v2wire.Frame{Kind: v2wire.Data, Stream: v2wire.Stream{Group: sel.GroupID(), Replica: "site-a", Store: "store-a/inc-1", MapDigest: doc.Digest(), Revision: 1, Epoch: 1, Producer: producer, Lane: "lane-0"}, Sequence: 1, VShard: v, Name: "metric.a", Timestamp: 100}
		if e = v2wire.Write(c, f, key); e != nil {
			return e
		}
		_, e = v2wire.Read(c, key)
		return e
	}
	if e = send("relay-a/inc-1", keyA); e != nil {
		t.Fatal(e)
	}
	if e = send("relay-b/inc-1", keyB); e != nil {
		t.Fatal(e)
	}
	if e = send("relay-b/inc-1", keyA); e == nil {
		t.Fatal("producer A impersonated producer B")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e = server.Wait(ctx); e != nil {
		t.Fatal(e)
	}
	var cursor uint64
	for _, p := range s.Producers {
		cursor += p.cursor
	}
	if cursor != 2 {
		t.Fatal("producer positions collided", cursor)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(root, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	cursor = 0
	for _, p := range s.Producers {
		cursor += p.cursor
	}
	if cursor != 2 {
		t.Fatal("independent producers did not recover")
	}
}

func TestRelayACKAndJournalSurviveMetadataMapAdoption(t *testing.T) {
	authorityRoot := t.TempDir()
	cfg, doc := relayCrashConfig(t, authorityRoot)
	root := t.TempDir()
	s, e := Open(root, cfg)
	if e != nil {
		t.Fatal(e)
	}
	key := bytes.Repeat([]byte{1}, 32)
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server, e := s.ReceivePlacedTCP(l, v2recv.Config{PlacementKey: key, PlacementProducer: "relay-a/inc-1"}, nil)
	if e != nil {
		t.Fatal(e)
	}
	sel, v, e := doc.Routing.Select("metric.a")
	if e != nil {
		t.Fatal(e)
	}
	f := v2wire.Frame{Kind: v2wire.Data, Stream: v2wire.Stream{Group: sel.GroupID(), Replica: "site-a", Store: "store-a/inc-1", MapDigest: doc.Digest(), Revision: 1, Epoch: 1, Producer: "relay-a/inc-1", Lane: "lane-0"}, Sequence: 1, VShard: v, Name: "metric.a", Timestamp: 100}
	c, e := net.Dial("tcp", l.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if e = v2wire.Write(c, f, key); e != nil {
		t.Fatal(e)
	}
	if _, e = v2wire.Read(c, key); e != nil {
		t.Fatal(e)
	}
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e = server.Wait(ctx); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	authority := cfg.Placement.(*v2placement.Authority)
	next := doc
	next.Revision++
	next.Predecessor = doc.Digest()
	next.Operation = "metadata"
	if e = authority.Adopt(next, nil); e != nil {
		t.Fatal(e)
	}
	// Reload the authority from disk, including its exact validated history.
	reopenedAuthority, e := v2placement.OpenAuthority(filepath.Join(authorityRoot, "authority"), next, f.Stream.Group, f.Stream.Replica, f.Stream.Store)
	if e != nil {
		t.Fatal(e)
	}
	cfg.Placement = reopenedAuthority
	authority = reopenedAuthority
	// The existing authority has persisted history; use its detached Document and
	// reopen storage to exercise historical stamp replay and dedup reconstruction.
	s, e = Open(root, cfg)
	if e != nil {
		t.Fatal("accepted stamped history lost on map adoption", e)
	}
	defer s.Close()
	if s.Producers[0].cursor != 1 {
		t.Fatal("history replay lost row")
	}
	nextFrame := f
	nextFrame.Stream.MapDigest = next.Digest()
	nextFrame.Stream.Revision = 2
	nextFrame.Sequence = 2
	receiver := &relayReceiver{store: s, locks: make([]sync.Mutex, 1), document: authority}
	if e = receiver.AcceptPlaced(f); e != nil {
		t.Fatal("old exact durable duplicate lost", e)
	}
	if e = receiver.AcceptPlaced(nextFrame); e != nil {
		t.Fatal("new map same epoch sequence continuity", e)
	}
	stale := f
	stale.Sequence = 3
	if e = receiver.AcceptPlaced(stale); e == nil {
		t.Fatal("history reopened live admission")
	}
	if s.Producers[0].cursor != 2 {
		t.Fatal("history duplicate mutated journal")
	}
}

func TestRelayFenceRequiresRecoveredDurableFinals(t *testing.T) {
	cfg, doc := relayCrashConfig(t, t.TempDir())
	root := t.TempDir()
	s, e := Open(root, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	a := cfg.Placement.(*v2placement.Authority)
	r := &relayReceiver{store: s, locks: make([]sync.Mutex, 1), document: a}
	sel, v, e := doc.Routing.Select("example.metric")
	if e != nil {
		t.Fatal(e)
	}
	f := v2wire.Frame{Kind: v2wire.Data, Stream: v2wire.Stream{Group: sel.GroupID(), Replica: "site-a", Store: "store-a/inc-1", MapDigest: doc.Digest(), Revision: 1, Epoch: 1, Producer: "relay-a/inc-1", Lane: "lane-0"}, Sequence: 1, VShard: v, Name: "example.metric", Timestamp: 100}
	if e = r.AcceptPlaced(f); e != nil {
		t.Fatal(e)
	}
	ends, e := s.DurableRelayEnds()
	if e != nil || len(ends) != 1 || ends[0].Durable != 1 {
		t.Fatal(ends, e)
	}
	bad := append([]v2placement.EndMarker(nil), ends...)
	bad[0].Final++
	if _, e = s.FenceRelayPlacement(context.Background(), "move", bad, nil); e == nil {
		t.Fatal("store fenced before final durable position")
	}
	h := strings.Repeat("a", 64)
	fence, e := s.FenceRelayPlacement(context.Background(), "move", ends, func(frontier [][]uint64) (v2placement.Roots, error) {
		if frontier[0][0] != 1 {
			t.Error("fence before durable core", frontier)
		}
		return v2placement.Roots{Parts: h, Catalog: h, Policy: h, Tombstones: h}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if e = fence.Validate(doc); e != nil {
		t.Fatal(e)
	}
	if e = r.AcceptPlaced(f); e != nil {
		t.Fatal("known old duplicate must retain durable ACK after fence", e)
	}
	f.Sequence++
	if e = r.AcceptPlaced(f); !errors.Is(e, v2placement.ErrStaleEpoch) {
		t.Fatal("novel write after fence", e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(root, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	r = &relayReceiver{store: s, locks: make([]sync.Mutex, 1), document: a}
	f.Sequence = 1
	if e = r.AcceptPlaced(f); e != nil {
		t.Fatal("durable fence lost duplicate receipt on restart", e)
	}
	f.Sequence++
	if e = r.AcceptPlaced(f); !errors.Is(e, v2placement.ErrStaleEpoch) {
		t.Fatal("restart reopened fenced intake", e)
	}
}
