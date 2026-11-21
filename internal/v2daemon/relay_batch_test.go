package v2daemon

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2placement"
	"github.com/go-graphite/go-carbon/internal/v2wire"
)

func TestRelayBatchRecoveryAndRetryWindow(t *testing.T) {
	root := t.TempDir()
	cfg, doc := relayCrashConfig(t, root)
	s, e := Open(filepath.Join(root, "data"), cfg)
	if e != nil {
		t.Fatal(e)
	}
	receiver := func(s *Store) *relayReceiver {
		return &relayReceiver{store: s, locks: make([]sync.Mutex, len(s.Producers)), document: cfg.Placement.(interface{ Document() v2placement.Document })}
	}
	r := receiver(s)
	selector, v, e := doc.Routing.Select("example.metric")
	if e != nil {
		t.Fatal(e)
	}
	frames := make([]v2wire.Frame, 256)
	for i := range frames {
		frames[i] = v2wire.Frame{Kind: v2wire.Data, Stream: v2wire.Stream{Group: selector.GroupID(), Replica: "site-a", Store: "store-a/inc-1", MapDigest: doc.Digest(), Revision: 1, Epoch: 1, Producer: "relay-a/inc-1", Lane: "lane-0"}, Sequence: uint64(i + 1), VShard: v, Name: "example.metric", Bits: 0x8000000000000000, Timestamp: 100}
	}
	before := s.Producers[0].SyncGroups
	if e = r.AcceptPlacedBatch(frames); e != nil {
		t.Fatal(e)
	}
	if delta := s.Producers[0].SyncGroups - before; delta != 1 {
		t.Fatalf("batch journal syncs=%d", delta)
	}
	if s.Producers[0].cursor != 256 {
		t.Fatal("batch cursor")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(filepath.Join(root, "data"), cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	r = receiver(s)
	checkRelayCrashRow(t, s)
	if e = r.AcceptPlacedBatch(frames); e != nil {
		t.Fatal("lost-ACK batch failed after recovery", e)
	}
	if s.Producers[0].cursor != 256 {
		t.Fatal("replayed batch duplicated")
	}
	conflict := frames[0]
	conflict.Bits++
	if e = r.AcceptPlaced(conflict); e == nil {
		t.Fatal("conflicting historical retry accepted")
	}
	next := frames[255]
	next.Sequence++
	if e = r.AcceptPlaced(next); e != nil {
		t.Fatal(e)
	}
	if e = r.AcceptPlaced(frames[0]); e == nil {
		t.Fatal("expired retry accepted")
	}
	if e = r.AcceptPlaced(frames[1]); e != nil {
		t.Fatal("live retry expired", e)
	}
}
