package v2daemon

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/points"
)

// TestMirrorAtDrain: the classic mirror sees each drained application with
// its canonical policy before core does, and never on recovery: classic
// replays its own input log. A mirror failure fails the producer.
func TestMirrorAtDrain(t *testing.T) {
	root := t.TempDir()
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID("mirror.metric"), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 120}}}})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	cfg := Config{Layer: 3, Producers: 1, Shards: 1, GroupInterval: time.Hour, MaxCoreRows: 256}
	prepared := false
	cfg.Prepare = func() error { prepared = true; return nil }
	cfg.Mirror = func(producer int, b []v2core.Batch) error {
		if !prepared || producer != 0 || len(b) != 1 || len(b[0].Points) != 1 || b[0].Order != 1 {
			t.Fatal("invalid mirror application")
		}
		if !bytes.Equal(b[0].Policy, policy) {
			t.Fatal("mirror must receive the canonical policy")
		}
		calls++
		return nil
	}
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := s.Producers[0]
	app := []Application{{Name: "mirror.metric", Policy: policy, Order: 1, Clock: 10000, Points: []points.Point{{Timestamp: 9990, Value: 3}}, Sequence: []uint64{1}}}
	if err := p.AppendApplications(app); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("mirror ran before the drain")
	}
	if err := p.Drain(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("mirror calls=%d", calls)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	prepared = false
	s, err = Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("recovery mirrored again: %d", calls)
	}
	sentinel := errors.New("classic unavailable")
	s.Producers[0].mirror = func(int, []v2core.Batch) error { return sentinel }
	app[0].Order = s.Producers[0].applyOrder + 1
	app[0].Name, app[0].Policy = "", nil
	app[0].Registered, app[0].LocalID, err = s.Producers[0].RegisterApplication("mirror.metric", policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Producers[0].AppendApplications(app); err != nil {
		t.Fatal(err)
	}
	if err := s.Producers[0].Drain(); !errors.Is(err, sentinel) {
		t.Fatalf("mirror failure ignored: %v", err)
	}
	_ = s.Close()
}
