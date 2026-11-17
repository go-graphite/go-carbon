package v2daemon

import (
	"errors"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/points"
)

func TestDurableApplicationsMirrorAndReplay(t *testing.T) {
	root := t.TempDir()
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID("mirror.metric"), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 120}}}})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	cfg := Config{Layer: 3, Producers: 1, Shards: 1, GroupInterval: time.Hour, coreMaxRows: 256}
	prepared := false
	cfg.Prepare = func() error { prepared = true; return nil }
	cfg.DurableApplications = func(producer int, b []v2core.Batch) error {
		if !prepared || producer != 0 || len(b) != 1 || len(b[0].Points) != 1 || b[0].Order != 1 {
			t.Fatal("invalid mirror application")
		}
		calls++
		return nil
	}
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := s.Producers[0]
	if err := p.AppendApplications([]Application{{Name: "mirror.metric", Policy: policy, Order: 1, Clock: 10000, Points: []points.Point{{Timestamp: 9990, Value: 3}}, Sequence: []uint64{1}}}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("mirror ran before journal sync")
	}
	if err := p.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := p.Drain(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || p.SyncGroups != 1 {
		t.Fatalf("mirror calls=%d groups=%d", calls, p.SyncGroups)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// A committed core frontier must not suppress recovery of the second sink.
	prepared = false
	s, err = Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("durable classic prefix not replayed: %d", calls)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("classic unavailable")
	cfg.DurableApplications = func(int, []v2core.Batch) error { return sentinel }
	if s, err := Open(root, cfg); !errors.Is(err, sentinel) {
		if s != nil {
			_ = s.Close()
		}
		t.Fatalf("classic recovery failure ignored: %v", err)
	}
}
