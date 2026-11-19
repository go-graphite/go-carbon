package v2daemon

import (
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/points"
)

func TestApplicationReceiverQuotaAndPolicyCache(t *testing.T) {
	s, err := Open(t.TempDir(), Config{Layer: 3, Producers: 1, Shards: 1, MaxCoreRows: 16, GroupInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	resolves := map[string]int{}
	owner := &applicationOwner{p: s.Producers[0], resolve: func(name string) ([]byte, error) {
		resolves[name]++
		return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "local", Config: sealrollup.Config{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 1, Count: 60}}}})
	}, throttle: func(p *points.Points, _ bool) bool { return p.Metric == "blocked" }}
	if err = owner.Append([]string{"accepted", "blocked", "accepted"}, nil, []points.Point{{Timestamp: 100, Value: 1}, {Timestamp: 100, Value: 9}, {Timestamp: 101, Value: 2}}, nil, 102); err != nil {
		t.Fatal(err)
	}
	if err = owner.Append([]string{"accepted"}, nil, []points.Point{{Timestamp: 102, Value: 3}}, nil, 103); err != nil {
		t.Fatal(err)
	}
	if s.Producers[0].applyOrder != 3 {
		t.Fatalf("order=%d, want3", s.Producers[0].applyOrder)
	}
	if resolves["accepted"] != 1 {
		t.Fatalf("policy reads=%d, want1", resolves["accepted"])
	}
	if err = owner.Seal(); err != nil {
		t.Fatal(err)
	}
	if err = owner.Drain(); err != nil {
		t.Fatal(err)
	}
	if err = s.Core.AppendBatches(0, nil, s.Producers[0].cursor, true); err != nil {
		t.Fatal(err)
	}
	names := 0
	for _, db := range s.Core.DBs() {
		snap := db.NewSnapshot()
		err = v2core.ScanRollups(snap, func(p v2core.RollupPolicy, _ v2core.RollupMetadata, _ []sealrollup.Record) error {
			if string(p.Name) == "blocked" {
				t.Error("rejected quota name persisted")
			}
			names++
			return nil
		})
		snap.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if names != 1 {
		t.Fatalf("stored %d names", names)
	}
}
