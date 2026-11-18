package v2core

import (
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"math"
	"testing"
	"time"
)

func TestClosedIdleWindowAndReplay(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	cfg := coreTestConfig()
	cfg.Shards = 1
	cfg.Producers = 1
	cfg.Rollups = true
	root := t.TempDir()
	e, err := Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e != nil {
			_ = e.Close()
		}
	}()
	now := int64(1794700800)
	e.owners[0].rollups.now = func() time.Time { return time.Unix(now, 0) }
	_, p := rollupTestPolicy(t, "idle.metric", "1s:1h,1m:1d,1h:1y", sealrollup.Average, 0)
	if err = e.AppendBatches(0, []Batch{{ID: 1, Policy: p, Order: 1, Clock: now, Points: []Row{{ID: 1, Time: now, Bits: math.Float64bits(7), Seq: 1}}}}, 1, true); err != nil {
		t.Fatal(err)
	}
	// Reopen in the middle of both coarse windows, then materialize the idle tail.
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e = nil
	e, err = Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	now += 3600
	e.owners[0].rollups.now = func() time.Time { return time.Unix(now, 0) }
	if err = e.AppendBatches(0, nil, 1, true); err != nil {
		t.Fatal(err)
	}
	check := func() {
		pin := e.DBs()[0].NewSnapshot()
		defer pin.Close()
		found := false
		if err := scanRollups(pin, nil, func(_ RollupPolicy, m RollupMetadata, rs []sealrollup.Record) error {
			found = true
			if m.ClosedThrough != now || m.Revision != 2 || m.Frontier.Order != 1 {
				t.Fatalf("idle metadata %+v", m)
			}
			for a := 1; a <= 2; a++ {
				ok := false
				for _, r := range rs {
					if r.Archive == a && r.Sample.Bits == math.Float64bits(7) {
						ok = true
					}
				}
				if !ok {
					t.Fatalf("idle archive%d not materialized", a)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatal("missing idle metric")
		}
	}
	check()
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e = nil
	e, err = Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	check()
}
