package v2core

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// rollupStateDigest lists every installed name's identity and committed scalars
// in ID order.
func rollupStateDigest(r *coreRollups) string {
	ids := make([]uint64, 0, len(r.metrics))
	for id := range r.metrics {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var b strings.Builder
	for _, id := range ids {
		m := r.metrics[id]
		fmt.Fprintf(&b, "%d %s %+v name=%v\n", id, m.definitionBytes(), r.meta[r.slotOf(id, m)], nameIndexed(r, m))
	}
	fmt.Fprintf(&b, "names=%d metrics=%d\n", r.names.Len(), len(r.metrics))
	return b.String()
}

func nameIndexed(r *coreRollups, m *rollupMetric) bool {
	_, ok := r.names.Get(m.fingerprint())
	return ok
}

func rollupScanDigest(t *testing.T, e *Engine) string {
	t.Helper()
	snap := e.DBs()[0].NewSnapshot()
	defer snap.Close()
	var lines []string
	if err := ScanRollups(snap, func(p RollupPolicy, m RollupMetadata, records []sealrollup.Record) error {
		lines = append(lines, fmt.Sprintf("%s %+v %+v", p.Name, m, records))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}

// recoveryFixture writes rounds of points for single- and multi-archive
// names, sealing after each round, from order base+1.
func recoveryFixture(t *testing.T, e *Engine, policies [][]byte, base, rounds int) {
	t.Helper()
	clock := int64(1794700800)
	for round := range rounds {
		order := uint64(base + round + 1)
		now := clock + int64(base+round)*7
		var batches []Batch
		for i, encoded := range policies {
			id := uint64(i + 1)
			if (base+round+i)%5 == 4 {
				continue // idle rounds, so names diverge in frontier and activity
			}
			batches = append(batches, Batch{ID: id, Order: order, Clock: now, Policy: encoded, Points: []Row{
				{ID: id, Time: now - int64(i%3)*61, Bits: math.Float64bits(float64(round*31 + i)), Seq: order*100 + id},
			}})
		}
		if err := e.AppendBatches(0, batches, order, true); err != nil {
			t.Fatal(err)
		}
	}
}

func recoveryTestSetup(t *testing.T) (Config, [][]byte) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups, cfg.MaxRows = 1, 1, true, 1024
	var policies [][]byte
	for i, def := range []string{"1s:1d", "1s:1d,1m:30d,1h:1y,1d:10y", "1m:30d,1h:1y,1d:10y", "10s:6h,5m:7d,1h:1y", "1m:14d,30m:2y"} {
		for j := range 4 {
			_, encoded := rollupTestPolicy(t, fmt.Sprintf("ckpt.m%d.n%d", i, j), def, sealrollup.Method(1+(i+j)%5), float32(j%2)/2)
			policies = append(policies, encoded)
		}
	}
	return cfg, policies
}

// Owner state belongs to the owner goroutine, including an idle seal's renumber.
func ownerStateDigest(t *testing.T, e *Engine) string {
	t.Helper()
	var digest string
	ack := make(chan error, 1)
	o := e.owners[0]
	op := func(o *coreOwner) error { digest = rollupStateDigest(o.rollups); return nil }
	if err := o.queues[0].push(&corePacket{ack: ack, operation: op}, o.dead); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ack:
		if err != nil {
			t.Fatal(err)
		}
	case <-o.dead:
		t.Fatal("owner stopped")
	}
	return digest
}
