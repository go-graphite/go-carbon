package v2core

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	v2part "github.com/go-graphite/go-carbon/internal/v2part"
)

// TestEventResetsEngineDifferential runs one expiry and re-incarnation history
// with background merges and restarts twice: envelopes that copy ResetOrder
// forward (classic form), and envelopes that store it only at the expiry seal
// (event form). Recovery, render snapshots and stored records must be
// identical after every seal, and the event form must store the reset only in
// the tombstone envelope and in merges of it.
func TestEventResetsEngineDifferential(t *testing.T) {
	classic := eventResetsHistory(t, false)
	event := eventResetsHistory(t, true)
	if len(classic.states) != len(event.states) {
		t.Fatalf("seal counts differ: %d vs %d", len(classic.states), len(event.states))
	}
	for i := range classic.states {
		if classic.states[i] != event.states[i] {
			t.Fatalf("seal %d differs:\nclassic: %s\nevent:   %s", i, classic.states[i], event.states[i])
		}
	}
	if classic.carried <= event.carried || event.tombstones == 0 {
		t.Fatalf("envelopes with ResetOrder: classic %d, event %d (tombstones %d)", classic.carried, event.carried, event.tombstones)
	}
	var order, reset uint64
	if _, err := fmt.Sscanf(classic.states[len(classic.states)-1], "event.metric order=%d reset=%d", &order, &reset); err != nil || reset == 0 || reset >= order {
		t.Fatalf("history never reset and re-incarnated: order %d reset %d (%v)", order, reset, err)
	}
	t.Logf("envelopes storing ResetOrder over all seals: classic %d, event %d (tombstone %d); live parts max %d", classic.carried, event.carried, event.tombstones, event.maxParts)
}

type eventResetsRun struct {
	states                        []string
	carried, tombstones, maxParts int
}

func eventResetsHistory(t *testing.T, event bool) eventResetsRun {
	rollupEventResets.Store(event)
	defer rollupEventResets.Store(false)
	_, policy := rollupTestPolicy(t, "event.metric", "10s:6h,5m:7d,1h:1y", sealrollup.Average, 0)
	_, other := rollupTestPolicy(t, "event.other", "10s:6h,5m:7d,1h:1y", sealrollup.Average, 0)
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups, cfg.MaxRows = 1, 1, true, 1024
	root := t.TempDir()
	now := int64(1794700800)
	var clock atomic.Int64
	clock.Store(now)
	expireAt := int64(math.MaxInt64)
	hooks := Hooks{
		Now: func() time.Time { return time.Unix(clock.Load(), 0) },
		Expire: func(p RollupPolicy, m RollupMetadata, at time.Time) bool {
			// One expiry window, so wall-clock maintenance cannot expire twice.
			return string(p.Name) == "event.metric" && at.Unix() >= expireAt && at.Unix() < expireAt+1800 && !m.Expired
		},
	}
	e, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e != nil {
			_ = e.Close()
		}
	}()
	var run eventResetsRun
	var order uint64
	for epoch := int64(0); epoch < 36; epoch++ {
		now += 600
		clock.Store(now)
		var batches []Batch
		// event.metric writes, goes silent, expires at epoch 10 and is re-born
		// at epoch 14 with points in slots its masked history still occupies.
		if epoch < 8 || epoch >= 14 {
			order++
			var rows []Row
			for j := int64(0); j < 30; j++ {
				rows = append(rows, Row{ID: 1, Time: now - 600 + j*20, Bits: math.Float64bits(float64(epoch*100 + j)), Seq: uint64(j + 1)})
			}
			if epoch >= 14 {
				rows = append(rows, Row{ID: 1, Time: now - (epoch-6)*600, Bits: math.Float64bits(-1), Seq: 99})
			}
			batches = append(batches, Batch{ID: 1, Policy: policy, Order: order, Clock: now, Points: rows})
		}
		order++
		batches = append(batches, Batch{ID: 2, Policy: other, Order: order, Clock: now, Points: []Row{{ID: 2, Time: now - 10, Bits: math.Float64bits(float64(epoch)), Seq: 1}}})
		if epoch == 9 {
			expireAt = now + 300
		}
		if err = e.AppendBatches(0, batches, order, true); err != nil {
			t.Fatal(err)
		}
		if epoch == 10 || epoch == 11 {
			// Give the expiry a seal of its own.
			clock.Store(now + 400)
			if err = e.AppendBatches(0, nil, order, true); err != nil {
				t.Fatal(err)
			}
		}
		run.states = append(run.states, eventResetsState(t, e, &run))
		if epoch == 12 || epoch == 20 || epoch == 29 {
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			if e, err = Open(root, cfg, hooks); err != nil {
				t.Fatal(err)
			}
			run.states = append(run.states, "reopen "+eventResetsState(t, e, &run))
		}
	}
	return run
}

// eventResetsState renders the recovered and snapshot view of every name and
// counts the stored envelopes that carry a reset.
func eventResetsState(t *testing.T, e *Engine, run *eventResetsRun) string {
	t.Helper()
	snap := e.DBs()[0].NewSnapshot()
	defer snap.Close()
	var out []string
	if err := scanRollups(snap, nil, func(p RollupPolicy, m RollupMetadata, records []sealrollup.Record) error {
		visible := 0
		var sum float64
		for _, r := range records {
			visible++
			sum += math.Float64frombits(r.Sample.Bits) * float64(r.Archive+1)
		}
		out = append(out, fmt.Sprintf("%s order=%d reset=%d expired=%v baseline=%d records=%d sum=%g", p.Name, m.Frontier.Order, m.ResetOrder, m.Expired, m.BaselineOrder, visible, sum))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	rs, err := NewRollupSnapshot(context.Background(), snap, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"event.metric", "event.other"} {
		m := rs.metrics[name]
		if m == nil {
			continue
		}
		records, err := rs.Records(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		var sum float64
		for _, r := range records {
			sum += math.Float64frombits(r.Sample.Bits) * float64(r.Archive+1)
		}
		out = append(out, fmt.Sprintf("snapshot %s reset=%d expired=%v records=%d sum=%g", name, m.latest.meta.ResetOrder, m.latest.meta.Expired, len(records), sum))
	}
	it, err := newArchiveIter(context.Background(), snap, nil)
	if err != nil {
		t.Fatal(err)
	}
	parts := map[v2part.PartID]bool{}
	for valid := it.First(); valid; valid = it.Next() {
		key := it.Key()
		if len(key) < 4 || string(key[:4]) != "GCP\x03" {
			continue
		}
		k, err := v2part.Decode(key)
		if err != nil {
			t.Fatal(err)
		}
		if k.Archive != v2part.MetadataArchive {
			continue
		}
		parts[k.Part] = true
		m, err := DecodeRollupMetadata(k.Metric, k.Group, it.Value())
		if err != nil {
			t.Fatal(err)
		}
		if m.ResetOrder != 0 {
			run.carried++
			if m.Expired {
				run.tombstones++
			}
		}
	}
	if err = it.Close(); err != nil {
		t.Fatal(err)
	}
	run.maxParts = max(run.maxParts, len(parts))
	sort.Strings(out)
	return strings.Join(out, "; ")
}
