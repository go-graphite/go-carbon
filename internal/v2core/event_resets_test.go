package v2core

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
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
	// Recovered records still hold points past retention until a merge drops
	// them, which background timing decides; compare the recovered metadata and
	// the snapshot's retention-filtered records.
	if err := scanRollups(snap, nil, func(p RollupPolicy, m RollupMetadata, _ []sealrollup.Record) error {
		out = append(out, fmt.Sprintf("%s order=%d reset=%d expired=%v baseline=%d", p.Name, m.Frontier.Order, m.ResetOrder, m.Expired, m.BaselineOrder))
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

// TestEventResetMergeKeepsTombstone merges a tombstone with a newer event-form
// part whose envelope stores no reset, while an older part outside the merge
// still holds the record the reset masks. The output must keep the reset.
func TestEventResetMergeKeepsTombstone(t *testing.T) {
	policy, encoded := rollupTestPolicy(t, "tomb.metric", "1s:120,10s:1h", sealrollup.Last, 0)
	digest, err := sealrollup.PolicyDigest(policy.Config)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err = os.Mkdir(filepath.Join(dir, "parts"), 0700); err != nil {
		t.Fatal(err)
	}
	write := func(order uint64, slot int64, md RollupMetadata) v2part.Descriptor {
		k := v2part.Key{Group: policy.Group, VShard: v2part.Route(policy.Name), Part: v2part.PartID{byte(order)}, Metric: policy.Name}
		var rows []KV
		if slot != 0 {
			k.Archive, k.BlockStart = 0, slot
			enc, err := v2part.Encode(k)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, KV{Key: enc, Value: sealrollup.EncodeRecord(sealrollup.Record{Delta: sealrollup.Delta{Slot: slot, Order: order, Sample: sealrollup.Sample{Time: 960 + slot, Bits: math.Float64bits(float64(order))}}})})
		}
		md.Version, md.ID, md.Policy, md.Frontier.Order, md.Frontier.Policy, md.LastUpdate, md.Revision = 2, 1, encoded, order, digest, 1001e9, order
		md.Frontier.MaxTime = 1001
		k.Archive, k.BlockStart = v2part.MetadataArchive, 0
		enc, err := v2part.Encode(k)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, KV{Key: enc, Value: AppendRollupMetadata(nil, md)})
		path := filepath.Join(dir, fmt.Sprintf("in-%d.sst", order))
		d, err := WriteDenseRollupPart(path, rows, v2part.Provenance{Stream: "event-reset", Incarnation: v2part.PartID{7}, MinOrder: order, MaxOrder: order})
		if err != nil {
			t.Fatal(err)
		}
		if err = os.Rename(path, filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")); err != nil {
			t.Fatal(err)
		}
		return d
	}
	older := write(1, 40, RollupMetadata{})
	tombstone := write(2, 0, RollupMetadata{Expired: true, ResetOrder: 2})
	newer := write(3, 41, RollupMetadata{})
	merge := func(name string, inputs ...v2part.Descriptor) (RollupMetadata, []int64) {
		path := filepath.Join(dir, name+".sst")
		d, err := mergePartFiles(context.Background(), dir, path, inputs, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		var meta RollupMetadata
		var slots []int64
		if err = v2part.Read(context.Background(), path, &d, func(k v2part.Key, v []byte) error {
			if k.Archive == v2part.MetadataArchive {
				meta, err = DecodeRollupMetadata(k.Metric, k.Group, v)
				return err
			}
			slots = append(slots, k.BlockStart)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return meta, slots
	}
	// The older part stays outside: its slot-40 record (order 1) must stay masked.
	meta, slots := merge("tombstone-newer", tombstone, newer)
	if meta.ResetOrder != 2 || meta.Expired || meta.Frontier.Order != 3 || !slices.Equal(slots, []int64{41}) {
		t.Fatalf("merge lost the tombstone: reset %d expired %v order %d slots %v", meta.ResetOrder, meta.Expired, meta.Frontier.Order, slots)
	}
	// With every part inside, the masked record goes and the reset stays.
	meta, slots = merge("all", older, tombstone, newer)
	if meta.ResetOrder != 2 || meta.Expired || !slices.Equal(slots, []int64{41}) {
		t.Fatalf("full merge: reset %d expired %v slots %v", meta.ResetOrder, meta.Expired, slots)
	}
}
