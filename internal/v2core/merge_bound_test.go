package v2core

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestMergeBoundSelection(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixture(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	for n := uint64(1); n <= 32; n++ {
		mergeAppend(t, e, n)
	}
	s := o.db.NewSnapshot()
	defer s.Close()
	inputs, err := o.selectMerge(s)
	if err != nil || len(inputs) != 16 {
		t.Fatalf("inputs=%d %v", len(inputs), err)
	}
	var total uint64
	for _, d := range inputs {
		total += d.Bytes
	}
	t.Setenv("CORE_MERGE_MAX_BYTES", strconv.FormatUint(total-1, 10))
	inputs, err = o.selectMerge(s)
	if err != nil || len(inputs) != 0 {
		t.Fatalf("aggregate cap ignored %d %v", len(inputs), err)
	}
	t.Setenv("CORE_MERGE_MAX_BYTES", "1")
	inputs, err = o.selectMerge(s)
	if err != nil || len(inputs) != 0 {
		t.Fatalf("oversized part selected %d %v", len(inputs), err)
	}
	t.Setenv("CORE_MERGE_MAX_BYTES", "0")
	if _, err = o.selectMerge(s); err == nil {
		t.Fatal("zero cap accepted")
	}
}

func TestMergeBoundExpirySaturates(t *testing.T) {
	c := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 60}}}
	r := sealrollup.Record{Delta: sealrollup.Delta{Sample: sealrollup.Sample{Time: math.MaxInt64 - 1}}}
	if got := rollupRecordExpiry(c, r); got != math.MaxInt64 {
		t.Fatalf("overflowed expiry: %d", got)
	}
}

func TestMergeBoundPrunesExpiredCombinedPart(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixture(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	p, enc := rollupTestPolicy(t, "merge.prune", "1s:120,10s:1h", sealrollup.Last, .5)
	digest, _ := sealrollup.PolicyDigest(p.Config)
	for n := byte(1); n <= 16; n++ {
		m := RollupMetadata{Version: 1, ID: 1, Policy: enc, Frontier: sealrollup.Frontier{Order: uint64(n), MaxTime: 2000, Policy: digest}, LastUpdate: 2000e9, Revision: uint64(n)}
		rs := []sealrollup.Record{{Delta: sealrollup.Delta{Archive: 0, Slot: 40, Order: uint64(n), Sample: sealrollup.Sample{Time: 1000, Bits: math.Float64bits(1)}}}, {Delta: sealrollup.Delta{Archive: 0, Slot: 41, Order: uint64(n), Sample: sealrollup.Sample{Time: 1961, Bits: math.Float64bits(2)}}}}
		installMergeRecords(t, o, n, []RollupMetadata{m}, [][]sealrollup.Record{rs})
	}
	pin := o.db.NewSnapshot()
	defer pin.Close()
	if len(mergeRead(t, pin)) != 2 {
		t.Fatal("fixture")
	}
	if mergeDrain(t, o) != 1 {
		t.Fatal("did not merge")
	}
	s := o.db.NewSnapshot()
	defer s.Close()
	got := mergeRead(t, s)
	if len(got) != 1 || got[0].Sample.Time != 1961 {
		t.Fatalf("expired point retained %+v", got)
	}
	if len(mergeRead(t, pin)) != 2 {
		t.Fatal("reader fence lost expired bytes")
	}
}

func TestMergeBoundPartitionReadDropAndRestart(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	// Cross several UTC days while retaining all input archives.
	now := time.Now().Unix()
	start := now - 3*86400
	root := t.TempDir()
	e := mergeFixture(t, root, nil)
	defer func() {
		if e != nil {
			_ = e.close()
		}
	}()
	o := e.owners[0]
	_, enc := rollupTestPolicy(t, "partition.metric", "1s:7d,1m:30d", sealrollup.Last, .5)
	for n := uint64(1); n <= 48; n++ {
		stamp := start + int64(n)*3600
		err := e.submitBatches(0, []coreBatch{{ID: 7, Policy: enc, Order: n, Clock: stamp + 1, Points: []row{{ID: 7, Time: stamp, Bits: math.Float64bits(float64(n)), Seq: n}}}}, n, true)
		if err != nil {
			t.Fatal(err)
		}
	}
	pin := o.db.NewSnapshot()
	want := mergeRead(t, pin)
	if len(want) == 0 {
		t.Fatal("empty fixture")
	}
	seen := map[int64]bool{}
	o.gc.mu.Lock()
	for _, v := range o.gc.inventory.bindings {
		d, err := v2part.ParseDescriptor(v)
		if err != nil {
			t.Fatal(err)
		}
		seen[d.Scope.Partition] = true
	}
	o.gc.mu.Unlock()
	if len(seen) < 4 || !seen[0] {
		t.Fatalf("partitions=%v", seen)
	}
	mergeDrain(t, o)
	s := o.db.NewSnapshot()
	got := mergeRead(t, s)
	_ = s.Close()
	if !reflect.DeepEqual(got, want) {
		t.Fatal("partition merge changed winners")
	}
	// The opened-name and cold-history paths must also retain partition scopes.
	o.gc.mu.Lock()
	inv := o.gc.inventory
	o.gc.mu.Unlock()
	s = o.db.NewSnapshot()
	opened, err := OpenRollupParts(context.Background(), s, inv, nil, nil)
	if err != nil || opened == nil {
		t.Fatalf("opened parts: %v", err)
	}
	index, err := opened.SnapshotForNames(context.Background(), s, []string{"partition.metric"}, &RollupReadCache{})
	if err != nil {
		t.Fatal(err)
	}
	rs, err := index.Records(context.Background(), "partition.metric")
	if err != nil || len(rs) != len(want) {
		t.Fatalf("opened records %d %v", len(rs), err)
	}
	opened.Close()
	_ = s.Close()
	_ = pin.Close()
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	e = nil
	e = mergeFixture(t, root, nil)
	o = e.owners[0]
	s = o.db.NewSnapshot()
	got = mergeRead(t, s)
	_ = s.Close()
	if !reflect.DeepEqual(got, want) {
		t.Fatal("partition restart changed winners")
	}
	before := e.PartGCStats()[0]
	pin = o.db.NewSnapshot()
	defer pin.Close()
	if err = o.expireSealedPartitions(now + 60*86400); err != nil {
		t.Fatal(err)
	}
	after := e.PartGCStats()[0]
	if after.MergeWrittenBytes != before.MergeWrittenBytes || after.LiveParts >= before.LiveParts {
		t.Fatalf("partition drop rewrote or retained parts: %+v -> %+v", before, after)
	}
	s = o.db.NewSnapshot()
	defer s.Close()
	if len(mergeRead(t, s)) != 0 {
		t.Fatal("expired partition rows remain")
	}
	if !reflect.DeepEqual(mergeRead(t, pin), want) {
		t.Fatal("drop violated pin")
	}
	var count int
	if err = ScanCatalogNames(context.Background(), s, func(string) error { count++; return nil }); err != nil || count == 0 {
		t.Fatalf("metadata authority lost %d %v", count, err)
	}
	b, _ := json.Marshal(after)
	t.Log(string(b))
}

func TestMergeBoundPartitionMove(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	now := time.Now().Unix()
	donor := mergeFixture(t, t.TempDir(), nil)
	defer donor.close()
	_, policy := rollupTestPolicy(t, "move.partition", "1s:1d,1m:30d", sealrollup.Last, .5)
	err := donor.submitBatches(0, []coreBatch{{ID: 7, Policy: policy, Order: 1, Clock: now, Points: []row{{ID: 7, Time: now - 10, Bits: 1 << 63, Seq: 1}, {ID: 7, Time: now - 5, Bits: 0x7ff8000000000042, Seq: 2}}}}, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	o := donor.owners[0]
	pin := o.db.NewSnapshot()
	want := mergeRead(t, pin)
	defer pin.Close()
	var parts []MovePart
	o.gc.mu.Lock()
	for _, v := range o.gc.inventory.bindings {
		d, err := v2part.ParseDescriptor(v)
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, MovePart{Path: gcPath(o.gc, d), Descriptor: d})
	}
	o.gc.mu.Unlock()
	if len(parts) < 2 {
		t.Fatal("fixture did not partition")
	}
	prepared, err := PrepareMove(context.Background(), t.TempDir(), parts)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	receiver := mergeFixture(t, t.TempDir(), nil)
	defer receiver.close()
	if err = receiver.AdoptMove(context.Background(), prepared, map[string]uint64{"move.partition": 7}, now); err != nil {
		t.Fatal(err)
	}
	s := receiver.owners[0].db.NewSnapshot()
	defer s.Close()
	got := mergeRead(t, s)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("partition move changed records: got=%v want=%v", got, want)
	}
}
