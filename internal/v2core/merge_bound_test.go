package v2core

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
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
	if err != nil || len(inputs) != smallMergeRatio {
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
	for n := byte(1); n <= smallMergeRatio; n++ {
		m := RollupMetadata{Version: 1, ID: 1, Policy: enc, Frontier: sealrollup.Frontier{Order: uint64(n), MaxTime: 2000, Policy: digest}, LastUpdate: 2000e9, Revision: uint64(n)}
		rs := []sealrollup.Record{{Delta: sealrollup.Delta{Archive: 0, Slot: 40, Order: uint64(n), Sample: sealrollup.Sample{Time: 1000, Bits: math.Float64bits(1)}}}, {Delta: sealrollup.Delta{Archive: 0, Slot: 41, Order: uint64(n), Sample: sealrollup.Sample{Time: 1961, Bits: math.Float64bits(2)}}}}
		installMergeRecords(t, o, n, []RollupMetadata{m}, [][]sealrollup.Record{rs})
	}
	pin := o.db.NewSnapshot()
	defer pin.Close()
	if len(mergeRead(t, pin)) != 2 {
		t.Fatal("fixture")
	}
	// The merge prunes by the owner's retire clock, as age retirement does:
	// the reader still returns the record until that clock passes its expiry.
	f := *o.frontier.Load()
	f.ArchiveClock = 2000
	o.frontier.Store(&f)
	// Four equal parts fold in one merge.
	if mergeDrain(t, o) != 1 || mergeCount(o) != 1 {
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
	// Straddle a UTC month boundary while retaining the coarse archive.
	now := time.Now().Unix()
	start := rollupMonthPartition(now-3*86400) - 86400
	root := t.TempDir()
	e := mergeFixture(t, root, nil)
	defer func() {
		if e != nil {
			_ = e.close()
		}
	}()
	o := e.owners[0]
	_, enc := rollupTestPolicy(t, "partition.metric", "1s:7d,1m:90d", sealrollup.Last, .5)
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
	if len(seen) != 2 || seen[0] {
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
	opened, err := OpenRollupParts(context.Background(), s, inv, nil, testBlockCache(t), 4<<20)
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
	var maxExpiry int64
	o.gc.mu.Lock()
	for _, v := range o.gc.inventory.bindings {
		d, err := v2part.ParseDescriptor(v)
		if err != nil {
			t.Fatal(err)
		}
		if d.Coverage.MaxExpiry == 0 || d.Coverage.MinExpiry > d.Coverage.MaxExpiry {
			t.Fatalf("expiry bounds %+v", d.Coverage)
		}
		maxExpiry = max(maxExpiry, d.Coverage.MaxExpiry)
	}
	o.gc.mu.Unlock()
	// Whole-part drop happens strictly after the part's max expiry.
	if err = o.expireSealedParts(maxExpiry); err != nil {
		t.Fatal(err)
	}
	if e.PartGCStats()[0].LiveParts == 0 {
		t.Fatal("dropped part at its max expiry")
	}
	if err = o.expireSealedParts(maxExpiry + 1); err != nil {
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
	// Every envelope expires no earlier than the data it masks; once all data
	// expired the name leaves the catalog, as an expired whisper file would.
	var count int
	if err = ScanCatalogNames(context.Background(), s, func(string) error { count++; return nil }); err != nil || count != 0 {
		t.Fatalf("expired name remains %d %v", count, err)
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
	if len(parts) != 1 || parts[0].Descriptor.Scope.Partition != rollupMonthPartition(now-5) {
		t.Fatal("fixture did not use the month partition")
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

// One part mixing 3 d and 1 y retention: a TTL rewrite drops exactly the
// expired fine records, keeps the coarse ones bit-exact, and is not repeated.
func TestMergeBoundTTLRewrite(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	now := time.Now().Unix() / 3600 * 3600
	e := mergeFixture(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	_, mixed := rollupTestPolicy(t, "ttl.mixed", "1m:3d,1h:1y", sealrollup.Last, 0)
	_, long := rollupTestPolicy(t, "ttl.long", "1m:1y", sealrollup.Last, .5)
	err := e.submitBatches(0, []coreBatch{{ID: 7, Policy: mixed, Order: 1, Clock: now, Points: []row{{ID: 7, Time: now - 7200, Bits: math.Float64bits(1), Seq: 1}, {ID: 7, Time: now - 60, Bits: math.Float64bits(2), Seq: 2}}}}, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	s := o.db.NewSnapshot()
	want := mergeRead(t, s)
	_ = s.Close()
	var fine, coarse int
	for _, r := range want {
		if r.Archive == 0 {
			fine++
		} else {
			coarse++
		}
	}
	if fine == 0 || coarse == 0 {
		t.Fatalf("fixture %+v", want)
	}
	clock := now + 4*86400
	err = e.submitBatches(0, []coreBatch{{ID: 8, Policy: long, Order: 2, Clock: clock, Points: []row{{ID: 8, Time: clock - 60, Bits: math.Float64bits(3), Seq: 3}}}}, 2, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := o.frontier.Load().ArchiveClock; got != clock {
		t.Fatalf("clock %d", got)
	}
	// The coarse windows the expiring raw records roll up to, read from the
	// stored versions: the TTL rewrite writes them down before it drops the
	// raw records, so the version read does not change.
	windows := []int64{now - 7200, now - 3600}
	s = o.db.NewSnapshot()
	wantWindows := versionWindows(t, s, mixed, 1, windows)
	_ = s.Close()
	before := e.PartGCStats()[0]
	changed, err := o.mergeSealedParts(nil, false)
	if err != nil || !changed {
		t.Fatalf("ttl rewrite changed=%v %v", changed, err)
	}
	s = o.db.NewSnapshot()
	defer s.Close()
	if got := versionWindows(t, s, mixed, 1, windows); len(wantWindows) != len(windows) || !reflect.DeepEqual(got, wantWindows) {
		t.Fatalf("coarse windows after ttl rewrite %v want %v", got, wantWindows)
	}
	var kept []sealrollup.Record
	for _, r := range mergeRead(t, s) {
		if r.Archive == 0 && r.Sample.Time < now {
			t.Fatalf("expired record retained %+v", r)
		}
		if r.Sample.Time < now {
			kept = append(kept, r)
		}
	}
	var wantKept []sealrollup.Record
	for _, r := range want {
		if r.Archive != 0 {
			wantKept = append(wantKept, r)
		}
	}
	// The open coarse window was projected before the clock jump and is
	// stored closed after it: only the Correction placement hint may differ.
	for i := range kept {
		kept[i].Correction = false
	}
	for i := range wantKept {
		wantKept[i].Correction = false
	}
	if !reflect.DeepEqual(kept, wantKept) {
		t.Fatalf("coarse records changed: %+v want %+v", kept, wantKept)
	}
	o.gc.mu.Lock()
	for _, v := range o.gc.inventory.bindings {
		d, err := v2part.ParseDescriptor(v)
		if err != nil {
			t.Fatal(err)
		}
		if d.Coverage.MinExpiry != 0 && d.Coverage.MinExpiry < clock {
			t.Fatalf("rewritten part still expired %+v", d.Coverage)
		}
	}
	o.gc.mu.Unlock()
	after := e.PartGCStats()[0]
	if after.LiveParts != before.LiveParts {
		t.Fatalf("parts %d -> %d", before.LiveParts, after.LiveParts)
	}
	o.ttlNext.Store(0)
	if changed, err = o.mergeSealedParts(nil, false); err != nil || changed {
		t.Fatalf("repeated ttl rewrite changed=%v %v", changed, err)
	}
}

type fakeFree uint64

func (f fakeFree) Free(string) (uint64, error) { return uint64(f), nil }

func withFreeSpace(t *testing.T, free uint64) {
	old := diskSpace
	diskSpace = fakeFree(free)
	t.Cleanup(func() { diskSpace = old })
}

func TestMergeDiskGuard(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixture(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	for n := uint64(1); n <= 16; n++ {
		mergeAppend(t, e, n)
	}
	s := o.db.NewSnapshot()
	defer s.Close()
	withFreeSpace(t, 1<<40)
	inputs, err := o.selectMerge(s)
	if err != nil || len(inputs) != smallMergeRatio {
		t.Fatalf("plenty of space: %d %v", len(inputs), err)
	}
	var total uint64
	for _, d := range inputs {
		total += d.Bytes
	}
	// free/4 bound: exactly enough, then one byte short.
	withFreeSpace(t, total*4)
	if inputs, err = o.selectMerge(s); err != nil || len(inputs) != smallMergeRatio {
		t.Fatalf("free/4 exact: %d %v", len(inputs), err)
	}
	withFreeSpace(t, total*4-4)
	if inputs, err = o.selectMerge(s); err != nil || len(inputs) != 0 {
		t.Fatalf("free/4 ignored: %d %v", len(inputs), err)
	}
	// In-flight staged output counts against free space.
	withFreeSpace(t, total*4+100)
	mergeInflight.Add(101)
	if inputs, err = o.selectMerge(s); err != nil || len(inputs) != 0 {
		t.Fatalf("in-flight ignored: %d %v", len(inputs), err)
	}
	mergeInflight.Add(^uint64(100))
	if inputs, err = o.selectMerge(s); err != nil || len(inputs) != smallMergeRatio {
		t.Fatalf("released in-flight: %d %v", len(inputs), err)
	}
	// Zero free space: no merge, no error, counted skip.
	withFreeSpace(t, 0)
	skips := MergeDiskSkips()
	changed, err := o.mergeSealedParts(make(chan struct{}), false)
	if err != nil || changed {
		t.Fatalf("zero free: changed=%v err=%v", changed, err)
	}
	if MergeDiskSkips() != skips+1 {
		t.Fatal("skip not counted")
	}
	if mergeInflight.Load() != 0 {
		t.Fatalf("in-flight leaked: %d", mergeInflight.Load())
	}
}

// versionWindows is the version read (sealrollup.OpenSlot) of archive's
// windows at starts of the one metric whose stored policy is policy.
func versionWindows(t *testing.T, s *pebble.Snapshot, policy []byte, archive int, starts []int64) map[int64]sealrollup.Sample {
	t.Helper()
	ctx := context.Background()
	index, err := NewRollupSnapshot(ctx, s, nil, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var name string
	index.Metadata(func(n string, p RollupPolicy, _ RollupMetadata) {
		if encoded, err := EncodeRollupPolicy(p); err == nil && bytes.Equal(encoded, policy) {
			name = n
		}
	})
	m := index.metrics[name]
	if m == nil {
		t.Fatalf("no metric with policy %q", policy)
	}
	out := map[int64]sealrollup.Sample{}
	for _, start := range starts {
		p, ok, err := sealrollup.OpenSlot(m.latest.schema(), archive, start, func(l int, first, last int64, from uint64) ([]sealrollup.Record, error) {
			var recs []sealrollup.Record
			err := index.eachPartRecord(ctx, m, index.metricParts(m), l, first, last, true, func(part *readPartMetric) bool {
				return from > 0 && part.meta.Frontier.Order != 0 && part.meta.Frontier.Order < from
			}, func(r sealrollup.Record, _ partOrigin) error {
				recs = append(recs, r)
				return nil
			})
			return recs, err
		})
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			out[start] = p
		}
	}
	return out
}

// Outside admissions, including a quiet name's later neighbours, bound TTL
// expiry. A legacy descriptor cannot certify that clock and must fall back.
func TestMergeAdmissionBound(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixture(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	for n := uint64(1); n <= 3; n++ {
		mergeAppend(t, e, n)
	}
	var parts []v2part.Descriptor
	for _, value := range o.gc.inventory.bindings {
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			t.Fatal(err)
		}
		if d.Coverage.MinClock != 1030 {
			t.Fatalf("seal clock %d", d.Coverage.MinClock)
		}
		parts = append(parts, d)
	}
	slices.SortFunc(parts, func(a, b v2part.Descriptor) int { return int(a.Source.MinOrder) - int(b.Source.MinOrder) })
	snapshot := o.db.NewSnapshot()
	defer snapshot.Close()
	original := o.gc.inventory
	for _, tc := range []struct {
		name   string
		clocks []int64
		inputs int
		want   int64
	}{
		{"oldest outside", []int64{100, 200, 300}, 1, 200},
		{"minimum outside", []int64{100, 300, 200}, 1, 200},
		{"all inputs", []int64{100, 200, 300}, 3, 400},
		{"legacy outside", []int64{100, 0, 300}, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o.gc.inventory = &RollupInventory{bindings: map[string][]byte{}}
			defer func() { o.gc.inventory = original }()
			for i, d := range parts {
				d.Coverage.MinClock = tc.clocks[i]
				value, err := d.MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}
				o.gc.inventory.bindings[string(partBinding(sealedPartPrefix, d.SHA256))] = value
			}
			got, err := o.mergeAdmissionBound(snapshot, parts[:tc.inputs], 400)
			if err != nil || got != tc.want {
				t.Fatalf("bound %d want %d: %v", got, tc.want, err)
			}
		})
	}
	// Rewrites preserve the oldest admission, rather than inventing a new one.
	d, err := mergePartFiles(context.Background(), o.dir, t.TempDir()+"/merged.sst", parts, 1030, nil)
	if err != nil || d.Coverage.MinClock != 1030 {
		t.Fatalf("merged clock %d: %v", d.Coverage.MinClock, err)
	}
}
