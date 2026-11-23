package v2core

import (
	"bytes"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func seqLiveParts(t *testing.T, o *coreOwner) []v2part.Descriptor {
	t.Helper()
	o.gc.mu.Lock()
	defer o.gc.mu.Unlock()
	var out []v2part.Descriptor
	for _, v := range o.gc.inventory.bindings {
		d, err := v2part.ParseDescriptor(v)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, d)
	}
	slices.SortFunc(out, seqCompare)
	return out
}

// seqContiguous fails unless inputs are, in order, a run of consecutive live
// parts of their scope in Seq order: the merge skips no live Seq.
func seqContiguous(t *testing.T, live, inputs []v2part.Descriptor) {
	t.Helper()
	var scope []v2part.Descriptor
	for _, d := range live {
		if d.Scope.Group == inputs[0].Scope.Group && d.Scope.Partition == inputs[0].Scope.Partition {
			scope = append(scope, d)
		}
	}
	at := slices.IndexFunc(scope, func(d v2part.Descriptor) bool { return d.SHA256 == inputs[0].SHA256 })
	if at < 0 || at+len(inputs) > len(scope) {
		t.Fatalf("merge window outside live parts: %d of %d", at, len(scope))
	}
	for i, d := range inputs {
		if scope[at+i].SHA256 != d.SHA256 {
			t.Fatalf("merge skipped live Seq %d: window input %d has Seq %d", scope[at+i].Source.Seq, i, d.Source.Seq)
		}
	}
}

// replaceDescriptors rebinds descriptors in the in-memory inventory only:
// selection reads nothing else.
func replaceDescriptors(t *testing.T, o *coreOwner, parts []v2part.Descriptor) {
	t.Helper()
	var added []coreKV
	for _, d := range parts {
		b, err := d.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		added = append(added, coreKV{Key: partBinding(sealedPartPrefix, d.SHA256), Value: b})
	}
	o.gc.mu.Lock()
	o.gc.inventory = o.gc.inventory.with(added, nil)
	o.gc.mu.Unlock()
}

// Seals number their parts; merges take only Seq-adjacent live parts and the
// output takes the inputs' maximum Seq, also when record orders disagree with
// seal order. Parts without Seq (older producers) sort first, by MinOrder.
func TestMergeSelectionFollowsSeq(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixtureIdle(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	order := uint64(0)
	seal := func(n int) {
		for range n {
			order++
			mergeAppend(t, e, order)
		}
	}
	seal(12)
	live := seqLiveParts(t, o)
	if len(live) != 12 {
		t.Fatalf("parts %d", len(live))
	}
	for i, d := range live {
		if d.Source.Seq == 0 || i > 0 && d.Source.Seq <= live[i-1].Source.Seq {
			t.Fatalf("part %d Seq %d after %d", i, d.Source.Seq, live[max(i-1, 0)].Source.Seq)
		}
	}
	// Background merges between seals: each window is Seq-contiguous and its
	// output keeps the newest input's Seq.
	limit, _, err := effectiveMergeLimit(o.dir)
	if err != nil {
		t.Fatal(err)
	}
	merges := 0
	for round := 0; round < 40; round++ {
		s := o.db.NewSnapshot()
		inputs, err := o.selectMergeLimit(s, limit, round%5 == 4)
		_ = s.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(inputs) == 0 {
			seal(1 + round%3)
			continue
		}
		before := seqLiveParts(t, o)
		seqContiguous(t, before, inputs)
		if changed, err := o.mergeSealedParts(nil, round%5 == 4); err != nil || !changed {
			t.Fatalf("merge changed=%v %v", changed, err)
		}
		merges++
		after := seqLiveParts(t, o)
		if len(after) != len(before)-len(inputs)+1 {
			t.Fatalf("parts %d -> %d after merging %d", len(before), len(after), len(inputs))
		}
		for _, d := range after {
			if !slices.ContainsFunc(before, func(b v2part.Descriptor) bool { return b.SHA256 == d.SHA256 }) && d.Source.Seq != inputs[len(inputs)-1].Source.Seq {
				t.Fatalf("output Seq %d, newest input %d", d.Source.Seq, inputs[len(inputs)-1].Source.Seq)
			}
		}
		for i := 1; i < len(after); i++ {
			if after[i].Source.Seq == after[i-1].Source.Seq {
				t.Fatalf("two live parts share Seq %d", after[i].Source.Seq)
			}
		}
		seal(round % 3)
	}
	if merges < 3 {
		t.Fatalf("merges %d", merges)
	}

	// Record orders that disagree with seal order: selection still follows Seq.
	live = seqLiveParts(t, o)
	if len(live) < 6 {
		seal(6)
		live = seqLiveParts(t, o)
	}
	rng := rand.New(rand.NewPCG(7, uint64(len(live))))
	shuffled := slices.Clone(live)
	for i, p := range rng.Perm(len(shuffled)) {
		shuffled[i].Source.MinOrder, shuffled[i].Source.MaxOrder = uint64(1000+p), uint64(1000+p)
	}
	replaceDescriptors(t, o, shuffled)
	s := o.db.NewSnapshot()
	defer s.Close()
	for _, full := range []bool{false, true} {
		inputs, err := o.selectMergeLimit(s, math.MaxUint64, full)
		if err != nil {
			t.Fatal(err)
		}
		if full && len(inputs) != len(live) {
			t.Fatalf("full merge took %d of %d", len(inputs), len(live))
		}
		if len(inputs) > 0 {
			seqContiguous(t, seqLiveParts(t, o), inputs)
		}
	}
	// Older parts without Seq: first, among themselves in MinOrder order.
	legacy := slices.Clone(shuffled)
	for i := range legacy[:len(legacy)/2] {
		legacy[i].Source.Seq = 0
	}
	replaceDescriptors(t, o, legacy)
	want := slices.Clone(legacy)
	slices.SortStableFunc(want, func(a, b v2part.Descriptor) int {
		if (a.Source.Seq == 0) != (b.Source.Seq == 0) {
			if a.Source.Seq == 0 {
				return -1
			}
			return 1
		}
		if a.Source.Seq == 0 {
			return int(a.Source.MinOrder) - int(b.Source.MinOrder)
		}
		return int(a.Source.Seq) - int(b.Source.Seq)
	})
	inputs, err := o.selectMergeLimit(s, math.MaxUint64, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := range want {
		if i >= len(inputs) || inputs[i].SHA256 != want[i].SHA256 {
			t.Fatalf("legacy order: input %d differs (%d inputs)", i, len(inputs))
		}
	}
	// A descriptor without Seq keeps its canonical bytes: Seq is omitted.
	b, err := legacy[0].MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if d, err := v2part.ParseDescriptor(b); err != nil || d.Source.Seq != 0 || bytes.Contains(b, []byte(`"Seq"`)) {
		t.Fatalf("legacy descriptor %v %q", err, b)
	}
}

// A TTL rewrite takes only a scope's oldest part: a prefix merge, which writes
// windows down before it drops expired records. With seals that store raw
// records only, a TTL rewrite of a newer part would drop raw records whose
// coarse windows no stored record holds.
func TestTTLRewriteOnlyOldestInScope(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	now := time.Now().Unix() / 3600 * 3600
	e := mergeFixture(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	_, mixed := rollupTestPolicy(t, "ttl.mixed", "1m:3d,1h:1y", sealrollup.Last, 0)
	_, long := rollupTestPolicy(t, "ttl.long", "1m:1y", sealrollup.Last, .5)
	// The scope's oldest part holds nothing that expires soon.
	if err := e.submitBatches(0, []coreBatch{{ID: 8, Policy: long, Order: 1, Clock: now, Points: []row{{ID: 8, Time: now - 60, Bits: math.Float64bits(5), Seq: 1}}}}, 1, true); err != nil {
		t.Fatal(err)
	}
	// A newer part whose raw records expire before their coarse windows.
	if err := e.submitBatches(0, []coreBatch{{ID: 7, Policy: mixed, Order: 2, Clock: now, Points: []row{{ID: 7, Time: now - 7200, Bits: math.Float64bits(1), Seq: 2}, {ID: 7, Time: now - 60, Bits: math.Float64bits(2), Seq: 3}}}}, 2, true); err != nil {
		t.Fatal(err)
	}
	clock := now + 4*86400
	if err := e.submitBatches(0, []coreBatch{{ID: 8, Policy: long, Order: 3, Clock: clock, Points: []row{{ID: 8, Time: clock - 60, Bits: math.Float64bits(3), Seq: 4}}}}, 3, true); err != nil {
		t.Fatal(err)
	}
	windows := []int64{now - 7200, now - 3600}
	read := func() map[int64]sealrollup.Sample {
		s := o.db.NewSnapshot()
		defer s.Close()
		return versionWindows(t, s, mixed, 1, windows)
	}
	want := read()
	if len(want) != len(windows) {
		t.Fatalf("fixture windows %v", want)
	}
	var newer v2part.Descriptor
	for _, d := range seqLiveParts(t, o) {
		if d.Coverage.MinExpiry != 0 && d.Coverage.MinExpiry < clock {
			newer = d
		}
	}
	if newer.Source.Seq == 0 || o.oldestInScope([]v2part.Descriptor{newer}) {
		t.Fatalf("fixture: expiring part %+v is the oldest in its scope", newer.Source)
	}
	// No size-tier merge is due, and the expiring part is not its scope's
	// oldest, so no TTL rewrite takes it.
	s := o.db.NewSnapshot()
	chosen, err := o.selectTTLRewrite(s, o.retireClock(), math.MaxUint64)
	_ = s.Close()
	if err != nil || len(chosen) != 0 {
		t.Fatalf("ttl rewrite chose %d parts: %v", len(chosen), err)
	}
	if _, err := o.mergeSealedParts(nil, false); err != nil {
		t.Fatal(err)
	}
	if got := read(); !reflect.DeepEqual(got, want) {
		t.Fatalf("coarse windows after ttl selection %v want %v", got, want)
	}
	// A prefix merge holds them: it writes the windows down before any drop.
	for {
		changed, err := o.mergeSealedParts(nil, true)
		if err != nil {
			t.Fatal(err)
		}
		if !changed {
			break
		}
	}
	if got := read(); !reflect.DeepEqual(got, want) {
		t.Fatalf("coarse windows after prefix merge %v want %v", got, want)
	}
}
