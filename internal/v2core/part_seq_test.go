package v2core

import (
	"bytes"
	"cmp"
	"encoding/hex"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func partSeqLive(t *testing.T, o *coreOwner) []v2part.Descriptor {
	t.Helper()
	o.gc.mu.Lock()
	defer o.gc.mu.Unlock()
	var out []v2part.Descriptor
	for _, b := range o.gc.inventory.bindings {
		d, err := v2part.ParseDescriptor(b)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, d)
	}
	slices.SortFunc(out, func(a, b v2part.Descriptor) int { return cmp.Compare(a.Seq, b.Seq) })
	for i := 1; i < len(out); i++ {
		if out[i].Seq == out[i-1].Seq {
			t.Fatalf("duplicate live Seq %d", out[i].Seq)
		}
	}
	return out
}

// Seq follows publication: each seal is above every earlier part, a merge is
// above its inputs, and a reopened owner continues above every live part. The
// clock range is the seal's batch clocks, and a merge's is their union.
func TestPartSeqClock(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	root := t.TempDir()
	e := mergeFixtureIdle(t, root, nil)
	defer func() {
		if e != nil {
			_ = e.close()
		}
	}()
	_, policy := rollupTestPolicy(t, "merge.metric", "1s:120,10s:1h", sealrollup.Last, .5)
	var last uint64
	seal := func(order uint64) {
		t.Helper()
		clock := 2000 + int64(order)
		stamp := int64(1000 + order%24)
		batches := []coreBatch{
			{ID: 7, Policy: policy, Order: 2*order - 1, Clock: clock - 5, Points: []row{{ID: 7, Time: stamp - 1, Bits: order, Seq: order * 3}}},
			{ID: 7, Policy: policy, Order: 2 * order, Clock: clock, Points: []row{{ID: 7, Time: stamp, Bits: order, Seq: order*3 + 1}}},
		}
		if err := e.submitBatches(0, batches, 2*order, true); err != nil {
			t.Fatal(err)
		}
		live := partSeqLive(t, e.owners[0])
		d := live[len(live)-1]
		if d.Seq <= last || d.MinClock != clock-5 || d.MaxClock != clock {
			t.Fatalf("seal %d: Seq %d after %d, clocks %d..%d", order, d.Seq, last, d.MinClock, d.MaxClock)
		}
		last = d.Seq
	}
	for order := uint64(1); order <= 24; order++ {
		seal(order)
	}
	before := map[v2part.Digest]bool{}
	for _, d := range partSeqLive(t, e.owners[0]) {
		before[d.SHA256] = true
	}
	if mergeDrain(t, e.owners[0]) == 0 {
		t.Fatal("fixture produced no merge")
	}
	merged := 0
	for _, d := range partSeqLive(t, e.owners[0]) {
		if before[d.SHA256] {
			continue
		}
		merged++
		if d.Seq <= last || d.MinClock < 1996 || d.MaxClock > 2024 || d.MinClock >= d.MaxClock {
			t.Fatalf("merged part Seq %d (last %d) clocks %d..%d", d.Seq, last, d.MinClock, d.MaxClock)
		}
		last = max(last, d.Seq)
	}
	if merged == 0 {
		t.Fatal("no merged part is live")
	}
	if err := e.close(); err != nil {
		t.Fatal(err)
	}
	e = mergeFixtureIdle(t, root, nil)
	live := partSeqLive(t, e.owners[0])
	if top := live[len(live)-1].Seq; e.owners[0].gc.seq != top {
		t.Fatalf("reopen seq %d, newest live %d", e.owners[0].gc.seq, top)
	}
	last = live[len(live)-1].Seq
	seal(25)

	// Replaying a byte-identical part under a new Seq keeps the first binding.
	d := partSeqLive(t, e.owners[0])[0]
	first, err := d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	dir := e.owners[0].dir
	src := filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	replay := filepath.Join(t.TempDir(), "replay.sst")
	if err := os.WriteFile(replay, data, 0o600); err != nil {
		t.Fatal(err)
	}
	again := d
	again.Seq, again.MinClock, again.MaxClock = d.Seq+100, 1, 2
	kv, _, err := retainSealedPart(dir, replay, again)
	if err != nil || !bytes.Equal(kv.Value, first) {
		t.Fatalf("replayed part rebinding: %v", err)
	}
	again.Coverage.Records++
	if _, _, err := retainSealedPart(dir, replay, again); err == nil {
		t.Fatal("changed descriptor accepted over an existing sidecar")
	}
}
