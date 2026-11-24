package v2core

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2dict"
)

// dictKeysPlanner returns a planner whose seal stages n metrics over three
// vshards, as a first seal of n names.
func dictKeysPlanner(n int) *coreRollups {
	def := &policyDefinition{Group: "g"}
	r := &coreRollups{stages: make([]rollupStage, n), stageMetrics: make([]*PolicyIdentity, n)}
	for i := range n {
		vs := uint16(i % 3)
		r.stageMetrics[i] = (&PolicyIdentity{policy: policyName{policyDefinition: def}, vshard: vs}).withName(fmt.Sprintf("dict.keys.%06d", n-i))
	}
	return r
}

func openTestDict(t *testing.T) *v2dict.Dict {
	t.Helper()
	d, err := v2dict.Open(t.TempDir(), v2dict.Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// TestDictKeysChunks seals more new names than one dictKeys chunk, some of
// them known already: every stage is keyed by (vshard, ID), IDs follow stage
// order within a vshard, and each name resolves to its ID in the dictionary.
func TestDictKeysChunks(t *testing.T) {
	const n = 3*dictChunk + 7
	d := openTestDict(t)
	r := dictKeysPlanner(n)
	// Names the dictionary holds from an earlier seal keep their IDs.
	known := map[int]bool{5: true, dictChunk: true, 2*dictChunk + 1: true}
	next := [3]uint32{}
	want := make([]uint32, n)
	for i := range n {
		if known[i] {
			m := r.stageMetrics[i]
			e, _, err := d.Append("g", m.vshard, []byte(m.metricName()), 0)
			if err != nil {
				t.Fatal(err)
			}
			m.dictID.Store(e.ID + 1)
			want[i] = e.ID
			next[m.vshard]++
		}
	}
	for i := range n {
		if !known[i] {
			want[i] = next[i%3]
			next[i%3]++
		}
	}
	if err := r.dictKeys(d); err != nil {
		t.Fatal(err)
	}
	if !r.dictDirty {
		t.Fatal("new names appended without dictDirty")
	}
	for i := range n {
		m := r.stageMetrics[i]
		if got := m.dictID.Load() - 1; got != want[i] {
			t.Fatalf("stage %d: ID %d, want %d", i, got, want[i])
		}
		if k := r.stages[i].sortKey; k != uint64(m.vshard)<<32|uint64(want[i]) {
			t.Fatalf("stage %d: sort key %#x", i, k)
		}
		if e, ok := d.Lookup("g", m.vshard, []byte(m.metricName())); !ok || e.ID != want[i] {
			t.Fatalf("stage %d: dictionary has %v %v, want ID %d", i, e, ok, want[i])
		}
	}
}

// TestDictKeysAllocBytes bounds what dictKeys allocates beyond the
// dictionary's own AppendBatch of the same names: nothing per new name, so a
// first-wave seal of many new names adds no heap peak of its own.
func TestDictKeysAllocBytes(t *testing.T) {
	const n = 8 * dictChunk
	alloc := func(f func()) uint64 {
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		f()
		runtime.ReadMemStats(&b)
		return b.TotalAlloc - a.TotalAlloc
	}
	best := ^uint64(0)
	for range 3 { // the fewest of three runs: other goroutines allocate too
		r, ref := dictKeysPlanner(n), dictKeysPlanner(n)
		names := make([]v2dict.NewName, n)
		for i, m := range ref.stageMetrics {
			names[i] = v2dict.NewName{Group: "g", VShard: m.vshard, Name: string(m.metricName())}
		}
		d, dref := openTestDict(t), openTestDict(t)
		base := alloc(func() {
			for i := 0; i < n; i += dictChunk {
				if err := dref.AppendBatch(names[i : i+dictChunk]); err != nil {
					t.Fatal(err)
				}
			}
		})
		got := alloc(func() {
			if err := r.dictKeys(d); err != nil {
				t.Fatal(err)
			}
		})
		if got > base {
			best = min(best, got-base)
		} else {
			best = 0
		}
	}
	if per := best / n; per > 8 {
		t.Fatalf("dictKeys allocates %d B per new name beyond AppendBatch, want at most 8", per)
	}
}
