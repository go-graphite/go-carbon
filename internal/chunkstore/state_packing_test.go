package chunkstore

import (
	"math/rand"
	"testing"
)

func TestStateRecordPacking(t *testing.T) {
	for _, packed := range []bool{false, true} {
		data := make([]byte, 4096)
		if !packed {
			rand.New(rand.NewSource(7)).Read(data)
		}
		st := newMetricState(Metadata{MetricConfig: MetricConfig{Name: "packing.metric"}, ID: 1})
		st.set([]byte("tail"), data)
		st.set([]byte("absent"), nil)
		st.set([]byte("empty"), []byte{})
		st.complete["n/"] = true
		want := cloneMutable(st)
		st.seal()
		if (st.packed != nil) != packed {
			t.Fatalf("packed=%v: packed size %d, original %d", packed, len(st.packed), len(st.kenc))
		}
		n := stateRecordLen(st.meta.Name, st)
		rec := appendStateRecord(make([]byte, 0, n), st.meta.Name, st)
		got := decodeStateRecord(rec)
		probes := [][]byte{[]byte("tail"), []byte("absent"), []byte("empty"), []byte("n/missing"), []byte("unknown")}
		sameStateKeys(t, want, got, probes)
		changed := got.clone(got.meta)
		changed.set([]byte("tail"), []byte("replacement"))
		changed.seal()
		sameStateKeys(t, want, got, probes)
		if v, _ := changed.lookup([]byte("tail")); string(v) != "replacement" {
			t.Fatal("clone kept the old tail")
		}
	}
}

func TestStatePackingSwitch(t *testing.T) {
	defer SetStatePacking(true)
	seal := func() *metricState {
		st := newMetricState(Metadata{MetricConfig: MetricConfig{Name: "packing.switch"}, ID: 1})
		st.set([]byte("tail"), make([]byte, 4096))
		return st.seal()
	}
	SetStatePacking(false)
	raw := seal()
	if raw.packed != nil {
		t.Fatal("packed with packing off")
	}
	SetStatePacking(true)
	packed := seal()
	if packed.packed == nil {
		t.Fatal("compressible keys not packed with packing on")
	}
	// Records sealed under either setting decode after it changes.
	for _, st := range []*metricState{raw, packed} {
		got := decodeStateRecord(appendStateRecord(nil, st.meta.Name, st))
		if v, ok := got.lookup([]byte("tail")); !ok || len(v) != 4096 {
			t.Fatalf("tail lost after decode: %d bytes, known %v", len(v), ok)
		}
	}
}
