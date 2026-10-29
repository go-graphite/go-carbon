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
