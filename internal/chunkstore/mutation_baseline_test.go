package chunkstore

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestMutationBaselineStateFence(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	defer closeTestStore(t, s)
	s.stateCache = newStateCache(64<<20, footprintStateKeys{})
	m := Metadata{ID: 41, Generation: 3, Revision: 9}
	for _, kind := range []string{"current", "id", "generation", "revision", "staged", "missing"} {
		t.Run(kind, func(t *testing.T) {
			batch := s.db.NewIndexedBatch()
			defer batch.Close()
			st := newMetricState(m)
			st.set([]byte("n/main.wsp"), []byte("before"))
			st = st.seal()
			switch kind {
			case "id":
				st.meta.ID++
			case "generation":
				st.meta.Generation++
			case "revision":
				st.meta.Revision++
			case "missing":
				st = nil
			case "staged":
				if err := batch.Set([]byte("earlier"), []byte("write"), pebble.NoSync); err != nil {
					t.Fatal(err)
				}
			}
			io := s.newContinuationIO(context.Background(), batch, batch, m)
			io.mutationTracked = true
			io.state = &stateView{keys: footprintStateKeys{}, base: st, meta: m}
			raw, closeBase := io.MutationBaseline()
			defer closeBase()
			base := raw.(*continuationIO)
			if kind != "current" {
				if base.state != nil {
					t.Fatal("baseline reused unfenced state")
				}
				return
			}
			if base.state == nil || base.state == io.state {
				t.Fatal("baseline must have an independent state view")
			}
			io.state.mutable().set([]byte("n/main.wsp"), []byte("after"))
			value, err := base.Get([]byte("n/main.wsp"))
			if err != nil || string(value) != "before" {
				t.Fatalf("baseline changed: %q %v", value, err)
			}
			value[0] = 'X'
			value, err = base.Get([]byte("n/main.wsp"))
			if err != nil || string(value) != "before" {
				t.Fatal("baseline returned shared bytes")
			}
		})
	}
}
