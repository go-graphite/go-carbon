package chunkstore

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/pebble"
)

// Frozen history staging before read reuse: the oracle performs its own read.
func (s *Store) stageMutationHistoryOracle(b *pebble.Batch, m Metadata, ranges []MutationRange, complete bool) error {
	h, err := readMutationHistory(bReader(b, s.db), m)
	if err != nil {
		return err
	}
	if h.Version == 0 {
		return nil
	}
	if h.Head != m.Revision {
		h.Floor = m.Revision
		h.Head = m.Revision
		h.Entries = nil
		for i := range h.Scopes {
			h.Scopes[i].Revision = m.Revision
			h.Scopes[i].Reason = "unknown mutation path"
		}
	}
	next := m.Revision + 1
	if !complete || len(ranges) > mutationRangeLimit {
		h.Floor = next
		h.Head = next
		h.Entries = nil
		for i := range h.Scopes {
			h.Scopes[i].Revision = next
			h.Scopes[i].Reason = "unknown mutation path"
		}
	} else {
		h.Head = next
		for i, scope := range h.Scopes {
			for _, r := range ranges {
				if r.From < scope.Until && r.Until > scope.From {
					h.Scopes[i].Revision = next
					h.Scopes[i].Reason = "in-window mutation"
					break
				}
			}
		}
		if len(ranges) > 0 {
			h.Entries = append(h.Entries, mutationEntry{next, ranges})
		}
		if len(h.Entries) > mutationHistoryLimit {
			h.Floor = h.Entries[0].Revision
			h.Entries = h.Entries[1:]
		}
	}
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	if len(data) > 32<<10 {
		h.Floor = next
		h.Entries = nil
		data, _ = json.Marshal(h)
	}
	return b.Set(mutationKey(m), data, nil)
}

func TestMutationHistoryReuseBitExact(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	defer closeTestStore(t, s)
	m := Metadata{ID: 1, Generation: 2, Revision: 19}
	entries := make([]mutationEntry, mutationHistoryLimit)
	for i := range entries {
		entries[i] = mutationEntry{Revision: uint64(i + 1), Ranges: []MutationRange{{Archive: 0, From: 10, Until: 11}}}
	}
	for _, h := range []mutationHistory{
		{},
		{Version: 1, Head: 19, Floor: 19},
		{Version: 1, Head: 18, Scopes: []mutationScope{{From: 10, Until: 20, Revision: 18}}},
		{Version: 1, Head: 19, Scopes: []mutationScope{{From: 10, Until: 20, Revision: 1}, {From: 30, Until: 40, Revision: 1}}},
		{Version: 1, Head: 19, Entries: entries},
	} {
		for _, complete := range []bool{false, true} {
			for _, ranges := range [][]MutationRange{nil, {{Archive: 0, From: 15, Until: 16}}, {{Archive: 1, From: 50, Until: 51}}, make([]MutationRange, mutationRangeLimit+1)} {
				if h.Version == 0 {
					if err := s.db.Delete(mutationKey(m), nil); err != nil {
						t.Fatal(err)
					}
				} else {
					data, err := json.Marshal(h)
					if err != nil {
						t.Fatal(err)
					}
					if err = s.db.Set(mutationKey(m), data, nil); err != nil {
						t.Fatal(err)
					}
				}
				before, after := s.db.NewIndexedBatch(), s.db.NewIndexedBatch()
				history, err := readMutationHistory(after, m)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.stageMutationHistoryOracle(before, m, ranges, complete); err != nil {
					t.Fatal(err)
				}
				if err = stageMutationHistoryFrom(after, m, history, ranges, complete); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(before.Repr(), after.Repr()) {
					t.Fatalf("history=%+v complete=%v ranges=%d: persisted batch bytes differ", h, complete, len(ranges))
				}
				before.Close()
				after.Close()
			}
		}
	}
}
