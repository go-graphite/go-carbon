package v2daemon

import (
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2recv"
)

// TestL0ProducerFromPackedName: L0 replay derives a NumericID's producer from
// the ID and a packed ID's producer from the name, both for a registered
// identity and for a name known only to the dictionary.
func TestL0ProducerFromPackedName(t *testing.T) {
	const producers = 4
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	c := admitConfig()
	c.Producers = producers
	s, err := Open(t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := s.Core
	for local := uint64(1); local < 9; local++ {
		for p := range producers {
			if got, err := l0Producer(e, NumericID(local, p, producers), producers); err != nil || got != p {
				t.Fatalf("NumericID(%d,%d): producer %d %v", local, p, got, err)
			}
		}
	}
	// A name admitted by its owner: in the dictionary, identity under its NumericID.
	seen := map[int]bool{}
	for i := 0; len(seen) < producers && i < 64; i++ {
		name := "packed.l0.metric." + string(rune('a'+i%26)) + string(rune('a'+i/26))
		owner := v2recv.OwnerFor(name, producers)
		p := s.Producers[owner]
		if err := p.AppendApplications([]Application{admitApp(t, name, p.applyOrder+1)}); err != nil {
			t.Fatal(err)
		}
		if err := p.Drain(); err != nil {
			t.Fatal(err)
		}
		vshard := v2part.Route(v2part.ID(name))
		entry, ok := e.Dictionary().Lookup("test", vshard, []byte(name))
		if !ok {
			t.Fatalf("%s not admitted", name)
		}
		id, err := e.PackedID("test", vshard, entry.ID)
		if err != nil {
			t.Fatal(err)
		}
		// Admission registers the identity under its packed ID.
		if h := e.ApplicationPolicy(id); h == nil || h.ID() != id {
			t.Fatalf("%s: admission did not register packed ID %d", name, id)
		}
		if got, err := l0Producer(e, id, producers); err != nil || got != owner {
			t.Fatalf("%s: registered producer %d %v, want %d", name, got, err, owner)
		}
		seen[owner] = true
	}
	if len(seen) < 2 {
		t.Fatalf("names covered %d producers", len(seen))
	}
	// An unknown packed ID fails loudly instead of picking a producer.
	id, _ := e.PackedID("test", 3, 1<<30)
	if _, err := l0Producer(e, id, producers); err == nil {
		t.Fatal("unknown packed ID got a producer")
	}
}
