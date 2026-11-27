package v2daemon

import (
	"fmt"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/points"
)

// TestDictLocalIDsAreDense: a dictionary store binds local IDs in
// registration order (from 1), so receivers and AppendApplications resolve
// them with one slice read. A registered pair is accepted, a by-name
// application binds nothing, and a local ID naming another identity is
// refused.
func TestDictLocalIDsAreDense(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	s, err := Open(t.TempDir(), admitConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	p := s.Producers[0]
	if !p.dictStore() {
		t.Fatal("not a dictionary store")
	}
	var hs []*v2core.ApplicationPolicy
	for i := range 3 {
		name := fmt.Sprintf("local.%d", i)
		h, id, err := p.RegisterApplication(name, admitPolicy(t, name))
		if err != nil {
			t.Fatal(err)
		}
		if id != uint64(i+1) || p.handle(id) != h || !v2core.IsPackedID(h.ID()) {
			t.Fatalf("%s: local ID %d, handle match %v, packed %v", name, id, p.handle(id) == h, v2core.IsPackedID(h.ID()))
		}
		hs = append(hs, h)
	}
	app := func(h *v2core.ApplicationPolicy, id, order uint64) Application {
		return Application{Registered: h, LocalID: id, Order: order, Clock: 10020, Points: []points.Point{{Timestamp: 10000 + int64(order), Value: float64(order)}}, Sequence: []uint64{order}}
	}
	if err := p.AppendApplications([]Application{app(hs[0], 1, 1), app(hs[2], 3, 2)}); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendApplications([]Application{admitApp(t, "local.1", 3)}); err != nil {
		t.Fatal(err)
	}
	if n := len(p.applications); n != 4 {
		t.Fatalf("by-name application bound a local ID: %d handles, want 4", n)
	}
	if err := p.AppendApplications([]Application{app(hs[0], 2, 4)}); err == nil {
		t.Fatal("a local ID naming another identity was accepted")
	}
}
