package v2daemon

import (
	"bytes"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

func TestOriginalApplicationsJournalCore(t *testing.T) { testOriginalApplicationsJournalCore(t, false) }
func TestRegisteredApplicationsJournalCore(t *testing.T) {
	testOriginalApplicationsJournalCore(t, true)
}
func testOriginalApplicationsJournalCore(t *testing.T, useRegistered bool) {
	t.Parallel()
	root := t.TempDir()
	cfg := Config{Layer: 3, Producers: 1, Shards: 2, MaxCoreRows: 256, GroupInterval: time.Hour}
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = s.Close()
		}
	}()
	makePolicy := func(name string) []byte {
		b, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 60, Count: 1440}}}})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	a, b := makePolicy("test.a"), makePolicy("test.b")
	apps := []Application{
		{Name: "test.a", Policy: a, Order: 7, Clock: 10000, Points: []points.Point{{Timestamp: 9990, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 9990, Value: math.Copysign(0, -1)}, {Timestamp: 9991, Value: 2}}, Sequence: []uint64{3, 4, 5}},
		{Name: "test.b", Policy: b, Order: 9, Clock: 10000, Points: []points.Point{{Timestamp: 9990, Value: 3}}, Sequence: []uint64{100}},
		{Name: "test.a", Policy: a, Order: 10, Clock: 9999, Points: []points.Point{{Timestamp: 9980, Value: 4}}, Sequence: []uint64{6}},
	}
	p := s.Producers[0]
	if useRegistered {
		registered, local, err := p.RegisterApplication("test.a", a)
		if err != nil {
			t.Fatal(err)
		}
		again, againID, err := p.RegisterApplication("test.a", append([]byte(nil), a...))
		if err != nil || registered != again || local != againID {
			t.Fatal("registration lost identity", err)
		}
		for _, i := range []int{0, 2} {
			apps[i].Registered, apps[i].LocalID = registered, local
			apps[i].Name = ""
			apps[i].Policy = nil
		}
		clear(a) // registration and all later appends own the canonical definition
	}
	if err = p.AppendApplications(apps[:2]); err != nil {
		t.Fatal(err)
	}
	if err = p.Seal(); err != nil {
		t.Fatal(err)
	}
	if err = p.AppendApplications(apps[2:]); err != nil {
		t.Fatal(err)
	}
	if len(p.packets) != 3 {
		t.Fatal("new queue or premature sync")
	}
	// Every caller-owned policy/point/sequence buffer may be reused immediately.
	for i := range apps {
		clear(apps[i].Points)
		clear(apps[i].Sequence)
		clear(apps[i].Policy)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	reader, err := v2core.Open(filepath.Join(root, "placement-group-00000001", "core"), coreConfig(cfg), v2core.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seen := 0
	for _, db := range reader.DBs() {
		snap := db.NewSnapshot()
		err = v2core.ScanRollups(snap, func(p v2core.RollupPolicy, m v2core.RollupMetadata, records []sealrollup.Record) error {
			seen++
			if len(records) == 0 {
				t.Fatal("no archives")
			}
			want := uint64(9)
			revision := uint64(2)
			if p.Name == "test.a" {
				want = 10
				revision = 3
			}
			if m.Frontier.Order != want || m.Revision != revision || m.LastUpdate != 10000e9 {
				t.Fatal("lost original metadata", m)
			}
			return nil
		})
		_ = snap.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if seen != 2 {
		t.Fatal("lost metrics", seen)
	}
}

func TestApplicationsRejectBeforeAdmission(t *testing.T) {
	t.Parallel()
	for _, apps := range [][]Application{
		{{Name: "a", Order: 1, Points: []points.Point{{}}, Sequence: nil}},
		{{Name: "a", Order: 2, Points: []points.Point{{}}, Sequence: []uint64{1}}, {Name: "a", Order: 1, Points: []points.Point{{}}, Sequence: []uint64{2}}},
	} {
		s, err := Open(t.TempDir(), Config{Layer: 3, Producers: 1, Shards: 1})
		if err != nil {
			t.Fatal(err)
		}
		p := s.Producers[0]
		if err = p.AppendApplications(apps); err == nil {
			t.Fatal("invalid group accepted")
		}
		if p.cursor != 0 || len(p.packets) != 0 || p.catalog != nil {
			t.Fatal("invalid group partially admitted")
		}
		if err = p.Drain(); err == nil {
			t.Fatal("lost sticky failure")
		}
		_ = s.Close()
	}
}

func TestClassicFixtureShapes(t *testing.T) {
	t.Parallel()
	for shape := 0; shape < 4; shape++ {
		slots, err := classicFixtureSlots(Fixture{Rounds: 8}, shape)
		if err != nil {
			t.Fatal(shape, err)
		}
		if len(slots) == 0 {
			t.Fatal("empty classic fixture shape", shape)
		}
	}
}

func TestRegisteredApplicationScope(t *testing.T) {
	for _, mode := range []string{"producer", "zero", "name", "policy", "restart", "local-zero", "local-other", "detached"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			cfg := Config{Layer: 3, Producers: 2, Shards: 1, MaxCoreRows: 16, GroupInterval: time.Hour}
			s, err := Open(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "scope", Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 120}}}})
			if err != nil {
				t.Fatal(err)
			}
			p := s.Producers[0]
			registered, local, err := p.RegisterApplication("scope", policy)
			if err != nil {
				t.Fatal(err)
			}
			bad := append([]byte(nil), policy...)
			bad[len(bad)-1] ^= 1
			if _, _, err := p.RegisterApplication("scope", bad); err == nil {
				t.Fatal("accepted changed catalog definition")
			}
			app := Application{Registered: registered, LocalID: local, Order: 1, Clock: 1000, Points: []points.Point{{Timestamp: 999, Value: 1}}, Sequence: []uint64{1}}
			switch mode {
			case "producer":
				p = s.Producers[1]
				if _, _, err := p.RegisterApplication("scope", policy); err != nil {
					t.Fatal(err)
				}
			case "zero":
				app.Registered = &v2core.ApplicationPolicy{}
			case "name":
				app.Name = "scope"
			case "policy":
				app.Policy = policy
			case "restart":
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(root, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = reopened.Close() }()
				p = reopened.Producers[0]
				if _, _, err := p.RegisterApplication("scope", policy); err != nil {
					t.Fatal(err)
				}
			case "local-zero":
				app.LocalID = 0
			case "local-other":
				other := bytes.ReplaceAll(policy, []byte("scope"), []byte("other"))
				_, app.LocalID, err = p.RegisterApplication("other", other)
				if err != nil {
					t.Fatal(err)
				}
			case "detached":
				app.Registered, err = v2core.NewApplicationPolicy("scope", policy)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := p.AppendApplications([]Application{app}); err == nil {
				t.Fatal("accepted invalid registered application")
			}
			if p.cursor != 0 {
				t.Fatal("invalid handle advanced cursor")
			}
		})
	}
}
