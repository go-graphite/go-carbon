package v2daemon

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A recovered ID-keyed identity already has its canonical definition validated
// by catalog.Intern. Filling the producer's empty handle slot must not decode
// and allocate its dictionary name once more than the cached-slot path does.
func TestRecoveredRegistrationReusesBoundHandle(t *testing.T) {
	v2part.IDKeys.Store(true)
	defer v2part.IDKeys.Store(false)
	root := t.TempDir()
	cfg := Config{Layer: 3, Producers: 1, Shards: 1, GroupInterval: time.Hour, MaxCoreRows: 64, Hooks: v2core.Hooks{IDKeys: true, Identities: &v2core.PolicyCatalog{}}}
	name := "recovered." + strings.Repeat("long.name.", 40) + "count"
	policy := v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 10, Count: 120}}}}
	raw, err := v2core.EncodeRollupPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := s.Producers[0]
	if err = p.AppendApplications([]Application{{Name: name, Policy: raw, Order: 1, Clock: 1000, Points: probePoints(1), Sequence: []uint64{1}}}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Hooks.Identities = &v2core.PolicyCatalog{}
	s, err = Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	p = s.Producers[0]
	if len(p.applications) > 1 && p.applications[1] != nil {
		t.Fatal("fixture already has a producer handle")
	}
	want := s.Core.ApplicationPolicy(NumericID(1, 0, 1))
	if want == nil {
		t.Fatal("catalog recovery did not bind the identity")
	}
	check := func() {
		h, id, err := p.RegisterApplication(name, raw)
		if err != nil || id != 1 || h != want {
			t.Fatalf("registration changed identity: id=%d same=%v err=%v", id, h == want, err)
		}
	}
	check() // size the producer slice before comparing cold and cached slots
	warm := testing.AllocsPerRun(100, check)
	cold := testing.AllocsPerRun(100, func() { p.applications[1] = nil; check() })
	t.Logf("cached-slot allocations=%g recovered-slot allocations=%g", warm, cold)
	if cold > warm {
		t.Fatalf("recovered registration repeats name allocation: cold=%g cached=%g", cold, warm)
	}
	policy.Config.Method = sealrollup.Max
	changed, err := v2core.EncodeRollupPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	p.applications[1] = nil
	if _, _, err = p.RegisterApplication(name, changed); err == nil {
		t.Fatal("accepted a changed recovered definition")
	}
	// An owner recreated after recovery must use the persisted policy for an
	// existing name. The resolver still owns admission and refusal of new names.
	calls := 0
	rejected := 0
	o := &applicationOwner{p: p, resolve: func(string) ([]byte, error) {
		calls++
		return nil, errors.New("new admission refused")
	}, reject: func(string, error) { rejected++ }}
	data := probePoints(1)
	if err := o.AppendBytes([]byte(name), []int{len(name)}, nil, data, nil, 1010); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || rejected != 0 || p.pendingRows != 1 || o.names.Len() != 1 {
		t.Fatalf("recovered receiver repeated admission: calls=%d rejected=%d pending=%d names=%d", calls, rejected, p.pendingRows, o.names.Len())
	}
	if err := o.Append([]string{"brand.new.name"}, nil, data, nil, 1010); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || rejected != 1 || p.catalog.Count() != 1 || p.pendingRows != 1 {
		t.Fatalf("new admission bypassed resolver: calls=%d rejected=%d catalog=%d pending=%d", calls, rejected, p.catalog.Count(), p.pendingRows)
	}
}

// A live classic writer still needs current-policy validation at admission.
func TestRecoveredReceiverKeepsMirrorAdmission(t *testing.T) {
	s, resolve := openProbeStore(t)
	p := s.Producers[0]
	raw, err := resolve("mirror.existing")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = p.RegisterApplication("mirror.existing", raw); err != nil {
		t.Fatal(err)
	}
	mirrorQueue(p)
	calls, rejected := 0, 0
	o := &applicationOwner{p: p, resolve: func(string) ([]byte, error) { calls++; return nil, errors.New("live classic mismatch") }, reject: func(string, error) { rejected++ }}
	if err := o.Append([]string{"mirror.existing"}, nil, probePoints(1), nil, 1010); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || rejected != 1 || p.pendingRows != 0 {
		t.Fatalf("mirror skipped admission: calls=%d rejected=%d pending=%d", calls, rejected, p.pendingRows)
	}
}
