package v2daemon

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A recovered ID-keyed identity already has its canonical definition validated
// by its first admission. Re-registering after a restart must not decode
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
	if p.catalog != nil {
		t.Fatal("dictionary store opened a producer catalog")
	}
	if _, err := os.Stat(filepath.Join(p.journalDir, "catalog")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dictionary store wrote a catalog: %v", err)
	}
	want, err := s.Core.FindIdentity(name)
	if err != nil || want == nil {
		t.Fatalf("restart lost the identity: %v", err)
	}
	check := func() {
		h, id, err := p.RegisterApplication(name, raw)
		if err != nil || id == 0 || p.handle(id) != h || h != want {
			t.Fatalf("registration changed identity: id=%d same=%v err=%v", id, h == want, err)
		}
	}
	// The catalog binding is the only owner of the packed handle (the
	// producer slice), so a bound local ID never has an empty slot; the
	// recovered handle must be the dictionary admission's packed identity.
	if !v2core.IsPackedID(want.ID()) {
		t.Fatalf("recovered identity %d is not packed", want.ID())
	}
	if again, err := s.Core.AdmitIdentity(name, raw); err != nil || again != want {
		t.Fatalf("re-admission changed identity: same=%v err=%v", again == want, err)
	}
	check()
	t.Logf("recovered registration allocations=%g", testing.AllocsPerRun(100, check))
	policy.Config.Method = sealrollup.Max
	changed, err := v2core.EncodeRollupPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = p.RegisterApplication(name, changed); err == nil {
		t.Fatal("accepted a changed recovered definition")
	}
	// An owner recreated after recovery must use the persisted policy for an
	// existing name. Its first receiver-table admission drains immediately; the
	// resolver still owns admission and refusal of new names.
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
	if calls != 0 || rejected != 0 || p.pendingRows != 0 || o.names.Len() != 1 {
		t.Fatalf("recovered receiver repeated admission: calls=%d rejected=%d pending=%d names=%d", calls, rejected, p.pendingRows, o.names.Len())
	}
	if err := o.Append([]string{"brand.new.name"}, nil, data, nil, 1010); err != nil {
		t.Fatal(err)
	}
	if fresh, _ := s.Core.FindIdentity("brand.new.name"); calls != 1 || rejected != 1 || fresh != nil || p.pendingRows != 0 {
		t.Fatalf("new admission bypassed resolver: calls=%d rejected=%d admitted=%v pending=%d", calls, rejected, fresh != nil, p.pendingRows)
	}
	// Exercise the recovered handle through pending classification and the
	// installed fingerprint table, then recover its sealed metadata again.
	data[0].Value = math.Float64frombits(0x8000000000000000)
	if err = o.Append([]string{name}, nil, data, nil, 1020); err != nil {
		t.Fatal(err)
	}
	if err = p.Drain(); err != nil {
		t.Fatal(err)
	}
	if err = s.Core.AppendBatches(0, nil, p.cursor, true); err != nil {
		t.Fatal(err)
	}
	checkSealed := func() {
		t.Helper()
		pin := s.Core.DBs()[0].NewSnapshot()
		defer pin.Close()
		snapshot, err := v2core.NewRollupSnapshot(context.Background(), pin, nil, cfg.Hooks.Identities, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, meta, ok := snapshot.LookupMetadata(name)
		if !ok || meta.ID != want.ID() || meta.Frontier.Order != p.applyOrder {
			t.Fatalf("recovered receiver lost installed metadata: ok=%v meta=%+v order=%d", ok, meta, p.applyOrder)
		}
		records, err := snapshot.Records(context.Background(), name)
		if err != nil || len(records) != 1 || records[0].Sample.Time != 1000 || records[0].Sample.Bits != 0x8000000000000000 {
			t.Fatalf("recovered receiver changed sealed point: records=%+v err=%v", records, err)
		}
	}
	checkSealed()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Hooks.Identities = &v2core.PolicyCatalog{}
	s, err = Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	p = s.Producers[0]
	checkSealed()
	if p.catalog != nil {
		t.Fatal("sealed read opened the producer catalog")
	}
	coldReceiver := &applicationOwner{p: p, resolve: func(string) ([]byte, error) {
		t.Fatal("cold recovered intake repeated admission")
		return nil, nil
	}}
	if err := coldReceiver.Append([]string{name}, nil, probePoints(1), nil, 1030); err != nil {
		t.Fatal(err)
	}
	if p.catalog != nil || p.pendingRows != 0 || coldReceiver.names.Len() != 1 {
		t.Fatal("cold intake did not reuse the dictionary's persisted policy")
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
