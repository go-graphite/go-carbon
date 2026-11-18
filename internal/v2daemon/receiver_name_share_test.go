package v2daemon

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/points"
)

// openNameShareStore opens a layer-3 store whose catalog binds names to the
// shared policy identities, as the daemon does.
func openNameShareStore(tb testing.TB, producers int) (*Store, PolicyResolver) {
	tb.Helper()
	r, err := v2read.New(2, 16<<20, 1<<20)
	if err != nil {
		tb.Fatal(err)
	}
	r.Identities = &v2core.PolicyCatalog{}
	s, err := Open(tb.TempDir(), Config{Layer: 3, Producers: producers, Shards: 2, MaxCoreRows: 1 << 16, GroupInterval: time.Hour, Hooks: v2core.Hooks{Identities: r.Identities, PublishBatches: r.Publish}})
	if err != nil {
		r.Close()
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		r.Close() // the reader closes first, as in the other store tests
		if err := s.Close(); err != nil {
			tb.Error(err)
		}
	})
	resolve := func(name string) ([]byte, error) {
		return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 10, Count: 120}}}})
	}
	return s, resolve
}

// batchNames splits one string the way v2recv's batch.named does: every name
// shares the socket batch's text.
func batchNames(names []string) (string, []string) {
	text := strings.Join(names, "")
	out := make([]string, len(names))
	start := 0
	for i, name := range names {
		out[i] = text[start : start+len(name)]
		start += len(name)
	}
	return text, out
}

// The receiver keeps each name once: its map key and handle share the
// catalog's identity string. A clone holds every name twice; a borrowed batch
// substring pins a whole socket batch per retained name.
func TestApplicationOwnerKeysByCatalogName(t *testing.T) {
	s, resolve := openNameShareStore(t, 1)
	o := &applicationOwner{p: s.Producers[0], resolve: resolve}
	raw := make([]string, 0, 96)
	for i := range 64 {
		raw = append(raw, fmt.Sprintf("share.host%03d.", i)+strings.Repeat("m", 16+i*7))
	}
	raw = append(raw, raw[:32]...) // known names later in the same batch
	text, names := batchNames(raw)
	data := make([]points.Point, len(names))
	for i := range data {
		data[i] = points.Point{Timestamp: 1000, Value: float64(i)}
	}
	if err := o.Append(names, data, nil, 1010); err != nil {
		t.Fatal(err)
	}
	if err := o.Drain(); err != nil {
		t.Fatal(err)
	}
	if len(o.policies) != 64 {
		t.Fatalf("policies=%d want 64", len(o.policies))
	}
	lo := uintptr(unsafe.Pointer(unsafe.StringData(text)))
	hi := lo + uintptr(len(text))
	for key, registered := range o.policies {
		owned, ok := o.p.catalog.Key(registered.id)
		if !ok || owned != key || registered.name != key {
			t.Fatalf("key %q: catalog %q/%v handle %q", key, owned, ok, registered.name)
		}
		p := unsafe.StringData(key)
		if at := uintptr(unsafe.Pointer(p)); at >= lo && at < hi {
			t.Fatalf("key %q pins the borrowed batch text", key)
		}
		if p != unsafe.StringData(owned) || p != unsafe.StringData(registered.name) {
			t.Fatalf("key %q is a separate copy, not the catalog's string", key)
		}
		if registered.policy == nil || unsafe.StringData(registered.policy.Name()) != p {
			t.Fatalf("key %q is not the identity's name", key)
		}
	}
	// A later batch with the same names reuses the handles and changes no key.
	before := make(map[string]*RegisteredApplication, len(o.policies))
	for k, v := range o.policies {
		before[k] = v
	}
	_, again := batchNames(raw[:64])
	for i := range data[:64] {
		data[i].Timestamp = 1010
	}
	if err := o.Append(again, data[:64], nil, 1020); err != nil {
		t.Fatal(err)
	}
	if len(o.policies) != len(before) {
		t.Fatalf("re-registered: %d != %d", len(o.policies), len(before))
	}
	for k, v := range o.policies {
		if before[k] != v {
			t.Fatalf("handle for %q changed", k)
		}
	}
}

// Direct registration (replay, harness) also retains the catalog's string, not
// the caller's.
func TestRegisterApplicationRetainsCatalogName(t *testing.T) {
	s, resolve := openNameShareStore(t, 1)
	p := s.Producers[0]
	caller := []byte("direct.registration.name")
	name := string(caller)
	policy, err := resolve(name)
	if err != nil {
		t.Fatal(err)
	}
	r, err := p.RegisterApplication(name, policy)
	if err != nil {
		t.Fatal(err)
	}
	owned, _ := p.catalog.Key(r.id)
	if unsafe.StringData(r.name) == unsafe.StringData(name) || unsafe.StringData(r.name) != unsafe.StringData(owned) || unsafe.StringData(owned) != unsafe.StringData(r.policy.Name()) {
		t.Fatal("registered name is not the catalog's identity string")
	}
}
