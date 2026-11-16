package v2convert

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

type catalogFixture struct {
	names     []string
	calls     []string
	snapshots atomic.Int64
	repeat    bool
}

func (f *catalogFixture) ListCatalogPage(_ context.Context, _, after string, limit int) ([]chunkstore.Metadata, error) {
	f.calls = append(f.calls, after)
	var out []chunkstore.Metadata
	for _, name := range f.names {
		if (f.repeat || name > after) && len(out) < limit {
			out = append(out, chunkstore.Metadata{MetricConfig: chunkstore.MetricConfig{Name: name}})
		}
	}
	return out, nil
}
func (f *catalogFixture) Snapshot(_ context.Context, name string) (chunkstore.Snapshot, error) {
	f.snapshots.Add(1)
	s := nativeTestSnapshot(name)
	return s, nil
}
func catalogPlan(t *testing.T) NativeCheckpointPlan {
	t.Helper()
	p := NativeCheckpointPlan{Version: 1, Now: 1794700800, Baseline: nativeTestBaseline(), Files: []File{{Name: "CHUNKSTORE", SHA256: strings.Repeat("0", 64)}, {Name: "CURRENT", SHA256: strings.Repeat("1", 64)}}}
	var err error
	p.Baseline.Snapshot, err = p.InventoryDigest()
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestNativeCatalogCoverageAndResume(t *testing.T) {
	f := &catalogFixture{names: []string{"a", "b", "c", "d", "e"}}
	p := catalogPlan(t)
	dir := t.TempDir()
	var names []string
	var parts []v2part.PartID
	walk := func() error {
		return ConvertNativeCatalog(context.Background(), f, p, dir, "", 2, 4<<20, 2, func(r NativeReceipt) error {
			names = append(names, r.Names...)
			parts = append(parts, r.Baseline.Part)
			return nil
		})
	}
	if err := walk(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, f.names) || !reflect.DeepEqual(f.calls, []string{"", "b", "d", "e"}) {
		t.Fatalf("coverage names=%v calls=%v", names, f.calls)
	}
	if len(parts) != 3 || parts[0] == parts[1] || parts[1] == parts[2] || parts[0] == parts[2] {
		t.Fatalf("page part IDs collide: %v", parts)
	}
	first := append([]v2part.PartID(nil), parts...)
	before := f.snapshots.Load()
	if err := walk(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parts[len(first):], first) {
		t.Fatal("resume changed page part IDs")
	}
	if f.snapshots.Load() != before {
		t.Fatal("resume rematerialized completed pages")
	}
}
func TestNativeCatalogRefusesNonadvancingAndStopsOnCallback(t *testing.T) {
	p := catalogPlan(t)
	f := &catalogFixture{names: []string{"a"}, repeat: true}
	if err := ConvertNativeCatalog(context.Background(), f, p, t.TempDir(), "", 1, 4<<20, 1, nil); err == nil || !strings.Contains(err.Error(), "advance") {
		t.Fatalf("got %v", err)
	}
	sentinel := errors.New("stop")
	f = &catalogFixture{names: []string{"a", "b"}}
	err := ConvertNativeCatalog(context.Background(), f, p, t.TempDir(), "", 1, 4<<20, 1, func(NativeReceipt) error { return sentinel })
	if !errors.Is(err, sentinel) || len(f.calls) != 1 {
		t.Fatalf("got %v, calls=%v", err, f.calls)
	}
}
