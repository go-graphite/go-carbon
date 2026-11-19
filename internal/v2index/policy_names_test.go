package v2index

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

func TestMappedNameSourcePinsReclaimedBase(t *testing.T) {
	var c Catalog
	if err := c.SetDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Publish(1, []string{"service.old"}); err != nil {
		t.Fatal(err)
	}
	source := c.Snapshot().MappedNames()
	path := c.Snapshot().levels[0].path
	pos, ok := source.FindName("service.old")
	if !ok {
		t.Fatal("missing source name")
	}
	// No reader snapshot pins the old run. Only the exported name source does.
	if err := c.Publish(2, []string{"service.new"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("old base file not retired", err)
	}
	for range 3 {
		runtime.GC()
	}
	if got := source.NameAt(pos); got != "service.old" {
		t.Fatal("reader resolved a reclaimed generation", got)
	}
	if source == c.Snapshot().MappedNames() {
		t.Fatal("generation owner reused")
	}
}

func BenchmarkMappedBaseRandomDecode(b *testing.B) {
	var c Catalog
	if err := c.SetDir(filepath.Join(b.TempDir(), "index")); err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	names := make([]string, 100000)
	for i := range names {
		names[i] = fmt.Sprintf("service.region.cluster.host%08d.%s", i, strings.Repeat("x", 80))
	}
	if err := c.Publish(1, names); err != nil {
		b.Fatal(err)
	}
	source := c.Snapshot().MappedNames()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if source.NameAt(uint32((uint64(i)*2654435761)%uint64(len(names)))) == "" {
			b.Fatal("empty")
		}
	}
}

// The pinned core reader resolves its handle through the retained old binding.
// Reusing position zero from the current base produces no old records, even
// though the part snapshot itself is still valid.
func TestMappedPolicyReaderAfterBaseReclamation(t *testing.T) {
	identities := &v2core.PolicyCatalog{}
	engine, err := v2core.Open(t.TempDir(), v2core.Config{Producers: 1, Shards: 1, Owners: 1, MaxRows: 100, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1", Rollups: true}, v2core.Hooks{Identities: identities})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	raw, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "service.old", Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 60}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.AppendBatches(0, []v2core.Batch{{ID: 1, Order: 1, Clock: 100, Policy: raw, Points: []v2core.Row{{ID: 1, Time: 100, Bits: math.Float64bits(42), Seq: 1}}}}, 1, true); err != nil {
		t.Fatal(err)
	}
	snapshot := engine.DBs()[0].NewSnapshot()
	defer snapshot.Close()
	reader, err := v2core.NewRollupSnapshot(context.Background(), snapshot, nil, identities, nil)
	if err != nil {
		t.Fatal(err)
	}
	want, err := reader.Records(context.Background(), "service.old")
	if err != nil || len(want) == 0 {
		t.Fatal("fixture has no records", err)
	}
	var c Catalog
	if err := c.SetDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Publish(1, []string{"service.old"}); err != nil {
		t.Fatal(err)
	}
	oldPath := c.Snapshot().levels[0].path
	if err := identities.BindMappedNames(c.Snapshot().MappedNames()); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(2, []string{"service.new"}); err != nil {
		t.Fatal(err)
	}
	if err := identities.BindMappedNames(c.Snapshot().MappedNames()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatal("old base not retired", err)
	}
	for range 3 {
		runtime.GC()
	}
	got, err := reader.Records(context.Background(), "service.old")
	if err != nil || !slices.Equal(got, want) {
		t.Fatal("reader resolved a reclaimed mapping generation", got, err)
	}
	runtime.KeepAlive(reader)
}
