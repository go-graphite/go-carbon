package carbonserver

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2index"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2read"
)

// A find must fence even an already restored projection: otherwise it can
// return while an owner is still unvalidated (or whose hydration will fail).
// Removing the binding fence makes the paused-query assertion fail.
// Exact renders validate requested immutable rows and need no writable inventory.
func TestV2RestartHydrationFindRenderDifferential(t *testing.T) {
	v2part.IDKeys.Store(true)
	t.Cleanup(func() { v2part.IDKeys.Store(false) })
	ctx := context.Background()
	root := t.TempDir()
	const now = int64(1794700800)
	cfg := v2core.Config{Rollups: true, AppendParts: true, JournalIdentity: "hydration-test", Shards: 2, Owners: 2, Producers: 1, MaxRows: 1024, PartitionSeconds: 86400, RawCodec: "run1", SealInterval: time.Hour}
	var reader *v2read.Reader
	var engine *v2core.Engine
	open := func(stage func(string)) {
		var err error
		reader, err = v2read.New(2, 8<<20, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		reader.Identities = &v2core.PolicyCatalog{}
		engine, err = v2core.Open(root, cfg, v2core.Hooks{IDKeys: true, Identities: reader.Identities, PublishPending: reader.PublishPending, Stage: stage, Now: func() time.Time { return time.Unix(1, 0) }})
		if err != nil {
			reader.Close()
			t.Fatal(err)
		}
	}
	closeStore := func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
		engine = nil
	}
	open(nil)
	defer func() {
		if engine != nil {
			closeStore()
		}
	}()
	names := []string{}
	for i := 0; i < 32; i++ {
		name := fmt.Sprintf("hydration.nodes.host%02d.cpu", i)
		names = append(names, name)
		policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}}}})
		if err != nil {
			t.Fatal(err)
		}
		rows := []v2core.Row{}
		for j, bits := range []uint64{math.Float64bits(float64(i)), 1 << 63, 0x7ff8000000000042, math.Float64bits(13)} {
			rows = append(rows, v2core.Row{ID: uint64(i + 1), Time: now - 60 + int64(j), Bits: bits, Seq: uint64(j + 1)})
		}
		if err = engine.AppendBatches(0, []v2core.Batch{{ID: uint64(i + 1), Order: uint64(i + 1), Clock: now, Policy: policy, Points: rows}}, uint64(i+1), true); err != nil {
			t.Fatal(err)
		}
	}
	capture := func() map[string][]uint64 {
		t.Helper()
		out := map[string][]uint64{}
		for _, name := range names {
			for _, span := range []int64{120, 30 * 86400} {
				s, err := reader.Fetch(ctx, name, int(now-span), int(now))
				if err != nil {
					t.Fatal(err)
				}
				bits := []uint64{uint64(s.FromTime()), uint64(s.UntilTime()), uint64(s.Step())}
				for _, v := range s.Values() {
					bits = append(bits, math.Float64bits(v))
				}
				out[fmt.Sprint(name, "/", span)] = bits
			}
		}
		return out
	}
	want := capture()
	wantNames, err := reader.Names(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var catalog v2index.Catalog
	if err = catalog.Publish(1, wantNames); err != nil {
		t.Fatal(err)
	}
	listener := &CarbonserverListener{maxGlobs: math.MaxInt, maxMetricsGlobbed: math.MaxInt}
	if err = listener.SetV2Catalog(&catalog, reader.AwaitHydration); err != nil {
		t.Fatal(err)
	}
	queries := []string{"*", "hydration.*", "hydration.nodes.*.cpu", "hydration.nodes.host0?.cpu", "missing.*", names[0]}
	eager := make([]*ExpandedGlobResponse, len(queries))
	for i, q := range queries {
		eager[i] = listener.expandV2Catalog(listener.v2Catalog.Load(), q)
		if eager[i].Err != nil {
			t.Fatal(eager[i].Err)
		}
	}
	closeStore()
	// Reopen with the very same bytes, but force eager completion before reads.
	open(nil)
	if err = reader.AwaitHydration(ctx); err != nil {
		t.Fatal(err)
	}
	if got := capture(); !reflect.DeepEqual(got, want) {
		t.Fatal("eager restart render differs")
	}
	closeStore()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	open(func(s string) {
		if s == "owner-hydration" {
			once.Do(func() { close(entered) })
			<-release
		}
	})
	defer onceRelease(release)()
	if err = listener.SetV2Catalog(&catalog, reader.AwaitHydration); err != nil {
		t.Fatal(err)
	}
	findDone := make(chan *ExpandedGlobResponse, 1)
	renderDone := make(chan error, 1)
	go func() { findDone <- listener.expandV2Catalog(listener.v2Catalog.Load(), queries[2]) }()
	go func() {
		s, err := reader.Fetch(ctx, names[0], int(now-120), int(now))
		if err == nil {
			bits := []uint64{uint64(s.FromTime()), uint64(s.UntilTime()), uint64(s.Step())}
			for _, v := range s.Values() {
				bits = append(bits, math.Float64bits(v))
			}
			if !reflect.DeepEqual(bits, want[fmt.Sprint(names[0], "/", 120)]) {
				err = fmt.Errorf("render during recovery differs: %v", bits)
			}
		}
		renderDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("hydration did not start")
	}
	select {
	case err := <-renderDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("exact render waited for writable inventory recovery")
	}
	select {
	case got := <-findDone:
		t.Fatalf("unfenced find returned during hydration: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case got := <-findDone:
		if !reflect.DeepEqual(v2Found(got.Files, got.Leafs), v2Found(eager[2].Files, eager[2].Leafs)) || got.Err != nil {
			t.Fatalf("find changed: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("find remained fenced")
	}
	for i, q := range queries {
		got := listener.expandV2Catalog(listener.v2Catalog.Load(), q)
		if !reflect.DeepEqual(v2Found(got.Files, got.Leafs), v2Found(eager[i].Files, eager[i].Leafs)) || got.Err != nil {
			t.Fatalf("lazy restart find changed: %q %+v", q, got)
		}
	}
	if got := capture(); !reflect.DeepEqual(got, want) {
		t.Fatal("lazy restart render differs")
	}
	gotNames, err := reader.Names(ctx)
	if err != nil || !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("lazy discovery: %v %v", gotNames, err)
	}
}

// Release a paused owner before the deferred engine close even after Fatal.
func onceRelease(ch chan struct{}) func() {
	return func() {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}
