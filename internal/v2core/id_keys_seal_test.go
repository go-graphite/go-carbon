package v2core

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// TestIDKeyedSealDifferential runs two stores side by side on the same
// writes: a name-keyed reference and one whose seals write ID-keyed (GCI2)
// parts through the store dictionary. Through merges, restarts and GC of the
// merged-away parts, every name's persisted winners must match exactly, and
// the second store must really hold ID-keyed parts only.
func TestIDKeyedSealDifferential(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	v2part.IDKeys.Store(true)
	t.Cleanup(func() { v2part.IDKeys.Store(false) })
	t.Setenv("CORE_MERGE_WORKERS", "0")
	names := int64(24)
	if raceEnabled {
		// Pebble records a leak-detection stack on each table iterator open
		// under -race. Scale only the independent names; keep every epoch,
		// schema, method, merge, restart and assertion in the race run.
		const raceNames = 4
		names = raceNames
	}
	for method := sealrollup.Average; method <= sealrollup.First; method += 3 {
		for _, schema := range []string{"1s:1h,1m:1d,1h:1y", "10s:6h,5m:7d,1h:1y,1d:10y"} {
			t.Run(fmt.Sprintf("%d/%s", method, schema), func(t *testing.T) {
				t.Parallel()
				policies := make([][]byte, names)
				metricNames := make([]string, names)
				for k := range names {
					// Names in reverse admission order, spread over vshards, so
					// ID order differs from name order.
					metricNames[k] = fmt.Sprintf("idkeys.%02d.metric", names-k)
					_, policies[k] = rollupTestPolicy(t, metricNames[k], schema, method, .1)
				}
				cfg := coreTestConfig()
				cfg.Shards = 1
				cfg.Producers = 1
				cfg.Rollups = true
				cfg.MaxRows = 4096
				roots := []string{t.TempDir(), t.TempDir()}
				hooks := []coreHooks{{}, {IDKeys: true}}
				engines := make([]*coreEngine, 2)
				// reopen closes and reopens store i; scan drops the owner
				// checkpoint first, so the open rebuilds every name from parts.
				reopen := func(i int, scan ...bool) {
					if engines[i] != nil {
						if err := engines[i].close(); err != nil {
							t.Fatal(err)
						}
					}
					if len(scan) > 0 && scan[0] {
						if err := os.Remove(filepath.Join(roots[i], "owner-000", rollupCheckpointFile)); err != nil {
							t.Fatal(err)
						}
					}
					h := hooks[i]
					h.Identities = &PolicyCatalog{}
					e, err := openCoreWithHooks(roots[i], cfg, h)
					if err != nil {
						t.Fatal(err)
					}
					engines[i] = e
				}
				reopen(0)
				reopen(1)
				defer func() {
					for _, e := range engines {
						_ = e.close()
					}
				}()
				less := func(a, b sealrollup.Record) int {
					if a.Archive != b.Archive {
						return a.Archive - b.Archive
					}
					if a.Slot < b.Slot {
						return -1
					}
					if a.Slot > b.Slot {
						return 1
					}
					return 0
				}
				compare := func(epoch int64) {
					snapshots := []*RollupSnapshot{}
					for _, e := range engines {
						s := e.owners[0].db.NewSnapshot()
						defer s.Close()
						index, err := NewRollupSnapshot(context.Background(), s, nil, e.identities, nil, 0)
						if err != nil {
							t.Fatal(err)
						}
						snapshots = append(snapshots, index)
					}
					for _, name := range metricNames {
						want, err := snapshots[0].Records(context.Background(), name)
						if err != nil {
							t.Fatal(err)
						}
						got, err := snapshots[1].Records(context.Background(), name)
						if err != nil {
							t.Fatal(err)
						}
						if len(want) == 0 {
							t.Fatalf("epoch %d: %s has no records", epoch, name)
						}
						slices.SortFunc(got, less)
						slices.SortFunc(want, less)
						for i := range got {
							got[i].Correction = false
						}
						for i := range want {
							want[i].Correction = false
						}
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("epoch %d %s: ID-keyed winners differ\ngot=%+v\nwant=%+v", epoch, name, got, want)
						}
					}
				}
				idParts := func() int {
					o := engines[1].owners[0]
					o.gc.mu.Lock()
					defer o.gc.mu.Unlock()
					n := 0
					for _, value := range o.gc.inventory.bindings {
						d, err := v2part.ParseDescriptor(value)
						if err != nil {
							t.Fatal(err)
						}
						if !descriptorByID(d) {
							t.Fatalf("name-keyed part %x in the ID-keyed store", d.SHA256)
						}
						n++
					}
					return n
				}
				// crashCopy checks every name a seal keyed is durable: a copy of
				// the dictionary files as they are on disk now (as a kill -9
				// leaves them) resolves each name to the live ID.
				crashCopy := func(epoch int64) {
					dir := t.TempDir()
					entries, err := os.ReadDir(storeDictDir(roots[1]))
					if err != nil {
						t.Fatal(err)
					}
					for _, f := range entries {
						b, err := os.ReadFile(filepath.Join(storeDictDir(roots[1]), f.Name()))
						if err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(dir, f.Name()), b, 0o600); err != nil {
							t.Fatal(err)
						}
					}
					copied, err := v2dict.Open(dir, v2dict.Options{NoMerge: true})
					if err != nil {
						t.Fatal(err)
					}
					defer copied.Close()
					for _, name := range metricNames {
						vshard := v2part.Route(v2part.ID(name))
						live, ok := engines[1].dict.Lookup("test", vshard, []byte(name))
						if !ok {
							t.Fatalf("epoch %d: %s not in the live dictionary", epoch, name)
						}
						if got, ok := copied.Lookup("test", vshard, []byte(name)); !ok || got != live {
							t.Fatalf("epoch %d: %s not durable after its seal: %+v %v, live %+v", epoch, name, got, ok, live)
						}
					}
				}
				now := int64(1794700800)
				values := []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, 1, math.Float64bits(7)}
				for epoch := int64(0); epoch < 32; epoch++ {
					now += 600
					var batches []coreBatch
					for k := range names {
						var rows []row
						for j := int64(0); j < 60; j++ {
							rows = append(rows, row{ID: uint64(k + 1), Time: now - 600 + j*10, Bits: values[(j+epoch+int64(k))%int64(len(values))], Seq: uint64(j + 1)})
						}
						for _, age := range []int64{120, 1800, 3500, 7200, 18000, 100000} {
							stamp := now - age
							rows = append(rows, row{ID: uint64(k + 1), Time: stamp, Bits: math.Float64bits(float64(epoch)), Seq: 100}, row{ID: uint64(k + 1), Time: stamp, Bits: values[epoch%int64(len(values))], Seq: 101})
						}
						order := uint64(epoch*names + int64(k) + 1)
						batches = append(batches, coreBatch{ID: uint64(k + 1), Policy: policies[k], Order: order, Clock: now, Points: rows})
					}
					cursor := uint64((epoch + 1) * names)
					for _, e := range engines {
						if err := e.submitBatches(0, batches, cursor, true); err != nil {
							t.Fatal(err)
						}
					}
					compare(epoch)
					if epoch < 2 {
						crashCopy(epoch)
					}
					if epoch == 11 || epoch == 23 {
						o := engines[1].owners[0]
						if n := mergeDrain(t, o); n == 0 {
							t.Fatal("no ID-keyed parts merged")
						}
						compare(epoch)
						reopen(0)
						reopen(1, epoch == 23)
						o = engines[1].owners[0]
						gcAdvance(o.gc)
						if _, err := o.gc.sweep(); err != nil {
							t.Fatal(err)
						}
						if o.gc.removedParts.Load() == 0 {
							t.Fatal("source parts not unlinked")
						}
						compare(epoch)
					}
				}
				if idParts() == 0 {
					t.Fatal("ID-keyed store has no parts")
				}
			})
		}
	}
}
