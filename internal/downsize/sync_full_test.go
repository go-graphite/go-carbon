package downsize

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

// Frozen pre-optimisation full-history path: the fixture compares complete
// ordered hole verdicts against this point-by-point oracle.
func (s *syncer) checkFullReference(ctx context.Context, row *syncRow, names []string, schemas map[string][]syncArchive, now int64) error {
	groups := map[string][]string{}
	keyed := map[string][]syncArchive{}
	for _, n := range names {
		key := fmt.Sprint(schemas[n])
		groups[key] = append(groups[key], n)
		keyed[key] = schemas[n]
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		group := groups[key]
		for _, a := range keyed[key] {
			from, until := syncWindow(now, a, s.o.settle)
			if from >= until {
				continue
			}
			target, err := s.fetch(ctx, s.o.to, group, from, until)
			if err != nil {
				return fmt.Errorf("target fetch: %w", err)
			}
			for src, source := range s.o.from {
				subset := s.holders(src, group)
				if len(subset) == 0 {
					continue
				}
				if s.isDown(source) {
					for _, n := range subset {
						row.Skipped = append(row.Skipped, syncSkip{n, "source down: " + source})
					}
					continue
				}
				got, err := s.fetch(ctx, source, subset, from, until)
				if errors.Is(err, errSyncStoreDown) {
					continue
				}
				if err != nil {
					return fmt.Errorf("source %s fetch: %w", source, err)
				}
				s.prog.reads.Add(int64(len(subset)))
				for _, n := range subset {
					sv, ok := got[n]
					if !ok || sv.Step != a.Step {
						continue // not served at this step: retention differs
					}
					if h, ok := syncCompare(n, source, sv, target[n], a.Step, from, until); ok {
						h.Window = "full"
						row.Holes = append(row.Holes, h)
					}
				}
			}
		}
	}
	return nil
}

func TestSyncFullDigestParity(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		for _, variant := range []string{"clean", "holes", "values", "retentions", "missing"} {
			t.Run(variant+"/fallback="+fmt.Sprint(fallback), func(t *testing.T) {
				f, o, holes := threeSources(t)
				names := []string{"a.clean", "b.holes", "c.peerhole"}
				switch variant {
				case "clean":
					for _, store := range []string{syncT, syncD1, syncD2, syncP} {
						for _, name := range names {
							f.put(store, name, syncTestSchema)
						}
					}
				case "values":
					f.store(syncT).points["a.clean"][60][holes[0]] = math.Copysign(0, -1)
					f.store(syncD1).points["a.clean"][60][holes[0]] = 0
					f.store(syncT).points["b.holes"][60][holes[0]] = 12345
				case "retentions":
					f.put(syncP, "c.peerhole", []syncArchive{{Step: 300, Points: 10000}})
				case "missing":
					delete(f.store(syncD1).archives, "a.clean")
				}
				f.noDigest[syncP] = fallback
				if fallback {
					f.noDigest[syncT] = true
				}
				s := &syncer{o: o, b: f}
				schemas := map[string][]syncArchive{}
				for _, n := range names {
					schemas[n] = syncTestSchema
					s.masks.Store(n, uint64(7))
				}
				var want, got syncRow
				if err := s.checkFullReference(context.Background(), &want, names, schemas, syncTestNow); err != nil {
					t.Fatal(err)
				}
				before := f.fetches.Load()
				if err := s.checkFull(context.Background(), &got, names, schemas, syncTestNow); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %+v; want %+v", got, want)
				}
				if variant == "clean" && !fallback && f.fetches.Load() != before {
					t.Fatal("equal full history fetched points")
				}
			})
		}
	}
}

func TestSyncFullConcurrentFallback(t *testing.T) {
	f, o, _ := threeSources(t)
	f.noDigest[syncT] = true
	names := []string{"b.holes", "c.peerhole"}
	backend := &rendezvousSync{fakeSync: f, ready: make(chan struct{}), want: 3}
	// One target and two holders must enter Fetch before any can complete.
	o.from = []string{syncD2, syncP}
	s := &syncer{o: o, b: backend}
	for _, n := range names {
		s.masks.Store(n, uint64(3))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var row syncRow
	if err := s.checkFullArchive(ctx, &row, names, 60, syncTestNow-86400, syncTestNow-3600); err != nil {
		t.Fatal(err)
	}
}

// Record only detail reads; matching neighbors must not be materialised on
// the target just because another name needs a point-by-point comparison.
type detailNamesSync struct {
	*fakeSync
	mu          sync.Mutex
	targetNames []string
}

func (b *detailNamesSync) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	if store == syncT {
		b.mu.Lock()
		b.targetNames = append(b.targetNames, names...)
		b.mu.Unlock()
	}
	return b.fakeSync.Fetch(ctx, store, names, from, until)
}
func TestSyncWindowReadsOnlyMismatches(t *testing.T) {
	f, o, holes := threeSources(t)
	// Keep this request on a single archive: the shared fake returns all
	// archive digests, while the production endpoint returns the serving one.
	for _, store := range f.stores {
		for n, archives := range store.archives {
			store.archives[n] = archives[:1]
		}
	}
	o.from = []string{syncD1, syncD2}
	backend := &detailNamesSync{fakeSync: f}
	s := &syncer{o: o, b: backend}
	names := []string{"a.clean", "b.holes"}
	s.masks.Store("a.clean", uint64(1))
	s.masks.Store("b.holes", uint64(2))
	var row syncRow
	if err := s.checkWindow(context.Background(), &row, names, nil, syncRange{"test", holes[0] - 600, holes[0] + 600}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(backend.targetNames, []string{"b.holes"}) {
		t.Fatalf("target detail names %v", backend.targetNames)
	}
	if len(row.Holes) != 1 || row.Holes[0].Name != "b.holes" || row.Holes[0].Points != 1 {
		t.Fatalf("holes %+v", row.Holes)
	}
}
