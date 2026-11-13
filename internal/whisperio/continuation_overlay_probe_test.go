package whisperio

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A referenced store holds classic metrics a native combine merged from two
// stores, as a relayouted host's are: every combined group is an alias onto
// immutable payloads, and each later write of a group first retires (probes)
// its alias. Its history and payloads are compacted into the bottom level,
// and an hour of persister-sized rounds, which retire the aliases they touch,
// is spread over the levels above it.
const (
	referencedProbeMetrics = 300
	referencedProbeHistory = 6 * 360 // 6 h of 10 s slots, split between both sides
	referencedProbeStart   = int64(1_792_368_000)
)

var referencedProbeRetentions = []store.Retention{{Step: 10, Count: 60480}, {Step: 60, Count: 20160}, {Step: 3600, Count: 8760}}

func referencedProbeName(i int) string { return fmt.Sprintf("probe.referenced.%04d", i) }

type referencedProbeTemplate struct {
	once sync.Once
	dir  string
	now  int64
	err  error
}

var referencedProbeTemplates [2]referencedProbeTemplate

func buildReferencedProbeStore(root string, buckets bool) (int64, error) {
	ctx := context.Background()
	start := referencedProbeStart
	stamp := start + referencedProbeHistory*10
	clock := func() time.Time { return time.Unix(stamp+1, 0) }
	dir, options := levelsBenchOptions(root, buckets, clock, false)
	target, err := store.Open(dir, options)
	if err != nil {
		return 0, err
	}
	defer func() {
		if target != nil {
			_ = target.Close()
		}
	}()
	sourceOptions := store.Options{ExactBlocks: true, TimeBuckets: buckets, Continuation: &ContinuationCodec{ReuseBlockKeys: true}, Now: clock,
		CacheSize: 32 << 20, MemTableSize: 16 << 20}
	source, err := store.Open("/db", sourceOptions.WithFS(vfs.NewMem()))
	if err != nil {
		return 0, err
	}
	defer source.Close()
	source.StopBackground()
	names := make([]string, referencedProbeMetrics)
	for i := range names {
		names[i] = referencedProbeName(i)
		cfg := store.MetricConfig{Name: names[i], Retentions: referencedProbeRetentions, AggregationMethod: store.Average, XFilesFactor: .5}
		for side, s := range []*store.Store{source, target} {
			if _, err := s.Create(ctx, cfg); err != nil {
				return 0, err
			}
			points := make([]store.Point, 0, referencedProbeHistory/2)
			for j := side; j < referencedProbeHistory; j += 2 {
				points = append(points, store.Point{Timestamp: start + int64(j)*10, Value: float64((i + j) % 97)})
			}
			if err := s.UpdateMany(ctx, names[i], points); err != nil {
				return 0, err
			}
		}
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		return 0, err
	}
	var wire bytes.Buffer
	err = pin.Write(ctx, names, &wire)
	pin.Close()
	if err != nil {
		return 0, err
	}
	r := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: int64(len(names)), Names: names, Combine: true}
	if _, err := target.NativeImport(ctx, r, &wire); err != nil {
		return 0, err
	}
	if err := target.Compact(); err != nil {
		return 0, err
	}
	// An hour of 60-point rounds, one flush each, pushed through the levels.
	input := make([]store.Point, 60)
	for round := 0; round < 60; round++ {
		stamp += 600
		for i, name := range names {
			for j := range input {
				input[j] = store.Point{Timestamp: stamp - int64(60-j)*10, Value: float64((i + j + round) % 89)}
			}
			if err := target.UpdateMany(ctx, name, input); err != nil {
				return 0, err
			}
		}
		if err := target.Flush(); err != nil {
			return 0, err
		}
	}
	for target.Stats().CompactionsInProgress > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	err = target.Close()
	target = nil
	return stamp, err
}

func (t *referencedProbeTemplate) template(buckets bool) (string, int64, error) {
	t.once.Do(func() {
		t.dir = filepath.Join(os.TempDir(), fmt.Sprintf("go-carbon-referenced-probe-v1-buckets-%v", buckets))
		marker := filepath.Join(t.dir, "built")
		if data, err := os.ReadFile(marker); err == nil {
			_, t.err = fmt.Sscan(string(data), &t.now)
			return
		}
		if t.err = os.RemoveAll(t.dir); t.err != nil {
			return
		}
		if t.now, t.err = buildReferencedProbeStore(t.dir, buckets); t.err == nil {
			t.err = os.WriteFile(marker, []byte(fmt.Sprint(t.now)), 0600)
		}
	})
	return filepath.Join(t.dir, "shared"), t.now, t.err
}

// openReferencedProbe opens a private copy of a referenced store whose
// memtable fills between two updates of a metric, as on a busy host.
func openReferencedProbe(tb testing.TB, buckets bool) (*store.Store, *int64) {
	tb.Helper()
	index := 0
	if buckets {
		index = 1
	}
	template, now, err := referencedProbeTemplates[index].template(buckets)
	if err != nil {
		tb.Fatal(err)
	}
	clock := now
	dir, options := levelsBenchOptions(tb.TempDir(), buckets, func() time.Time { return time.Unix(clock+1, 0) }, true)
	options.CacheSize, options.MemTableSize, options.L0CompactionThreshold = 64<<20, 4<<20, 8
	if err := os.CopyFS(dir, os.DirFS(template)); err != nil {
		tb.Fatal(err)
	}
	db, err := store.Open(dir, options)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Error(err)
		}
	})
	return db, &clock
}

// referencedProbeInput fills input with update i: a live head of 10 s points
// and, every third update, restored backlog points inside the combined
// history, whose groups still hold their aliases.
func referencedProbeInput(input []store.Point, i int, now int64) []store.Point {
	input = input[:0]
	n := cap(input)
	if i%3 == 0 {
		for j := 0; j < n/4; j++ {
			slot := ((i/3)*37)%(referencedProbeHistory-n) + j
			input = append(input, store.Point{Timestamp: referencedProbeStart + int64(slot)*10, Value: float64(i - j)})
		}
	}
	for j := len(input); j < n; j++ {
		input = append(input, store.Point{Timestamp: now - int64(n-j)*10, Value: float64(i + j)})
	}
	return input
}

func setOverlayProbes(tb testing.TB, on bool) {
	tb.Helper()
	saved := overlayProbes
	overlayProbes = on
	tb.Cleanup(func() { overlayProbes = saved })
}

// TestOverlayProbeDifferential runs the same updates over two copies of a
// referenced store, retiring aliases through fresh lookups (the oracle) and
// through the overlay's shared-iterator probes. Every key and value each
// metric persists, and every point it serves, must be identical.
//
// The race pass skips it: pebble's race build records a stack trace on every
// table iterator it opens, and the oracle's fresh lookups open one in every
// level per probe, so the run takes minutes and checks nothing more. The
// differential is sequential; the plain pass runs it in full.
func TestOverlayProbeDifferential(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a multi-level store")
	}
	if raceDetector {
		t.Skip("pebble's race build captures a stack per table iterator; the plain pass runs this oracle")
	}
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("buckets=%v", buckets), func(t *testing.T) {
			ctx := context.Background()
			var exports [2][]store.TransferMetric
			var snaps [2][]store.Snapshot
			names := make([]string, referencedProbeMetrics)
			for i := range names {
				names[i] = referencedProbeName(i)
			}
			for run, probes := range []bool{false, true} {
				func() {
					setOverlayProbes(t, probes)
					db, now := openReferencedProbe(t, buckets)
					input := make([]store.Point, 0, 120)
					for i := 0; i < 3*referencedProbeMetrics; i++ {
						if i%referencedProbeMetrics == 0 {
							*now += 1200
						}
						if err := db.UpdateMany(ctx, names[i%referencedProbeMetrics], referencedProbeInput(input, i, *now)); err != nil {
							t.Fatal(err)
						}
					}
					records, err := db.ExportTransfer(ctx, names)
					if err != nil {
						t.Fatal(err)
					}
					exports[run] = records
					for _, name := range names {
						s, err := db.Snapshot(ctx, name)
						if err != nil {
							t.Fatal(err)
						}
						snaps[run] = append(snaps[run], s)
					}
				}()
			}
			aliases := 0
			if len(exports[0]) != len(exports[1]) {
				t.Fatal("metric count differs", len(exports[0]), len(exports[1]))
			}
			for m := range exports[0] {
				a, b := exports[0][m], exports[1][m]
				if a.Config.Name != b.Config.Name || a.Version != b.Version || len(a.Keys) != len(b.Keys) {
					t.Fatalf("%s: version or key count differs: %v/%d vs %v/%d", a.Config.Name, a.Version, len(a.Keys), b.Version, len(b.Keys))
				}
				sortKV(a.Keys)
				sortKV(b.Keys)
				for k := range a.Keys {
					if !bytes.Equal(a.Keys[k].Key, b.Keys[k].Key) || !bytes.Equal(a.Keys[k].Value, b.Keys[k].Value) {
						t.Fatalf("%s: key %x differs", a.Config.Name, a.Keys[k].Key)
					}
					if len(a.Keys[k].Key) == 20 && a.Keys[k].Key[9] == 'a' {
						aliases++
					}
				}
				if err := store.EqualSnapshots(snaps[0][m], snaps[1][m]); err != nil {
					t.Fatal(names[m], err)
				}
			}
			if aliases == 0 {
				t.Fatal("fixture has no remaining aliases")
			}
		})
	}
}

func sortKV(keys []store.RelayoutKV) {
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i].Key, keys[j].Key) < 0 })
}

// BenchmarkReferencedUpdateProbes updates each metric of a referenced store in
// turn with 120 points, a quarter of them restored backlog every third update,
// retiring aliases through fresh lookups or through shared-iterator probes.
func BenchmarkReferencedUpdateProbes(b *testing.B) {
	for _, buckets := range []bool{false, true} {
		for _, probes := range []bool{false, true} {
			b.Run(fmt.Sprintf("buckets=%v/shared-probes=%v", buckets, probes), func(b *testing.B) {
				setOverlayProbes(b, probes)
				ctx := context.Background()
				db, now := openReferencedProbe(b, buckets)
				input := make([]store.Point, 0, 120)
				before := db.Stats()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if i%referencedProbeMetrics == 0 {
						*now += 1200
					}
					if err := db.UpdateMany(ctx, referencedProbeName(i%referencedProbeMetrics), referencedProbeInput(input, i, *now)); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				after := db.Stats()
				n := float64(b.N)
				b.ReportMetric(float64(after.TableCacheHits+after.TableCacheMisses-before.TableCacheHits-before.TableCacheMisses)/n, "tables/op")
				b.ReportMetric(float64(after.CacheHits+after.CacheMisses-before.CacheHits-before.CacheMisses)/n, "blocks/op")
			})
		}
	}
}
