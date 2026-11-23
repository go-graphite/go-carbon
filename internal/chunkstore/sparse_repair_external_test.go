package chunkstore_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func repairVersion(t testing.TB, s *store.Store, name string) store.TransferVersion {
	t.Helper()
	rows, err := s.ListPage(context.Background(), name, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 || rows[0].Name != name {
		return store.TransferVersion{Name: name}
	}
	m := rows[0]
	return store.TransferVersion{Name: m.Name, ID: m.ID, Generation: m.Generation, Revision: m.Revision}
}

// Mirror verifier missing-timestamp selection, retaining exact donor bits. The
// raw archive domain is intentional: serving-window holes alone cannot promise
// full-import equivalence for older source-only history.
func repairMissing(source, target store.Snapshot, version store.TransferVersion) store.SparseRepair {
	p := store.SparseRepair{Expected: version}
	for ai, a := range source.Archives {
		seen := map[int64]bool{}
		if ai < len(target.Archives) {
			for _, v := range target.Archives[ai].Points {
				if !math.IsNaN(v.Value) {
					seen[v.Timestamp] = true
				}
			}
		}
		work := store.RepairArchive{Archive: ai, Retention: a.Retention}
		for _, v := range a.Points {
			if !seen[v.Timestamp] {
				work.Points = append(work.Points, store.RepairPoint{Timestamp: v.Timestamp, Bits: math.Float64bits(v.Value)})
			}
		}
		p.Archives = append(p.Archives, work)
	}
	return p
}

// Compare the archive read image, explicitly including presence, timestamp and
// every float bit in every canonical circular slot. Physical group rotation is
// an encoding detail and may differ after ordinary generation replacement.
func assertRepairArchives(t testing.TB, want, got store.Snapshot) {
	t.Helper()
	if len(want.Archives) != len(got.Archives) {
		t.Fatal("archive count")
	}
	type cell struct {
		Present   bool
		Timestamp int64
		Bits      uint64
	}
	for ai, a := range want.Archives {
		b := got.Archives[ai]
		if a.Retention != b.Retention {
			t.Fatal("geometry")
		}
		slots := func(a store.Archive) []cell {
			cells := make([]cell, a.Retention.Count)
			for _, v := range a.Points {
				at := v.Timestamp / int64(a.Retention.Step) % int64(a.Retention.Count)
				if cells[at].Present {
					t.Fatal("aliased read")
				}
				cells[at] = cell{true, v.Timestamp, math.Float64bits(v.Value)}
			}
			return cells
		}
		x, y := slots(a), slots(b)
		for i := range x {
			if x[i] != y[i] {
				t.Fatalf("archive %d slot %d: want %+v got %+v", ai, i, x[i], y[i])
			}
		}
	}
}

func TestSparseRepairImportDifferential(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1_700_000_000, 0) }
	source, sparse, ordinary := windowStore(t, true, clock), windowStore(t, true, clock), windowStore(t, true, clock)
	rng := rand.New(rand.NewPCG(81, 93))
	successes, fallbacks := 0, 0
	values := []uint64{0, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 1, 0x3ff4000000000000}
	for n := range 72 {
		name := fmt.Sprintf("repair.random.%03d", n)
		cfg := store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 60, Count: 257}, {Step: 1800, Count: 129}, {Step: 3600, Count: 127}}, AggregationMethod: store.Sum}
		src := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: make([]store.Archive, 3)}
		dst := store.Snapshot{Metadata: src.Metadata, Archives: make([]store.Archive, 3)}
		for ai, r := range cfg.Retentions {
			src.Archives[ai].Retention, dst.Archives[ai].Retention = r, r
			end := clock().Unix() / int64(r.Step) * int64(r.Step)
			for i := 0; i < r.Count; i++ {
				ts := end - int64((r.Count-1-i)*r.Step)
				if rng.IntN(4) != 0 {
					sourceTS := ts
					// Different circular epochs, including retention boundary, expired and
					// future aliases. Each individual image has one owner per slot.
					switch n % 4 {
					case 1:
						sourceTS -= int64(r.Step * r.Count)
					case 2:
						sourceTS += int64(r.Step * r.Count)
					}
					bits := values[rng.IntN(len(values))]
					if i%31 == 0 {
						bits = 0x7ff8000000000042 + uint64(i)
					}
					src.Archives[ai].Points = append(src.Archives[ai].Points, store.Point{Timestamp: sourceTS, Value: math.Float64frombits(bits)})
				}
				if rng.IntN(3) != 0 {
					bits := values[rng.IntN(len(values))]
					if n%9 == 0 && i%23 == 0 {
						bits = 0x7ff8000000000091 + uint64(i)
					}
					dst.Archives[ai].Points = append(dst.Archives[ai].Points, store.Point{Timestamp: ts, Value: math.Float64frombits(bits)})
				}
			}
		}
		seedWindowSnapshot(t, source, src, true, clock)
		missingName := n%11 == 0
		if !missingName {
			seedWindowSnapshot(t, sparse, dst, true, clock)
			seedWindowSnapshot(t, ordinary, dst, true, clock)
		} else {
			dst.Archives = nil
		}
		records, err := source.ExportTransfer(ctx, []string{name})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = ordinary.ImportTransfer(ctx, records); err != nil {
			t.Fatal(err)
		}
		p := repairMissing(src, dst, repairVersion(t, sparse, name))
		result, err := sparse.RepairSparse(ctx, p)
		var unsupported *store.SparseRepairUnsupported
		if errors.As(err, &unsupported) {
			fallbacks++
			if _, err = sparse.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(n, err)
		} else {
			successes++
			if result.Version.ID != p.Expected.ID || result.Version.Generation != p.Expected.Generation {
				t.Fatal("generation replaced")
			}
			// Retry uses a fresh revision and becomes a no-op; target ties preserve bits.
			p.Expected = result.Version
			again, err := sparse.RepairSparse(ctx, p)
			if err != nil || again.GroupsWritten != 0 || again.Version != result.Version {
				t.Fatal("replay", again, err)
			}
		}
		want, err := ordinary.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := sparse.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		assertRepairArchives(t, want, got)
	}
	if successes < 40 || fallbacks < 8 {
		t.Fatal("insufficient path coverage", successes, fallbacks)
	}
	t.Logf("%d sparse, %d ordinary fallbacks; all archive slots bit-exact", successes, fallbacks)
}

// A write after preparation must reject the whole multi-archive patch, with no
// partial visibility and no stale value replacing the newly committed target.
type racingRepairCodec struct {
	*whisperio.ContinuationCodec
	after func()
}

func (c *racingRepairCodec) PlanSparseRepair(ctx context.Context, kv store.ContinuationIO, m store.Metadata, p store.SparseRepair, clock func() time.Time) (store.SparseRepairPlan, error) {
	plan, err := c.ContinuationCodec.PlanSparseRepair(ctx, kv, m, p, clock)
	if c.after != nil {
		f := c.after
		c.after = nil
		f()
	}
	return plan, err
}
func TestSparseRepairConcurrentRevision(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(12000, 0) }
	codec := &racingRepairCodec{ContinuationCodec: &whisperio.ContinuationCodec{}}
	s, err := store.Open("/race", (store.Options{ExactBlocks: true, Now: clock, CacheSize: 1 << 20, MemTableSize: 1 << 20, Continuation: codec}).WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	s.StopBackground()
	defer s.Close()
	cfg := store.MetricConfig{Name: "repair.race", Retentions: []store.Retention{{Step: 10, Count: 512}, {Step: 60, Count: 512}}, AggregationMethod: store.Sum}
	snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: cfg.Retentions[0], Points: []store.Point{{Timestamp: 9000, Value: 1}, {Timestamp: 11000, Value: 2}}}, {Retention: cfg.Retentions[1], Points: []store.Point{{Timestamp: 9000, Value: 3}}}}}
	seedWindowSnapshot(t, s, snap, true, clock)
	p := store.SparseRepair{Expected: repairVersion(t, s, cfg.Name), Archives: []store.RepairArchive{{Archive: 0, Retention: cfg.Retentions[0], Points: []store.RepairPoint{{Timestamp: 9010, Bits: math.Float64bits(8)}}}, {Archive: 1, Retention: cfg.Retentions[1], Points: []store.RepairPoint{{Timestamp: 9060, Bits: math.Float64bits(9)}}}}}
	var after store.Snapshot
	codec.after = func() {
		if err := s.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: 9010, Value: 42}}); err != nil {
			t.Fatal(err)
		}
		after, err = s.Snapshot(ctx, cfg.Name)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.RepairSparse(ctx, p); !errors.Is(err, store.ErrConflict) {
		t.Fatal("expected revision conflict", err)
	}
	got, err := s.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	assertRepairArchives(t, after, got)
	if _, err = s.RepairSparse(ctx, p); !errors.Is(err, store.ErrConflict) {
		t.Fatal("stale caller revision", err)
	}
}

func TestSparseRepairStoreFallbacks(t *testing.T) {
	clock := func() time.Time { return time.Unix(100000, 0) }
	for _, continuation := range []bool{false, true} {
		t.Run(fmt.Sprint(continuation), func(t *testing.T) {
			s := windowStore(t, continuation, clock)
			_, err := s.RepairSparse(context.Background(), store.SparseRepair{Expected: store.TransferVersion{Name: "absent"}})
			var unsupported *store.SparseRepairUnsupported
			if !errors.As(err, &unsupported) {
				t.Fatal("want codec/missing-target fallback", err)
			}
		})
	}
}

func TestSparseRepairValidation(t *testing.T) {
	valid := store.SparseRepair{Expected: store.TransferVersion{Name: "repair.valid"}, Archives: []store.RepairArchive{{Archive: 0, Retention: store.Retention{Step: 60, Count: 1024}, Points: []store.RepairPoint{{Timestamp: 60000, Bits: 1}}}}}
	for _, kind := range []string{"name", "negative-archive", "zero-step", "zero-count", "duplicate-archive", "off-grid", "zero-time", "overflow-time", "unordered"} {
		t.Run(kind, func(t *testing.T) {
			p := valid
			p.Archives = append([]store.RepairArchive(nil), valid.Archives...)
			p.Archives[0].Points = append([]store.RepairPoint(nil), valid.Archives[0].Points...)
			switch kind {
			case "name":
				p.Expected.Name = ""
			case "negative-archive":
				p.Archives[0].Archive = -1
			case "zero-step":
				p.Archives[0].Retention.Step = 0
			case "zero-count":
				p.Archives[0].Retention.Count = 0
			case "duplicate-archive":
				p.Archives = append(p.Archives, p.Archives[0])
			case "off-grid":
				p.Archives[0].Points[0].Timestamp++
			case "zero-time":
				p.Archives[0].Points[0].Timestamp = 0
			case "overflow-time":
				p.Archives[0].Points[0].Timestamp = 1 << 33
			case "unordered":
				p.Archives[0].Points = append(p.Archives[0].Points, p.Archives[0].Points[0])
			}
			if !errors.Is(p.Validate(), store.ErrFormat) {
				t.Fatal("accepted malformed repair")
			}
		})
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
}
