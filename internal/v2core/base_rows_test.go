package v2core

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Archive records are the only render source. Stores with and without raw base
// rows must hold identical records through randomized multi- and single-archive
// writes, late corrections across a month boundary, restarts that toggle the
// setting on an old store, and merges.
func TestOmitBaseRowsRenderDifferential(t *testing.T) {
	t.Parallel()
	type metric struct {
		id     uint64
		name   string
		policy []byte
		step   int64
	}
	defs := []string{"1m:1d", "10s:6h,1m:1d,1h:30d", "1s:1h,1m:1d,1h:1y", "1m:7d"}
	methods := []sealrollup.Method{sealrollup.Average, sealrollup.Sum, sealrollup.Last, sealrollup.Max, sealrollup.Min, sealrollup.First}
	rng := rand.New(rand.NewSource(1541))
	var metrics []metric
	for i := 0; i < 12; i++ {
		name := fmt.Sprintf("base.rows.m%02d", i)
		_, policy := rollupTestPolicy(t, name, defs[i%len(defs)], methods[i%len(methods)], float32(i%3)*.25)
		step := int64(60)
		if i%len(defs) == 1 {
			step = 10
		} else if i%len(defs) == 2 {
			step = 1
		}
		metrics = append(metrics, metric{uint64(i + 1), name, policy, step})
	}
	cfg := adoptConfig()
	cfg.SealInterval = time.Hour
	roots := []string{t.TempDir(), t.TempDir()}
	open := func(i int, omit bool) *coreEngine {
		c := cfg
		c.KeepBaseRows = !omit
		e, err := openCore(roots[i], c)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	// Engine 0 is the old format until the restart, then reopens without rows.
	es := []*coreEngine{open(0, false), open(1, true)}
	defer func() {
		for _, e := range es {
			_ = e.close()
		}
	}()
	values := []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, math.Float64bits(-3.5)}
	clock := int64(1801958400 - 2*3600) // 2027-02-01 00:00 UTC minus two hours
	order := uint64(0)
	write := func(rounds int) {
		for r := 0; r < rounds; r++ {
			clock += 60 + rng.Int63n(240)
			order++
			var batches []coreBatch
			for _, m := range metrics {
				if rng.Intn(4) == 0 {
					continue
				}
				var rows []row
				for j := 0; j < 1+rng.Intn(8); j++ {
					t := clock - rng.Int63n(120)
					if rng.Intn(6) == 0 {
						t = clock - 600 - rng.Int63n(5*3600) // late correction
					}
					if m.step > 1 && rng.Intn(3) == 0 {
						t -= t % m.step // duplicate slot rewrite
					}
					bits := math.Float64bits(float64(rng.Intn(1000)) / 8)
					if rng.Intn(10) == 0 {
						bits = values[rng.Intn(len(values))]
					}
					rows = append(rows, row{ID: m.id, Time: t, Bits: bits, Seq: uint64(j + 1)})
				}
				batches = append(batches, coreBatch{ID: m.id, Policy: m.policy, Order: order, Clock: clock, Points: rows})
			}
			for _, e := range es {
				if err := e.submitBatches(0, batches, order, true); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	compare := func(stage string) {
		t.Helper()
		var records [2]map[string][]sealrollup.Record
		var metas [2]map[string]RollupMetadata
		for i, e := range es {
			pin := e.owners[0].db.NewSnapshot()
			index, err := NewRollupSnapshot(context.Background(), pin, nil, nil, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			records[i], metas[i] = map[string][]sealrollup.Record{}, map[string]RollupMetadata{}
			var readErr error
			// Render path: each archive's retention window ending at the clock.
			// Expired records that a background merge may or may not have pruned
			// yet are outside every window.
			index.Metadata(func(name string, policy RollupPolicy, meta RollupMetadata) {
				var rs []sealrollup.Record
				for a, archive := range policy.Config.Archives {
					readErr = errors.Join(readErr, index.ScanArchive(context.Background(), name, a, 0, archive.Count, func(r sealrollup.Record) error {
						if r.Sample.Time <= clock-archive.Step*archive.Count || r.Sample.Time > clock {
							return nil
						}
						// Single-archive run bounds depend on background merge cadence,
						// not render values. History and legacy masks keep exact clocks.
						if len(policy.Config.Archives) == 1 && (meta.Version >= 4 || meta.ResetOrder == 0) {
							r.Order, r.Sample.Sequence = 0, 0
						}
						r.Correction = false // placement hint, not a value
						rs = append(rs, r)
						return nil
					}))
				}
				slices.SortFunc(rs, func(a, b sealrollup.Record) int {
					return cmp.Or(cmp.Compare(a.Archive, b.Archive), cmp.Compare(a.Slot, b.Slot))
				})
				records[i][name], metas[i][name] = rs, meta
			})
			_ = pin.Close()
			if readErr != nil {
				t.Fatal(readErr)
			}
		}
		if len(records[0]) != len(metrics) || !reflect.DeepEqual(metas[0], metas[1]) {
			t.Fatalf("%s: metadata differs: %d names %+v vs %+v", stage, len(records[0]), metas[0], metas[1])
		}
		for name, want := range records[0] {
			got := records[1][name]
			for j := range want {
				if j < len(got) && got[j] != want[j] {
					t.Fatalf("%s: %s record%d differs: base rows %+v, omitted %+v", stage, name, j, want[j], got[j])
				}
			}
			if len(got) != len(want) {
				t.Fatalf("%s: %s records: base rows %d, omitted %d", stage, name, len(want), len(got))
			}
		}
	}
	raw := func(i int) int {
		pin := es[i].owners[0].db.NewSnapshot()
		defer pin.Close()
		n := 0
		if err := ScanRaw(pin, func(Row) error { n++; return nil }); err != nil {
			t.Fatal(err)
		}
		return n
	}
	write(30)
	compare("written")
	oldRows := raw(0)
	if oldRows == 0 || raw(1) != 0 {
		t.Fatalf("raw rows: old %d, omitted %d", oldRows, raw(1))
	}
	for i := range es {
		if err := es[i].close(); err != nil {
			t.Fatal(err)
		}
	}
	es = []*coreEngine{open(0, true), open(1, true)}
	compare("reopened")
	write(40) // crosses the month boundary
	compare("after restart")
	if clock <= 1801958400 {
		t.Fatalf("writes ended at %d, before the month boundary", clock)
	}
	if raw(0) != oldRows || raw(1) != 0 {
		t.Fatalf("raw rows after restart: old %d want %d, omitted %d", raw(0), oldRows, raw(1))
	}
	for _, e := range es {
		mergeDrain(t, e.owners[0])
	}
	compare("merged")
}
