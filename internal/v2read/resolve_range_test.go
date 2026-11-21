package v2read

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	whisper "github.com/go-graphite/go-whisper"
)

type renderedSlots struct {
	from, until, step int64
	bits              []uint64
	occupied          []bool
	missing           bool
}

// fetchBoth renders every name twice in one pinned state: over the
// full-history base and over the range-limited base. Each render must match
// bit for bit, including occupancy, shape and absence.
func fetchBoth(t *testing.T, reader *Reader, names []string, from, until, now int64) int {
	t.Helper()
	collect := func(rangeBase bool) map[string]renderedSlots {
		reader.fullHistory = !rangeBase
		out := map[string]renderedSlots{}
		err := reader.FetchMany(context.Background(), names, from, until, now, func(name string, s *Series) error {
			if s == nil {
				out[name] = renderedSlots{missing: true}
				return nil
			}
			r := renderedSlots{from: s.From, until: s.Until, step: s.Step, occupied: slices.Clone(s.Occupied[:len(s.Values)])}
			for _, v := range s.Values {
				r.bits = append(r.bits, math.Float64bits(v))
			}
			out[name] = r
			return nil
		})
		if err != nil {
			t.Fatalf("rangeBase=%t [%d,%d) now=%d: %v", rangeBase, from, until, now, err)
		}
		return out
	}
	full, limited := collect(false), collect(true)
	for _, name := range names {
		a, b := full[name], limited[name]
		if a.missing != b.missing || a.from != b.from || a.until != b.until || a.step != b.step || !slices.Equal(a.bits, b.bits) || !slices.Equal(a.occupied, b.occupied) {
			t.Fatalf("%s [%d,%d) now=%d: full %+v range %+v", name, from, until, now, a, b)
		}
	}
	return len(names)
}

func TestResolveRangeDifferential(t *testing.T) {
	t.Parallel()
	var compared atomic.Int64
	t.Cleanup(func() { t.Logf("compared %d renders", compared.Load()) })
	for _, def := range []string{"1s:1d,1m:30d,1h:1y,1d:10y", "1m:14d,30m:2y", "10s:6h,1m:7d,10m:30d", "1s:1d"} {
		for method := sealrollup.Average; method <= sealrollup.First; method++ {
			// The full-history oracle decodes every slot per render; one XFF per
			// method keeps every edge (0, partial, all) per schema.
			xff := []float32{0, .5, 1}[int(method)%3]
			t.Run(fmt.Sprintf("%s/%d/%g", def, method, xff), func(t *testing.T) {
				t.Parallel() // each case owns its engine, reader and oracle
				compared.Add(int64(resolveRangeCase(t, def, method, xff, int64(method)*7+int64(xff*10))))
			})
		}
	}
}

func resolveRangeCase(t *testing.T, def string, method sealrollup.Method, xff float32, seed int64) int {
	rng := rand.New(rand.NewSource(seed))
	retentions := whisper.MustParseRetentionDefs(def)
	var archives []sealrollup.Archive
	for _, a := range retentions {
		archives = append(archives, sealrollup.Archive{Step: int64(a.SecondsPerPoint()), Count: int64(a.NumberOfPoints())})
	}
	const names = 4
	encoded := make([][]byte, names)
	list := make([]string, names)
	for i := range encoded {
		list[i] = fmt.Sprintf("diff.m%d", i)
		policy := v2core.RollupPolicy{Name: partsformat.ID(list[i]), Group: "test", Config: sealrollup.Config{Method: method, XFilesFactor: xff, Archives: archives}}
		var err error
		if encoded[i], err = v2core.EncodeRollupPolicy(policy); err != nil {
			t.Fatal(err)
		}
	}
	reader, _ := New(1, 1<<30, 64<<20)
	engine, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishBatchesWithInventory: reader.PublishWithInventory, Now: func() time.Time { return time.Unix(1, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	fine, coarse := archives[0], archives[len(archives)-1]
	advance := fine.Step * fine.Count
	clock := int64(1794700800)
	order := uint64(0)
	specials := []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, 1}
	compared := 0
	for step := 0; step < 8; step++ {
		switch rng.Intn(4) {
		case 0:
			clock += advance + fine.Step*int64(rng.Intn(5)) // wrap the finest ring
		case 1:
			clock += archives[min(1, len(archives)-1)].Step * int64(1+rng.Intn(3))
		default:
			clock += fine.Step * int64(1+rng.Intn(40))
		}
		var batches []v2core.Batch
		for id := range names {
			if step > 0 && rng.Intn(3) == 0 {
				continue
			}
			order++
			var rows []v2core.Row
			for range 1 + rng.Intn(40) {
				var at int64
				switch rng.Intn(6) {
				case 0: // older than the finest retention: lands in a coarse archive
					at = clock - advance - rng.Int63n(min(coarse.Step*coarse.Count-advance, 40*coarse.Step)+1)
				case 1: // a dense run ending at the clock
					at = clock - fine.Step*int64(rng.Intn(120))
				default:
					at = clock - rng.Int63n(min(advance, 400*fine.Step))
				}
				at = max(at, 1)
				bits := math.Float64bits(float64(rng.Intn(33) - 16))
				if rng.Intn(8) == 0 {
					bits = specials[rng.Intn(len(specials))]
				}
				rows = append(rows, v2core.Row{ID: uint64(id + 1), Time: at, Bits: bits, Seq: uint64(len(rows) + 1)})
			}
			batches = append(batches, v2core.Batch{ID: uint64(id + 1), Policy: encoded[id], Order: order, Clock: clock, Points: rows})
		}
		if len(batches) > 0 {
			previous := reader.owners[0].Load()
			if err = engine.AppendBatches(0, batches, order, false); err != nil {
				t.Fatal(err)
			}
			await(t, func() bool { return reader.owners[0].Load() != previous })
		}
		if rng.Intn(3) == 0 {
			if err = engine.AppendBatches(0, nil, order, true); err != nil {
				t.Fatal(err)
			}
		}
		for _, span := range []int64{fine.Step * 120, advance + fine.Step, archives[min(1, len(archives)-1)].Step * 50, coarse.Step * coarse.Count} {
			for _, bounds := range [][2]int64{{clock - span, clock}, {clock, clock}, {clock + 1, clock + 2}, {clock - span, clock - span}, {clock - span - 7, clock - span/2 + 3}} {
				compared += fetchBoth(t, reader, list, bounds[0], bounds[1], clock)
			}
		}
	}
	return compared
}
