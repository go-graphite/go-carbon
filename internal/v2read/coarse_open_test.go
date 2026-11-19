package v2read

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	whisper "github.com/go-graphite/go-whisper"
)

// TestClassicCoarseOpenWindow compares coarse-archive renders against classic
// Whisper while a rollup window is still open: pending-only points, sealed and
// pending points sharing the open window, a window just closed by the idle
// clock, and a late correction into that closed window. Classic propagates
// every point on write, so the open window must render its partial aggregate.
// The engine and oracle share an injected clock; no step waits for wall time
// or for the engine's throttled head publication.
func TestClassicCoarseOpenWindow(t *testing.T) {
	retentions := whisper.MustParseRetentionDefs("1s:2m,10s:20m,1m:2h")
	for _, method := range []sealrollup.Method{sealrollup.Average, sealrollup.Sum, sealrollup.Min, sealrollup.Max, sealrollup.Last} {
		for _, xff := range []float32{0, .5} {
			t.Run(fmt.Sprintf("%d/%g", method, xff), func(t *testing.T) {
				t.Parallel()
				testClassicCoarseOpenWindow(t, retentions, method, xff)
			})
		}
	}
}

// coarsePublication records the latest engine publication. It keeps no
// snapshot: ownership transfers to the reader, which may close the pin at once
// when it already holds one for the same sealed generation and inventory.
type coarsePublication struct {
	number    uint64
	inventory *v2core.RollupInventory
	pending   int
}

func testClassicCoarseOpenWindow(t *testing.T, retentions whisper.Retentions, method sealrollup.Method, xff float32) {
	const window = int64(1794700800) // aligned to every archive step
	var wall atomic.Int64
	wall.Store(window + 5)
	now := func() time.Time { return time.Unix(wall.Load(), 0) }
	policy := v2core.RollupPolicy{Name: partsformat.ID("coarse.metric"), Group: "test", Config: sealrollup.Config{Method: method, XFilesFactor: xff}}
	for _, a := range retentions {
		policy.Config.Archives = append(policy.Config.Archives, sealrollup.Archive{Step: int64(a.SecondsPerPoint()), Count: int64(a.NumberOfPoints())})
	}
	encoded, err := v2core.EncodeRollupPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	classic, err := whisper.CreateWithOptions(filepath.Join(t.TempDir(), "classic.wsp"), retentions, whisper.AggregationMethod(method), xff, &whisper.Options{Sparse: true, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	defer classic.Close()
	reader, err := New(1, 1<<30, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	// Every engine publication goes through one lock. A pending view below is
	// published from the latest sealed snapshot under that lock, so a
	// concurrent engine seal cannot replace it while it is being compared.
	var mu sync.Mutex
	var last coarsePublication
	publish := func(owner int, snapshot *pebble.Snapshot, pending []v2core.Batch, number uint64, inventory *v2core.RollupInventory) error {
		mu.Lock()
		defer mu.Unlock()
		if err := reader.PublishWithInventory(owner, snapshot, pending, number, inventory); err != nil {
			return err
		}
		last = coarsePublication{number, inventory, len(pending)}
		return nil
	}
	cfg := testConfig()
	cfg.SealInterval = time.Hour // only explicit and clock-driven closure seals
	engine, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{PublishBatchesWithInventory: publish, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	compare := func(stage string) {
		t.Helper()
		clock := wall.Load()
		coarse := map[int64]int{}
		for _, span := range []int64{30, 120, 1200, 7200} {
			for _, bounds := range [][2]int64{{clock - span, clock}, {clock - span, clock - 1}, {window - 1, clock}} {
				want, err := classic.Fetch(int(bounds[0]), int(bounds[1]))
				if err != nil {
					t.Fatal(err)
				}
				err = reader.FetchMany(context.Background(), []string{"coarse.metric"}, bounds[0], bounds[1], clock, func(_ string, got *Series) error {
					if (got == nil) != (want == nil) {
						return fmt.Errorf("nil mismatch got %v", got)
					}
					if got == nil {
						return nil
					}
					if got.From != int64(want.FromTime()) || got.Until != int64(want.UntilTime()) || got.Step != int64(want.Step()) || len(got.Values) != len(want.Values()) {
						return fmt.Errorf("shape got %d %d %d %d want %d %d %d %d", got.From, got.Until, got.Step, len(got.Values), want.FromTime(), want.UntilTime(), want.Step(), len(want.Values()))
					}
					for j, v := range want.Values() {
						if !math.IsNaN(v) {
							coarse[got.Step]++
						}
						if math.Float64bits(v) != math.Float64bits(got.Values[j]) {
							return fmt.Errorf("step%d slot%d (t=%d) got %v want %v", got.Step, j, got.From+int64(j)*got.Step, got.Values[j], v)
						}
					}
					return nil
				})
				if err != nil {
					t.Fatalf("%s range%v: %v", stage, bounds, err)
				}
			}
		}
		// Without an XFF gate every stage has an open or closed value in each
		// coarse archive; the comparison must not pass on empty renders.
		if xff == 0 && (coarse[10] == 0 || coarse[60] == 0) {
			t.Fatalf("%s: no coarse values compared %v", stage, coarse)
		}
	}
	order := uint64(0)
	var unsealed []v2core.Batch
	// Publications carry the engine's registered handle, never raw policy bytes.
	registered, err := engine.RegisterApplicationPolicy(7, "", encoded)
	if err != nil {
		t.Fatal(err)
	}
	write := func(clock int64, times ...int64) {
		t.Helper()
		wall.Store(clock)
		order++
		rows := make([]v2core.Row, len(times))
		ptrs := make([]*whisper.TimeSeriesPoint, len(times))
		for i, ts := range times {
			v := float64(order*10+uint64(i))*1.25 - 7
			rows[i] = v2core.Row{ID: 7, Time: ts, Bits: math.Float64bits(v), Seq: order*100 + uint64(i)}
			ptrs[i] = &whisper.TimeSeriesPoint{Time: int(ts), Value: v}
		}
		if err := classic.UpdateMany(ptrs); err != nil {
			t.Fatal(err)
		}
		b := v2core.Batch{ID: 7, Policy: encoded, Order: order, Clock: clock, Points: rows}
		if err := engine.AppendBatches(0, []v2core.Batch{b}, order, false); err != nil {
			t.Fatal(err)
		}
		b.Policy, b.Registered = nil, registered
		unsealed = append(unsealed, b)
	}
	// pending renders the not yet sealed applications on the latest sealed
	// snapshot, exactly as the engine's next head publication carries them:
	// a fresh pin with the latest generation and inventory. The engine's own
	// throttled pending-only publications are such pins; the reader keeps its
	// sealed resource and closes them, so a pin handed over earlier is never
	// published again.
	pending := func(stage string) {
		t.Helper()
		mu.Lock()
		defer mu.Unlock()
		sealed := reader.owners[0].Load().sealed
		if err := reader.PublishWithInventory(0, engine.DBs()[0].NewSnapshot(), unsealed, last.number, last.inventory); err != nil {
			t.Fatal(err)
		}
		if reader.owners[0].Load().sealed != sealed {
			t.Fatalf("%s: pending view replaced the latest sealed resource", stage)
		}
		compare(stage)
	}
	seal := func(stage string, clock int64) {
		t.Helper()
		wall.Store(clock)
		if err := engine.AppendBatches(0, nil, order, true); err != nil {
			t.Fatal(err)
		}
		unsealed = nil
		mu.Lock()
		empty := last.pending == 0
		mu.Unlock()
		if !empty {
			t.Fatalf("%s: seal left pending applications", stage)
		}
		compare(stage)
	}
	// 1. A new name: points in open 10s and 60s windows, nothing sealed.
	write(window+5, window, window+1, window+2, window+3)
	pending("pending-only")
	// 2. Seal before either window closes: rollups must be projected.
	seal("sealed-open", window+5)
	// 3. Pending replacements and new points in the same open windows.
	write(window+7, window+2, window+4, window+5, window+6)
	pending("sealed+pending")
	seal("sealed-open-2", window+7)
	// 4. The idle clock closes the first 10s window; the 60s window stays open.
	seal("closed-10s", window+12)
	// 5. A late correction into the closed window plus the next open window.
	write(window+14, window+8, window+12, window+13)
	pending("closed+late-pending")
	seal("closed+late-sealed", window+14)
	// 6. The idle clock closes the 60s window, then the next minute opens.
	seal("closed-60s", window+61)
	write(window+62, window+60, window+61)
	pending("next-minute-pending")
	seal("next-minute-sealed", window+62)
}
