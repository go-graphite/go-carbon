package v2read

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	whisper "github.com/go-graphite/go-whisper"
)

// TestBackgroundSealRendersMatchClassic holds a threshold seal open, first
// before and then after its part becomes durable, while new rows arrive. New
// rows must become visible during the seal, and every render must stay
// bit-exact against whisper: the frozen head is never invisible and never
// counted twice. Concurrent readers keep fetching through all seals.
func TestBackgroundSealRendersMatchClassic(t *testing.T) {
	t.Parallel()
	testBackgroundSealRendersMatchClassic(t, 1<<20, 24, time.Hour)
}

// TestBackgroundSealRendersMatchClassicLargeSeals repeats it with the largest
// row cap and a 2 s interval, so the held seals are time-only seals of a
// head that was published many times. Test config only; defaults unchanged.
func TestBackgroundSealRendersMatchClassicLargeSeals(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("waits for time-only seals")
	}
	testBackgroundSealRendersMatchClassic(t, 2<<20, 2<<20, 2*time.Second)
}

// TestBackgroundSealRendersMatchClassicIDKeys repeats it with seals writing
// ID-keyed parts: renders resolve names through the store dictionary.
func TestBackgroundSealRendersMatchClassicIDKeys(t *testing.T) {
	v2part.IDKeys.Store(true)
	t.Cleanup(func() { v2part.IDKeys.Store(false) })
	testBackgroundSealRendersMatchClassic(t, 1<<20, 24, time.Hour, true)
}

func testBackgroundSealRendersMatchClassic(t *testing.T, maxRows, sealRows int, interval time.Duration, idKeys ...bool) {
	rng := rand.New(rand.NewSource(1559))
	now := int64(1794700800)
	clock := now
	retentions := whisper.MustParseRetentionDefs("1s:1d,1m:30d,1h:1y")
	const metrics = 10
	cfg := testConfig()
	cfg.MaxRows = maxRows
	cfg.SealMaxRows = sealRows
	cfg.SealInterval = interval
	var orders sync.Map
	var sealStarts, commits atomic.Int64
	var armed, holdSeal, holdCommit atomic.Bool
	atSeal, atCommit := make(chan struct{}), make(chan struct{})
	releaseSeal, releaseCommit := make(chan struct{}), make(chan struct{})
	hold := func(at, release chan struct{}) {
		close(at)
		select {
		case <-release:
		case <-time.After(20 * time.Second):
		}
	}
	reader, _ := New(1, 1<<30, 64<<20)
	byID := len(idKeys) > 0 && idKeys[0]
	if byID {
		reader.Identities = &v2core.PolicyCatalog{}
	}
	engine, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{
		Identities:     reader.Identities,
		IDKeys:         byID,
		PublishPending: reader.PublishPending,
		Visible: func(_ int, batches []v2core.Batch) {
			for _, b := range batches {
				orders.Store(b.Order, true)
			}
		},
		Stage: func(s string) {
			// Hold only a background (threshold) seal and its commit. A
			// maintenance flush is a synchronous seal on the owner loop; holding
			// it would stop publication by construction.
			switch s {
			case "seal":
				sealStarts.Add(1)
			case "background-seal":
				if armed.Load() && holdSeal.CompareAndSwap(false, true) {
					hold(atSeal, releaseSeal)
				}
			case "commit":
				commits.Add(1)
				if holdSeal.Load() && holdCommit.CompareAndSwap(false, true) {
					hold(atCommit, releaseCommit)
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	type metric struct {
		name    string
		policy  []byte
		classic *whisper.Whisper
		id      uint64
	}
	ms := make([]metric, metrics)
	names := make([]string, metrics)
	for i := range ms {
		method := sealrollup.Method(1 + i%int(sealrollup.First))
		p := v2core.RollupPolicy{Name: partsformat.ID(fmt.Sprintf("bg.%d", i)), Group: "test", Config: sealrollup.Config{Method: method, XFilesFactor: .5}}
		for _, a := range retentions {
			p.Config.Archives = append(p.Config.Archives, sealrollup.Archive{Step: int64(a.SecondsPerPoint()), Count: int64(a.NumberOfPoints())})
		}
		encoded, err := v2core.EncodeRollupPolicy(p)
		if err != nil {
			t.Fatal(err)
		}
		w, err := whisper.CreateWithOptions(filepath.Join(t.TempDir(), "c.wsp"), retentions, whisper.AggregationMethod(method), .5, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		ms[i] = metric{name: string(p.Name), policy: encoded, classic: w, id: admittedID(t, engine, uint64(i+1), string(p.Name), encoded)}
		names[i] = ms[i].name
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, name := range names {
					err := reader.FetchMany(context.Background(), []string{name}, now-7200, now+600, now+600, func(string, *Series) error { return nil })
					if err != nil && !errors.Is(err, ErrNotFound) {
						t.Error(err)
						return
					}
				}
			}
		})
	}
	defer func() { close(stop); readers.Wait() }()

	var cursor, seq uint64
	written := make([]bool, metrics)
	appendOne := func(i int) {
		var rows []v2core.Row
		var pts []*whisper.TimeSeriesPoint
		for k := 1 + rng.Intn(5); k > 0; k-- {
			seq++
			ts := clock - int64(rng.Intn(600))
			v := float64(rng.Intn(1000)) / 8
			rows = append(rows, v2core.Row{ID: ms[i].id, Time: ts, Bits: math.Float64bits(v), Seq: seq})
			pts = append(pts, &whisper.TimeSeriesPoint{Time: int(ts), Value: v})
		}
		if err := ms[i].classic.UpdateMany(pts); err != nil {
			t.Fatal(err)
		}
		cursor++
		written[i] = true
		b := v2core.Batch{ID: ms[i].id, Policy: ms[i].policy, Order: cursor, Clock: clock, Points: rows}
		if err := engine.AppendBatches(0, []v2core.Batch{b}, cursor, false); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { _, ok := orders.Load(b.Order); return ok })
	}
	compare := func(label string) {
		t.Helper()
		for i := range ms {
			if !written[i] {
				continue
			}
			for _, bounds := range [][2]int64{{clock - 600, clock}, {clock - 7200, clock}} {
				want, err := ms[i].classic.Fetch(int(bounds[0]), int(bounds[1]))
				if err != nil {
					t.Fatal(err)
				}
				err = reader.FetchMany(context.Background(), []string{ms[i].name}, bounds[0], bounds[1], clock, func(_ string, got *Series) error {
					if (got == nil) != (want == nil) {
						return fmt.Errorf("nil mismatch")
					}
					if got == nil {
						return nil
					}
					if got.From != int64(want.FromTime()) || got.Until != int64(want.UntilTime()) || got.Step != int64(want.Step()) || len(got.Values) != len(want.Values()) {
						return fmt.Errorf("shape")
					}
					for j, v := range want.Values() {
						if math.Float64bits(v) != math.Float64bits(got.Values[j]) {
							return fmt.Errorf("slot%d got%x want%x", j, math.Float64bits(got.Values[j]), math.Float64bits(v))
						}
					}
					return nil
				})
				if err != nil {
					t.Fatalf("%s %s %v: %v", label, ms[i].name, bounds, err)
				}
			}
		}
	}

	// Rollup owners seal synchronously to establish metadata for each new
	// metric. Install every metric first so the held seal is a threshold seal.
	for i := range ms {
		appendOne(i)
	}
	if err := engine.AppendBatches(0, nil, cursor, true); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return commits.Load() == sealStarts.Load() })
	armed.Store(true)
	// Fill the head until the first threshold seal starts and blocks.
	for started := false; !started; {
		appendOne(rng.Intn(metrics))
		select {
		case <-atSeal:
			started = true
		default:
		}
	}
	// The seal holds the frozen head. New rows still become visible and
	// renders cover the frozen head and the new head exactly once.
	for range 3 {
		clock += 3
		appendOne(rng.Intn(metrics))
		compare("frozen head in flight")
	}
	close(releaseSeal)
	// The part is durable but the owner has not retired the frozen head.
	select {
	case <-atCommit:
	case <-time.After(10 * time.Second):
		t.Fatal("seal did not reach commit")
	}
	for range 3 {
		clock += 3
		appendOne(rng.Intn(metrics))
		compare("frozen part durable")
	}
	close(releaseCommit)
	// Random intake with frequent threshold seals and occasional explicit ones.
	for step := 0; step < 12; step++ {
		clock = now + 100 + int64(step)*7
		for _, i := range rng.Perm(metrics)[:1+rng.Intn(4)] {
			appendOne(i)
		}
		if rng.Intn(6) == 0 {
			if err := engine.AppendBatches(0, nil, cursor, true); err != nil {
				t.Fatal(err)
			}
		}
		compare(fmt.Sprintf("step%d", step))
	}
	if n := sealStarts.Load(); n < 4 {
		t.Fatalf("seals %d, want at least 4", n)
	}
}

// TestBackgroundSealFullHeadKeepsPublishing fills the new head to MaxRows while
// a threshold seal is held. The packet that no longer fits stays queued (a full
// queue then blocks its producer), but the owner keeps publishing: another producer's rows that
// still fit become visible during the seal. Once the seal is published, the
// queued packet is admitted in order. Every render is bit-exact against whisper.
func TestBackgroundSealFullHeadKeepsPublishing(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(1561))
	clock := int64(1794700800)
	retentions := whisper.MustParseRetentionDefs("1s:1d,1m:30d,1h:1y")
	const metrics = 6 // producer 0 writes metrics 0-2, producer 1 metrics 3-5
	cfg := testConfig()
	cfg.Producers = 2
	cfg.SealMaxRows = 16
	cfg.MaxRows = 64
	cfg.SealInterval = time.Hour
	var orders sync.Map
	var sealStarts atomic.Int64
	atSeal, releaseSeal := make(chan struct{}), make(chan struct{})
	reader, _ := New(1, 1<<30, 64<<20)
	engine, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{
		PublishPending: reader.PublishPending,
		Visible: func(_ int, batches []v2core.Batch) {
			for _, b := range batches {
				orders.Store(b.Order, true)
			}
		},
		Stage: func(s string) {
			if s == "seal" && sealStarts.Add(1) == 1 {
				close(atSeal)
				select {
				case <-releaseSeal:
				case <-time.After(60 * time.Second):
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	type metric struct {
		name    string
		policy  []byte
		classic *whisper.Whisper
		id      uint64
	}
	ms := make([]metric, metrics)
	for i := range ms {
		method := sealrollup.Method(1 + i%int(sealrollup.First))
		p := v2core.RollupPolicy{Name: partsformat.ID(fmt.Sprintf("full.%d", i)), Group: "test", Config: sealrollup.Config{Method: method, XFilesFactor: .5}}
		for _, a := range retentions {
			p.Config.Archives = append(p.Config.Archives, sealrollup.Archive{Step: int64(a.SecondsPerPoint()), Count: int64(a.NumberOfPoints())})
		}
		encoded, err := v2core.EncodeRollupPolicy(p)
		if err != nil {
			t.Fatal(err)
		}
		w, err := whisper.CreateWithOptions(filepath.Join(t.TempDir(), "c.wsp"), retentions, whisper.AggregationMethod(method), .5, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		ms[i] = metric{name: string(p.Name), policy: encoded, classic: w, id: admittedID(t, engine, uint64(i+1), string(p.Name), encoded)}
	}
	var order, seq uint64
	cursors := make([]uint64, 2)
	written := make([]bool, metrics)
	// batch builds one original batch of n rows and the whisper points it writes.
	batch := func(producer, n int) (v2core.Batch, int, []*whisper.TimeSeriesPoint) {
		i := producer*3 + rng.Intn(3)
		var rows []v2core.Row
		var pts []*whisper.TimeSeriesPoint
		for range n {
			seq++
			ts := clock - int64(rng.Intn(600))
			v := float64(rng.Intn(1000)) / 8
			rows = append(rows, v2core.Row{ID: ms[i].id, Time: ts, Bits: math.Float64bits(v), Seq: seq})
			pts = append(pts, &whisper.TimeSeriesPoint{Time: int(ts), Value: v})
		}
		order++
		return v2core.Batch{ID: ms[i].id, Policy: ms[i].policy, Order: order, Clock: clock, Points: rows}, i, pts
	}
	classic := func(i int, pts []*whisper.TimeSeriesPoint) {
		if err := ms[i].classic.UpdateMany(pts); err != nil {
			t.Fatal(err)
		}
		written[i] = true
	}
	visible := func(b v2core.Batch) bool { _, ok := orders.Load(b.Order); return ok }
	// appendOne returns once the batch is queued. Each poll admits at most one
	// packet per producer and checks the seal threshold after it, so producers
	// are admitted in their own FIFO order but not in call order across them.
	var outstanding []v2core.Batch
	appendOne := func(producer, n int) v2core.Batch {
		b, i, pts := batch(producer, n)
		classic(i, pts)
		cursors[producer]++
		if err := engine.AppendBatches(producer, []v2core.Batch{b}, cursors[producer], false); err != nil {
			t.Fatal(err)
		}
		outstanding = append(outstanding, b)
		return b
	}
	appendVisible := func(producer, n int) {
		b := appendOne(producer, n)
		await(t, func() bool { return visible(b) })
	}
	compare := func(label string) {
		t.Helper()
		for _, b := range outstanding {
			await(t, func() bool { return visible(b) })
		}
		outstanding = outstanding[:0]
		for i := range ms {
			if !written[i] {
				continue
			}
			for _, bounds := range [][2]int64{{clock - 600, clock}, {clock - 7200, clock}} {
				want, err := ms[i].classic.Fetch(int(bounds[0]), int(bounds[1]))
				if err != nil {
					t.Fatal(err)
				}
				err = reader.FetchMany(context.Background(), []string{ms[i].name}, bounds[0], bounds[1], clock, func(_ string, got *Series) error {
					if (got == nil) != (want == nil) {
						return fmt.Errorf("nil mismatch")
					}
					if got == nil {
						return nil
					}
					if got.From != int64(want.FromTime()) || got.Until != int64(want.UntilTime()) || got.Step != int64(want.Step()) || len(got.Values) != len(want.Values()) {
						return fmt.Errorf("shape")
					}
					for j, v := range want.Values() {
						if math.Float64bits(v) != math.Float64bits(got.Values[j]) {
							return fmt.Errorf("slot%d got%x want%x", j, math.Float64bits(got.Values[j]), math.Float64bits(v))
						}
					}
					return nil
				})
				if err != nil {
					t.Fatalf("%s %s %v: %v", label, ms[i].name, bounds, err)
				}
			}
		}
	}

	// Single-row batches: the 16th row reaches SealMaxRows and freezes the head.
	for range 15 {
		appendOne(0, 1)
	}
	appendVisible(0, 1)
	select {
	case <-atSeal:
	case <-time.After(10 * time.Second):
		t.Fatal("threshold seal did not start")
	}
	// Fill the new head to 60 of 64 rows while the frozen head seals.
	for range 5 {
		clock++
		appendOne(0, 10)
	}
	appendVisible(0, 10)
	compare("new head near MaxRows")
	// Ten more rows do not fit: the packet stays queued, unpopped.
	blocked, blockedMetric, blockedPoints := batch(0, 10)
	cursors[0]++
	if err := engine.AppendBatches(0, []v2core.Batch{blocked}, cursors[0], false); err != nil {
		t.Fatal(err)
	}
	// Publication continues: rows that still fit become visible during the seal.
	for range 4 {
		clock++
		appendVisible(1, 1)
	}
	if visible(blocked) {
		t.Fatal("packet beyond MaxRows admitted while the seal was in flight")
	}
	compare("head full, seal in flight")
	close(releaseSeal)
	await(t, func() bool { return visible(blocked) })
	classic(blockedMetric, blockedPoints)
	compare("queued packet admitted")
	// Keep both producers busy across further threshold seals and an explicit one.
	for step := range 8 {
		clock += 5
		for range rng.Intn(6) {
			appendOne(rng.Intn(2), 1+rng.Intn(8))
		}
		appendVisible(rng.Intn(2), 1+rng.Intn(8))
		if step == 4 {
			cursors[0]++
			if err := engine.AppendBatches(0, nil, cursors[0], true); err != nil {
				t.Fatal(err)
			}
		}
		compare(fmt.Sprintf("step%d", step))
	}
	if n := sealStarts.Load(); n < 3 {
		t.Fatalf("seals %d, want at least 3", n)
	}
}

// admittedID is the engine ID a writer uses for name: its packed dictionary
// ID when the store has a dictionary (core refuses NumericIDs there), else
// numeric.
func admittedID(t *testing.T, e *v2core.Engine, numeric uint64, name string, policy []byte) uint64 {
	t.Helper()
	h, err := e.AdmitIdentity(name, policy)
	if err != nil {
		t.Fatal(err)
	}
	if h == nil {
		return numeric
	}
	return h.ID()
}
