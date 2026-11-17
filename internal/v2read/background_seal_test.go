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
	whisper "github.com/go-graphite/go-whisper"
)

// TestBackgroundSealRendersMatchClassic holds a threshold seal open, first
// before and then after its part becomes durable, while new rows arrive. New
// rows must become visible during the seal, and every render must stay
// bit-exact against whisper: the frozen head is never invisible and never
// counted twice. Concurrent readers keep fetching through all seals.
func TestBackgroundSealRendersMatchClassic(t *testing.T) {
	rng := rand.New(rand.NewSource(1559))
	now := int64(1794700800)
	clock := now
	retentions := whisper.MustParseRetentionDefs("1s:1d,1m:30d,1h:1y")
	const metrics = 10
	cfg := testConfig()
	cfg.SealMaxRows = 24
	cfg.SealInterval = time.Hour
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
	engine, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{
		PublishBatchesWithInventory: reader.PublishWithInventory,
		Visible: func(_ int, batches []v2core.Batch) {
			for _, b := range batches {
				orders.Store(b.Order, true)
			}
		},
		Stage: func(s string) {
			switch s {
			case "seal":
				if sealStarts.Add(1) > 0 && armed.Load() && holdSeal.CompareAndSwap(false, true) {
					hold(atSeal, releaseSeal)
				}
			case "commit":
				commits.Add(1)
				if armed.Load() && holdCommit.CompareAndSwap(false, true) {
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
		ms[i] = metric{name: string(p.Name), policy: encoded, classic: w}
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
			rows = append(rows, v2core.Row{ID: uint64(i + 1), Time: ts, Bits: math.Float64bits(v), Seq: seq})
			pts = append(pts, &whisper.TimeSeriesPoint{Time: int(ts), Value: v})
		}
		if err := ms[i].classic.UpdateMany(pts); err != nil {
			t.Fatal(err)
		}
		cursor++
		written[i] = true
		b := v2core.Batch{ID: uint64(i + 1), Policy: ms[i].policy, Order: cursor, Clock: clock, Points: rows}
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
