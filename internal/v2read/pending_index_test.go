package v2read

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	whisper "github.com/go-graphite/go-whisper"
)

// linearPending is the pre-index forNames selection, kept as the oracle.
func linearPending(pending []v2core.Batch, names []string) ([]v2core.Batch, error) {
	wanted := make(map[string]bool, len(names))
	for _, n := range names {
		wanted[n] = true
	}
	var out []v2core.Batch
	for _, b := range pending {
		if b.Registered != nil {
			if wanted[b.Registered.Name()] {
				out = append(out, b)
			}
			continue
		}
		var p v2core.RollupPolicy
		if err := json.Unmarshal(b.Policy, &p); err != nil {
			return nil, err
		}
		if wanted[string(p.Name)] {
			out = append(out, b)
		}
	}
	return out, nil
}

func indexedPending(pending []v2core.Batch, names []string) ([]v2core.Batch, error) {
	return selectPending(context.Background(), nil, v2core.PendingBatches(pending), names)
}

// samePendingBatch reports whether two descriptors are the same application.
func samePendingBatch(a, b *v2core.Batch) bool {
	if a.ID != b.ID || a.Order != b.Order || a.Clock != b.Clock || a.Registered != b.Registered || len(a.Points) != len(b.Points) || len(a.Policy) != len(b.Policy) {
		return false
	}
	if len(a.Points) > 0 && &a.Points[0] != &b.Points[0] {
		return false
	}
	return len(a.Policy) == 0 || &a.Policy[0] == &b.Policy[0]
}

func testPolicy(t testing.TB, name string) []byte {
	t.Helper()
	p := v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}}}}
	b, err := v2core.EncodeRollupPolicy(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestPendingIndexMatchesLinearScan publishes randomized registered and JSON
// policy cold heads through prefix extensions, unchanged republications and
// seals, and compares every selection with the linear oracle.
func TestPendingIndexMatchesLinearScan(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(18))
	const names = 300
	policies := make([][]byte, names)
	registered := make([]*v2core.ApplicationPolicy, names)
	for i := range policies {
		n := fmt.Sprintf("a.b%d.c%d", i%7, i)
		policies[i] = testPolicy(t, n)
		h, err := v2core.NewApplicationPolicy(n, policies[i])
		if err != nil {
			t.Fatal(err)
		}
		if h.NameHash() != namefp.Hash(n) {
			t.Fatalf("cached name hash differs for %s", n)
		}
		registered[i] = h
	}
	var head []v2core.Batch
	var published []v2core.Batch
	order := uint64(0)
	for step := 0; step < 400; step++ {
		switch r := rng.Intn(20); {
		case r == 0:
			head = head[:0] // seal
		case r < 18:
			for k := rng.Intn(200); k > 0; k-- {
				i := rng.Intn(names)
				order++
				b := v2core.Batch{ID: uint64(i + 1), Order: order, Clock: int64(order), Points: make([]v2core.Row, 1+rng.Intn(3))}
				if rng.Intn(2) == 0 {
					b.Registered = registered[i]
				} else {
					b.Policy = policies[i]
				}
				head = append(head, b)
			}
		}
		pending := slices.Clone(head)
		published = pending
		for q := 0; q < 8; q++ {
			var want []string
			for k := 1 + rng.Intn(4); k > 0; k-- {
				want = append(want, fmt.Sprintf("a.b%d.c%d", rng.Intn(7), rng.Intn(names+20)))
			}
			got, err := indexedPending(pending, want)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := linearPending(pending, want)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(ref) {
				t.Fatalf("step%d %v: got %d batches want %d", step, want, len(got), len(ref))
			}
			for i := range got {
				if !samePendingBatch(&got[i], &ref[i]) {
					t.Fatalf("step%d %v: batch %d differs", step, want, i)
				}
			}
		}
	}
	// A malformed JSON policy fails selection exactly like the linear scan.
	bad := append(slices.Clone(published), v2core.Batch{ID: 1, Order: order + 1, Policy: []byte("{")})
	_, refErr := linearPending(bad, []string{"x"})
	_, gotErr := indexedPending(bad, []string{"x"})
	if refErr == nil || gotErr == nil || refErr.Error() != gotErr.Error() {
		t.Fatalf("errors got %v want %v", gotErr, refErr)
	}
}

// TestPendingIndexRendersMatchClassic writes randomized multi-name histories
// through several owners, unsealed publications and seals, then compares every
// rendered slot bit-for-bit with whisper.
func TestPendingIndexRendersMatchClassic(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(1818))
	now := int64(1794700800)
	clock := now
	retentions := whisper.MustParseRetentionDefs("1s:1d,1m:30d,1h:1y")
	const owners, metrics = 3, 12
	cfg := testConfig()
	cfg.PublishInterval = time.Nanosecond
	cfg.Shards, cfg.Owners = owners, owners
	reader, _ := New(owners, 1<<30, 64<<20)
	engine, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{PublishPending: reader.PublishPending})
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
	for i := range ms {
		method := sealrollup.Method(1 + i%int(sealrollup.First))
		p := v2core.RollupPolicy{Name: partsformat.ID(fmt.Sprintf("m.%d", i)), Group: "test", Config: sealrollup.Config{Method: method, XFilesFactor: .5}}
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
	}
	// Concurrent readers scan every name's pending head while the owners keep
	// appending, publishing and sealing. Publications share the owner's
	// backing array, so the race detector checks no published batch is written.
	names := make([]string, len(ms))
	for i := range ms {
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
	cursor := uint64(0)
	written := make([]bool, metrics)
	seq := uint64(0)
	published := func(id, order uint64) bool {
		for o := range reader.owners {
			if g := reader.owners[o].Load(); g != nil && g.view.Len() > 0 {
				if b, _, _ := g.view.Application(g.view.Len() - 1); b.ID == id && b.Order == order {
					return true
				}
			}
		}
		return false
	}
	for step := 0; step < 30; step++ {
		clock = now + int64(step)*7
		{
			for _, i := range rng.Perm(metrics)[:1+rng.Intn(4)] {
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
				await(t, func() bool { return published(b.ID, b.Order) })
			}
			if rng.Intn(5) == 0 {
				if err := engine.AppendBatches(0, nil, cursor, true); err != nil {
					t.Fatal(err)
				}
			}
		}
		for _, i := range rng.Perm(metrics)[:6] {
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
					t.Fatalf("step%d %s %v: %v", step, ms[i].name, bounds, err)
				}
			}
		}
	}
}

// BenchmarkPendingSelection compares one-name pending selection over a large
// unsealed cold head: the linear scan versus the by-hash scan.
func BenchmarkPendingSelection(b *testing.B) {
	const names, batches = 200000, 1000000
	registered := make([]*v2core.ApplicationPolicy, names)
	for i := range registered {
		n := fmt.Sprintf("bench.metric.name.%d", i)
		h, err := v2core.NewApplicationPolicy(n, testPolicy(b, n))
		if err != nil {
			b.Fatal(err)
		}
		registered[i] = h
	}
	rng := rand.New(rand.NewSource(1))
	pending := make([]v2core.Batch, batches)
	for i := range pending {
		pending[i] = v2core.Batch{ID: uint64(i), Order: uint64(i), Registered: registered[rng.Intn(names)], Points: make([]v2core.Row, 1)}
	}
	want := []string{registered[17].Name()}
	b.Run("linear", func(b *testing.B) {
		for b.Loop() {
			if _, err := linearPending(pending, want); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("select", func(b *testing.B) {
		for b.Loop() {
			if _, err := indexedPending(pending, want); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Duplicate request names must select each cold application once, in arrival
// order, even when another metric shares the old hash bucket.
func TestPendingSelectionDeduplicatesNames(t *testing.T) {
	const first = "bucket.metric.0"
	var second string
	for i := 1; ; i++ {
		second = fmt.Sprintf("bucket.metric.%d", i)
		if namefp.Hash(first)&7 == namefp.Hash(second)&7 {
			break
		}
	}
	pending := []v2core.Batch{
		{ID: 1, Order: 1, Policy: testPolicy(t, first)},
		{ID: 2, Order: 2, Policy: testPolicy(t, second)},
		{ID: 1, Order: 3, Policy: testPolicy(t, first)},
	}
	got, err := selectPending(context.Background(), nil, v2core.PendingBatches(pending), []string{first, first})
	if err != nil || len(got) != 2 || got[0].Order != 1 || got[1].Order != 3 {
		t.Fatalf("duplicate names changed selection: %+v, %v", got, err)
	}
}
