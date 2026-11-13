package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type fakePaceClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakePaceClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakePaceClock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func samePacedKV(a, b mapContinuationIO) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !bytes.Equal(v, w) {
			return false
		}
	}
	return true
}

// A deferred write publishes nothing and keeps its points for a retry; after
// the sweep repairs the archive the retried write lands, and every archive is
// bit-identical to the unpaced inline repair followed by the write.
func TestClassicReanchorPacedMatchesInline(t *testing.T) {
	ctx := context.Background()
	inline := &ContinuationCodec{}
	clock := &fakePaceClock{t: time.Unix(1_000_000, 0)}
	deferredFixtures := 0
	for _, fx := range classicDiffFixtures(t) {
		if fx.next == nil {
			continue
		}
		want, got := cloneContinuationIO(fx.kv), cloneContinuationIO(fx.kv)
		errWant := inline.Update(ctx, want, fx.m, fx.next, -1, classicTestClock, nil)
		pacer := newClassicReanchorPacer(1e-9, time.Hour, clock.now)
		pacer.tokens = 0
		paced := &ContinuationCodec{Reanchor: pacer}
		var err error
		for round := 0; round <= len(fx.m.Retentions)+1; round++ {
			before := cloneContinuationIO(got)
			err = paced.Update(ctx, got, fx.m, fx.next, -1, classicTestClock, nil)
			if !errors.Is(err, store.ErrReanchorDeferred) {
				break
			}
			if !samePacedKV(before, got) {
				t.Fatalf("%s: deferred write published state", fx.name)
			}
			if !pacer.Holding(fx.m.Name) {
				t.Fatalf("%s: deferred metric not held for the sweep", fx.name)
			}
			// The sweep's repair: an update without points under the grant.
			if err := paced.Update(WithClassicReanchorGrant(ctx), got, fx.m, nil, -1, classicTestClock, nil); err != nil {
				t.Fatalf("%s: sweep repair: %v", fx.name, err)
			}
			if pacer.Holding(fx.m.Name) {
				t.Fatalf("%s: swept metric still held", fx.name)
			}
			deferredFixtures++
		}
		if (errWant == nil) != (err == nil) {
			t.Fatalf("%s: errors differ: inline %v, paced %v", fx.name, errWant, err)
		}
		if errWant != nil {
			continue
		}
		requireSameClassicSnapshots(t, inline, want, got, fx.m)
		if st := pacer.Stats(); st.Backlog != 0 || st.Swept != st.Deferred {
			t.Fatalf("%s: stats %+v", fx.name, st)
		}
	}
	if deferredFixtures == 0 {
		t.Fatal("no fixture exercised deferral")
	}
	t.Logf("deferred and swept %d repairs", deferredFixtures)
}

// Within the budget the write repairs inline exactly as before, and a metric
// held past max-defer is repaired by its next write whatever the budget.
func TestClassicReanchorPacerInlineAndAged(t *testing.T) {
	ctx := context.Background()
	kv, snap := mixedClassicFixture(t)
	next := []store.Point{{Timestamp: 999980, Value: 9}}
	want := cloneContinuationIO(kv)
	if err := (&ContinuationCodec{}).Update(ctx, want, snap.Metadata, next, -1, classicTestClock, nil); err != nil {
		t.Fatal(err)
	}
	clock := &fakePaceClock{t: time.Unix(1_000_000, 0)}
	pacer := newClassicReanchorPacer(1, 10*time.Minute, clock.now)
	got := cloneContinuationIO(kv)
	if err := (&ContinuationCodec{Reanchor: pacer}).Update(ctx, got, snap.Metadata, next, -1, classicTestClock, nil); err != nil {
		t.Fatal(err)
	}
	requireSameClassicSnapshots(t, &ContinuationCodec{}, want, got, snap.Metadata)
	if st := pacer.Stats(); st.Inline != 1 || st.Deferred != 0 {
		t.Fatalf("stats %+v", st)
	}

	pacer = newClassicReanchorPacer(1e-9, 10*time.Minute, clock.now)
	pacer.tokens = 0
	codec := &ContinuationCodec{Reanchor: pacer}
	got = cloneContinuationIO(kv)
	if err := codec.Update(ctx, got, snap.Metadata, next, -1, classicTestClock, nil); !errors.Is(err, store.ErrReanchorDeferred) {
		t.Fatalf("want deferral, got %v", err)
	}
	clock.add(10 * time.Minute)
	if pacer.Holding(snap.Metadata.Name) {
		t.Fatal("aged metric still held from writeout")
	}
	if err := codec.Update(ctx, got, snap.Metadata, next, -1, classicTestClock, nil); err != nil {
		t.Fatal(err)
	}
	requireSameClassicSnapshots(t, &ContinuationCodec{}, want, got, snap.Metadata)
	if st := pacer.Stats(); st.Aged != 1 || st.Inline != 1 || st.Backlog != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// Repairs granted over T seconds never exceed rate*T + burst.
func TestClassicReanchorPacerBudget(t *testing.T) {
	clock := &fakePaceClock{t: time.Unix(1_000_000, 0)}
	const rate, seconds = 20.0, 5
	pacer := newClassicReanchorPacer(rate, time.Hour, clock.now)
	ctx := context.Background()
	granted := 0
	for i := 0; i < seconds*1000; i++ {
		if pacer.admit(ctx, fmt.Sprintf("m%d", i), 0) {
			granted++
		}
		clock.add(time.Millisecond)
	}
	if max := int(rate*seconds + pacer.burst); granted > max || granted < int(rate*seconds) {
		t.Fatalf("granted %d, want %d..%d", granted, int(rate*seconds), max)
	}
	if st := pacer.Stats(); int(st.Deferred)+granted != seconds*1000 || st.Backlog != int64(st.Deferred) {
		t.Fatalf("stats %+v granted %d", st, granted)
	}
}

// The sweep drains the backlog oldest first, within the shared budget.
func TestClassicReanchorSweepDrains(t *testing.T) {
	pacer := newClassicReanchorPacer(2000, time.Hour, time.Now)
	pacer.tokens = 0
	ctx := context.Background()
	const n = 200
	for i := 0; i < n; i++ {
		if pacer.admit(ctx, fmt.Sprintf("m%03d", i), 0) {
			t.Fatal("admitted with an empty bucket")
		}
	}
	var mu sync.Mutex
	var order []string
	sweepCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		pacer.Sweep(sweepCtx, nil, func(ctx context.Context, name string) error {
			if !classicReanchorGranted(ctx) {
				t.Error("sweep repair without grant")
			}
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			pacer.repaired(ctx, name)
			return nil
		})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for pacer.Stats().Backlog != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(start)
	cancel()
	<-done
	st := pacer.Stats()
	if st.Backlog != 0 || st.Swept != n || len(order) != n {
		t.Fatalf("stats %+v, swept %d", st, len(order))
	}
	for i, name := range order {
		if name != fmt.Sprintf("m%03d", i) {
			t.Fatalf("sweep order %d: %s", i, name)
		}
	}
	// 200 repairs at 2000/s from an empty bucket take at least ~100 ms.
	if elapsed < 80*time.Millisecond {
		t.Fatalf("sweep finished in %v, faster than the budget", elapsed)
	}
}

func TestClassicReanchorPacerDisabled(t *testing.T) {
	if NewClassicReanchorPacer(0, time.Hour) != nil {
		t.Fatal("rate 0 must disable pacing")
	}
	var p *ClassicReanchorPacer
	if p.Holding("x") || !p.admit(context.Background(), "x", 0) {
		t.Fatal("nil pacer must repair inline")
	}
}
