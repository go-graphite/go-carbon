package cache

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

const restoreSpreadDelay = 100 * time.Second

func restoreSpreadCache(t *testing.T, spread bool) *Cache {
	t.Helper()
	c := New()
	c.SetMaxSize(0)
	if err := c.SetWriteoutBatching(1000, restoreSpreadDelay); err != nil {
		t.Fatal(err)
	}
	c.SetWriteoutRestoreSpread(spread)
	return c
}

// deadlineOf returns name's writeout deadline; zero means eligible now.
func deadlineOf(c *Cache, name string) time.Time {
	shard := c.GetShard(name)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	values := shard.items.get(name)
	if values == nil {
		return time.Time{}
	}
	return writeoutDeadline(values, c.settings.Load().(*cacheSettings))
}

func eligibleNow(c *Cache) map[string]bool {
	q, _ := c.makeQueueAt(time.Now())
	out := map[string]bool{}
	for {
		select {
		case m := <-q:
			out[m] = true
		default:
			return out
		}
	}
}

func TestRestoreSpreadDumpPoints(t *testing.T) {
	t.Parallel()
	names := make([]string, 200)
	for i := range names {
		names[i] = fmt.Sprintf("restore.spread.%d", i)
	}
	t.Run("spread with input log", func(t *testing.T) {
		c := restoreSpreadCache(t, true)
		l, _ := startInputLog(t, t.TempDir(), c, inputlog.Options{})
		defer func() { _ = l.Close() }()
		before := time.Now()
		for _, name := range names {
			c.AddRestored(&points.Points{Metric: name, Data: []points.Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}}})
		}
		after := time.Now()
		var late int
		for _, name := range names {
			d := deadlineOf(c, name)
			if d.IsZero() || d.Before(before) || d.After(after.Add(restoreSpreadDelay)) {
				t.Fatalf("%s: deadline %v outside [%v, %v]", name, d, before, after.Add(restoreSpreadDelay))
			}
			if d.After(after.Add(restoreSpreadDelay / 2)) {
				late++
			}
		}
		if late < len(names)/4 || late > len(names)*3/4 {
			t.Fatalf("deadlines not spread: %d of %d in the second half", late, len(names))
		}
		if n := len(eligibleNow(c)); n > len(names)/20 {
			t.Fatalf("%d of %d restored metrics eligible at once", n, len(names))
		}
		// A live point joins the restored item and keeps its deadline: one write.
		d := deadlineOf(c, names[0])
		c.AddPoint([]byte(names[0]), 3, 3)
		if got := c.Get(names[0]); len(got) != 3 {
			t.Fatalf("live point not merged: %v", got)
		}
		if !deadlineOf(c, names[0]).Equal(d) {
			t.Fatal("live point changed the restored deadline")
		}
	})
	t.Run("off", func(t *testing.T) {
		c := restoreSpreadCache(t, false)
		l, _ := startInputLog(t, t.TempDir(), c, inputlog.Options{})
		defer func() { _ = l.Close() }()
		for _, name := range names {
			c.AddRestored(points.OnePoint(name, 1, 1))
		}
		if n := len(eligibleNow(c)); n != len(names) {
			t.Fatalf("%d of %d restored metrics eligible with the spread off", n, len(names))
		}
	})
	t.Run("no input log", func(t *testing.T) {
		c := restoreSpreadCache(t, true)
		for _, name := range names {
			c.AddRestored(points.OnePoint(name, 1, 1))
		}
		if n := len(eligibleNow(c)); n != len(names) {
			t.Fatalf("%d of %d restored metrics eligible without an input log", n, len(names))
		}
	})
	t.Run("recovered", func(t *testing.T) {
		c := restoreSpreadCache(t, true)
		l, _ := startInputLog(t, t.TempDir(), c, inputlog.Options{})
		defer func() { _ = l.Close() }()
		for _, name := range names {
			c.AddRecovered(points.OnePoint(name, 1, 1))
		}
		if n := len(eligibleNow(c)); n != len(names) {
			t.Fatalf("%d of %d recovered metrics eligible", n, len(names))
		}
	})
}

// A crash before the spread deadlines loses nothing: the input log holds the
// restored points until they are written.
func TestRestoreSpreadCrashBeforeDeadline(t *testing.T) {
	dir := t.TempDir()
	opts := inputlog.Options{SyncInterval: 10 * time.Millisecond}
	c := restoreSpreadCache(t, true)
	l, _ := startInputLog(t, dir, c, opts)
	want := map[string][]points.Point{}
	for i := range 50 {
		name := fmt.Sprintf("restore.crash.%d", i)
		pts := []points.Point{{Timestamp: 1700000000, Value: float64(i)}, {Timestamp: 1700000060, Value: float64(i) + .5}}
		c.AddRestored(&points.Points{Metric: name, Data: pts})
		want[name] = pts
	}
	syncLog(t, l) // the dump file may go now
	if n := len(eligibleNow(c)); n == len(want) {
		t.Fatal("restored points were not deferred")
	}
	l.Kill()
	restarted := New()
	restarted.SetMaxSize(0)
	l, _ = startInputLog(t, dir, restarted, opts)
	defer func() { _ = l.Close() }()
	for name, pts := range want {
		if got := restarted.Get(name); !slices.Equal(got, pts) {
			t.Fatalf("%s after crash: %v, want %v", name, got, pts)
		}
	}
}

// pendingFixtureKnown is pendingFixture with "metric" in the saved read index
// and "other" new.
func pendingFixtureKnown(t testing.TB) *recovery.Bundle {
	t.Helper()
	bundle := pendingFixtureWithKnown(t, true)
	if bundle.NewNameCount() != 1 {
		t.Fatalf("fixture has %d new names, want 1", bundle.NewNameCount())
	}
	return bundle
}

func TestRestoreSpreadPendingClaims(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name               string
		spread, prepared   bool
		deferMetric        bool
		urgentAfterClaimed uint64
	}{
		{"spread", true, true, true, 1},
		{"off", false, true, false, 2},
		{"unprepared names", true, false, false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := restoreSpreadCache(t, tc.spread)
			attach := c.AttachPendingRecovery
			if tc.prepared {
				attach = c.AttachPendingRecoveryWithPreparedNames
			}
			if err := attach(pendingFixtureKnown(t)); err != nil {
				t.Fatal(err)
			}
			before := time.Now()
			if err := c.RecoverPending(make(chan struct{}), 0); err != nil {
				t.Fatal(err)
			}
			if c.PendingOutstanding() != 2 || c.PendingUrgentOutstanding() != tc.urgentAfterClaimed {
				t.Fatalf("outstanding %d, urgent %d; want 2, %d", c.PendingOutstanding(), c.PendingUrgentOutstanding(), tc.urgentAfterClaimed)
			}
			d := deadlineOf(c, "metric")
			if tc.deferMetric {
				if d.Before(before) || d.After(time.Now().Add(restoreSpreadDelay)) {
					t.Fatalf("known metric deadline %v outside one max delay", d)
				}
			} else if !d.IsZero() {
				t.Fatalf("metric deferred to %v", d)
			}
			if !deadlineOf(c, "other").IsZero() {
				t.Fatal("new metric was deferred; the first scan waits for it")
			}
			eligible := eligibleNow(c)
			if !eligible["other"] || eligible["metric"] == tc.deferMetric {
				t.Fatalf("eligible %v", eligible)
			}
			// A live point merges into the claimed item: one write.
			c.AddPoint([]byte("metric"), 9, 9)
			if got := c.Get("metric"); len(got) != 4 {
				t.Fatalf("metric points %v", got)
			}
			persist(c, []string{"other"})
			urgent := uint64(1)
			if tc.deferMetric {
				urgent = 0 // the first scan need not wait for the deferred metric
			}
			if c.PendingUrgentOutstanding() != urgent {
				t.Fatalf("urgent %d after the new metric was written, want %d", c.PendingUrgentOutstanding(), urgent)
			}
			if tc.deferMetric && c.PendingOutstanding() != 1 {
				t.Fatalf("outstanding %d: sources must wait for the deferred metric", c.PendingOutstanding())
			}
			persist(c, []string{"metric"})
			if c.PendingOutstanding() != 0 {
				t.Fatalf("outstanding %d after every write", c.PendingOutstanding())
			}
			if err := c.FinishPendingRecovery(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestRestoreSpreadHalvesFirstCycleWrites models a restart: every metric has
// restored points and then a live point every minute at its own phase. Written
// at once, a restored metric is written again when its first live batch is
// due; spread, its restored points wait and join that batch.
func TestRestoreSpreadHalvesFirstCycleWrites(t *testing.T) {
	const metrics, maxDelay = 10000, 960
	s := &cacheSettings{writeoutMinPoints: 960, writeoutMaxDelay: maxDelay * time.Second, writeoutSpread: 0.25}
	window := maxDelay + 60
	count := func(spread bool) float64 {
		writes := 0
		for i := range metrics {
			it := newItem(fmt.Sprintf("db.metric.%d", i), 0)
			delay := int(s.delayFor(it) / time.Second)
			phase := i % 60
			if spread {
				// Restored deadline in [0, maxDelay); live points join it.
				at := int(s.restoreArrival(it, time.Unix(0, 0)).Unix()) + delay
				writes++
				// The next item starts with the next live point after that.
				next := at + (phase-at%60+60)%60
				if next+delay < window {
					writes++
				}
				continue
			}
			writes++ // the restored points, at once
			if phase+delay < window {
				writes++ // the first live batch
			}
		}
		return float64(writes) / metrics
	}
	now, later := count(false), count(true)
	t.Logf("writes per metric in the first cycle: %.2f at once, %.2f spread", now, later)
	if now < 1.8 || later > 1.3 {
		t.Fatalf("writes per metric %.2f at once, %.2f spread", now, later)
	}
}
