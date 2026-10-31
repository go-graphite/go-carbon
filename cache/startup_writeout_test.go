package cache

import (
	"fmt"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

func TestStartupGateSpreadStartsAtRelease(t *testing.T) {
	for _, restored := range []bool{false, true} {
		t.Run(fmt.Sprint(restored), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := batchingCache(t, 1, 10*time.Second)
				c.SetMaxSize(0)
				c.SetWriteoutStartupSpread(!restored)
				c.SetWriteoutRestoreSpread(restored)
				if restored {
					c.EnableInputLog()
				}
				c.PauseStartupWriteout()
				for i := range 128 {
					p := &points.Points{Metric: fmt.Sprintf("gated.%d", i), Data: []points.Point{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 1}}}
					if restored {
						c.AddRestored(p)
					} else {
						c.Add(p)
					}
				}
				// Cache one paused deadline summary, then pass the entire old window.
				q, _ := c.makeQueueAt(time.Now())
				if len(q) != 0 {
					t.Fatal("held points became eligible")
				}
				time.Sleep(20 * time.Second)
				released := time.Now()
				c.ResumeStartupWriteout(released)
				c.ResumeStartupWriteout(released.Add(time.Hour)) // replacement must not shift twice
				var quarters [4]int
				for i := range 128 {
					name := fmt.Sprintf("gated.%d", i)
					d := deadlineOf(c, name)
					if d.Before(released) || !d.Before(released.Add(10*time.Second)) {
						t.Fatalf("%s outside release window: %v", name, d.Sub(released))
					}
					quarters[int(d.Sub(released)/(2500*time.Millisecond))]++
				}
				for _, n := range quarters {
					if n < 16 || n > 48 {
						t.Fatal("release spread not distributed", quarters)
					}
				}
				// Release invalidates region summaries without walking the items.
				q, _ = c.makeQueueAt(released.Add(10 * time.Second))
				if len(q) != 128 {
					t.Fatalf("stale paused summary hid %d points", 128-len(q))
				}
				for len(q) > 0 {
					name := <-q
					p, ok := c.PopNotConfirmed(name)
					if !ok || !reflect.DeepEqual(p.Data, []points.Point{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 1}}) {
						t.Fatal("release changed points/order", p)
					}
					c.Confirm(p)
				}
				if !c.IsEmpty() {
					t.Fatal("points retained after confirmation")
				}
			})
		})
	}
}

func TestStartupGateBacklogDrainsAcrossFullWindow(t *testing.T) {
	for _, startupSpread := range []bool{false, true} {
		t.Run(fmt.Sprint(startupSpread), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const metrics = 3072
				const window = 960 * time.Second
				c := batchingCache(t, 2000, window)
				c.SetMaxSize(0)
				c.SetWriteoutStartupSpread(startupSpread)
				c.SetWriteoutRestoreSpread(startupSpread)
				if err := c.SetWriteoutSpread(.25); err != nil {
					t.Fatal(err)
				}
				c.PauseStartupWriteout()
				want := []points.Point{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 1}}
				for i := range metrics {
					c.Add(&points.Points{Metric: fmt.Sprintf("held.%d", i), Data: want})
				}
				// Populate a paused region summary, then hold longer than a window.
				c.makeQueueAt(time.Now())
				time.Sleep(2 * window)
				released := time.Now()
				c.ResumeStartupWriteout(released)
				checkFresh := func() {
					t.Helper()
					arrived := time.Now()
					c.Add(&points.Points{Metric: "fresh", Data: want})
					deadline := deadlineOf(c, "fresh")
					if deadline.Before(arrived.Add(window*3/4)) || deadline.After(arrived.Add(window)) {
						t.Fatal("post-release arrival did not retain its normal deadline", deadline.Sub(arrived))
					}
					p, _ := c.PopNotConfirmed("fresh")
					c.Confirm(p)
				}
				checkFresh()
				abort, done := make(chan bool), make(chan struct{})
				defer func() { close(abort); <-done }()
				var minutes [16]int
				var seconds [960]int
				var first time.Duration
				written := 0
				go func() {
					defer close(done)
					for {
						name := c.WriteoutQueue().Get(abort)
						if name == "" {
							return
						}
						p, ok := c.PopNotConfirmed(name)
						if !ok || !reflect.DeepEqual(p.Data, want) {
							t.Error("drain changed points/order", p)
							return
						}
						elapsed := time.Since(released)
						if written == 0 {
							first = elapsed
						}
						minutes[min(int(elapsed/time.Minute), len(minutes)-1)]++
						seconds[min(int(elapsed/time.Second), len(seconds)-1)]++
						written++
						c.Confirm(p)
					}
				}()
				time.Sleep(5 * time.Second)
				synctest.Wait()
				if written == 0 {
					t.Fatal("held backlog did not begin within five seconds of release")
				}
				checkFresh()
				// The queue coalesces rebuilds for 100 ms; allow its last wake-up.
				time.Sleep(window + time.Second - 5*time.Second)
				synctest.Wait()
				if written != metrics || !c.IsEmpty() {
					t.Fatalf("full window left %d metrics unwritten", metrics-written)
				}
				steady := metrics / len(minutes)
				for minute, n := range minutes {
					if n < steady*8/10 || n > steady*12/10 {
						t.Fatalf("minute %d wrote %d, steady %d; all minutes %v", minute, n, steady, minutes)
					}
				}
				rolling, peak := 0, 0
				for second, n := range seconds {
					rolling += n
					if second >= 60 {
						rolling -= seconds[second-60]
					}
					peak = max(peak, rolling)
				}
				if peak > steady*12/10 {
					t.Fatalf("rolling minute peaked at %d, steady %d", peak, steady)
				}
				t.Logf("first write %v; writes/minute %v (steady %d, rolling peak %d)", first, minutes, steady, peak)
			})
		})
	}
}

func TestStartupGateRecoveryRetainsSourceAndRate(t *testing.T) {
	c := batchingCache(t, 8, time.Hour)
	c.SetMaxSize(100)
	c.SetWriteoutRestoreSpread(true)
	c.PauseStartupWriteout()
	if err := c.AttachPendingRecovery(pendingFixtureWithKnown(t, true)); err != nil {
		t.Fatal(err)
	}
	c.AddPoint([]byte("metric"), 4, 3)
	want := c.Get("metric")
	if !c.Backlogged() {
		t.Fatal("urgent restoration lost its rate before the gate")
	}
	if err := c.FinishPendingRecovery(); err == nil {
		t.Fatal("gate retired unwritten recovery")
	}
	c.ResumeStartupWriteout(time.Now())
	if c.Backlogged() || !reflect.DeepEqual(c.Get("metric"), want) {
		t.Fatal("release lifted rate or changed data")
	}
	for i := range 76 {
		c.Add(points.OnePoint(fmt.Sprintf("pressure.%d", i), 1, 1))
	}
	if !c.Backlogged() || !c.WriteoutReady("metric") {
		t.Fatal("point pressure did not lift rate and batching")
	}
	c.SetMaxSize(0)
	c.SetMaxBytes(uint64(c.EstimatedBytes()))
	if !c.Backlogged() || !c.WriteoutReady("metric") {
		t.Fatal("byte pressure did not lift rate and batching")
	}
}

func TestStartupGateConcurrentIngestRelease(t *testing.T) {
	c := batchingCache(t, 1000, time.Hour)
	c.SetWriteoutStartupSpread(true)
	c.PauseStartupWriteout()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 256 {
			c.AddPoint([]byte("concurrent"), float64(i), int64(i))
		}
	}()
	c.ResumeStartupWriteout(time.Now())
	<-done
	got := c.Get("concurrent")
	for i, p := range got {
		if p.Value != float64(i) || p.Timestamp != int64(i) {
			t.Fatal("concurrent release changed order", i, p)
		}
	}
	if len(got) != 256 {
		t.Fatal("concurrent points lost", len(got))
	}
}

func TestStartupGateSpreadRespectsBatchingReload(t *testing.T) {
	c := batchingCache(t, 1, time.Hour)
	c.SetWriteoutStartupSpread(true)
	c.SetWriteoutRestoreSpread(true)
	c.PauseStartupWriteout()
	c.Add(points.OnePoint("reload", 1, 1))
	c.ResumeStartupWriteout(time.Now())
	if c.WriteoutReady("reload") {
		t.Fatal("startup phase was not held")
	}
	c.SetWriteoutRestoreSpread(false)
	c.SetWriteoutStartupSpread(false)
	if c.WriteoutReady("reload") {
		t.Fatal("ordinary spread settings disabled the gated phase")
	}
	if err := c.SetWriteoutBatching(0, 0); err != nil {
		t.Fatal(err)
	}
	if !c.WriteoutReady("reload") {
		t.Fatal("disabling batching retained the gated phase")
	}
}

func TestStartupGateOrderedRestoreStaysDurable(t *testing.T) {
	c := batchingCache(t, 1, time.Hour)
	c.SetWriteoutRestoreSpread(true)
	c.SetWriteoutStartupSpread(true)
	c.PauseStartupWriteout()
	dir := t.TempDir()
	log, _ := startInputLog(t, dir, c, inputlog.Options{SyncInterval: time.Millisecond})
	want := points.Points{Metric: "ordered", Data: []points.Point{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 1}}}
	c.AddRestoredBeforeInput(&want)
	if !c.WriteoutReady(want.Metric) {
		t.Fatal("ordered restore waited for intake readiness")
	}
	syncLog(t, log)
	log.Kill()
	restarted := New()
	defer restarted.Stop()
	replayed, _ := startInputLog(t, dir, restarted, inputlog.Options{})
	defer replayed.Close()
	if got := restarted.Get(want.Metric); !reflect.DeepEqual(got, want.Data) {
		t.Fatal("urgent dump points were not durable", got)
	}
}
