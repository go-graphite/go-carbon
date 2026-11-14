package cache

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestAdmissionStatsThrottleAcrossInputPaths(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.AddPoint([]byte("cached"), 1, 1)
	var ref PointRef
	var slot any = c.ResolveStreamName([]byte("cached"))
	c.SetThrottle(func(*points.Points, bool) bool { return true })
	c.Add(&points.Points{Metric: "new", Data: make([]points.Point, 3)})
	c.AddPoint([]byte("cached"), 2, 2)
	c.AddPoints([]byte("cached"), make([]points.Point, 4))
	c.AddPointRef(&ref, []byte("cached"), 3, 3)
	c.AddStreamPoint([]byte("cached"), 4, 4, &slot)
	if got := c.AdmissionStats(); got != (AdmissionStats{Received: 11, Throttled: 10}) {
		t.Fatalf("stats %+v, want 10 throttled points", got)
	}
	// Graphite collection must not reset the cumulative accounting.
	c.Stat(func(string, float64) {})
	c.Stat(func(string, float64) {})
	c.AddRecovered(points.OnePoint("recovered", 1, 1))
	if got := c.AdmissionStats(); got.Throttled != 10 || c.Size() != 2 {
		t.Fatalf("stats %+v, cache %d; recovery must bypass live rejection", got, c.Size())
	}
}

func TestAdmissionStatsCountsPointsInRejectedBatches(t *testing.T) {
	t.Run("tags", func(t *testing.T) {
		c := New()
		c.SetTagsEnabled(true)
		c.Add(&points.Points{Metric: "metric;broken", Data: make([]points.Point, 7)})
		if got := c.AdmissionStats(); got != (AdmissionStats{Received: 7, InvalidTags: 7}) || c.Size() != 0 {
			t.Fatalf("stats %+v, cache %d", got, c.Size())
		}
	})
	t.Run("overflow", func(t *testing.T) {
		c := New()
		c.SetMaxSize(1)
		c.Add(&points.Points{Metric: "cached", Data: make([]points.Point, 2)})
		c.Add(&points.Points{Metric: "new", Data: make([]points.Point, 7)})
		c.AddPoint([]byte("cached"), 3, 3)
		c.Stat(func(string, float64) {})
		if got := c.AdmissionStats(); got != (AdmissionStats{Received: 10, Overflow: 8}) || c.Size() != 2 {
			t.Fatalf("stats %+v, cache %d", got, c.Size())
		}
	})
	t.Run("diversion", func(t *testing.T) {
		c := New()
		s := *c.settings.Load().(*cacheSettings)
		s.xlog = func(*points.Points) error { return errors.New("write failed") }
		c.settings.Store(&s)
		c.Add(&points.Points{Metric: "metric", Data: make([]points.Point, 7)})
		s.xlog = func(*points.Points) error { return nil }
		c.settings.Store(&s)
		c.Add(points.OnePoint("metric", 1, 1))
		if got := c.AdmissionStats(); got != (AdmissionStats{Received: 8, XlogWriteFailed: 7, XlogDiverted: 1}) || c.Size() != 0 {
			t.Fatalf("stats %+v, cache %d", got, c.Size())
		}
	})
}

func TestPointsBalanceTracksRetryAndDuplicateTimestamps(t *testing.T) {
	c := New()
	c.Add(points.OnePoint("metric", 1, 10).Add(2, 10))
	c.AddPoints([]byte("metric"), []points.Point{{Value: 3, Timestamp: 10}, {Value: 4, Timestamp: 10}})
	c.AddRecovered(points.OnePoint("replay", 5, 10))
	check := func(committed uint64) {
		t.Helper()
		s := c.AdmissionStats()
		got := s.Received + s.Recovered
		want := committed + uint64(c.Size()+s.InFlight) + s.Throttled + s.Overflow + s.InvalidTags + s.XlogWriteFailed + s.XlogDiverted + c.NameTooLongRejectedPoints()
		if got != want {
			t.Fatalf("input %d != fates %d: %+v, queued %d", got, want, s, c.Size())
		}
	}
	check(0)
	batch, ok := c.PopNotConfirmed("metric")
	if !ok || len(batch.Data) != 4 || c.AdmissionStats().InFlight != 4 {
		t.Fatal("missing in-flight input", batch, c.AdmissionStats())
	}
	check(0)
	c.Requeue(batch)
	check(0)
	if c.AdmissionStats().Received != 4 || c.AdmissionStats().Recovered != 1 {
		t.Fatal("retry counted as input", c.AdmissionStats())
	}
	batch, _ = c.PopNotConfirmed("metric")
	c.Confirm(batch)
	check(4)
	var graphite = map[string]float64{}
	c.Stat(func(name string, value float64) { graphite[name] = value })
	if graphite["received_points_total"] != 4 || graphite["recovered_points_total"] != 1 || graphite["in_flight_points"] != 0 {
		t.Fatal(graphite)
	}
}

func TestPointsBalanceClaimsPendingHistoryOnce(t *testing.T) {
	c := New()
	if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
		t.Fatal(err)
	}
	c.AddPoint([]byte("metric"), 7, 7)
	c.AddPoint([]byte("metric"), 8, 8)
	c.AddPoint([]byte("other"), 9, 9)
	s := c.AdmissionStats()
	if s.Received != 3 || s.Recovered != 5 || c.Size() != 8 {
		t.Fatal(s, c.Size())
	}
}

func TestAdmissionStatsConcurrentPointOracle(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	var want, recovered atomic.Uint64
	var wg sync.WaitGroup
	done := make(chan struct{})
	var scrape sync.WaitGroup
	scrape.Add(1)
	go func() {
		defer scrape.Done()
		for {
			select {
			case <-done:
				return
			default:
				c.AdmissionStats()
			}
		}
	}()
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			name := fmt.Sprintf("oracle.%d", w)
			nb := []byte(name)
			for i := 0; i < 128; i++ {
				n := i%17 + 1
				data := make([]points.Point, n)
				for j := range data {
					data[j] = points.Point{Value: float64(j), Timestamp: int64(i)}
				}
				if i%3 == 2 {
					c.AddRecovered(&points.Points{Metric: name, Data: data})
					recovered.Add(uint64(n))
				} else if i%2 == 0 {
					c.AddPoints(nb, data)
				} else {
					c.Add(&points.Points{Metric: name, Data: data})
				}
				if i%3 != 2 {
					want.Add(uint64(n))
				}
				if i%8 == 7 {
					batch, ok := c.PopNotConfirmed(name)
					if !ok {
						t.Error("missing batch")
						return
					}
					c.Confirm(batch)
				}
			}
			if batch, ok := c.PopNotConfirmed(name); ok {
				c.Confirm(batch)
			}
		}(w)
	}
	wg.Wait()
	close(done)
	scrape.Wait()
	if got := c.AdmissionStats(); got.Received != want.Load() || got.Recovered != recovered.Load() || got.InFlight != 0 || c.Size() != 0 {
		t.Fatalf("got %+v size=%d want received=%d", got, c.Size(), want.Load())
	}
}

func TestAdmissionStatsBusyShardPublication(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	for i := 0; i < 65; i++ {
		c.AddPoint([]byte("publication"), float64(i), int64(i))
	}
	sh := c.GetShard("publication")
	sh.mu.Lock()
	got := c.AdmissionStats()
	sh.mu.Unlock()
	if got.Received != 64 {
		t.Fatalf("busy publication %d want 64", got.Received)
	}
	if got := c.AdmissionStats().Received; got != 65 {
		t.Fatalf("partial publication %d want 65", got)
	}
}
