package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestWriteoutSpreadBounds(t *testing.T) {
	const maxDelay, minPoints = 960 * time.Second, 960
	s := &cacheSettings{writeoutMinPoints: minPoints, writeoutMaxDelay: maxDelay, writeoutSpread: 0.25}
	const n = 20000
	var sum time.Duration
	var quarters [4]int
	for i := 0; i < n; i++ {
		it := newItem(fmt.Sprintf("db.host-%d.mysql.status.metric_%d", i%97, i), 0)
		d, m := s.delayFor(it), s.minPointsFor(it)
		if d < maxDelay*3/4 || d > maxDelay {
			t.Fatalf("delay %v of %s outside [%v, %v]", d, it.Metric, maxDelay*3/4, maxDelay)
		}
		if m < minPoints*3/4 || m > minPoints {
			t.Fatalf("min points %d of %s outside [%d, %d]", m, it.Metric, minPoints*3/4, minPoints)
		}
		if again := newItem(it.Metric, 0); s.delayFor(again) != d || s.minPointsFor(again) != m {
			t.Fatalf("spread of %s not stable", it.Metric)
		}
		sum += d
		quarters[min(3, int(float64(maxDelay-d)/float64(maxDelay/16)))]++
	}
	if mean := sum / n; mean < maxDelay*86/100 || mean > maxDelay*89/100 {
		t.Fatalf("mean delay %v, want about %v", mean, maxDelay*7/8)
	}
	for i, q := range quarters {
		if q < n/5 || q > n*3/10 {
			t.Fatalf("spread is not uniform: quarter %d has %d of %d", i, q, n)
		}
	}
	s.writeoutSpread = 0
	if it := newItem("any", 0); s.delayFor(it) != maxDelay || s.minPointsFor(it) != minPoints {
		t.Fatal("spread 0 changed the threshold or delay")
	}
}

func TestWriteoutSpreadValidation(t *testing.T) {
	c := New()
	for _, bad := range []float64{-0.1, 0.51, 2} {
		if err := c.SetWriteoutSpread(bad); err == nil {
			t.Fatalf("spread %v accepted", bad)
		}
	}
	if err := c.SetWriteoutSpread(0.5); err != nil {
		t.Fatal(err)
	}
}

// spreadName returns a metric name whose spread share is at least share.
func spreadName(t *testing.T, s *cacheSettings, share float64) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		name := fmt.Sprintf("sparse.%d", i)
		if s.spreadShare(newItem(name, 0)) >= share {
			return name
		}
	}
	t.Fatal("no name with the wanted spread share")
	return ""
}

func TestWriteoutSpreadDeadline(t *testing.T) {
	const maxDelay = 100 * time.Second
	c := batchingCache(t, 8, maxDelay)
	if err := c.SetWriteoutSpread(0.5); err != nil {
		t.Fatal(err)
	}
	s := c.settings.Load().(*cacheSettings)
	name := spreadName(t, s, 0.2)
	c.Add(points.OnePoint(name, 1, 1))
	shard := c.GetShard(name)
	values := shard.items.get(name)
	first := values.firstArrival
	delay := s.delayFor(values)
	if delay > 80*time.Second {
		t.Fatalf("delay %v not shortened", delay)
	}
	q, deadline := c.makeQueueAt(first.Add(delay - time.Millisecond))
	if q != nil || !deadline.Equal(first.Add(delay)) {
		t.Fatalf("premature queue or deadline %v, want %v", deadline, first.Add(delay))
	}
	q, _ = c.makeQueueAt(first.Add(delay))
	requireMetric(t, q, name)
	// The persister's recheck uses the same deadline.
	if time.Now().Before(first.Add(delay)) && c.WriteoutReady(name) {
		t.Fatal("ready before its spread deadline")
	}
}

func TestWriteoutSpreadThreshold(t *testing.T) {
	c := batchingCache(t, 8, time.Hour)
	if err := c.SetWriteoutSpread(0.5); err != nil {
		t.Fatal(err)
	}
	s := c.settings.Load().(*cacheSettings)
	name := spreadName(t, s, 0.4) // threshold 8-int(8*share) = 5
	for i := 1; ; i++ {
		c.Add(points.OnePoint(name, float64(i), int64(i)))
		shard := c.GetShard(name)
		values := shard.items.get(name)
		threshold := s.minPointsFor(values)
		if threshold >= 8 {
			t.Fatalf("threshold %d not lowered", threshold)
		}
		q, _ := c.makeQueueAt(time.Now())
		if i < threshold {
			if q != nil {
				t.Fatalf("eligible at %d points, below threshold %d", i, threshold)
			}
			continue
		}
		requireMetric(t, q, name)
		if !c.WriteoutReady(name) {
			t.Fatal("WriteoutReady disagrees with the queue at the threshold")
		}
		return
	}
}

// TestWriteoutSpreadFlattensWaves models metrics that all start batching at
// once, as after a restart: slow metrics are written at their deadline, fast
// ones at the point threshold, and the next point after a write lands anywhere
// in the metric's interval. Without a spread both stay in synchronized waves;
// with one, writes spread out within a few cycles.
func TestWriteoutSpreadFlattensWaves(t *testing.T) {
	const metrics = 20000
	const maxDelay, minPoints = 960, 960
	peak := func(spread float64) float64 {
		s := &cacheSettings{writeoutMinPoints: minPoints, writeoutMaxDelay: maxDelay * time.Second, writeoutSpread: spread}
		perMinute := make([]int, 0, 1)
		horizon, measureFrom := 12*maxDelay, 8*maxDelay
		for i := 0; i < metrics; i++ {
			it := newItem(fmt.Sprintf("db.metric.%d", i), 0)
			cycle := int(s.delayFor(it) / time.Second)
			if i%2 == 1 { // 2 points/s: written at the threshold
				cycle = s.minPointsFor(it) / 2
			}
			rnd := uint32(i)*2654435761 + 1
			gap := func() int { rnd = rnd*1664525 + 1013904223; return int(rnd>>8) % 60 }
			for t := i % 60; t < horizon; t += cycle + gap() {
				if at := t + cycle; at >= measureFrom && at < horizon {
					minute := (at - measureFrom) / 60
					for len(perMinute) <= minute {
						perMinute = append(perMinute, 0)
					}
					perMinute[minute]++
				}
			}
		}
		total, most := 0, 0
		for _, n := range perMinute {
			total += n
			most = max(most, n)
		}
		return float64(most) / (float64(total) / float64(len(perMinute)))
	}
	synchronized, spread := peak(0), peak(0.25)
	t.Logf("peak/mean writes per minute: %.1f without spread, %.1f with 0.25", synchronized, spread)
	if synchronized < 2 || spread > 1.5 || spread > 0.6*synchronized {
		t.Fatalf("peak/mean %.1f without spread, %.1f with spread", synchronized, spread)
	}
}
