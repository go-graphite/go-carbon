package cache

import (
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestWriteoutSparseSpreadPlacement(t *testing.T) {
	const maxDelay = 960 * time.Second
	s := &cacheSettings{writeoutMinPoints: 960, writeoutMaxDelay: maxDelay, writeoutSpread: 0.25, writeoutSparseSpread: true}
	const n = 20000
	var quarters [4]int
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("dense.rollup.host-%d.metric_%d", i%97, i) // numbered last
		dense := newItem(name, 0)
		dense.compact.append(1, 3600)
		sparse := newItem(name, 0)
		sparse.compact.append(1, 3600)
		sparse.sparse = true
		d := s.delayFor(sparse)
		if d < 0 || d >= maxDelay {
			t.Fatalf("sparse delay %v of %s outside [0, %v)", d, name, maxDelay)
		}
		if again := newItem(name, 0); again.spread != sparse.spread || again.slot != sparse.slot {
			t.Fatalf("hash of %s not stable", name)
		}
		if dd := s.delayFor(dense); dd < maxDelay*3/4 || dd > maxDelay {
			t.Fatalf("dense delay %v of %s changed", dd, name)
		}
		quarters[int(d*4/maxDelay)]++
	}
	for i, q := range quarters {
		if q < n*22/100 || q > n*28/100 {
			t.Fatalf("sparse placement is not uniform: quarter %d has %d of %d", i, q, n)
		}
	}
}

// sparseName returns a metric name whose sparse deadline is at least share
// of maxDelay after its arrival.
func sparseName(t *testing.T, prefix string, share float64) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		name := fmt.Sprintf("%s.%d", prefix, i)
		if float64(newItem(name, 0).sparseShare())/(1<<16) >= share {
			return name
		}
	}
	t.Fatal("no name with the wanted share")
	return ""
}

// writeOnce pops name as the persister does and confirms the write.
func writeOnce(t *testing.T, c *Cache, name string) {
	t.Helper()
	p, ok := c.PopNotConfirmed(name)
	if !ok {
		t.Fatalf("%s not cached", name)
	}
	c.Confirm(p)
}

func sparseItem(c *Cache, name string) (*item, time.Time) {
	shard := c.GetShard(name)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	values := shard.items.get(name)
	return values, values.firstArrival
}

func TestWriteoutSparseSpreadClassifies(t *testing.T) {
	const maxDelay = 960 * time.Second
	c := batchingCache(t, 960, maxDelay)
	c.SetWriteoutSparseSpread(true)
	s := c.settings.Load().(*cacheSettings)
	hourly := sparseName(t, "rollup.hourly", 0.3)
	minutely := sparseName(t, "rollup.minutely", 0.3)
	const t0 = 1_792_000_800

	// Unknown history: dense, at the full delay.
	c.Add(points.OnePoint(hourly, 1, t0))
	c.Add(points.OnePoint(minutely, 1, t0))
	if values, _ := sparseItem(c, hourly); values.sparse {
		t.Fatal("a metric without written history is sparse")
	}
	writeOnce(t, c, hourly)
	writeOnce(t, c, minutely)

	c.Add(points.OnePoint(hourly, 2, t0+3600))
	c.Add(points.OnePoint(minutely, 2, t0+60))
	values, first := sparseItem(c, hourly)
	if !values.sparse {
		t.Fatal("hourly metric not sparse after a written point an hour older")
	}
	delay := s.delayFor(values)
	if want := time.Duration(float64(maxDelay) * float64(values.sparseShare()) / (1 << 16)); delay != want || delay >= maxDelay*3/4 {
		t.Fatalf("sparse delay %v, want %v", delay, want)
	}
	if dense, _ := sparseItem(c, minutely); dense.sparse || s.delayFor(dense) != maxDelay {
		t.Fatalf("minutely metric sparse=%v delay %v", dense.sparse, s.delayFor(dense))
	}
	q, next := c.makeQueueAt(first.Add(delay - time.Millisecond))
	if q != nil || !next.Equal(first.Add(delay)) {
		t.Fatalf("premature queue or next deadline %v, want %v", next, first.Add(delay))
	}
	q, _ = c.makeQueueAt(first.Add(delay))
	requireMetric(t, q, hourly)
	if time.Now().Before(first.Add(delay)) && c.WriteoutReady(hourly) {
		t.Fatal("ready before its sparse deadline")
	}

	// A point less than the window after the last written one stays dense,
	// even for a metric sparse before.
	writeOnce(t, c, hourly)
	c.Add(points.OnePoint(hourly, 3, t0+3600+959))
	if values, _ := sparseItem(c, hourly); values.sparse {
		t.Fatal("sparse with a written point 959s older")
	}
	writeOnce(t, c, hourly)
	c.Add(points.OnePoint(hourly, 4, t0+3600+959+960))
	if values, _ := sparseItem(c, hourly); !values.sparse {
		t.Fatal("not sparse with a written point 960s older")
	}
}

func TestWriteoutSparseSpreadReturnsDenseWithMorePoints(t *testing.T) {
	const maxDelay = 960 * time.Second
	c := batchingCache(t, 960, maxDelay)
	c.SetWriteoutSparseSpread(true)
	s := c.settings.Load().(*cacheSettings)
	name := sparseName(t, "rollup.changed", 0.2)
	c.Add(points.OnePoint(name, 1, 1_792_000_800))
	writeOnce(t, c, name)
	c.Add(points.OnePoint(name, 1, 1_792_004_400))
	c.Add(points.OnePoint(name, 1, 1_792_004_400)) // a resent duplicate
	values, first := sparseItem(c, name)
	if delay := s.delayFor(values); !values.sparse || delay >= maxDelay {
		t.Fatalf("two points: sparse=%v delay %v", values.sparse, delay)
	}
	c.Add(points.OnePoint(name, 1, 1_792_004_460))
	if delay := s.delayFor(values); delay != maxDelay {
		t.Fatalf("three points keep sparse delay %v", delay)
	}
	if q, next := c.makeQueueAt(first.Add(maxDelay - time.Millisecond)); q != nil || !next.Equal(first.Add(maxDelay)) {
		t.Fatalf("dense again: queue %v, next %v", q, next)
	}
}

func TestWriteoutSparseSpreadOffKeepsDeadlines(t *testing.T) {
	const maxDelay = 960 * time.Second
	for _, spread := range []float64{0, 0.25} {
		c := batchingCache(t, 960, maxDelay)
		if err := c.SetWriteoutSpread(spread); err != nil {
			t.Fatal(err)
		}
		s := c.settings.Load().(*cacheSettings)
		name := sparseName(t, "rollup.off", 0.1)
		c.Add(points.OnePoint(name, 1, 1_792_000_800))
		writeOnce(t, c, name)
		c.Add(points.OnePoint(name, 1, 1_792_004_400))
		values, _ := sparseItem(c, name)
		want := maxDelay - time.Duration(float64(maxDelay)*spread*float64(values.spread)/(1<<16))
		if values.sparse || s.delayFor(values) != want {
			t.Fatalf("spread %v, option off: sparse=%v delay %v, want %v", spread, values.sparse, s.delayFor(values), want)
		}
		for _, shard := range c.data {
			if shard.sparseLast != nil {
				t.Fatal("option off allocated a sparse table")
			}
		}
	}
}

func TestWriteoutSparseSpreadReload(t *testing.T) {
	const maxDelay = 960 * time.Second
	c := batchingCache(t, 960, maxDelay)
	c.SetWriteoutSparseSpread(true)
	name := sparseName(t, "rollup.reload", 0.2)
	c.Add(points.OnePoint(name, 1, 1_792_000_800))
	writeOnce(t, c, name)
	c.Add(points.OnePoint(name, 1, 1_792_004_400))
	values, _ := sparseItem(c, name)
	if !values.sparse || c.settings.Load().(*cacheSettings).delayFor(values) >= maxDelay {
		t.Fatal("not sparse")
	}

	c.SetWriteoutSparseSpread(false)
	if d := c.settings.Load().(*cacheSettings).delayFor(values); d != maxDelay {
		t.Fatalf("disabled, cached sparse item keeps delay %v", d)
	}
	for _, shard := range c.data {
		if shard.sparseLast != nil {
			t.Fatal("disabling kept a sparse table")
		}
	}

	// Re-enabled, history starts over: the next write is remembered again.
	c.SetWriteoutSparseSpread(true)
	writeOnce(t, c, name)
	c.Add(points.OnePoint(name, 1, 1_792_008_000))
	if values, _ := sparseItem(c, name); !values.sparse {
		t.Fatal("not sparse after a write with the option re-enabled")
	}
}

func TestWriteoutSparseTableGrowthKeepsHistory(t *testing.T) {
	var shard Shard
	s := &cacheSettings{writeoutSparseSpread: true}
	items := make([]*item, 1000)
	for i := range items {
		items[i] = newItem(fmt.Sprintf("grow.%d", i), 0)
		items[i].compact.append(1, int64(1000+i))
		shard.noteWritten(items[i], s) // with no items cached: 64 slots
	}
	if len(shard.sparseLast) != 64 {
		t.Fatalf("table has %d slots", len(shard.sparseLast))
	}
	slots := shard.sparseSlots(len(items))
	if len(slots) != 4096 {
		t.Fatalf("grown table has %d slots, want 4096", len(slots))
	}
	for _, it := range items {
		// A metric's slot keeps at least its own last write.
		if got := slots[it.slot&uint32(len(slots)-1)]; int64(got) < it.compact.ts {
			t.Fatalf("%s: slot %d < written %d", it.Metric, got, it.compact.ts)
		}
	}
}

// TestWriteoutSparseSpreadFlattensHourlyPulse simulates a dense-like store:
// dense metrics every 10s or 60s, and hourly rollups that all arrive in the
// first minute of the hour. Without sparse spreading the hourly ones are all
// written in the window's last spread share, a pulse at :12-:17; with it they
// spread over the whole window. Each metric is written exactly as often
// either way.
func TestWriteoutSparseSpreadFlattensHourlyPulse(t *testing.T) {
	t.Parallel()
	const maxDelay, minPoints = 960, 960
	const dense10, dense60, hourly = 3000, 1000, 3000
	const horizon, measureFrom = 6 * 3600, 2 * 3600
	type metric struct {
		name     string
		interval int
		phase    int // arrival second within the interval
		start    int // first arrival
		shard    int
		it       *item
		first    int
		next     int
	}
	type result struct {
		perMinute    []int       // all writes
		hourOfMinute [60]int     // hourly metrics' writes by minute of the hour
		writes       map[int]int // writes by interval
		sparse       int         // hourly writes placed as sparse
	}
	run := func(sparse bool) result {
		s := &cacheSettings{writeoutMinPoints: minPoints, writeoutMaxDelay: maxDelay * time.Second, writeoutSpread: 0.25, writeoutSparseSpread: sparse}
		var shards [16]Shard
		for i := range shards {
			shards[i].items = itemMap{}
		}
		var ms []*metric
		add := func(n int, prefix string, interval int) {
			for i := 0; i < n; i++ {
				m := &metric{name: fmt.Sprintf("%s.%d", prefix, i), interval: interval}
				rnd := uint32(i)*2654435761 + uint32(interval)*40503
				m.phase = int(rnd>>8) % min(interval, 60) // hourly ones arrive at :00-:01
				if interval < 3600 {
					m.start = int(rnd>>4) % maxDelay // desynchronise dense windows
				}
				m.shard = int(newItem(m.name, 0).slot >> 28)
				ms = append(ms, m)
			}
		}
		add(dense10, "dense10", 10)
		add(dense60, "dense60", 60)
		add(hourly, "hourly", 3600)
		r := result{perMinute: make([]int, (horizon-measureFrom)/60), writes: map[int]int{}}
		// An arrival is at most an hour away; deadlines are closer still.
		// Reuse the buckets after that window without changing event order.
		events := make([][]int, 3600+1)
		for i, m := range ms {
			m.next = m.phase
			if m.next < m.start {
				m.next += (m.start - m.next + m.interval - 1) / m.interval * m.interval
			}
			events[m.next%len(events)] = append(events[m.next%len(events)], i)
		}
		for now := 0; now < horizon; now++ {
			due := events[now%len(events)]
			// Preserve the per-second metric order, including sparse-table collisions.
			slices.Sort(due)
			for _, i := range due {
				m := ms[i]
				shard := &shards[m.shard]
				if m.it != nil {
					deadline := m.first + int(s.delayFor(m.it)/time.Second)
					if now >= deadline || m.it.count() >= s.minPointsFor(m.it) {
						if now >= measureFrom {
							r.perMinute[(now-measureFrom)/60]++
							r.writes[m.interval]++
							if m.interval == 3600 {
								r.hourOfMinute[now%3600/60]++
								if m.it.sparse {
									r.sparse++
								}
							}
						}
						shard.items.remove(m.name)
						shard.noteWritten(m.it, s)
						m.it = nil
					}
				}
				if now == m.next {
					if m.it == nil {
						m.it, m.first = newItem(m.name, 0), now
						shard.items.set(m.name, m.it)
						if s.writeoutSparseSpread {
							m.it.sparse = shard.sparseSince(m.it, int64(now), s.writeoutMaxDelay, shard.items.len())
						}
					}
					m.it.compact.append(1, int64(now))
					m.next += m.interval
				}
				next := m.next
				if m.it != nil {
					deadline := m.first + int(s.delayFor(m.it)/time.Second)
					if m.it.count() >= s.minPointsFor(m.it) {
						deadline = now + 1
					}
					next = min(next, max(now+1, deadline))
				}
				if next < horizon {
					events[next%len(events)] = append(events[next%len(events)], i)
				}
			}
			events[now%len(events)] = due[:0]
		}
		return r
	}
	peak := func(perMinute []int) float64 {
		total, most := 0, 0
		for _, n := range perMinute {
			total += n
			most = max(most, n)
		}
		return float64(most) / (float64(total) / float64(len(perMinute)))
	}
	cv := func(perMinute []int) float64 { // coefficient of variation
		total := 0
		for _, n := range perMinute {
			total += n
		}
		mean, sq := float64(total)/float64(len(perMinute)), 0.0
		for _, n := range perMinute {
			sq += (float64(n) - mean) * (float64(n) - mean)
		}
		return math.Sqrt(sq/float64(len(perMinute))) / mean
	}
	baseline := make(chan result, 1)
	go func() { baseline <- run(false) }()
	after := run(true)
	before := <-baseline
	hourlyWrites := before.writes[3600]
	t.Logf("hourly writes: peak/mean per minute of the hour %.2f without, %.2f with sparse spreading (%d of %d sparse)",
		peak(before.hourOfMinute[:]), peak(after.hourOfMinute[:]), after.sparse, hourlyWrites)
	t.Logf("all writes: peak/mean per minute %.2f without, %.2f with; writes by interval %v vs %v",
		peak(before.perMinute), peak(after.perMinute), before.writes, after.writes)
	t.Logf("all writes: CV per minute %.3f without, %.3f with; hourly writes by minute of the hour: CV %.3f without, %.3f with",
		cv(before.perMinute), cv(after.perMinute), cv(before.hourOfMinute[:]), cv(after.hourOfMinute[:]))
	if fmt.Sprint(before.writes) != fmt.Sprint(after.writes) || before.sparse != 0 {
		t.Fatalf("writes changed: %v without, %v with", before.writes, after.writes)
	}
	if after.sparse < hourlyWrites*7/10 {
		t.Fatalf("only %d of %d hourly writes sparse", after.sparse, hourlyWrites)
	}
	if peak(before.hourOfMinute[:]) < 10 || peak(after.hourOfMinute[:]) > 7 || peak(after.perMinute) >= peak(before.perMinute) || cv(after.perMinute) >= cv(before.perMinute) {
		t.Fatal("hourly pulse not flattened")
	}
}
