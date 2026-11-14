package cache

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// Preserve the original metric walk as an independent correctness oracle.
func scanWriteoutStateAt(c *Cache, now time.Time) WriteoutState {
	s := c.settings.Load().(*cacheSettings)
	state := WriteoutState{MaxPoints: s.maxSize, MaxDelay: s.writeoutMaxDelay,
		MeanDelay: time.Duration(float64(s.writeoutMaxDelay) * (1 - s.writeoutSpread/2)),
		Pressure:  c.bytePressure(s), Recovering: c.recovering(s)}
	if pending := c.pending.Load(); pending != nil {
		state.Restored = pending.remaining.Load() == 0
	}
	if !s.writeoutPhase.IsZero() {
		state.MeanDelay = s.writeoutMaxDelay
	}
	for _, shard := range c.data {
		shard.mu.RLock()
		state.Metrics += int64(shard.items.len())
		state.LivePoints += shard.livePoints
		for _, it := range shard.items.all {
			state.Points += int64(it.count())
			if !now.Before(writeoutDeadline(it, s)) {
				state.Eligible++
			}
			if !it.firstArrival.IsZero() {
				state.OldestAge = max(state.OldestAge, now.Sub(s.writeoutWall(it.firstArrival)))
			}
		}
		shard.mu.RUnlock()
	}
	return state
}

func checkWriteoutOracle(t *testing.T, c *Cache) {
	t.Helper()
	now := time.Now()
	got := c.writeoutStateAt(now)
	want := scanWriteoutStateAt(c, now)
	lower := scanWriteoutStateAt(c, now.Add(-time.Second))
	if got.Eligible < lower.Eligible || got.Eligible > want.Eligible {
		t.Fatalf("eligible=%d outside [%d,%d] at %s", got.Eligible, lower.Eligible, want.Eligible, now)
	}
	if got.OldestAge < want.OldestAge || got.OldestAge-want.OldestAge >= time.Second {
		t.Fatalf("oldest age=%s, oracle=%s", got.OldestAge, want.OldestAge)
	}
	got.Eligible, got.OldestAge = want.Eligible, want.OldestAge
	if got != want {
		t.Fatalf("state=%+v, oracle=%+v", got, want)
	}
}

func TestWriteoutStateRandomOracle(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(fmt.Sprint(held), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := New()
				c.SetMaxSize(0)
				c.EnableInputLog()
				if err := c.SetWriteoutBatching(20, time.Minute); err != nil {
					t.Fatal(err)
				}
				if held {
					c.PauseStartupWriteout()
				}
				r := rand.New(rand.NewPCG(17, 83))
				names := make([]string, 2048)
				for i := range names {
					names[i] = fmt.Sprintf("oracle.metric.%d", i)
				}
				var flight []*points.Points
				for step := range 40000 {
					name := names[r.IntN(len(names))]
					switch r.IntN(16) {
					case 0:
						c.Pop(name) // Eviction, including never-written paths.
					case 1:
						if p, ok := c.PopNotConfirmed(name); ok {
							flight = append(flight, p)
						}
					case 2:
						if len(flight) > 0 {
							i := r.IntN(len(flight))
							p := flight[i]
							if r.IntN(2) == 0 {
								c.Requeue(p)
							} else {
								c.Confirm(p)
							}
							flight[i] = flight[len(flight)-1]
							flight = flight[:len(flight)-1]
						}
					case 3:
						c.AddRestored(points.OnePoint(name, 2, 1))
					case 4:
						c.AddRecovered(points.OnePoint(name, 2, 1<<40))
					case 5:
						c.AddPoints([]byte(name), []points.Point{{Value: 1, Timestamp: 3}, {Value: 2, Timestamp: -7}, {Value: 3, Timestamp: 1 << 40}})
					default:
						c.AddPoint([]byte(name), 1, int64(r.IntN(1000)))
					}
					if step%251 == 0 {
						switch r.IntN(9) {
						case 0:
							minPoints := r.IntN(30)
							delay := time.Duration(1+r.IntN(600)) * time.Second
							if minPoints == 0 {
								delay = 0
							}
							if err := c.SetWriteoutBatching(minPoints, delay); err != nil {
								t.Fatal(err)
							}
						case 1:
							if err := c.SetWriteoutSpread(float64(r.IntN(51)) / 100); err != nil {
								t.Fatal(err)
							}
						case 2:
							c.SetWriteoutPacing(r.IntN(2) == 0)
						case 3:
							c.SetWriteoutSparseSpread(r.IntN(2) == 0)
						case 4:
							c.SetWriteoutRestoreSpread(r.IntN(2) == 0)
						case 5:
							c.SetWriteoutStartupSpread(r.IntN(2) == 0)
						case 6:
							c.SetMaxSize(uint64(r.IntN(20000)))
						case 7:
							c.SetMaxBytes(uint64(r.IntN(3)) * (1 << 20))
						case 8:
							s := *c.settings.Load().(*cacheSettings)
							s.writeoutPhase = time.Now().Add(-time.Duration(r.IntN(900)) * time.Second)
							c.settings.Store(&s) // A SIGHUP-style immutable settings swap.
						}
					}
					if held && step == 10000 {
						c.ResumeStartupWriteout(time.Now())
					}
					if step%8 == 0 {
						time.Sleep(time.Duration(r.IntN(2000000000)))
					}
					if step%73 == 0 {
						checkWriteoutOracle(t, c)
					}
				}
				for _, p := range flight {
					c.Requeue(p)
				}
				checkWriteoutOracle(t, c)
				for _, name := range names {
					c.Pop(name)
				}
				checkWriteoutOracle(t, c)
			})
		})
	}
}

func TestWriteoutIndexReloadDuringRehash(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		c.SetMaxSize(0)
		if err := c.SetWriteoutBatching(20, time.Minute); err != nil {
			t.Fatal(err)
		}
		var names []string
		shard := c.data[0]
		for i := 0; len(names) < 1024; i++ {
			name := fmt.Sprintf("rehash.metric.%d", i)
			if c.GetShard(name) == shard {
				names = append(names, name)
			}
		}
		for _, name := range names[:400] {
			c.AddPoint([]byte(name), 1, 1)
		}
		checkWriteoutOracle(t, c)
		if err := c.SetWriteoutSpread(.5); err != nil {
			t.Fatal(err)
		}
		s := c.settings.Load().(*cacheSettings)
		w := &shard.writeout
		b := &writeoutBuild{writeoutIndex: writeoutIndex{settings: s, epoch: w.epoch}, seen: make(map[*item]struct{}), cursor: 64, slots: &shard.items.slots[0]}
		w.building = b
		for _, slot := range shard.items.slots[:64] {
			if it := slot.item; it != nil {
				b.add(it, 1)
				b.seen[it] = struct{}{}
			}
		}
		oldSlots := len(shard.items.slots)
		for i, name := range names {
			c.AddPoint([]byte(name), 1, 1)
			if i%3 == 0 {
				if p, ok := c.PopNotConfirmed(name); ok {
					c.AddPoint([]byte(name), 2, 2)
					c.Requeue(p)
				}
			}
			if i%7 == 0 {
				c.AddRestored(points.OnePoint(name, 3, 3))
			}
			if i%11 == 0 {
				c.Pop(name)
			}
		}
		if len(shard.items.slots) <= oldSlots {
			t.Fatal("fixture did not rehash during rebuild")
		}
		checkWriteoutOracle(t, c)
		time.Sleep(61 * time.Second)
		checkWriteoutOracle(t, c)
	})
}

func TestTimeCountsOracle(t *testing.T) {
	r := rand.New(rand.NewPCG(4, 9))
	var index timeCounts
	want := map[int64]int64{}
	for step := 0; step < 10000; step++ {
		key := int64(r.IntN(20000) - 10000)
		delta := int64(1)
		if want[key] > 0 && r.IntN(2) == 0 {
			delta = -1
		}
		want[key] += delta
		index.add(key, delta)
		if step%31 != 0 {
			continue
		}
		at := int64(r.IntN(22000) - 11000)
		var count int64
		var oldest int64 = 1<<63 - 1
		for k, v := range want {
			if k <= at {
				count += v
			}
			if v > 0 {
				oldest = min(oldest, k)
			}
		}
		if got := index.through(at); got != count {
			t.Fatalf("through(%d)=%d want %d", at, got, count)
		}
		if first, ok := index.oldest(); !ok || first != oldest {
			t.Fatalf("oldest=%d, %t, want %d", first, ok, oldest)
		}
	}
	for key, n := range want {
		if n > 0 {
			index.add(key, -n)
		}
	}
	if index.through(1<<62) != 0 {
		t.Fatal("empty index retained counts")
	}
	index.add(-1, 1)
	index.add(1, 1)
	index.add(1<<60, 1)
	if index.through(1) != 2 {
		t.Fatal("recycled page retained counts")
	}
	// A collided page can subsequently occupy a vacant direct slot. Its
	// seconds then have counts in both paths until removals drain them.
	var split timeCounts
	split.add(0, 1)
	split.add(2048, 2)
	split.add(0, -1)
	split.add(2048, 1)
	if split.through(2047) != 0 || split.through(2048) != 3 {
		t.Fatal("split page prefix")
	}
	split.add(2048, -3)
	if _, ok := split.oldest(); ok {
		t.Fatal("split page did not drain")
	}
}

func TestWriteoutStatePendingRecoveryOracle(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.EnableInputLog()
	if err := c.SetWriteoutBatching(20, time.Minute); err != nil {
		t.Fatal(err)
	}
	c.SetWriteoutRestoreSpread(true)
	c.PauseStartupWriteout()
	if err := c.AttachPendingRecovery(pendingFixtureWithKnown(t, true)); err != nil {
		t.Fatal(err)
	}
	if c.WriteoutState().Restored {
		t.Fatal("unclaimed checkpoint reported a complete restore")
	}
	if err := c.RecoverPending(nil, 0); err != nil {
		t.Fatal(err)
	}
	checkWriteoutOracle(t, c)
	if state := c.WriteoutState(); !state.Recovering || !state.Restored {
		t.Fatal("urgent recovery flag lost")
	}
	c.AddPoint([]byte("metric"), 7, 7)
	checkWriteoutOracle(t, c)
	if p, ok := c.PopNotConfirmed("metric"); ok {
		c.AddPoint([]byte("metric"), 8, 8)
		c.Requeue(p)
	} else {
		t.Fatal("saved metric not claimed")
	}
	checkWriteoutOracle(t, c)
	c.ResumeStartupWriteout(time.Now())
	if state := c.WriteoutState(); state.Recovering || !state.Restored {
		t.Fatal("release lost deferred history or retained urgent recovery", state)
	}
	checkWriteoutOracle(t, c)
	for _, name := range []string{"metric", "other"} {
		p, ok := c.PopNotConfirmed(name)
		if !ok {
			t.Fatal("missing saved metric", name)
		}
		c.Confirm(p)
	}
	if err := c.FinishPendingRecovery(); err != nil {
		t.Fatal(err)
	}
	if c.WriteoutState().Restored {
		t.Fatal("retired source still reports restoration")
	}
	checkWriteoutOracle(t, c)
}

func TestWriteoutStateConcurrentReload(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	if err := c.SetWriteoutBatching(20, time.Minute); err != nil {
		t.Fatal(err)
	}
	names := make([][]byte, 2048)
	for i := range names {
		names[i] = fmt.Appendf(nil, "concurrent.metric.%d", i)
		c.AddPoint(names[i], 1, 1)
	}
	stop := make(chan struct{})
	var workers sync.WaitGroup
	for worker := range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			r := rand.New(rand.NewPCG(uint64(worker), 12))
			for {
				select {
				case <-stop:
					return
				default:
				}
				name := names[r.IntN(len(names))]
				if r.IntN(5) == 0 {
					if p, ok := c.PopNotConfirmed(string(name)); ok {
						c.Requeue(p)
					}
				}
				c.AddPoint(name, 1, 1)
			}
		}()
	}
	var readers sync.WaitGroup
	for range 2 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 25 {
				c.WriteoutState()
			}
		}()
	}
	for i := range 25 {
		if err := c.SetWriteoutBatching(10+i, time.Duration(60+i)*time.Second); err != nil {
			t.Fatal(err)
		}
		if err := c.SetWriteoutSpread(float64(i%3) / 10); err != nil {
			t.Fatal(err)
		}
		c.SetWriteoutPacing(i%2 == 0)
	}
	readers.Wait()
	close(stop)
	workers.Wait()
	checkWriteoutOracle(t, c)
}

// The measurement fixture deliberately edits residence times after insertion.
// Rebuild outside timed regions; this hook is absent from the baseline binary.
func (c *Cache) refreshSamplerFixture() {
	s := c.settings.Load().(*cacheSettings)
	for _, shard := range c.data {
		shard.mu.Lock()
		w := &shard.writeout
		*w = writeoutSummary{writeoutIndex: writeoutIndex{settings: s, epoch: c.writeoutEpoch}, stableSettings: s}
		for _, it := range shard.items.all {
			w.add(it, 1)
		}
		shard.mu.Unlock()
	}
}

func (c *Cache) measureSamplerShard(shard *Shard, now time.Time) WriteoutState {
	w := &shard.writeout
	state := WriteoutState{Points: shard.sizePublished + shard.sizeDelta, Eligible: w.immediate + w.deadlines.through(floorSecond(now.Sub(c.writeoutEpoch)))}
	if first, ok := w.arrivals.oldest(); ok {
		state.OldestAge = max(time.Duration(0), now.Sub(c.writeoutEpoch.Add(secondDuration(first))))
	}
	return state
}
