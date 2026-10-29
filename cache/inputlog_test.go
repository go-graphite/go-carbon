package cache

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// startInputLog opens the log in dir as a restarting instance would: replay
// into c before input, then start group commit. Cycles run on Sync only.
func startInputLog(t testing.TB, dir string, c *Cache, opts inputlog.Options) (*inputlog.Log, inputlog.ReplayStats) {
	t.Helper()
	if opts.SyncInterval == 0 {
		opts.SyncInterval = time.Hour
	}
	if opts.MaxAge == 0 {
		opts.MaxAge = 1000 * opts.SyncInterval // relogging would repeat points
	}
	l, err := inputlog.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	c.EnableInputLog()
	stats, err := l.Replay(c.AddRecovered)
	if err != nil {
		t.Fatal(err)
	}
	l.Start(c, ShardCount)
	return l, stats
}

func syncLog(t testing.TB, l *inputlog.Log) {
	t.Helper()
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
}

func liveSegments(t testing.TB, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			n++
		}
	}
	return n
}

// persist writes out every listed metric the way the persister does.
func persist(c *Cache, names []string) {
	for _, name := range names {
		if p, ok := c.PopNotConfirmed(name); ok {
			c.Confirm(p)
		}
	}
}

// Points accepted before the last sync survive a crash; once persisted, their
// segments are reused.
func TestInputLogCrashReplay(t *testing.T) {
	dir := t.TempDir()
	opts := inputlog.Options{SegmentSize: 64 << 10}
	c := New()
	c.SetMaxSize(0)
	l, _ := startInputLog(t, dir, c, opts)

	want := map[string][]points.Point{}
	var names []string
	for i := range 30000 {
		name := fmt.Sprintf("crash.test.metric.%04d", i%3000)
		if len(want[name]) == 0 {
			names = append(names, name)
		}
		value, timestamp := float64(i), int64(1700000000+i)
		switch i % 3 {
		case 0:
			c.AddPoint([]byte(name), value, timestamp)
		case 1:
			c.Add(points.OnePoint(name, value, timestamp))
		default:
			c.Add(points.OnePoint(name, value, timestamp).Add(value+0.5, timestamp+1))
			want[name] = append(want[name], points.Point{Value: value, Timestamp: timestamp})
			value, timestamp = value+0.5, timestamp+1
		}
		want[name] = append(want[name], points.Point{Value: value, Timestamp: timestamp})
		if i%10000 == 9999 {
			syncLog(t, l)
		}
	}
	// A batch in flight when the process dies was never confirmed.
	inFlight, _ := c.PopNotConfirmed(names[0])
	if inFlight == nil {
		t.Fatal("missing in-flight batch")
	}
	// Persisted points need not be replayed, but may be.
	persist(c, names[1:100])
	syncLog(t, l)
	c.AddPoint([]byte("crash.test.unsynced"), 1, 1)
	l.Kill() // kill -9: no final sync, no close

	restarted := New()
	restarted.SetMaxSize(0)
	l, stats := startInputLog(t, dir, restarted, opts)
	defer l.Close()
	for _, name := range names {
		if got := restarted.Get(name); !slices.Equal(got, want[name]) {
			t.Fatalf("%s replayed as %v, want %v", name, got, want[name])
		}
	}
	if stats.Corrupt != 0 || stats.Points < 30000 {
		t.Fatalf("replay stats %+v", stats)
	}

	// New input goes to a new segment. Once everything is persisted, the
	// replayed segments are recycled, leaving the active one.
	restarted.AddPoint([]byte("crash.test.after.restart"), 1, 1)
	syncLog(t, l)
	before := liveSegments(t, dir)
	persist(restarted, append(names, "crash.test.unsynced", "crash.test.after.restart"))
	syncLog(t, l)
	if after := liveSegments(t, dir); before < 3 || after != 1 {
		t.Fatalf("live segments %d before persisting, %d after", before, after)
	}
	if low := restarted.LogLowWater(); low == 0 {
		t.Fatal("low-water did not advance")
	}
}

// A failed write keeps the segments of its points, however much input
// follows and is persisted.
func TestInputLogRequeueKeepsSegments(t *testing.T) {
	dir := t.TempDir()
	opts := inputlog.Options{SegmentSize: 16 << 10}
	c := New()
	c.SetMaxSize(0)
	l, _ := startInputLog(t, dir, c, opts)
	c.AddPoint([]byte("requeued.metric"), 42, 4200)
	syncLog(t, l)
	failed, _ := c.PopNotConfirmed("requeued.metric")

	traffic := func(round int) {
		var names []string
		for i := range 2000 {
			name := fmt.Sprintf("other.metric.%04d", i)
			names = append(names, name)
			c.AddPoint([]byte(name), float64(round), int64(round))
		}
		syncLog(t, l)
		persist(c, names)
	}
	for round := range 5 {
		traffic(round)
	}
	c.Requeue(failed)
	c.AddPoint([]byte("requeued.metric"), 43, 4300)
	for round := range 5 {
		traffic(5 + round)
	}
	if low := c.LogLowWater(); low != 0 {
		t.Fatalf("low-water %d with a generation-zero point unpersisted", low)
	}
	l.Kill()

	restarted := New()
	restarted.SetMaxSize(0)
	l, _ = startInputLog(t, dir, restarted, opts)
	defer l.Close()
	want := []points.Point{{Value: 42, Timestamp: 4200}, {Value: 43, Timestamp: 4300}}
	if got := restarted.Get("requeued.metric"); !slices.Equal(got, want) {
		t.Fatalf("requeued metric replayed as %v, want %v", got, want)
	}
}

// A point that stays unpersisted past MaxAge is recorded again, so it no
// longer pins the segments behind it.
func TestInputLogRelogsStragglers(t *testing.T) {
	dir := t.TempDir()
	opts := inputlog.Options{SegmentSize: 16 << 10, SyncInterval: time.Hour, MaxAge: 3 * time.Hour}
	c := New()
	c.SetMaxSize(0)
	l, _ := startInputLog(t, dir, c, opts)
	c.Add(points.OnePoint("stuck.metric", 1, 100).Add(2, 200))
	stuck, _ := c.PopNotConfirmed("stuck.metric") // in flight forever
	for round := range 12 {
		var names []string
		for i := range 2000 {
			name := fmt.Sprintf("other.metric.%04d", i)
			names = append(names, name)
			c.AddPoint([]byte(name), float64(round), int64(round))
		}
		syncLog(t, l)
		persist(c, names)
	}
	if low := c.LogLowWater(); low < 5 {
		t.Fatalf("low-water %d still pinned by the straggler", low)
	}
	if live := liveSegments(t, dir); live > 6 {
		t.Fatalf("%d live segments", live)
	}
	var relogged float64
	l.Stat(func(metric string, value float64) {
		if metric == "relogged" {
			relogged = value
		}
	})
	if relogged < 2 {
		t.Fatalf("relogged %v points", relogged)
	}
	l.Kill()

	restarted := New()
	restarted.SetMaxSize(0)
	l, _ = startInputLog(t, dir, restarted, opts)
	defer l.Close()
	if got := restarted.Get("stuck.metric"); !slices.Equal(got, stuck.Data) {
		t.Fatalf("straggler replayed as %v, want %v", got, stuck.Data)
	}
}

// A straggler that stays queued is re-logged from its compact points, not
// from Data, which a queued item does not hold.
func TestInputLogRelogsQueuedStragglers(t *testing.T) {
	dir := t.TempDir()
	opts := inputlog.Options{SegmentSize: 16 << 10, SyncInterval: time.Hour, MaxAge: 3 * time.Hour}
	c := New()
	c.SetMaxSize(0)
	l, _ := startInputLog(t, dir, c, opts)
	c.Add(points.OnePoint("stuck.metric", 1, 100).Add(2, 200))
	want := c.Get("stuck.metric") // queued forever, held compact
	for round := range 12 {
		var names []string
		for i := range 2000 {
			name := fmt.Sprintf("other.metric.%04d", i)
			names = append(names, name)
			c.AddPoint([]byte(name), float64(round), int64(round))
		}
		syncLog(t, l)
		persist(c, names)
	}
	if low := c.LogLowWater(); low < 5 {
		t.Fatalf("low-water %d still pinned by the straggler", low)
	}
	if live := liveSegments(t, dir); live > 6 {
		t.Fatalf("%d live segments", live)
	}
	var relogged float64
	l.Stat(func(metric string, value float64) {
		if metric == "relogged" {
			relogged = value
		}
	})
	if relogged < 2 {
		t.Fatalf("relogged %v points", relogged)
	}
	l.Kill()

	restarted := New()
	restarted.SetMaxSize(0)
	l, _ = startInputLog(t, dir, restarted, opts)
	defer l.Close()
	if got := restarted.Get("stuck.metric"); !slices.Equal(got, want) {
		t.Fatalf("straggler replayed as %v, want %v", got, want)
	}
}

// Recovered points have their own durable source: they are tracked, so the
// segments of live points appended to them stay, but not recorded again.
func TestInputLogRecoveredTrackedNotRecorded(t *testing.T) {
	c := New()
	c.EnableInputLog()
	c.AddRecovered(points.OnePoint("recovered.metric", 1, 1))
	for shard := range ShardCount {
		if entries := c.TakeLogEntries(shard, 5, nil); len(entries) != 0 {
			t.Fatalf("recovered point recorded: %v", entries)
		}
	}
	c.AddPoint([]byte("recovered.metric"), 2, 2)
	if low := c.LogLowWater(); low != 0 {
		t.Fatalf("low-water %d, want 0", low)
	}
	p, _ := c.PopNotConfirmed("recovered.metric")
	c.Confirm(p)
	if low := c.LogLowWater(); low != 5 {
		t.Fatalf("low-water %d after persisting, want 5", low)
	}
}

// A legacy dump can contain more points in one metric than a shard's entire
// log buffer. Its source is removed after Sync, so replay must retain every
// point even when restore outruns the writer and live input shares the shard.
func TestInputLogRestoredCrashReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		opts := inputlog.Options{SyncInterval: time.Hour, SegmentSize: 64 << 10}
		c := New()
		c.SetMaxSize(0)
		l, _ := startInputLog(t, dir, c, opts)
		defer func() {
			c.DisableInputLog()
			_ = l.Close()
		}()
		const name = "restore.large.metric"
		want := make([]points.Point, 3*maxShardLogEntries+37)
		for i := range want {
			want[i] = points.Point{Timestamp: 1700000000 + int64(i), Value: float64(i) + 0.25}
		}
		liveName := ""
		for i := 0; liveName == ""; i++ {
			candidate := fmt.Sprintf("restore.live.metric.%d", i)
			if c.GetShard(candidate) == c.GetShard(name) {
				liveName = candidate
			}
		}
		done := make(chan struct{})
		go func() {
			c.AddRestored(&points.Points{Metric: name, Data: want})
			close(done)
		}()
		var live []points.Point
	restoreLoop:
		for {
			// Restore fills its bounded log buffer before each explicit drain.
			synctest.Wait()
			select {
			case <-done:
				break restoreLoop
			default:
			}
			p := points.Point{Timestamp: 1800000000 + int64(len(live)), Value: float64(len(live))}
			c.AddPoint([]byte(liveName), p.Value, p.Timestamp)
			live = append(live, p)
			syncLog(t, l)
		}
		if n := c.LogDropped(); n != 0 {
			t.Fatalf("restore overflowed %d input-log points", n)
		}
		syncLog(t, l)
		l.Kill()
		restarted := New()
		restarted.SetMaxSize(0)
		l, _ = startInputLog(t, dir, restarted, opts)
		if got := restarted.Get(name); !slices.Equal(got, want) {
			t.Fatalf("crash replay restored %d points, want %d", len(got), len(want))
		}
		if got := restarted.Get(liveName); !slices.Equal(got, live) {
			t.Fatalf("live input changed during restore: got %v, want %v", got, live)
		}
	})
}

func TestInputLogDisableReleasesRestoredAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		c.SetMaxSize(0)
		c.EnableInputLog()
		defer c.DisableInputLog()
		const name = "restore.waiting.metric"
		for i := range maxShardLogEntries {
			c.AddPoint([]byte(name), float64(i), int64(i))
		}
		done := make(chan struct{})
		go func() {
			c.AddRestored(points.OnePoint(name, 42, 1800000000))
			close(done)
		}()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("restore bypassed a full, undrained input log")
		default:
		}
		c.DisableInputLog()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("disabling the input log did not release restore")
		}
		if got := c.Get(name); len(got) != maxShardLogEntries+1 || got[len(got)-1].Value != 42 {
			t.Fatal("restored point missing after input-log shutdown")
		}
	})
}

// Ingestion with the log enabled must not add a contended lock or counter:
// records go to the buffer of the shard whose lock AddPoint holds anyway.
// "record" isolates that hot path cost: a drainer takes the buffers every
// 10ms and discards them. "log" runs the real log, whose encoding competes
// for the same CPUs at this unrealistic rate.
func BenchmarkAddPointParallel(b *testing.B) {
	for _, mode := range []string{"off", "record", "log"} {
		b.Run("mode="+mode, func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			switch mode {
			case "record":
				c.EnableInputLog()
				stop := drainInputLog(c)
				defer stop()
			case "log":
				l, _ := startInputLog(b, b.TempDir(), c, inputlog.Options{SyncInterval: time.Second})
				defer l.Close()
			}
			var worker atomic.Int64
			b.SetParallelism(4)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := worker.Add(1)
				names := make([][]byte, 256)
				for i := range names {
					names[i] = []byte(fmt.Sprintf("containers.cluster-%02d.namespace.pod-%06d.metric.%03d", id%8, id, i))
				}
				for i := 0; pb.Next(); i++ {
					name := names[i&255]
					c.AddPoint(name, float64(i), int64(i))
					if i&1023 == 1023 {
						// A writeout: the item goes and its successor is created.
						c.Pop(string(name))
					}
				}
			})
		})
	}
}

// drainInputLog takes and discards recorded entries like a log cycle every
// 10ms, without encoding them.
func drainInputLog(c *Cache) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		spare := make([][]inputlog.Entry, ShardCount)
		for gen := uint64(1); ; gen++ {
			select {
			case <-done:
				return
			case <-time.After(10 * time.Millisecond):
			}
			for shard := range spare {
				taken := c.TakeLogEntries(shard, gen, spare[shard])
				clear(taken)
				spare[shard] = taken[:0]
			}
			c.LogLowWater()
		}
	}()
	return func() { close(done); <-finished }
}

// Requeue replaces both batches with a new item. The log follows it the way
// firstArrival and recovery do: the new item keeps the older generation and
// neither old identity stays tracked.
func TestInputLogRequeueMovesTracking(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.EnableInputLog()
	shard := c.GetShard("metric")
	index := slices.Index(c.data, shard)
	c.AddPoint([]byte("metric"), 1, 1)
	c.TakeLogEntries(index, 5, nil)
	failed, _ := c.PopNotConfirmed("metric")
	c.AddPoint([]byte("metric"), 2, 2)
	current := &shard.items.get("metric").Points
	c.Requeue(failed)
	requeued := &shard.items.get("metric").Points
	if gen, ok := shard.log.first[requeued]; !ok || gen != 0 || len(shard.log.first) != 1 {
		t.Fatalf("requeued item tracked %v at %d, %d tracked", ok, gen, len(shard.log.first))
	}
	if _, ok := shard.log.first[failed]; ok {
		t.Fatal("failed batch still tracked")
	}
	if _, ok := shard.log.first[current]; ok {
		t.Fatal("replaced item still tracked")
	}
	if got := c.Get("metric"); len(got) != 2 || got[0].Value != 1 || got[1].Value != 2 {
		t.Fatalf("got %v", got)
	}
	c.Pop("metric")
	if len(shard.log.first) != 0 || len(shard.log.items) != 0 {
		t.Fatalf("tracking left after pop: %v %v", shard.log.first, shard.log.items)
	}
}
