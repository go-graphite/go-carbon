package cache

import (
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// Forced/pressure commits must not make a one-hour age expire in seconds.
func TestInputLogAgeUsesElapsedTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		l, _ := startInputLog(t, t.TempDir(), c, inputlog.Options{SyncInterval: time.Second, MaxAge: time.Hour})
		defer l.Kill()
		c.Add(points.OnePoint("age.straggler", 1, 100))
		for range 3602 {
			syncLog(t, l)
		}
		if got := l.Metrics().Relogged; got != 0 {
			t.Fatalf("one hour expired after %d rapid cycles: relogged=%d", l.Metrics().Cycles, got)
		}
		time.Sleep(time.Hour)
		syncLog(t, l)
		if got := l.Metrics().Relogged; got == 0 {
			t.Fatal("elapsed hour did not relog live straggler")
		}
		t.Logf("rapid_cycles=3602 relogged_before_hour=0 relogged_after_hour=%d", l.Metrics().Relogged)
	})
}

func TestInputLogRelogFullBatchAdmission(t *testing.T) {
	for _, flight := range []bool{false, true} {
		for _, size := range []int{2, maxShardLogEntries + 1} {
			t.Run(fmt.Sprintf("flight=%t/points=%d", flight, size), func(t *testing.T) {
				c := New()
				c.SetMaxSize(0)
				c.EnableInputLog()
				name := "relog.capacity"
				data := make([]points.Point, size)
				for i := range data {
					data[i] = points.Point{Value: float64(i), Timestamp: int64(i)}
				}
				c.Add(&points.Points{Metric: name, Data: data})
				if flight {
					c.PopNotConfirmed(name)
				}
				sh := c.GetShard(name)
				// Original data was drained at generation zero. Fill the next staging
				// buffer so only a prefix (or none) of this relog would fit.
				sh.log.entries.Reset()
				sh.log.gen = 2
				sh.log.entries.AppendPoints(inputlog.NewStreamRef("live.filler"), make([]points.Point, maxShardLogEntries-1))
				if got := c.RelogBefore(1, size); got != 0 {
					t.Fatalf("reported %d relogged with room for only one of %d points", got, size)
				}
				if sh.log.lowWater() != 0 || sh.log.entries.Len() != maxShardLogEntries-1 || c.LogDropped() != uint64(max(size-maxShardLogEntries, 0)) {
					t.Fatal("failed relog changed ownership, staging, or drop accounting")
				}
				sh.log.entries.Reset()
				got := c.RelogBefore(1, size)
				if size <= maxShardLogEntries {
					if got != size || sh.log.lowWater() != 2 || sh.log.entries.Len() != size {
						t.Fatal("retry did not log complete batch", got)
					}
					for i, p := range sh.log.entries.Points {
						if p != data[i] {
							t.Fatal("relog changed point", i)
						}
					}
				} else if got != 0 || sh.log.lowWater() != 0 {
					t.Fatal("oversized batch lost its original source")
				}
			})
		}
	}
}

func TestInputLogDoesNotRelogHistory(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		for _, flight := range []bool{false, true} {
			t.Run(fmt.Sprintf("recovered=%t/flight=%t", recovered, flight), func(t *testing.T) {
				c := New()
				c.EnableInputLog()
				name := "history.metric"
				if recovered {
					c.AddRecovered(points.OnePoint(name, 1, 100))
				} else {
					c.AddRestored(points.OnePoint(name, 1, 100))
				}
				sh := c.GetShard(name)
				sh.log.entries.Reset()
				sh.log.gen = 10
				c.Add(points.OnePoint(name, 2, 200)) // mixed live/history stays protected
				sh.log.entries.Reset()
				var p *points.Points
				if flight {
					p, _ = c.PopNotConfirmed(name)
				}
				if got := c.RelogBefore(9, 100); got != 0 {
					t.Fatalf("relogged %d history points", got)
				}
				if flight {
					c.Add(points.OnePoint(name, 3, 300))
					c.Requeue(p)
					sh.log.entries.Reset()
					if got := c.RelogBefore(9, 100); got != 0 {
						t.Fatalf("requeue lost history protection: %d", got)
					}
				}
				if sh.log.lowWater() != 0 {
					t.Fatal("history lost its source generation")
				}
			})
		}
	}
}

func TestInputLogDoesNotRelogDuringRecoveryDrain(t *testing.T) {
	c := New()
	c.EnableInputLog()
	c.Add(points.OnePoint("drain.live", 1, 100))
	sh := c.GetShard("drain.live")
	sh.log.entries.Reset()
	sh.log.gen = 10
	c.SetRecoveryDrain(true)
	if got := c.RelogBefore(9, 100); got != 0 {
		t.Fatalf("relogged %d points during drain", got)
	}
	c.SetRecoveryDrain(false)
	if got := c.RelogBefore(9, 100); got != 1 {
		t.Fatalf("live relog did not resume: %d", got)
	}
}

// A single live straggler otherwise retains unrelated, already-persisted
// traffic. MaxAge is a compaction policy rather than a point expiry policy.
func TestInputLogRelogLimitsStragglerRetention(t *testing.T) {
	type result struct {
		disk, replay int64
		segments     int
	}
	var results [2]result
	for mode, age := range []time.Duration{365 * 24 * time.Hour, 3 * time.Second} {
		synctest.Test(t, func(t *testing.T) {
			dir := t.TempDir()
			c := New()
			c.SetMaxSize(0)
			opts := inputlog.Options{SegmentSize: 16 << 10, SyncInterval: time.Hour, MaxAge: age}
			l, _ := startInputLog(t, dir, c, opts)
			c.Add(points.OnePoint("retained.straggler", 1, 100))
			syncLog(t, l)
			names := make([]string, 2000)
			for i := range names {
				names[i] = fmt.Sprintf("retention.metric.%04d", i)
			}
			for round := range 24 {
				time.Sleep(time.Second)
				for _, name := range names {
					c.Add(points.OnePoint(name, float64(round), int64(round)))
				}
				syncLog(t, l)
				persist(c, names)
			}
			results[mode].disk = l.Metrics().DiskBytes
			results[mode].segments = liveSegments(t, dir)
			l.Kill()
			reopened, err := inputlog.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			found := false
			stats, err := reopened.Replay(func(p *points.Points) {
				if p.Metric == "retained.straggler" {
					found = true
				}
			})
			if err != nil || !found {
				t.Fatal("lost straggler", err)
			}
			results[mode].replay = stats.Points
		})
	}
	before, after := results[0], results[1]
	t.Logf("24 cycles / 48000 transient points: without expiry segments=%d disk=%d replay_points=%d; 3s elapsed age segments=%d disk=%d replay_points=%d", before.segments, before.disk, before.replay, after.segments, after.disk, after.replay)
	if before.segments <= after.segments || before.disk <= after.disk || before.replay <= after.replay {
		t.Fatal("relog did not release straggler retention", results)
	}
}

func TestInputLogHistorySourcesSurviveRequeue(t *testing.T) {
	for _, journal := range []bool{false, true} {
		t.Run(fmt.Sprintf("journal=%t", journal), func(t *testing.T) {
			c := New()
			c.EnableInputLog()
			var stream RecoveryStream
			source := stream.Next()
			if journal {
				source = stream.NextJournal()
			}
			c.AddRecoveredFrom(points.OnePoint("history.source", 1, 1), source)
			p, _ := c.PopNotConfirmed("history.source")
			c.Add(points.OnePoint("history.source", 2, 2))
			c.Requeue(p)
			sh := c.GetShard("history.source")
			sh.log.entries.Reset()
			sh.log.gen = 10
			if got := c.RelogBefore(9, 100); got != 0 {
				t.Fatal("requeued source was relogged", got)
			}
			if source.Pending() != 1 {
				t.Fatal("requeue released durable source")
			}
			p, _ = c.PopNotConfirmed("history.source")
			c.Confirm(p)
			if source.Pending() != 0 || len(sh.log.items) != 0 {
				t.Fatal("persisted history retained source")
			}
		})
	}
}
