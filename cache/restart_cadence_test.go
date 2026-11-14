package cache

import (
	"fmt"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

func cadenceCache(t *testing.T) *Cache {
	c := batchingCache(t, 960, 1800*time.Second)
	c.SetMaxSize(0)
	c.SetWriteoutPacing(true)
	c.SetWriteoutRestoreSpread(true)
	if err := c.SetWriteoutSpread(.25); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRestartRestoresArrivalAndPhaseInEveryDumpFormat(t *testing.T) {
	for _, format := range []recovery.Format{recovery.FormatLegacy, recovery.FormatCompact, recovery.FormatBlocks} {
		t.Run(format.String(), func(t *testing.T) {
			c := cadenceCache(t)
			defer c.Stop()
			c.AddPoint([]byte("residence"), math.Float64frombits(0x7ff8000000000042), 123)
			want := deadlineOf(c, "residence")
			bundle, _ := writeCheckpoint(t, c, format, nil)
			slot, found, err := bundle.Find("residence")
			if err != nil || !found {
				t.Fatal(found, err)
			}
			p, err := bundle.Read(slot)
			if err != nil {
				t.Fatal(err)
			}
			if p.FirstArrival == 0 {
				t.Fatal("dump lost arrival age")
			}
			next := cadenceCache(t)
			defer next.Stop()
			next.PauseStartupWriteout()
			next.AddRestored(p)
			next.ResumeStartupWriteout(time.Now().Add(time.Hour))
			if got := deadlineOf(next, "residence"); !got.Equal(want) {
				t.Fatalf("phase moved: %v -> %v", want, got)
			}
			if !sameValues(c.Get("residence"), next.Get("residence")) {
				t.Fatal("dump changed point bits")
			}
		})
	}
}

func TestInputLogReplayRestoresArrivalAndPhase(t *testing.T) {
	c := cadenceCache(t)
	defer c.Stop()
	dir := t.TempDir()
	log, _ := startInputLog(t, dir, c, inputlog.Options{})
	c.AddPoint([]byte("journal.age"), math.Copysign(0, -1), -17)
	want := deadlineOf(c, "journal.age")
	syncLog(t, log)
	log.Kill()
	next := cadenceCache(t)
	defer next.Stop()
	next.PauseStartupWriteout()
	replay, _ := startInputLog(t, dir, next, inputlog.Options{})
	defer replay.Close()
	next.ResumeStartupWriteout(time.Now())
	if got := deadlineOf(next, "journal.age"); !got.Equal(want) {
		t.Fatalf("replay moved phase: %v -> %v", want, got)
	}
	if !sameValues(c.Get("journal.age"), next.Get("journal.age")) {
		t.Fatal("replay changed point bits")
	}
}

func TestRestartSimulationKeepsSteadyCommitCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const metrics = 2048
		baseline, restarted := cadenceCache(t), cadenceCache(t)
		defer baseline.Stop()
		defer func() { restarted.Stop() }()
		names := make([]string, metrics)
		for i := range names {
			names[i] = fmt.Sprintf("cadence.%06d", i)
		}
		var after [41]int
		drain := func(c *Cache) int {
			total := 0
			q, _ := c.makeQueueAt(time.Now())
			for len(q) > 0 {
				p, ok := c.PopNotConfirmed(<-q)
				if ok {
					total += len(p.Data)
					c.Confirm(p)
				}
			}
			return total
		}
		for second := 0; second < 76*60; second += 5 {
			if second == 35*60 {
				var saved []*points.Points
				if err := restarted.DumpShards(0, ShardCount, func(p *points.Points) error {
					cp := *p
					cp.Data = append([]points.Point(nil), p.Data...)
					saved = append(saved, &cp)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				restarted.Stop()
				restarted = cadenceCache(t)
				restarted.PauseStartupWriteout()
				for _, p := range saved {
					restarted.AddRestored(p)
				}
				restarted.ResumeStartupWriteout(time.Now())
				for _, name := range names {
					if !deadlineOf(restarted, name).Equal(deadlineOf(baseline, name)) || !sameValues(restarted.Get(name), baseline.Get(name)) {
						t.Fatal("restart changed age/phase/bits", name)
					}
				}
			}
			if second%60 == 0 {
				for i, name := range names {
					p := points.OnePoint(name, math.Float64frombits(uint64(i)*0x9e3779b97f4a7c15), int64(second))
					baseline.Add(p)
					restarted.Add(p)
				}
			}
			a, b := drain(baseline), drain(restarted)
			if a != b {
				t.Fatalf("at %ds baseline committed %d points, restarted %d", second, a, b)
			}
			if second >= 35*60 {
				after[(second-35*60)/60] += b
			}
			time.Sleep(5 * time.Second)
		}
		early := 0
		for _, n := range after[:5] {
			early += n
		}
		received := 5 * metrics
		if early*100 < 80*received {
			t.Fatalf("first five minutes committed %d/%d (<80%%)", early, received)
		}
		for minute, n := range after[25:40] {
			if n > metrics*3/2 {
				t.Fatalf("minute +%d flooded: %d", minute+25, n)
			}
		}
		t.Logf("first 5m committed/received=%d/%d; +25–40m=%v", early, received, after[25:40])
	})
}

func TestLegacyRestartDoesNotInventArrivalAge(t *testing.T) {
	c := cadenceCache(t)
	defer c.Stop()
	c.PauseStartupWriteout()
	c.AddRestored(points.OnePoint("legacy", 1, 10))
	c.ResumeStartupWriteout(time.Now())
	if got := deadlineOf(c, "legacy"); !got.IsZero() {
		t.Fatalf("legacy dump delayed until %v", got)
	}
}
