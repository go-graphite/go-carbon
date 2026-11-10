package cache

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// Previously the second chunk of an oversized restored record could take the
// live overflow/drop branch. Restore must wait for persistence and keep bits.
func TestRestoredCapacityWaitsWithoutDropping(t *testing.T) {
	c := New()
	c.SetMaxSize(4)
	var quotaCalls atomic.Int64
	c.SetThrottleWithHash(func(*points.Points, bool, uint64, *any) bool {
		quotaCalls.Add(1)
		return true
	})
	const total = maxShardRestoredLogEntries + 16
	data := make([]points.Point, total)
	for i := range data {
		data[i] = points.Point{Value: math.Float64frombits(uint64(i) * 0x9e3779b97f4a7c15), Timestamp: int64(i / 3)}
	}
	done := make(chan struct{})
	go func() {
		c.AddRestoredBeforeInput(&points.Points{Metric: "accepted.restore", Data: data})
		close(done)
	}()
	got := 0
	deadline := time.Now().Add(5 * time.Second)
	for got < total && time.Now().Before(deadline) {
		if batch, ok := c.PopNotConfirmed("accepted.restore"); ok {
			for _, p := range batch.Data {
				if got >= total || p.Timestamp != data[got].Timestamp || math.Float64bits(p.Value) != math.Float64bits(data[got].Value) {
					t.Fatal("restored capacity changed bits/order", got)
				}
				got++
			}
			c.Confirm(batch)
		} else {
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("restore remained blocked after capacity drained")
	}
	if got != total || quotaCalls.Load() != 0 || c.stat.overflowCnt != 0 {
		t.Fatal("already accepted restoration dropped", got, quotaCalls.Load(), c.stat.overflowCnt)
	}
}

func TestRestoredCapacityAllowsGracefulDiversion(t *testing.T) {
	c := New()
	c.SetMaxSize(4)
	c.AddRecovered(&points.Points{Metric: "full", Data: make([]points.Point, 4)})
	done := make(chan struct{})
	go func() {
		c.AddRestoredBeforeInput(points.OnePoint("accepted", 42, 7))
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for !c.RecoveryBlocked() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	var diverted atomic.Int64
	c.DivertToPointWriter(func(p *points.Points) error {
		if p.Metric != "accepted" || len(p.Data) != 1 || p.Data[0].Value != 42 || p.Data[0].Timestamp != 7 {
			t.Error("diverted record changed", p)
		}
		diverted.Add(int64(len(p.Data)))
		return nil
	})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("graceful diversion held behind stopped persistence")
	}
	if diverted.Load() != 1 || c.Size() != 4 {
		t.Fatal("diverted restore entered cache or was lost", diverted.Load(), c.Size())
	}
}

// A concentrated metric used to advance by only 65,536 points per periodic
// commit. Exercise the real journal with its timer deliberately an hour away.
// Pressure must make progress, retain the bounded buffers, and survive reopen.
func TestRestoredLogPressureCommitsAndReopens(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	dir := t.TempDir()
	l, _ := startInputLog(t, dir, c, inputlog.Options{SyncInterval: time.Hour, SegmentSize: 1 << 20})
	defer c.DisableInputLog()
	defer l.Kill()
	data := make([]points.Point, 4*maxShardRestoredLogEntries+17)
	bits := []uint64{0, 1 << 63, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000, 1, 0x7fefffffffffffff}
	for i := range data {
		data[i] = points.Point{Value: math.Float64frombits(bits[i%len(bits)]), Timestamp: int64(1700000000 + i/3 - i%5)}
	}
	done := make(chan struct{})
	go func() {
		c.AddRestoredBeforeInput(&points.Points{Metric: "restore.one.concentrated.metric", Data: data})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		c.DisableInputLog() // release a failed test's restore goroutine
		<-done
		t.Fatal("restore waited for the periodic journal timer")
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if c.Size() != int64(len(data)) || l.Metrics().Unlogged != 0 {
		t.Fatal("restore lost points", c.Size(), l.Metrics())
	}
	l.Kill()
	reopened, err := inputlog.Open(dir, inputlog.Options{SegmentSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	count := 0
	_, err = reopened.Replay(func(p *points.Points) {
		if p.Metric != "restore.one.concentrated.metric" {
			t.Fatal("metric changed", p.Metric)
		}
		for _, got := range p.Data {
			if count >= len(data) || got.Timestamp != data[count].Timestamp || math.Float64bits(got.Value) != math.Float64bits(data[count].Value) {
				t.Fatal("restored journal bit/order mismatch", count)
			}
			count++
		}
	})
	if err != nil || count != len(data) {
		t.Fatal("missing/duplicate replay", count, len(data), err)
	}
	t.Logf("restored=%d cycles=%d timer=1h unlogged=%d", count, l.Metrics().Cycles, l.Metrics().Unlogged)
}

// Many blocked producers share a single wake and must all retry admission.
func TestRestoredLogPressureConcurrentWaiters(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	l, _ := startInputLog(t, t.TempDir(), c, inputlog.Options{SyncInterval: time.Hour, SegmentSize: 1 << 20})
	defer c.DisableInputLog()
	defer l.Kill()
	const writers, count = 8, 3 * maxShardRestoredLogEntries
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			c.AddRestoredBeforeInput(&points.Points{Metric: "same.shard", Data: make([]points.Point, count)})
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		c.DisableInputLog()
		<-done
		t.Fatal("coalesced pressure stranded a waiter")
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if c.Size() != writers*count || l.Metrics().Unlogged != 0 {
		t.Fatal("concurrent restore lost points", c.Size(), l.Metrics())
	}
}
