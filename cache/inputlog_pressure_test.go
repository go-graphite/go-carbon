package cache

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

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
