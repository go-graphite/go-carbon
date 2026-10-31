package cache

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

func TestWriteoutQueueWakeHintBounds(t *testing.T) {
	q := New().WriteoutQueue()
	for _, delta := range []time.Duration{-1 << 63, -1, 0, 1, 1<<63 - 1} {
		q.wakeAt.Store(0)
		q.notifyAt(q.epoch.Add(delta))
		if got := q.wakeAt.Load(); got <= 0 {
			t.Fatalf("delta %v encoded as unset/overflowed hint %d", delta, got)
		}
		q.notifyAt(q.epoch)
		if got := q.wakeAt.Load(); got != 1 {
			t.Fatalf("immediate notification lost after delta %v: hint %d", delta, got)
		}
	}
}

func startupRecoveryFixture(t *testing.T, metrics int) *recovery.Bundle {
	t.Helper()
	dir, root := t.TempDir(), t.TempDir()
	builder := recovery.NewBuilder(func(name string) bool {
		return name != fmt.Sprintf("held.%d", metrics-1) && name != fmt.Sprintf("held.%d", metrics-2)
	})
	dump, err := recovery.NewWriterFormat(filepath.Join(dir, "cache-compact.1.2.bin"), 0, 64<<10, builder, recovery.FormatCompact)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := recovery.NewWriter(filepath.Join(dir, "input.1.2.bin"), 1, 4096, builder)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]points.Point, 90)
	for i := range data {
		data[i] = points.Point{Timestamp: int64(i + 1), Value: float64(i)}
	}
	for i := range metrics {
		name := fmt.Sprintf("held.%d", i)
		if err := dump.WritePoints(&points.Points{Metric: name, Data: data}); err != nil {
			t.Fatal(err)
		}
		if err := wal.WritePoints(points.OnePoint(name, 100, 90)); err != nil {
			t.Fatal(err)
		}
	}
	cacheFile, err := dump.Close()
	if err != nil {
		t.Fatal(err)
	}
	inputFile, err := wal.Close()
	if err != nil {
		t.Fatal(err)
	}
	indexFile, err := recovery.WriteIndex(dir, builder)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.Publish(dir, root, cacheFile, inputFile, indexFile, "test-read-index"); err != nil {
		t.Fatal(err)
	}
	bundle, err := recovery.OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bundle.Close() })
	return bundle
}

func TestStartupGatePendingRecoveryWhileQueueWaits(t *testing.T) {
	const metrics = 3072
	bundle := startupRecoveryFixture(t, metrics)
	synctest.Test(t, func(t *testing.T) {
		c := batchingCache(t, 960, 960*time.Second)
		c.SetMaxSize(10_000_000_000)
		c.SetWriteStrategy("noop")
		c.SetWriteoutRestoreSpread(true)
		if err := c.SetWriteoutSpread(.25); err != nil {
			t.Fatal(err)
		}
		c.EnableInputLog()
		c.PauseStartupWriteout()
		if err := c.AttachPendingRecoveryWithPreparedNames(bundle); err != nil {
			t.Fatal(err)
		}
		c.AddPoint([]byte("held.0"), 200, 91)
		abort, gate, stopped := make(chan bool), make(chan struct{}), make(chan struct{})
		var written atomic.Int64
		var released time.Time
		var first time.Duration
		var minutes [16]int
		var seconds [960]int
		checkData := func(p *points.Points) {
			t.Helper()
			i, _ := strconv.Atoi(strings.TrimPrefix(p.Metric, "held."))
			want := make([]points.Point, 90, 101)
			for j := range want {
				want[j] = points.Point{Timestamp: int64(j + 1), Value: float64(j)}
			}
			want = append(want, points.Point{Value: 100, Timestamp: 90})
			switch i {
			case 0, 2:
				want = append(want, points.Point{Value: 200, Timestamp: 91})
			case 1:
				want = append(want, points.Point{Value: 201, Timestamp: 90})
			case 3:
				want = append(want, points.Point{Value: 202, Timestamp: 92})
			}
			for j := range 8 {
				want = append(want, points.Point{Value: float64(300 + j), Timestamp: int64(100 + j)})
			}
			if !reflect.DeepEqual(p.Data, want) {
				t.Error("pending/recovered/live point order changed", p.Metric, len(p.Data))
			}
		}
		go func() {
			defer close(stopped)
			for {
				name := c.WriteoutQueue().Get(abort)
				if name == "" {
					return
				}
				select {
				case <-gate:
				case <-abort:
					return
				}
				p, ok := c.PopNotConfirmed(name)
				if !ok {
					continue
				}
				checkData(p)
				elapsed := time.Since(released)
				if written.Load() == 0 {
					first = elapsed
				}
				minutes[min(int(elapsed/time.Minute), 15)]++
				seconds[min(int(elapsed/time.Second), 959)]++
				written.Add(1)
				c.Confirm(p)
			}
		}()
		defer func() { close(abort); <-stopped }()
		// Unlike the earlier bench, the queue is already sleeping before more
		// held claims and live arrivals notify it of paused deadlines.
		synctest.Wait()
		c.AddPoint([]byte("held.2"), 200, 91)
		synctest.Wait()
		c.AddRecovered(points.OnePoint("held.1", 201, 90))
		c.AddRestored(points.OnePoint("held.3", 202, 92))
		if err := c.RecoverPending(make(chan struct{}), 0); err != nil {
			t.Fatal(err)
		}
		for j := range 8 {
			time.Sleep(30 * time.Second)
			for i := range metrics {
				c.AddPoint([]byte(fmt.Sprintf("held.%d", i)), float64(300+j), int64(100+j))
			}
		}
		synctest.Wait()
		if written.Load() != 0 {
			t.Fatal("wrote before gate release")
		}
		if err := c.FinishPendingRecovery(); err == nil {
			t.Fatal("retired unwritten recovery source")
		}
		released = time.Now()
		c.ResumeStartupWriteout(released)
		close(gate)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if written.Load() == 0 {
			ready := 0
			for i := range metrics {
				if c.WriteoutReady(fmt.Sprintf("held.%d", i)) {
					ready++
				}
			}
			t.Fatalf("queue stayed asleep after release: ready=%d wakeAt=%d", ready, c.writeoutQueue.wakeAt.Load())
		}
		time.Sleep(956 * time.Second)
		synctest.Wait()
		if written.Load() != metrics || !c.IsEmpty() {
			t.Fatalf("pending backlog stalled: written=%d", written.Load())
		}
		steady, rolling, peak := metrics/16, 0, 0
		for minute, n := range minutes {
			if n < steady*8/10 || n > steady*12/10 {
				t.Fatalf("minute%d=%d steady%d", minute, n, steady)
			}
		}
		for i, n := range seconds {
			rolling += n
			if i >= 60 {
				rolling -= seconds[i-60]
			}
			peak = max(peak, rolling)
		}
		if peak > steady*12/10 {
			t.Fatal("rolling minute burst", peak, steady)
		}
		if err := c.FinishPendingRecovery(); err != nil {
			t.Fatal(err)
		}
		t.Logf("first=%v minutes=%v rolling_peak=%d steady=%d", first, minutes, peak, steady)
	})
}
