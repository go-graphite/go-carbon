package downsize

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type pipelineWindowBackend struct {
	*fakeSync
	arrived      chan string
	release      chan struct{}
	active, peak atomic.Int64
	once         sync.Once
}

func (b *pipelineWindowBackend) WindowPresenceRanges(ctx context.Context, source string, names []string, windows syncRanges) (map[string][]syncDigest, error) {
	return b.Digest(ctx, source, names, windows[0].From, windows[0].Until)
}
func (b *pipelineWindowBackend) CopyWindow(ctx context.Context, source, target string, names []string, journal string, windows syncRanges) (syncCopyResult, error) {
	n := b.active.Add(1)
	defer b.active.Add(-1)
	for old := b.peak.Load(); n > old; old = b.peak.Load() {
		if b.peak.CompareAndSwap(old, n) {
			break
		}
	}
	if b.arrived != nil {
		b.arrived <- names[0]
		select {
		case <-b.release:
		case <-ctx.Done():
			return syncCopyResult{}, ctx.Err()
		}
	}
	return b.fakeSync.CopyWindow(ctx, source, target, names, journal, windows)
}

func pipelineFixture(t *testing.T, streams int) (*pipelineWindowBackend, syncOptions) {
	t.Helper()
	b := &pipelineWindowBackend{fakeSync: newFakeSync()}
	w := syncRange{From: syncTestNow - 600, Until: syncTestNow - 120}
	values := []float64{math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64, -3.125}
	for i := 0; i < 24; i++ {
		name := fmt.Sprintf("metric.%03d", i)
		for src, host := range []string{syncD1, syncD2} {
			b.store(host).archives[name] = []syncArchive{{Step: 60, Points: 10000}}
			b.store(host).points[name] = map[int64]map[int64]float64{60: {w.From: values[(i+src)%len(values)], w.From + int64(60*(src+1)): float64(i + src + 1)}}
		}
	}
	return b, syncOptions{to: syncT, from: []string{syncD1, syncD2}, state: t.TempDir(), batch: 2, streams: streams, maxRounds: 1, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
}

func TestSyncWindowPipelineDifferentialAndResume(t *testing.T) {
	var oracle map[string]map[int64]map[int64]uint64
	for _, streams := range []int{1, 8} {
		b, o := pipelineFixture(t, streams)
		if err := runSync(context.Background(), o, b); err != nil {
			t.Fatal(err)
		}
		got := map[string]map[int64]map[int64]uint64{}
		for name, steps := range b.store(syncT).points {
			got[name] = map[int64]map[int64]uint64{}
			for step, points := range steps {
				got[name][step] = map[int64]uint64{}
				for ts, v := range points {
					got[name][step][ts] = math.Float64bits(v)
				}
			}
		}
		if oracle == nil {
			oracle = got
		} else if !reflect.DeepEqual(oracle, got) {
			t.Fatal("pipelining changed point bits or donor priority")
		}
		before := len(b.copies)
		if err := runSync(context.Background(), o, b); err != nil {
			t.Fatal(err)
		}
		if len(b.copies) != before {
			t.Fatal("resume repeated completed copies")
		}
	}
}

func TestSyncWindowPipelineOverlapAndDrain(t *testing.T) {
	b, o := pipelineFixture(t, 4) // two disjoint groups, each with two donor requests
	b.arrived = make(chan string, 64)
	b.release = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runSync(ctx, o, b) }()
	seen := map[string]bool{}
	for range 2 {
		select {
		case name := <-b.arrived:
			seen[name] = true
		case <-time.After(10 * time.Second):
			close(b.release)
			t.Fatal("groups did not overlap")
		}
	}
	if len(seen) != 2 || b.peak.Load() != 2 {
		close(b.release)
		t.Fatal("overlap or bound", seen, b.peak.Load())
	}
	cancel()
	select {
	case err := <-done:
		close(b.release)
		t.Fatal("returned before draining issued work", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(b.release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("drain did not finish")
	}
	b.arrived = nil
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal("resume after drain", err)
	}
	if len(b.store(syncT).points) != 24 {
		t.Fatal("resume lost undispatched groups")
	}
}
