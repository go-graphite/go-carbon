package persister

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lomik/go-carbon/helper/qa"
	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

func makeRecvPopFromChan(ch chan *points.Points) (func(chan bool) string, func(string) (*points.Points, bool)) {
	cache := make(map[string]*points.Points)
	var m sync.Mutex

	recv := func(abort chan bool) string {
		select {
		case <-abort:
			return ""
		case p := <-ch:
			m.Lock()
			cache[p.Metric] = p
			m.Unlock()
			return p.Metric
		}
	}

	pop := func(metric string) (*points.Points, bool) {
		m.Lock()
		p, exists := cache[metric]
		if exists {
			delete(cache, metric)
		}
		m.Unlock()
		return p, exists
	}
	return recv, pop
}

func TestGracefullyStop(t *testing.T) {
	assert := assert.New(t)

	do := func(maxUpdatesPerSecond int, workers int) {
		ch := make(chan *points.Points, 1000)

		qa.Root(t, func(root string) {
			recv, pop := makeRecvPopFromChan(ch)
			p := NewWhisper(root, nil, nil, recv, pop, nil)
			p.SetMaxUpdatesPerSecond(maxUpdatesPerSecond)
			p.SetWorkers(workers)

			storeWait := make(chan bool)
			var storeCount uint32

			p.mockStore = func() (StoreFunc, func()) {
				return func(metric string) {
					pop(metric)
					<-storeWait
					atomic.AddUint32(&storeCount, 1)
				}, nil
			}

			var sentCount int

			p.Start()

		SEND_LOOP:
			for {
				select {
				case ch <- points.NowPoint(fmt.Sprintf("%d", sentCount), float64(sentCount)):
					sentCount++
				default:
					break SEND_LOOP
				}
			}

			time.Sleep(10 * time.Millisecond)
			close(storeWait)

			p.Stop()

			storeCount += uint32(len(ch))
			assert.Equal(sentCount, int(storeCount), "maxUpdatesPerSecond: %d, workers: %d", maxUpdatesPerSecond, workers)

		})
	}

	startGoroutineNum := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		for _, maxUpdatesPerSecond := range []int{0, 4000} {
			for _, workers := range []int{1, 4} {
				do(maxUpdatesPerSecond, workers)
			}
		}
	}
	endGoroutineNum := runtime.NumGoroutine()
	// GC worker etc
	assert.InDelta(startGoroutineNum, endGoroutineNum, 2)
}

func TestStopEmptyThrottledPersister(t *testing.T) {
	// Test bug: not stopped without traffic

	assert := assert.New(t)
	for _, maxUpdatesPerSecond := range []int{0, 4000} {
		for _, workers := range []int{1, 4} {
			qa.Root(t, func(root string) {

				ch := make(chan *points.Points, 10)
				recv, pop := makeRecvPopFromChan(ch)
				p := NewWhisper(root, nil, nil, recv, pop, nil)
				p.SetMaxUpdatesPerSecond(maxUpdatesPerSecond)
				p.SetWorkers(workers)

				p.mockStore = func() (StoreFunc, func()) {
					return func(string) {}, nil
				}

				p.Start()
				time.Sleep(10 * time.Millisecond)

				start := time.Now()
				p.Stop()
				worktime := time.Now().Sub(start)

				assert.True(worktime.Seconds() < 1, "maxUpdatesPerSecond: %d, workers: %d", maxUpdatesPerSecond, workers)
			})
		}
	}
}
