package persister

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/qa"
	"github.com/stretchr/testify/assert"
)

func makeRecvFromChan(ch chan *points.Points) func(chan bool) *points.Points {
	return func(abort chan bool) *points.Points {
		select {
		case <-abort:
			return nil
		case p := <-ch:
			return p
		}
	}
}

func TestGracefullyStop(t *testing.T) {
	assert := assert.New(t)

	do := func(maxUpdatesPerSecond int, workers int) {
		ch := make(chan *points.Points, 1000)

		qa.Root(t, func(root string) {
			p := NewWhisper(root, nil, nil, makeRecvFromChan(ch), nil, nil)
			p.SetMaxUpdatesPerSecond(maxUpdatesPerSecond)
			p.SetWorkers(workers)

			storeWait := make(chan bool)
			var storeCount uint32

			p.mockStore = func() (StoreFunc, func()) {
				return func(p *Whisper, values *points.Points) {
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
				p := NewWhisper(root, nil, nil, makeRecvFromChan(ch), nil, nil)
				p.SetMaxUpdatesPerSecond(maxUpdatesPerSecond)
				p.SetWorkers(workers)

				p.mockStore = func() (StoreFunc, func()) {
					return func(p *Whisper, values *points.Points) {}, nil
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
