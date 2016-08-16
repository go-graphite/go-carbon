package persister

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lomik/go-carbon/cache"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/qa"
	"github.com/lomik/go-whisper"
	"github.com/stretchr/testify/assert"
)

func TestGracefullyStop(t *testing.T) {
	assert := assert.New(t)

	do := func(maxUpdatesPerSecond int, workers int) {
		qa.Root(t, func(root string) {
			cache := cache.New()
			cache.SetNumPersisters(workers)
			assert.NoError(cache.Start()) // to read from cache.queue.writeoutComplete
			p := NewWhisper(root, nil, nil, cache.Out(), cache.WriteoutQueue().Process)
			p.SetMaxUpdatesPerSecond(maxUpdatesPerSecond)
			p.SetWorkers(workers)

			storeWait := make(chan bool)
			var storeCount, sentCount uint32

			p.mockStore = func() (StoreFunc, func()) {
				return func(p *Whisper, metric string, values []*whisper.TimeSeriesPoint) error {
					<-storeWait
					atomic.AddUint32(&storeCount, 1)
					return nil
				}, nil
			}
			p.Start()
			for sentCount = 0; sentCount < 1000; sentCount++ {
				cache.Add(points.NowPoint(fmt.Sprintf("%d", sentCount), float64(sentCount)))
			}
			close(storeWait)
			p.Stop()
			cache.Stop()

			// number of processed metrics + number unprocessed ones (== cache size, because we had 1 point per metric)
			// should be equal to number of metrics sent
			atomic.AddUint32(&storeCount, uint32(cache.Size()))
			assert.Equal(int(sentCount), int(storeCount), "maxUpdatesPerSecond: %d, workers: %d", maxUpdatesPerSecond, workers)
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

				cache := cache.New()
				cache.SetNumPersisters(workers)
				assert.NoError(cache.Start()) // to read from writeout complete channel
				p := NewWhisper(root, nil, nil, cache.Out(), cache.WriteoutQueue().Process)
				p.SetMaxUpdatesPerSecond(maxUpdatesPerSecond)
				p.SetWorkers(workers)

				p.mockStore = func() (StoreFunc, func()) {
					return func(p *Whisper, metric string, values []*whisper.TimeSeriesPoint) error { return nil }, nil
				}

				p.Start()
				start := time.Now()
				p.Stop()
				cache.Stop()
				worktime := time.Now().Sub(start)

				assert.True(worktime.Seconds() < 2, "maxUpdatesPerSecond: %d, workers: %d", maxUpdatesPerSecond, workers)
			})
		}
	}
}
