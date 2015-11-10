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

func TestGracefullyStop(t *testing.T) {
	assert := assert.New(t)

	do := func(maxUpdatesPerSecond int, workers int) {
		ch := make(chan *points.Points, 1000)

		qa.Root(t, func(root string) {
			p := NewWhisper(root, nil, nil, ch)
			p.SetMaxUpdatesPerSecond(maxUpdatesPerSecond)
			p.SetWorkers(workers)

			storeWait := make(chan bool)
			var storeCount uint32

			p.mockStore = func(p *Whisper, values *points.Points) {
				<-storeWait
				atomic.AddUint32(&storeCount, 1)
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
		do(0, 1)
		do(0, 4)
		do(4000, 1)
		do(4000, 4)
	}
	endGoroutineNum := runtime.NumGoroutine()

	// GC worker etc
	assert.InDelta(startGoroutineNum, endGoroutineNum, 2)
}
