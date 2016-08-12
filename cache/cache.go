package cache

import (
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
)

type queue []*points.Points

type byLength queue
type byTimestamp queue

func (v byLength) Len() int           { return len(v) }
func (v byLength) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byLength) Less(i, j int) bool { return len(v[i].Data) < len(v[j].Data) }

func (v byTimestamp) Len() int           { return len(v) }
func (v byTimestamp) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byTimestamp) Less(i, j int) bool { return v[i].Data[0].Timestamp > v[j].Data[0].Timestamp }

type WriteStrategy int

const (
	MaximumLength WriteStrategy = iota
	TimestampOrder
	Noop
)

// Cache stores and aggregate metrics in memory
type Cache struct {
	helper.Stoppable
	data                          map[string]*points.Points
	size                          uint32 // local read-only copy of sizeShared for worker goroutine
	sizeShared                    uint32 // changing via atomic
	metricCount                   uint32 // metrics count, changing via atomic
	maxSize                       uint32
	inputChan                     chan *points.Points // from receivers
	inputCapacity                 int                 // buffer size of inputChan
	outputChan                    chan *points.Points // to persisters
	queryChan                     chan *Query         // from carbonlink
	confirmChan                   chan *points.Points // for persisted confirmation
	queryCnt                      uint32
	overflowCnt                   uint32 // drop packages if cache full
	writeStrategy                 WriteStrategy
	queue                         queue
	queueBuildCnt                 uint32 // number of times writeout queue was built
	queueBuildTime                uint32 // time spent building writeout queue in milliseconds
	maxInputLenBeforeQueueRebuild uint32
	maxInputLenAfterQueueRebuild  uint32
	queueWriteoutStart            time.Time
	queueWriteoutTime             uint32 // in milliseconds
}

// New create Cache instance and run in/out goroutine
func New() *Cache {
	cache := &Cache{
		data:               make(map[string]*points.Points, 0),
		size:               0,
		maxSize:            1000000,
		queryChan:          make(chan *Query, 16),
		queue:              make(queue, 0),
		confirmChan:        make(chan *points.Points, 2048),
		inputCapacity:      51200,
		writeStrategy:      MaximumLength,
		queueWriteoutStart: time.Time{},
		// inputChan:   make(chan *points.Points, 51200), create in In() getter
	}
	return cache
}

// SetWriteStrategy ...
func (c *Cache) SetWriteStrategy(s string) (err error) {
	switch s {
	case "max":
		c.writeStrategy = MaximumLength
	case "sort":
		c.writeStrategy = TimestampOrder
	case "noop":
		c.writeStrategy = Noop
	default:
		return fmt.Errorf("Unknown write strategy '%s', should be one of: max, sort, noop", s)
	}
	return nil
}

// SetInputCapacity set buffer size of input channel. Call before In() getter
func (c *Cache) SetInputCapacity(size int) {
	c.inputCapacity = size
}

func (c *Cache) getNext() *points.Points {
	size := len(c.queue)
	if size == 0 {
		return nil
	}
	values := c.queue[size-1]
	c.queue = c.queue[:size-1]
	return values
}

// Get any key/values pair from Cache
func (c *Cache) Get() *points.Points {
	if values := c.getNext(); values != nil {
		return values
	}

	if (c.queueWriteoutStart != time.Time{}) {
		atomic.AddUint32(&c.queueWriteoutTime, uint32(time.Now().Sub(c.queueWriteoutStart).Nanoseconds()/1000000))
	}

	c.updateQueue()

	value := c.getNext()

	if value != nil {
		c.queueWriteoutStart = time.Now()
	} else {
		c.queueWriteoutStart = time.Time{}
	}

	return value
}

// Remove key from cache
func (c *Cache) Remove(key string, size int) {
	delete(c.data, key)
	c.size = atomic.AddUint32(&c.sizeShared, -uint32(size))
	atomic.AddUint32(&c.metricCount, ^uint32(0))
}

// Pop return and remove next for save point from cache
func (c *Cache) Pop() *points.Points {
	if c.size == 0 {
		return nil
	}
	v := c.Get()
	if v != nil {
		c.Remove(v.Metric, len(v.Data))
	}
	return v
}

// Add points to cache
func (c *Cache) Add(p *points.Points) {
	if values, exists := c.data[p.Metric]; exists {
		values.Data = append(values.Data, p.Data...)
	} else {
		atomic.AddUint32(&c.metricCount, 1)
		c.data[p.Metric] = p
	}
	c.size = atomic.AddUint32(&c.sizeShared, uint32(len(p.Data)))
}

// SetMaxSize of cache
func (c *Cache) SetMaxSize(maxSize uint32) {
	c.maxSize = maxSize
}

// Size returns size
func (c *Cache) Size() uint32 {
	return atomic.LoadUint32(&c.sizeShared)
}

func (c *Cache) updateQueue() {
	start := time.Now()
	inputLenBeforeQueueRebuild := len(c.inputChan)

	newQueue := c.queue[:0]

	for _, values := range c.data {
		newQueue = append(newQueue, values)
	}

	switch c.writeStrategy {
	case MaximumLength:
		sort.Sort(byLength(newQueue))
	case TimestampOrder:
		sort.Sort(byTimestamp(newQueue))
	case Noop:
	}
	c.queue = newQueue

	atomic.AddUint32(&c.queueBuildTime, uint32(time.Now().Sub(start).Nanoseconds()/10000000))
	atomic.AddUint32(&c.queueBuildCnt, 1)

	inputLenAfterQueueRebuild := len(c.inputChan)

	if uint32(inputLenAfterQueueRebuild) > atomic.LoadUint32(&c.maxInputLenAfterQueueRebuild) {
		atomic.StoreUint32(&c.maxInputLenAfterQueueRebuild, uint32(inputLenAfterQueueRebuild))
		atomic.StoreUint32(&c.maxInputLenBeforeQueueRebuild, uint32(inputLenBeforeQueueRebuild))
	}
}

// Collect cache metrics
func (c *Cache) Stat(send func(metric string, value float64)) {
	send("size", float64(c.Size()))
	send("metrics", float64(atomic.LoadUint32(&c.metricCount)))

	queryCnt := atomic.LoadUint32(&c.queryCnt)
	atomic.AddUint32(&c.queryCnt, -queryCnt)
	send("queries", float64(queryCnt))

	overflowCnt := atomic.LoadUint32(&c.overflowCnt)
	atomic.AddUint32(&c.overflowCnt, -overflowCnt)
	send("overflow", float64(overflowCnt))

	queueBuildCnt := atomic.LoadUint32(&c.queueBuildCnt)
	atomic.AddUint32(&c.queueBuildCnt, -queueBuildCnt)
	send("queueBuildCount", float64(queueBuildCnt))

	queueBuildTime := atomic.LoadUint32(&c.queueBuildTime)
	atomic.AddUint32(&c.queueBuildTime, -queueBuildTime)
	send("queueBuildTime", float64(queueBuildTime)/1000.0)

	queueWriteoutTime := atomic.LoadUint32(&c.queueWriteoutTime)
	atomic.AddUint32(&c.queueWriteoutTime, -queueWriteoutTime)
	send("queueWriteoutTime", float64(queueWriteoutTime)/1000.0)

	maxInputLenBeforeQueueRebuild := atomic.LoadUint32(&c.maxInputLenBeforeQueueRebuild)
	atomic.CompareAndSwapUint32(&c.maxInputLenBeforeQueueRebuild, maxInputLenBeforeQueueRebuild, 0)
	send("maxInputLenBeforeQueueRebuild", float64(maxInputLenBeforeQueueRebuild))

	maxInputLenAfterQueueRebuild := atomic.LoadUint32(&c.maxInputLenAfterQueueRebuild)
	atomic.CompareAndSwapUint32(&c.maxInputLenAfterQueueRebuild, maxInputLenAfterQueueRebuild, 0)
	send("maxInputLenAfterQueueRebuild", float64(maxInputLenAfterQueueRebuild))
}

func (c *Cache) worker(exitChan chan bool) {
	var values *points.Points
	var sendTo chan *points.Points
	var forceReceive bool

	toConfirmTracker := make(chan *points.Points)

	confirmTracker := &notConfirmed{
		data:      make(map[string][]*points.Points),
		queryChan: make(chan *Query, 16),
		in:        toConfirmTracker,
		out:       c.outputChan,
		confirmed: c.confirmChan,
		cacheIn:   c.inputChan,
	}

	c.Go(func(exit chan bool) {
		confirmTracker.worker(exit)
	})

	forceReceiveThreshold := cap(c.inputChan) / 10

MAIN_LOOP:
	for {

		if len(c.inputChan) > forceReceiveThreshold {
			forceReceive = true
		} else {
			forceReceive = false
		}

		if values == nil && !forceReceive {
			values = c.Pop()
		}

		if values != nil {
			sendTo = toConfirmTracker
		} else {
			sendTo = nil
		}

		select {
		case query := <-c.queryChan: // carbonlink
			atomic.AddUint32(&c.queryCnt, 1)

			if values != nil && values.Metric == query.Metric {
				query.CacheData = values
			} else if v, ok := c.data[query.Metric]; ok {
				query.CacheData = v.Copy()
			}

			confirmTracker.queryChan <- query
		case sendTo <- values: // to persister
			values = nil
		case msg := <-c.inputChan: // from receiver
			if c.maxSize == 0 || c.size < c.maxSize {
				c.Add(msg)
			} else {
				atomic.AddUint32(&c.overflowCnt, 1)
			}
		case <-exitChan: // exit
			break MAIN_LOOP
		}
	}

}

// In returns input channel
func (c *Cache) In() chan *points.Points {
	if c.inputChan == nil {
		c.inputChan = make(chan *points.Points, c.inputCapacity)
	}
	return c.inputChan
}

// Out returns output channel
func (c *Cache) Out() chan *points.Points {
	if c.outputChan == nil {
		c.outputChan = make(chan *points.Points, 1024)
	}
	return c.outputChan
}

// Query returns carbonlink query channel
func (c *Cache) Query() chan *Query {
	return c.queryChan
}

// Confirm returns confirmation channel for persister
func (c *Cache) Confirm() chan *points.Points {
	return c.confirmChan
}

// SetOutputChanSize ...
func (c *Cache) SetOutputChanSize(size int) {
	c.outputChan = make(chan *points.Points, size)
}

// Start worker
func (c *Cache) Start() error {
	return c.StartFunc(func() error {
		if c.inputChan == nil {
			c.inputChan = make(chan *points.Points, c.inputCapacity)
		}
		if c.outputChan == nil {
			c.outputChan = make(chan *points.Points, 1024)
		}

		c.Go(func(exit chan bool) {
			c.worker(exit)
		})

		return nil
	})
}

// Dump all cache to writer
func (c *Cache) Dump(w io.Writer) error {

	for _, p := range c.data { // every metric
		for _, d := range p.Data { // every metric point
			_, err := w.Write([]byte(fmt.Sprintf("%s %v %v\n", p.Metric, d.Value, d.Timestamp)))
			if err != nil {
				return err
			}
		}
	}

	return nil
}
