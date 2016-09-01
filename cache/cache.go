package cache

import (
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"github.com/lomik/go-carbon/cache/cmap"
	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
)

type queue []points.Points

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
	data               cmap.ConcurrentMap
	sizeShared         int32  // changing via atomic
	metricCount        uint32 // metrics count, changing via atomic
	maxSize            int32
	outputChan         chan *points.Points // to persisters
	queryChan          chan *Query         // from carbonlink
	confirmChan        chan *points.Points // for persisted confirmation
	queryCnt           uint32
	overflowCnt        uint32 // drop packages if cache full
	writeStrategy      WriteStrategy
	queue              queue
	queueBuildCnt      uint32 // number of times writeout queue was built
	queueBuildTime     uint32 // time spent building writeout queue in milliseconds
	queueWriteoutStart time.Time
	queueWriteoutTime  uint32 // in milliseconds
	xlog               io.Writer
}

// New create Cache instance and run in/out goroutine
func New() *Cache {
	cache := &Cache{
		data:               cmap.New(),
		maxSize:            1000000,
		queryChan:          make(chan *Query, 16),
		queue:              make(queue, 0),
		confirmChan:        make(chan *points.Points, 2048),
		writeStrategy:      MaximumLength,
		queueWriteoutStart: time.Time{},
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

func (c *Cache) getNext() (values points.Points, exists bool) {
	size := len(c.queue)
	if size == 0 {
		return points.Points{}, false
	}
	values = c.queue[size-1]
	c.queue = c.queue[:size-1]
	return values, true
}

// Get any key/values pair from Cache
func (c *Cache) Get() (values points.Points, exists bool) {
	if values, exists = c.getNext(); exists {
		return values, exists
	}

	if (c.queueWriteoutStart != time.Time{}) {
		atomic.AddUint32(&c.queueWriteoutTime, uint32(time.Now().Sub(c.queueWriteoutStart)/time.Millisecond))
	}

	c.updateQueue()

	values, exists = c.getNext()

	if exists {
		c.queueWriteoutStart = time.Now()
	} else {
		c.queueWriteoutStart = time.Time{}
	}

	return values, exists
}

// Pop return and remove next for save point from cache
func (c *Cache) Pop() (v *points.Points) {
	valInQueue, exists := c.Get()
	if exists {
		v, _ = c.data.Pop(valInQueue.Metric)
		atomic.AddInt32(&c.sizeShared, -int32(len(v.Data)))
	}

	return v
}

func upsertCb(exists bool, valueInMap *points.Points, p *points.Points) *points.Points {
	if !exists {
		return p
	}
	valueInMap.Data = append(valueInMap.Data, p.Data...)
	return valueInMap
}

// Add points to cache
func (c *Cache) Add(p *points.Points) {
	if c.xlog != nil {
		p.WriteTo(c.xlog)
	}
	if c.maxSize > 0 && c.Size() > c.maxSize {
		c.overflowCnt++
		atomic.AddUint32(&c.overflowCnt, 1)
		return
	}

	c.data.Upsert(p.Metric, p, upsertCb)
	atomic.AddInt32(&c.sizeShared, int32(len(p.Data)))
}

func upsertSingleCb(exists bool, valueInMap *points.Points, p points.SinglePoint) *points.Points {
	if !exists {
		return points.OnePoint(p.Metric, p.Point.Value, p.Point.Timestamp)
	}
	valueInMap.Data = append(valueInMap.Data, p.Point)
	return valueInMap
}

// Add single point to cache
func (c *Cache) AddSinglePoint(p points.SinglePoint) {
	if c.xlog != nil {
		p.WriteTo(c.xlog)
	}
	if c.maxSize > 0 && c.Size() > c.maxSize {
		c.overflowCnt++
		atomic.AddUint32(&c.overflowCnt, 1)
		return
	}

	c.data.UpsertSingle(p.Metric, p, upsertSingleCb)
	atomic.AddInt32(&c.sizeShared, 1)
}

func (c *Cache) GetMetric(key string) (*points.Points, bool) {
	return c.data.Get(key)
}

// SetMaxSize of cache
func (c *Cache) SetMaxSize(maxSize int32) {
	c.maxSize = maxSize
}

// Size returns size
func (c *Cache) Size() int32 {
	return atomic.LoadInt32(&c.sizeShared)
}

func (c *Cache) updateQueue() {
	if c.Size() == 0 {
		return
	}

	start := time.Now()

	newQueue := c.queue[:0]

	c.data.IterCb(func(_ string, v *points.Points) { newQueue = append(newQueue, *v) })

	switch c.writeStrategy {
	case MaximumLength:
		sort.Sort(byLength(newQueue))
	case TimestampOrder:
		sort.Sort(byTimestamp(newQueue))
	case Noop:
	}

	c.queue = newQueue

	atomic.AddUint32(&c.queueBuildTime, uint32(time.Now().Sub(start)/time.Millisecond))
	atomic.AddUint32(&c.queueBuildCnt, 1)
}

// Collect cache metrics
func (c *Cache) Stat(send helper.StatCb) {
	send("size", float64(c.Size()))
	send("metrics", float64(c.data.Count()))

	helper.SendAndSubstractUint32("queries", &c.queryCnt, send)
	helper.SendAndSubstractUint32("overflow", &c.overflowCnt, send)
	helper.SendAndSubstractUint32("queueBuildCount", &c.queueBuildCnt, send)
	helper.SendAndSubstractUint32("queueBuildTimeMs", &c.queueBuildTime, send)
	helper.SendAndSubstractUint32("queueWriteoutTimeMs", &c.queueWriteoutTime, send)
}

func (c *Cache) worker(exitChan chan bool) {
	var values *points.Points
	var sendTo chan *points.Points

	toConfirmTracker := make(chan *points.Points)

	confirmTracker := &notConfirmed{
		data:      make(map[string][]*points.Points),
		queryChan: make(chan *Query, 16),
		in:        toConfirmTracker,
		out:       c.outputChan,
		confirmed: c.confirmChan,
	}

	c.Go(func(exit chan bool) {
		confirmTracker.worker(exit)
	})

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

MAIN_LOOP:
	for {

		if values == nil {
			values = c.Pop()
		}

		if values != nil {
			sendTo = toConfirmTracker
		} else {
			sendTo = nil
		}

		select {
		case <-ticker.C:
		case query := <-c.queryChan: // carbonlink
			atomic.AddUint32(&c.queryCnt, 1)

			if values != nil && values.Metric == query.Metric {
				query.CacheData = values.Copy()
			} else if v, ok := c.GetMetric(query.Metric); ok {
				query.CacheData = v.Copy()
			}

			confirmTracker.queryChan <- query
		case sendTo <- values: // to persister
			values = nil
		case <-exitChan: // exit
			break MAIN_LOOP
		}
	}

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
		if c.outputChan == nil {
			c.outputChan = make(chan *points.Points, 1024)
		}

		c.Go(func(exit chan bool) {
			c.worker(exit)
		})

		return nil
	})
}

// when xlog is !nil, calls to Add/AddSingle make an entry in a xlog
func (c *Cache) DivertToXlog(w io.Writer) {
	c.xlog = w
}

// Dump all cache to writer
func (c *Cache) Dump(w io.Writer) error {

	var err error

	c.data.IterCb(func(_ string, p *points.Points) {
		for _, d := range p.Data { // every metric point
			_, err = w.Write([]byte(fmt.Sprintf("%s %v %v\n", p.Metric, d.Value, d.Timestamp)))
		}
	})

	return err
}
