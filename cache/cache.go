package cache

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/lomik/go-carbon/cache/cmap"
	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
)

type cacheStats struct {
	sizeShared          int32  // changing via atomic
	queueBuildCnt       uint32 // number of times writeout queue was built
	queueBuildTimeMs    uint32 // time spent building writeout queue in milliseconds
	queueWriteoutTimeMs uint32 // in milliseconds
	overflowCnt         uint32 // drop packages if cache full
	queryCnt            uint32 // number of queries
}

// Cache stores and aggregate metrics in memory
type Cache struct {
	helper.Stoppable
	data          cmap.ConcurrentMap
	queue         WriteoutQueue
	cacheStats    cacheStats
	maxSize       int32
	numPersisters int
	queryChan     chan *Query // from carbonlink
	dispatchChan  chan []*points.Points
	xlog          io.Writer
}

// New create Cache instance and run in/out goroutine
func New() *Cache {
	cache := &Cache{
		data:      cmap.New(),
		maxSize:   1000000,
		queryChan: make(chan *Query, 16),
	}

	cache.queue = NewQueue(&cache.data, &cache.cacheStats)
	return cache
}

func upsertCb(exists bool, valueInMap *points.Points, p *points.Points) (*points.Points, bool) {
	if !exists {
		return p, false
	}
	valueInMap.AppendPoints(p.Data)
	return valueInMap, false
}

func (c *Cache) Stats() *cacheStats {
	return &c.cacheStats
}

// Add points to cache
func (c *Cache) Add(p *points.Points) {
	if c.xlog != nil {
		p.WriteTo(c.xlog)
	}
	if c.maxSize > 0 && c.Size() > c.maxSize {
		atomic.AddUint32(&c.cacheStats.overflowCnt, 1)
		return
	}

	count := len(p.Data) // save count before adding to map, otherwise
	// we'd need a lock to read len

	c.data.Upsert(p.Metric, p, upsertCb)
	atomic.AddInt32(&c.cacheStats.sizeShared, int32(count))
}

func upsertSingleCb(exists bool, valueInMap *points.Points, p points.SinglePoint) *points.Points {
	if !exists {
		return points.OnePoint(p.Metric, p.Point.Value, int64(p.Point.Timestamp))
	}
	valueInMap.AppendSinglePoint(&p)
	return valueInMap
}

// Add single point to cache
func (c *Cache) AddSinglePoint(p points.SinglePoint) {
	if c.xlog != nil {
		p.WriteTo(c.xlog)
	}
	if c.maxSize > 0 && c.Size() > c.maxSize {
		atomic.AddUint32(&c.cacheStats.overflowCnt, 1)
		return
	}

	c.data.UpsertSingle(p.Metric, p, upsertSingleCb)
	atomic.AddInt32(&c.cacheStats.sizeShared, 1)
}

func (c *Cache) SetWriteStrategy(s string) (err error) {
	switch s {
	case "max":
		c.queue.writeStrategy = MaximumLength
	case "sort":
		c.queue.writeStrategy = TimestampOrder
	case "noop":
		c.queue.writeStrategy = Noop
	default:
		return fmt.Errorf("Unknown write strategy '%s', should be one of: max, sort, noop", s)
	}
	return nil
}

func (c *Cache) GetMetric(key string) (*points.Points, bool) {
	return c.data.Get(key)
}

// SetMaxSize of cache
func (c *Cache) SetMaxSize(maxSize int32) {
	c.maxSize = maxSize
}

// Sets number of persisters cache should distribute write jobs to
func (c *Cache) SetNumPersisters(numPersisters int) (err error) {
	if c.dispatchChan != nil {
		// TODO: need to check all invariants and allow clean resize, or dont bother
		//       and just go with stupidly high number like 256
		return errors.New("Dynamic change of number of persisters is not supported yet (can cause batches to be lost)")
	}
	c.dispatchChan = make(chan []*points.Points, numPersisters)
	c.numPersisters = numPersisters
	return nil
}

// Size returns size
func (c *Cache) Size() int32 {
	return atomic.LoadInt32(&c.cacheStats.sizeShared)
}

// Get reference to writeout queue
func (c *Cache) WriteoutQueue() *WriteoutQueue {
	return &c.queue
}

func (c *Cache) updateQueue() {
	if c.Size() == 0 {
		return
	}

	if !c.queue.Update() {
		return
	}

	batches := c.queue.Chop(c.numPersisters)
	c.queue.activeWorkers = int32(len(batches))

	for i := range batches {
		c.dispatchChan <- batches[i]
	}
}

// Collect cache metrics
func (c *Cache) Stat(send helper.StatCb) {
	send("size", float64(c.Size()))
	send("metrics", float64(c.data.Count()))

	helper.SendAndSubstractUint32("queries", &c.cacheStats.queryCnt, send)
	helper.SendAndSubstractUint32("overflow", &c.cacheStats.overflowCnt, send)
	helper.SendAndSubstractUint32("queueBuildCount", &c.cacheStats.queueBuildCnt, send)
	helper.SendAndSubstractUint32("queueBuildTimeMs", &c.cacheStats.queueBuildTimeMs, send)
	helper.SendAndSubstractUint32("queueWriteoutTimeMs", &c.cacheStats.queueWriteoutTimeMs, send)
}

func (c *Cache) worker(exitChan chan bool) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

MAIN_LOOP:
	for {
		c.updateQueue()

		select {
		case <-c.queue.writeoutCompleteChan:
		case <-ticker.C: // ticker is here to kick off queue update and writeout

		case query := <-c.queryChan: // carbonlink
			atomic.AddUint32(&c.cacheStats.queryCnt, 1)
			if v, ok := c.GetMetric(query.Metric); ok {
				query.CacheData = v
			}
			close(query.Wait)

		case <-exitChan: // exit
			break MAIN_LOOP
		}
	}
}

func (c *Cache) Out() <-chan []*points.Points {
	return c.dispatchChan
}

// Query returns carbonlink query channel
func (c *Cache) Query() chan *Query {
	return c.queryChan
}

// Start worker
func (c *Cache) Start() error {
	if c.numPersisters <= 0 {
		return errors.New("numPersisters should be >0 when starting a cache")
	}
	if c.dispatchChan == nil {
		return errors.New("BUG: dispatch chan was left unconfigured")
	}

	return c.StartFunc(func() error {
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

func (c *Cache) Stop() {
	c.StopFunc(func() {
		// cache might be sending batches and with stopped
		// persisters it wont recieve our exit channel message
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-c.dispatchChan:
			case <-ticker.C:
				return
			}
		}
	})
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
