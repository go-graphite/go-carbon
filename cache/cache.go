package cache

import (
	"fmt"
	"sort"
	"time"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"

	"github.com/Sirupsen/logrus"
)

type queueItem struct {
	metric string
	count  int
	ts     int64
}

type queue []*queueItem

type byLength []*queueItem
type byTimestamp []*queueItem

func (v byLength) Len() int           { return len(v) }
func (v byLength) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byLength) Less(i, j int) bool { return v[i].count < v[j].count }

func (v byTimestamp) Len() int           { return len(v) }
func (v byTimestamp) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byTimestamp) Less(i, j int) bool { return v[i].ts > v[j].ts }

// Cache stores and aggregate metrics in memory
type Cache struct {
	helper.Stoppable
	data           map[string]*points.Points
	size           int
	maxSize        int
	inputChan      chan *points.Points // from receivers
	inputCapacity  int                 // buffer size of inputChan
	outputChan     chan *points.Points // to persisters
	queryChan      chan *Query         // from carbonlink
	confirmChan    chan *points.Points // for persisted confirmation
	metricInterval time.Duration       // checkpoint interval
	graphPrefix    string
	queryCnt       int
	overflowCnt    int    // drop packages if cache full
	writeStrategy  string // max or sorted
	queue          queue
}

// New create Cache instance and run in/out goroutine
func New() *Cache {
	cache := &Cache{
		data:           make(map[string]*points.Points, 0),
		size:           0,
		maxSize:        1000000,
		metricInterval: time.Minute,
		queryChan:      make(chan *Query, 16),
		graphPrefix:    "carbon.",
		queryCnt:       0,
		queue:          make(queue, 0),
		confirmChan:    make(chan *points.Points, 2048),
		inputCapacity:  51200,
		writeStrategy:  "max",
		// inputChan:   make(chan *points.Points, 51200), create in In() getter
	}
	return cache
}

// SetWriteStrategy ...
func (c *Cache) SetWriteStrategy(s string) {
	c.writeStrategy = s
}

// SetInputCapacity set buffer size of input channel. Call before In() getter
func (c *Cache) SetInputCapacity(size int) {
	c.inputCapacity = size
}

// SetMetricInterval sets doChekpoint interval
func (c *Cache) SetMetricInterval(interval time.Duration) {
	c.metricInterval = interval
}

func (c *Cache) getNext() *points.Points {
	for {
		size := len(c.queue)
		if size == 0 {
			break
		}
		cacheRecord := c.queue[size-1]
		c.queue = c.queue[:size-1]

		if values, ok := c.data[cacheRecord.metric]; ok {
			return values
		}
	}
	return nil
}

func (c *Cache) getAny() *points.Points {
	for _, values := range c.data {
		return values
	}
	return nil
}

// Get any key/values pair from Cache
func (c *Cache) Get() *points.Points {
	if values := c.getNext(); values != nil {
		return values
	}

	c.updateQueue()

	if values := c.getNext(); values != nil {
		return values
	}

	return c.getAny()
}

// Remove key from cache
func (c *Cache) Remove(key string) {
	if value, exists := c.data[key]; exists {
		c.size -= len(value.Data)
		delete(c.data, key)
	}
}

// Pop return and remove next for save point from cache
func (c *Cache) Pop() *points.Points {
	if c.size == 0 {
		return nil
	}
	v := c.Get()
	if v != nil {
		c.Remove(v.Metric)
	}
	return v
}

// Add points to cache
func (c *Cache) Add(p *points.Points) {
	if values, exists := c.data[p.Metric]; exists {
		values.Data = append(values.Data, p.Data...)
	} else {
		c.data[p.Metric] = p
	}
	c.size += len(p.Data)
}

// SetGraphPrefix for internal cache metrics
func (c *Cache) SetGraphPrefix(prefix string) {
	c.graphPrefix = prefix
}

// SetMaxSize of cache
func (c *Cache) SetMaxSize(maxSize int) {
	c.maxSize = maxSize
}

// Size returns size
func (c *Cache) Size() int {
	return c.size
}

 
// stat send internal statistics of cache
func (c *Cache) stat(metric string, value float64) {
	key := fmt.Sprintf("%scache.%s", c.graphPrefix, metric)
	c.Add(points.OnePoint(key, value, time.Now().Unix()))
	c.queue = append(c.queue, &queueItem{key, 1, 0})
}

func (c *Cache) updateQueue() {
	newQueue := make(queue, 0)

	for key, values := range c.data {
		newQueue = append(newQueue,	&queueItem{key, len(values.Data), values.Data[0].Timestamp})
	}

	if c.writeStrategy == "max" {
		sort.Sort(byLength(newQueue))
	} else {
		sort.Sort(byTimestamp(newQueue))
	}
	c.queue = newQueue
}

// doCheckpoint reorder save queue, add carbon metrics to queue
func (c *Cache) doCheckpoint() {
	start := time.Now()

	inputLenBeforeCheckpoint := len(c.inputChan)

	c.updateQueue()

	inputLenAfterCheckpoint := len(c.inputChan)

	worktime := time.Now().Sub(start)

	c.stat("size", float64(c.size))
	c.stat("metrics", float64(len(c.data)))
	c.stat("queries", float64(c.queryCnt))
	c.stat("overflow", float64(c.overflowCnt))
	c.stat("checkpointTime", worktime.Seconds())
	c.stat("inputLenBeforeCheckpoint", float64(inputLenBeforeCheckpoint))
	c.stat("inputLenAfterCheckpoint", float64(inputLenAfterCheckpoint))

	logrus.WithFields(logrus.Fields{
		"time":                     worktime.String(),
		"size":                     c.size,
		"metrics":                  len(c.data),
		"queries":                  c.queryCnt,
		"overflow":                 c.overflowCnt,
		"inputLenBeforeCheckpoint": inputLenBeforeCheckpoint,
		"inputLenAfterCheckpoint":  inputLenAfterCheckpoint,
		"inputCapacity":            cap(c.inputChan),
	}).Info("[cache] doCheckpoint()")

	c.queryCnt = 0
	c.overflowCnt = 0
}

func (c *Cache) worker(exitChan chan bool) {
	var values *points.Points
	var sendTo chan *points.Points
	var forceReceive bool

	toConfirmTracker := make(chan *points.Points)

	confirmTracker := &notConfirmed{
		data:           make(map[string][]*points.Points),
		queryChan:      make(chan *Query, 16),
		metricInterval: c.metricInterval,
		graphPrefix:    c.graphPrefix,
		in:             toConfirmTracker,
		out:            c.outputChan,
		confirmed:      c.confirmChan,
		cacheIn:        c.inputChan,
	}

	c.Go(func(exit chan bool) {
		confirmTracker.worker(exit)
	})

	forceReceiveThreshold := cap(c.inputChan) / 10

	ticker := time.NewTicker(c.metricInterval)
	defer ticker.Stop()

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
		case <-ticker.C: // checkpoint
			c.doCheckpoint()
		case query := <-c.queryChan: // carbonlink
			c.queryCnt++

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
				c.overflowCnt++
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
