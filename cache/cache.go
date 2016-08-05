package cache

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"

	"github.com/Sirupsen/logrus"
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
	size                          int
	maxSize                       int
	inputChan                     chan *points.Points // from receivers
	inputCapacity                 int                 // buffer size of inputChan
	outputChan                    chan *points.Points // to persisters
	queryChan                     chan *Query         // from carbonlink
	confirmChan                   chan *points.Points // for persisted confirmation
	metricInterval                time.Duration       // checkpoint interval
	graphPrefix                   string
	queryCnt                      int
	overflowCnt                   int // drop packages if cache full
	writeStrategy                 WriteStrategy
	queue                         queue
	queueBuildCnt                 int           // number of times writeout queue was built
	queueBuildTime                time.Duration // time spent building writeout queue
	maxInputLenBeforeQueueRebuild int
	maxInputLenAfterQueueRebuild  int
	queueWriteoutStart            time.Time
	queueWriteoutTime             time.Duration
}

// New create Cache instance and run in/out goroutine
func New() *Cache {
	cache := &Cache{
		data:               make(map[string]*points.Points, 0),
		size:               0,
		maxSize:            1000000,
		metricInterval:     time.Minute,
		queryChan:          make(chan *Query, 16),
		graphPrefix:        "carbon.",
		queueBuildCnt:      0,
		queueBuildTime:     0,
		queryCnt:           0,
		queue:              make(queue, 0),
		confirmChan:        make(chan *points.Points, 2048),
		inputCapacity:      51200,
		writeStrategy:      MaximumLength,
		queueWriteoutTime:  0,
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

// SetMetricInterval sets doChekpoint interval
func (c *Cache) SetMetricInterval(interval time.Duration) {
	c.metricInterval = interval
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
		c.queueWriteoutTime += time.Now().Sub(c.queueWriteoutStart)
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
	c.size -= size
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

	p := points.OnePoint(key, value, time.Now().Unix())
	c.Add(p)
	c.queue = append(c.queue, p)
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

	c.queueBuildTime += time.Now().Sub(start)
	c.queueBuildCnt++
	inputLenAfterQueueRebuild := len(c.inputChan)

	if inputLenAfterQueueRebuild > c.maxInputLenAfterQueueRebuild {
		c.maxInputLenAfterQueueRebuild = inputLenAfterQueueRebuild
		c.maxInputLenBeforeQueueRebuild = inputLenBeforeQueueRebuild
	}
}

// add carbon metrics to queue
func (c *Cache) doCheckpoint() {
	c.stat("size", float64(c.size))
	c.stat("metrics", float64(len(c.data)))
	c.stat("queries", float64(c.queryCnt))
	c.stat("overflow", float64(c.overflowCnt))
	c.stat("queueBuildCount", float64(c.queueBuildCnt))
	c.stat("queueBuildTime", c.queueBuildTime.Seconds())
	c.stat("maxInputLenBeforeQueueRebuild", float64(c.maxInputLenBeforeQueueRebuild))
	c.stat("maxInputLenAfterQueueRebuild", float64(c.maxInputLenAfterQueueRebuild))
	c.stat("queueWriteoutTime", float64(c.queueWriteoutTime.Seconds()))

	logrus.WithFields(logrus.Fields{
		"size":                          c.size,
		"metrics":                       len(c.data),
		"queries":                       c.queryCnt,
		"overflow":                      c.overflowCnt,
		"maxInputLenBeforeQueueRebuild": c.maxInputLenBeforeQueueRebuild,
		"maxInputLenAfterQueueRebuild":  c.maxInputLenAfterQueueRebuild,
		"inputCapacity":                 cap(c.inputChan),
		"queueBuildCount":               c.queueBuildCnt,
		"queueBuildTime":                c.queueBuildTime.String(),
		"queueWriteoutTime":             c.queueWriteoutTime.String(),
	}).Info("[cache] doCheckpoint()")

	c.queryCnt = 0
	c.overflowCnt = 0
	c.queueBuildCnt = 0
	c.queueBuildTime = 0
	c.queueWriteoutTime = 0
	c.maxInputLenBeforeQueueRebuild = 0
	c.maxInputLenAfterQueueRebuild = 0
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
