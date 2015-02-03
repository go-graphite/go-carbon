package carbon

import (
	"fmt"
	"sort"
	"time"

	"github.com/Sirupsen/logrus"
)

// CacheQuery request from carbonlink
type CacheQuery struct {
	Metric    string
	ReplyChan chan *CacheReply
}

// NewCacheQuery create CacheQuery instance
func NewCacheQuery(metric string) *CacheQuery {
	return &CacheQuery{
		Metric:    metric,
		ReplyChan: make(chan *CacheReply, 1),
	}
}

// CacheReply response to carbonlink
type CacheReply struct {
	Data []struct {
		Value     float64
		Timestamp int64
	}
}

// NewCacheReply create CacheReply instance
func NewCacheReply() *CacheReply {
	return &CacheReply{}
}

// CacheValues one metric data
type CacheValues struct {
	Metric string
	Data   []struct {
		Value     float64
		Timestamp int64
	}
}

// Append new metric value
func (values *CacheValues) Append(value float64, timestamp int64) {
	values.Data = append(values.Data, struct {
		Value     float64
		Timestamp int64
	}{value, timestamp})
}

type cacheQueue []*cacheQueueItem

func (v cacheQueue) Len() int           { return len(v) }
func (v cacheQueue) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v cacheQueue) Less(i, j int) bool { return v[i].count < v[j].count }

// Cache stores and aggregate metrics in memory
type Cache struct {
	data        map[string]*CacheValues
	size        int
	inputChan   chan *Message     // from receivers
	outputChan  chan *CacheValues // to persisters
	queryChan   chan *CacheQuery  // from carbonlink
	exitChan    chan bool         // close for stop worker
	graphPrefix string
	queryCnt    int
	queue       cacheQueue
}

// NewCache create Cache instance and run in/out goroutine
func NewCache() *Cache {
	cache := &Cache{
		data:        make(map[string]*CacheValues, 0),
		size:        0,
		inputChan:   make(chan *Message, 1024),
		exitChan:    make(chan bool),
		queryChan:   make(chan *CacheQuery, 16),
		graphPrefix: "carbon",
		queryCnt:    0,
		queue:       make(cacheQueue, 0),
	}
	return cache
}

// Get any key/values pair from Cache
func (c Cache) Get() (string, *CacheValues) {
	for {
		size := len(c.queue)
		if size == 0 {
			break
		}
		cacheRecord := c.queue[size-1]
		c.queue = c.queue[:size-1]

		if values, ok := c.data[cacheRecord.metric]; ok {
			return cacheRecord.metric, values
		}
	}
	for key, values := range c.data {
		return key, values
	}
	return "", nil
}

// Remove key from cache
func (c *Cache) Remove(key string) {
	if value, exists := c.data[key]; exists {
		c.size -= len(value.Data)
		delete(c.data, key)
	}
}

// Add value to cache
func (c *Cache) Add(key string, value float64, timestamp int64) {
	if values, exists := c.data[key]; exists {
		values.Append(value, timestamp)
	} else {
		values := &CacheValues{}
		values.Append(value, timestamp)
		c.data[key] = values
	}
	c.size++
}

// SetGraphPrefix for internal cache metrics
func (c *Cache) SetGraphPrefix(prefix string) {
	c.graphPrefix = prefix
}

// Size returns size
func (c *Cache) Size() int {
	return c.size
}

type cacheQueueItem struct {
	metric string
	count  int
}

// doCheckoint reorder save queue, add carbon metrics to queue
func (c *Cache) doCheckpoint() {
	start := time.Now()

	newQueue := make(cacheQueue, 0)

	for key, values := range c.data {
		newQueue = append(newQueue, &cacheQueueItem{key, len(values.Data)})
	}

	sort.Sort(newQueue)

	c.queue = newQueue

	worktime := time.Now().Sub(start)

	c.Add(fmt.Sprintf("%s%s", c.graphPrefix, "cache.size"), float64(c.size), start.Unix())
	c.Add(fmt.Sprintf("%s%s", c.graphPrefix, "cache.metrics"), float64(len(c.data)), start.Unix())
	c.Add(fmt.Sprintf("%s%s", c.graphPrefix, "cache.queries"), float64(c.queryCnt), start.Unix())
	c.Add(fmt.Sprintf("%s%s", c.graphPrefix, "cache.checkpoint_time"), worktime.Seconds(), start.Unix())

	logrus.WithFields(logrus.Fields{
		"time":    worktime.String(),
		"size":    c.size,
		"metrics": len(c.data),
		"queries": c.queryCnt,
	}).Info("doCheckpoint()")

	c.queryCnt = 0
}

func (c *Cache) worker() {
	var key string
	var values *CacheValues
	var sendTo chan *CacheValues

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		if values == nil {
			key, values = c.Get()

			if values != nil {
				c.Remove(key)
				values.Metric = key
				sendTo = c.outputChan
			} else {
				sendTo = nil
			}
		}

		select {
		case <-ticker.C:
			c.doCheckpoint()
		case query := <-c.queryChan:
			c.queryCnt++
			reply := NewCacheReply()

			if values != nil && values.Metric == query.Metric {
				reply.Data = values.Data
			} else if v, ok := c.data[query.Metric]; ok {
				reply.Data = v.Data
			}

			query.ReplyChan <- reply
		case sendTo <- values:
			values = nil
		case msg := <-c.inputChan:
			c.Add(msg.Name, msg.Value, msg.Timestamp)
		case <-c.exitChan:
			break
		}
	}

}

// In returns input channel
func (c *Cache) In() chan *Message {
	return c.inputChan
}

// Out returns output channel
func (c *Cache) Out() chan *CacheValues {
	return c.outputChan
}

// Query returns carbonlink query channel
func (c *Cache) Query() chan *CacheQuery {
	return c.queryChan
}

// SetOutputChanSize ...
func (c *Cache) SetOutputChanSize(size int) {
	c.outputChan = make(chan *CacheValues, size)
}

// Start worker
func (c *Cache) Start() {
	if c.outputChan == nil {
		c.outputChan = make(chan *CacheValues, 1024)
	}
	go c.worker()
}

// Stop worker
func (c *Cache) Stop() {
	close(c.exitChan)
}
