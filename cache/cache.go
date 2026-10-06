package cache

/*
Based on https://github.com/orcaman/concurrent-map
*/

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/tags"
	"github.com/greatroar/blobloom"
)

type WriteStrategy int

const (
	MaximumLength WriteStrategy = iota
	TimestampOrder
	Noop
)

const shardCount = 1 << 10 // 1024 - an arbitrary sized power of 2

type cacheSettings struct {
	maxSize           int64
	xlog              func(*points.Points) error
	tagsEnabled       bool
	writeoutMinPoints int
	writeoutMaxDelay  time.Duration
}

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (shardCount) map shards.
type Cache struct {
	mu sync.Mutex

	queueLastBuild time.Time

	data []*Shard

	writeStrategy WriteStrategy
	writeoutQueue *WriteoutQueue

	settings atomic.Value // cacheSettings

	stat struct {
		size                int64  // changing via atomic
		queueBuildCnt       uint32 // number of times writeout queue was built
		queueBuildTimeMs    uint32 // time spent building writeout queue in milliseconds
		queueWriteoutTime   uint32 // in milliseconds
		overflowCnt         uint32 // drop packages if cache full
		queryCnt            uint32 // number of queries
		tagsNormalizeErrors uint32 // tags normalize errors count

		droppedRealtimeIndex uint32 // new metrics failed to be indexed in realtime
	}

	newMetricsChan      chan string
	newMetricCf         *blobloom.SyncFilter
	newMetricCfCapacity uint64
	metricExists        func(string) bool

	throttle func(ps *points.Points, inCache bool) bool
}

// A "thread" safe string to anything map.
type Shard struct {
	mu               sync.RWMutex // Read Write mutex, guards access to internal map.
	items            map[string]*points.Points
	notConfirmed     []*points.Points    // linear search for value/slot
	notConfirmedUsed int                 // search value in notConfirmed[:notConfirmedUsed]
	adds             map[string]struct{} // map to maintain all the new metric names
	// Keyed by batch so in-flight writes retain their arrival time on retry.
	firstArrival map[*points.Points]time.Time
}

// Creates a new cache instance
func New() *Cache {
	c := &Cache{
		data:          make([]*Shard, shardCount),
		writeStrategy: Noop,
	}

	for i := 0; i < shardCount; i++ {
		c.data[i] = &Shard{
			items:        make(map[string]*points.Points),
			notConfirmed: make([]*points.Points, 4),
		}
	}

	settings := cacheSettings{
		maxSize:     1000000,
		tagsEnabled: false,
		xlog:        nil,
	}

	c.settings.Store(&settings)

	c.writeoutQueue = NewWriteoutQueue(c)
	c.newMetricCf = nil
	return c
}

func (c *Cache) InitCacheScanAdds() {
	for _, shard := range c.data {
		shard.adds = make(map[string]struct{})
	}
}

// SetWriteStrategy ...
func (c *Cache) SetWriteStrategy(s string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch s {
	case "max":
		c.writeStrategy = MaximumLength
	case "sorted":
		c.writeStrategy = TimestampOrder
	case "noop":
		c.writeStrategy = Noop
	default:
		return fmt.Errorf("Unknown write strategy '%s', should be one of: max, sorted, noop", s)
	}
	return nil
}

// SetMaxSize of cache
func (c *Cache) SetMaxSize(maxSize uint64) {
	s := c.settings.Load().(*cacheSettings)
	newSettings := *s
	newSettings.maxSize = int64(maxSize)
	c.settings.Store(&newSettings)
	c.writeoutQueue.notifyAt(time.Now())
}

// SetBloomSize of bloom filter
func (c *Cache) SetBloomSize(bloomSize uint64) {
	if bloomSize > 0 {
		c.newMetricCf = blobloom.NewSyncOptimized(blobloom.Config{
			Capacity: bloomSize, // Expected number of keys.
			FPRate:   1e-4,      // Accept one false positive per 10,000 lookups.
		})
		c.newMetricCfCapacity = bloomSize
	}
}

func (c *Cache) SetTagsEnabled(value bool) {
	s := c.settings.Load().(*cacheSettings)
	newSettings := *s
	newSettings.tagsEnabled = value
	c.settings.Store(&newSettings)
}

func (c *Cache) SetNewMetricsChan(ch chan string) { c.newMetricsChan = ch }

// SetMetricExists avoids notifying the realtime index about metrics it already
// contains. Configure this callback before starting ingestion.
func (c *Cache) SetMetricExists(exists func(string) bool) { c.metricExists = exists }

func (*Cache) Stop() {}

// Collect cache metrics
func (c *Cache) Stat(send helper.StatCallback) {
	s := c.settings.Load().(*cacheSettings)

	send("size", float64(c.Size()))
	send("metrics", float64(c.Len()))
	send("maxSize", float64(s.maxSize))
	send("notConfirmed", float64(c.NotConfirmedLength()))
	// report elements in bloom filter
	if c.newMetricCf != nil {
		cfCount := c.newMetricCf.Cardinality()
		if uint64(cfCount) > c.newMetricCfCapacity {
			// full filter report +Inf cardinality
			cfCount = float64(c.newMetricCfCapacity)
		}
		send("cfCount", cfCount)
	}

	helper.SendAndSubstractUint32("queries", &c.stat.queryCnt, send)
	helper.SendAndSubstractUint32("tagsNormalizeErrors", &c.stat.tagsNormalizeErrors, send)
	helper.SendAndSubstractUint32("overflow", &c.stat.overflowCnt, send)

	helper.SendAndSubstractUint32("queueBuildCount", &c.stat.queueBuildCnt, send)
	helper.SendAndSubstractUint32("queueBuildTimeMs", &c.stat.queueBuildTimeMs, send)
	helper.SendUint32("queueWriteoutTime", &c.stat.queueWriteoutTime, send)

	helper.SendAndSubstractUint32("droppedRealtimeIndex", &c.stat.droppedRealtimeIndex, send)
}

// GetShard returns shard under given key
func (c *Cache) GetShard(key string) *Shard {
	return c.data[helper.HashString(key)&(shardCount-1)]
}

func (c *Cache) Get(key string) []points.Point {
	atomic.AddUint32(&c.stat.queryCnt, 1)

	shard := c.GetShard(key)

	var data []points.Point
	shard.mu.Lock()
	for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
		if p != nil && p.Metric == key {
			if data == nil {
				data = p.Data
			} else {
				data = append(data, p.Data...)
			}
		}
	}

	if p, exists := shard.items[key]; exists {
		if data == nil {
			data = p.Data
		} else {
			data = append(data, p.Data...)
		}
	}
	shard.mu.Unlock()
	return data
}

func (c *Cache) Confirm(p *points.Points) {
	shard := c.GetShard(p.Metric)

	shard.mu.Lock()
	removeNotConfirmed(shard, p)
	delete(shard.firstArrival, p)
	shard.mu.Unlock()
}

func removeNotConfirmed(shard *Shard, p *points.Points) bool {
	for i := 0; i < shard.notConfirmedUsed; i++ {
		if shard.notConfirmed[i] != p {
			continue
		}

		copy(shard.notConfirmed[i:], shard.notConfirmed[i+1:shard.notConfirmedUsed])
		shard.notConfirmedUsed--
		shard.notConfirmed[shard.notConfirmedUsed] = nil
		return true
	}
	return false
}

// Requeue moves a failed write from the in-flight list back to the live cache.
func (c *Cache) Requeue(p *points.Points) {
	shard := c.GetShard(p.Metric)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if !removeNotConfirmed(shard, p) {
		return
	}

	count := len(p.Data)
	if current, exists := shard.items[p.Metric]; exists {
		p.Data = append(p.Data, current.Data...)
		// A missing arrival marks restored or previously unbatched data ready.
		first, other := shard.firstArrival[p], shard.firstArrival[current]
		if other.IsZero() || (!first.IsZero() && other.Before(first)) {
			if shard.firstArrival != nil {
				shard.firstArrival[p] = other
			}
		}
		delete(shard.firstArrival, current)
	}
	shard.items[p.Metric] = p
	atomic.AddInt64(&c.stat.size, int64(count))
	c.writeoutQueue.notifyAt(time.Now())
}

func (c *Cache) Len() int32 {
	l := 0
	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shard.mu.RLock()
		l += len(shard.items)
		shard.mu.RUnlock()
	}
	return int32(l)
}

func (c *Cache) NotConfirmedLength() int32 {
	l := 0
	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shard.mu.RLock()
		l += shard.notConfirmedUsed
		shard.mu.RUnlock()
	}
	return int32(l)
}

// IsEmpty reports whether the cache has neither queued nor in-flight points.
func (c *Cache) IsEmpty() bool {
	for _, shard := range c.data {
		shard.mu.RLock()
		empty := len(shard.items) == 0 && shard.notConfirmedUsed == 0
		shard.mu.RUnlock()
		if !empty {
			return false
		}
	}
	return true
}

func (c *Cache) Size() int64 {
	return atomic.LoadInt64(&c.stat.size)
}

func (c *Cache) DivertToXlog(w io.Writer) {
	s := c.settings.Load().(*cacheSettings)
	newSettings := *s
	newSettings.xlog = nil
	if w != nil {
		newSettings.xlog = func(p *points.Points) error { _, err := p.WriteTo(w); return err }
	}
	c.settings.Store(&newSettings)
}

// DivertToBinaryXlog writes complete binary records under one lock. Binary dump
// readers in older releases already support this format when the file ends in .bin.
func (c *Cache) DivertToBinaryXlog(w io.Writer) {
	var mu sync.Mutex
	var buf []byte
	s := *c.settings.Load().(*cacheSettings)
	s.xlog = func(p *points.Points) error {
		mu.Lock()
		defer mu.Unlock()
		buf = p.AppendBinary(buf[:0])
		_, err := w.Write(buf)
		return err
	}
	c.settings.Store(&s)
}

// send metric to the new metrics channel
func sendMetricToNewMetricChan(c *Cache, metric string) bool {
	select {
	case c.newMetricsChan <- metric:
		return true
	default:
		atomic.AddUint32(&c.stat.droppedRealtimeIndex, 1)
		return false
	}
}

// Sets the given value under the specified key.
func (c *Cache) Add(p *points.Points) {
	c.add(p, false)
}

// AddRestored adds dump data without starting a new buffering deadline.
func (c *Cache) AddRestored(p *points.Points) {
	c.add(p, true)
}

func (c *Cache) add(p *points.Points, restored bool) {
	s := c.settings.Load().(*cacheSettings)

	if s.xlog != nil {
		s.xlog(p)
		return
	}

	if s.tagsEnabled {
		var err error
		p.Metric, err = tags.Normalize(p.Metric)
		if err != nil {
			atomic.AddUint32(&c.stat.tagsNormalizeErrors, 1)
			return
		}
	}

	// Get map shard.
	shard := c.GetShard(p.Metric)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	values, exists := shard.items[p.Metric]
	if c.throttle != nil && c.throttle(p, exists) {
		return
	}

	count := len(p.Data)
	if s.maxSize > 0 && c.Size() > s.maxSize {
		atomic.AddUint32(&c.stat.overflowCnt, uint32(count))
		return
	}

	if exists {
		values.Data = append(values.Data, p.Data...)
	} else {
		shard.items[p.Metric] = p
		values = p
		if s.batching() && !restored {
			if shard.firstArrival == nil {
				shard.firstArrival = make(map[*points.Points]time.Time)
			}
			shard.firstArrival[p] = time.Now()
		}

		if shard.adds != nil {
			shard.adds[p.Metric] = struct{}{}
		}
		// if no bloom filter - just add metric to new channel
		// if missed in cache, as it was before
		if c.newMetricsChan != nil && c.newMetricCf == nil {
			sendMetricToNewMetricChan(c, p.Metric)
		}
	}

	// if we have both new metric channel and bloom filter
	if c.newMetricsChan != nil && c.newMetricCf != nil {
		// add metric to new metric channel if missed in bloom
		// despite what we have it in cache (new behaviour)
		if hash := helper.HashString(p.Metric); !c.newMetricCf.Has(hash) {
			// Suppress notifications only for an indexed metric or a successful
			// enqueue. A full queue must allow unknown metrics to retry.
			if (c.metricExists != nil && c.metricExists(p.Metric)) || sendMetricToNewMetricChan(c, p.Metric) {
				c.newMetricCf.Add(hash)
			}
		}

	}
	size := atomic.AddInt64(&c.stat.size, int64(count))
	if restored {
		delete(shard.firstArrival, values)
	}
	if s.batching() {
		crossedThreshold := len(values.Data) >= s.writeoutMinPoints && len(values.Data)-count < s.writeoutMinPoints
		if restored || crossedThreshold || (s.underPressure(size) && !s.underPressure(size-int64(count))) {
			c.writeoutQueue.notifyAt(time.Now())
		} else if !exists {
			c.writeoutQueue.notifyAt(shard.firstArrival[values].Add(s.writeoutMaxDelay))
		}
	}
}

// Pop removes an element from the map and returns it
func (c *Cache) Pop(key string) (p *points.Points, exists bool) {
	// Try to get shard.
	shard := c.GetShard(key)
	shard.mu.Lock()
	p, exists = shard.items[key]
	delete(shard.items, key)
	delete(shard.firstArrival, p)
	shard.mu.Unlock()

	if exists {
		atomic.AddInt64(&c.stat.size, -int64(len(p.Data)))
	}

	return p, exists
}

func (c *Cache) PopNotConfirmed(key string) (p *points.Points, exists bool) {
	// Try to get shard.
	shard := c.GetShard(key)
	shard.mu.Lock()
	p, exists = shard.items[key]
	delete(shard.items, key)

	if exists {
		if shard.notConfirmedUsed < len(shard.notConfirmed) {
			shard.notConfirmed[shard.notConfirmedUsed] = p
		} else {
			shard.notConfirmed = append(shard.notConfirmed, p)
		}
		shard.notConfirmedUsed++
	}
	shard.mu.Unlock()

	if exists {
		atomic.AddInt64(&c.stat.size, -int64(len(p.Data)))
	}

	return p, exists
}

func (c *Cache) WriteoutQueue() *WriteoutQueue {
	return c.writeoutQueue
}

// called at every scan-frequency by fileListUpdater in carbonserver.
// Iterates over every shard to:
// - collect the new metric names (append shard.adds map to slice)
// - replace shard.adds with new empty map
func (c *Cache) GetRecentNewMetrics() []map[string]struct{} {
	metricNames := make([]map[string]struct{}, shardCount)
	for i := 0; i < shardCount; i++ {
		shard, newAdds := c.data[i], make(map[string]struct{})
		shard.mu.Lock()
		currNames := shard.adds
		shard.adds = newAdds
		shard.mu.Unlock()
		metricNames[i] = currNames
	}
	return metricNames
}

func (c *Cache) SetThrottle(throttle func(ps *points.Points, inCache bool) bool) {
	c.throttle = throttle
}

func (c *Cache) GetInfo() map[string]interface{} {
	s, ok := c.settings.Load().(*cacheSettings)
	if !ok {
		return map[string]interface{}{}
	}

	return map[string]interface{}{
		"size":  c.stat.size,
		"limit": s.maxSize,
	}
}
