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

	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/tags"
)

type WriteStrategy int

const (
	MaximumLength WriteStrategy = iota
	TimestampOrder
	Noop
)

const shardCount = 1024

type cacheSettings struct {
	maxSize     int32
	xlog        io.Writer
	tagsEnabled bool
}

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (shardCount) map shards.
type Cache struct {
	sync.Mutex

	queueLastBuild time.Time

	data []*Shard

	writeStrategy WriteStrategy
	writeoutQueue *WriteoutQueue

	settings atomic.Value // cacheSettings

	stat struct {
		size                int32  // changing via atomic
		queueBuildCnt       uint32 // number of times writeout queue was built
		queueBuildTimeMs    uint32 // time spent building writeout queue in milliseconds
		queueWriteoutTime   uint32 // in milliseconds
		overflowCnt         uint32 // drop packages if cache full
		queryCnt            uint32 // number of queries
		tagsNormalizeErrors uint32 // tags normalize errors count
	}
}

// A "thread" safe string to anything map.
type Shard struct {
	sync.RWMutex     // Read Write mutex, guards access to internal map.
	items            map[string]*points.Points
	notConfirmed     []*points.Points // linear search for value/slot
	notConfirmedUsed int              // search value in notConfirmed[:notConfirmedUsed]
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
	return c
}

// SetWriteStrategy ...
func (c *Cache) SetWriteStrategy(s string) (err error) {
	c.Lock()
	defer c.Unlock()

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
func (c *Cache) SetMaxSize(maxSize uint32) {
	s := c.settings.Load().(*cacheSettings)
	newSettings := *s
	newSettings.maxSize = int32(maxSize)
	c.settings.Store(&newSettings)
}

func (c *Cache) SetTagsEnabled(value bool) {
	s := c.settings.Load().(*cacheSettings)
	newSettings := *s
	newSettings.tagsEnabled = value
	c.settings.Store(&newSettings)
}

func (c *Cache) Stop() {}

// Collect cache metrics
func (c *Cache) Stat(send helper.StatCallback) {
	s := c.settings.Load().(*cacheSettings)

	send("size", float64(c.Size()))
	send("metrics", float64(c.Len()))
	send("maxSize", float64(s.maxSize))

	helper.SendAndSubstractUint32("queries", &c.stat.queryCnt, send)
	helper.SendAndSubstractUint32("tagsNormalizeErrors", &c.stat.tagsNormalizeErrors, send)
	helper.SendAndSubstractUint32("overflow", &c.stat.overflowCnt, send)

	helper.SendAndSubstractUint32("queueBuildCount", &c.stat.queueBuildCnt, send)
	helper.SendAndSubstractUint32("queueBuildTimeMs", &c.stat.queueBuildTimeMs, send)
	helper.SendUint32("queueWriteoutTime", &c.stat.queueWriteoutTime, send)
}

// hash function
// @TODO: try crc32 or something else?
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// Returns shard under given key
func (c *Cache) GetShard(key string) *Shard {
	// @TODO: remove type casts?
	return c.data[uint(fnv32(key))%uint(shardCount)]
}

func (c *Cache) Get(key string) []points.Point {
	atomic.AddUint32(&c.stat.queryCnt, 1)

	shard := c.GetShard(key)

	var data []points.Point
	shard.Lock()
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
	shard.Unlock()
	return data
}

func (c *Cache) Confirm(p *points.Points) {
	var i, j int
	shard := c.GetShard(p.Metric)

	shard.Lock()
	for i = 0; i < shard.notConfirmedUsed; i++ {
		if shard.notConfirmed[i] == p {
			shard.notConfirmed[i] = nil

			for j = i + 1; j < shard.notConfirmedUsed; j++ {
				shard.notConfirmed[j-1] = shard.notConfirmed[j]
			}

			shard.notConfirmedUsed--
		}
	}
	shard.Unlock()
}

func (c *Cache) Len() int32 {
	l := 0
	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shard.Lock()
		l += len(shard.items)
		shard.Unlock()
	}
	return int32(l)
}

func (c *Cache) Size() int32 {
	return atomic.LoadInt32(&c.stat.size)
}

func (c *Cache) DivertToXlog(w io.Writer) {
	s := c.settings.Load().(*cacheSettings)
	newSettings := *s
	newSettings.xlog = w
	c.settings.Store(&newSettings)
}

// Sets the given value under the specified key.
func (c *Cache) Add(p *points.Points) {
	s := c.settings.Load().(*cacheSettings)

	if s.xlog != nil {
		p.WriteTo(s.xlog)
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
	count := len(p.Data)

	if s.maxSize > 0 && c.Size() > s.maxSize {
		atomic.AddUint32(&c.stat.overflowCnt, uint32(count))
		return
	}

	shard := c.GetShard(p.Metric)

	shard.Lock()
	if values, exists := shard.items[p.Metric]; exists {
		values.Data = append(values.Data, p.Data...)
	} else {
		shard.items[p.Metric] = p
	}
	shard.Unlock()

	atomic.AddInt32(&c.stat.size, int32(count))
}

// Removes an element from the map and returns it
func (c *Cache) Pop(key string) (p *points.Points, exists bool) {
	// Try to get shard.
	shard := c.GetShard(key)
	shard.Lock()
	p, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()

	if exists {
		atomic.AddInt32(&c.stat.size, -int32(len(p.Data)))
	}

	return p, exists
}

func (c *Cache) PopNotConfirmed(key string) (p *points.Points, exists bool) {
	// Try to get shard.
	shard := c.GetShard(key)
	shard.Lock()
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
	shard.Unlock()

	if exists {
		atomic.AddInt32(&c.stat.size, -int32(len(p.Data)))
	}

	return p, exists
}

func (c *Cache) WriteoutQueue() *WriteoutQueue {
	return c.writeoutQueue
}
