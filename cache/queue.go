package cache

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/lomik/go-carbon/points"
)

type queueItem struct {
	points   *points.Points
	orderKey int64
}

type queue []queueItem

type byOrderKey queue

func (v byOrderKey) Len() int           { return len(v) }
func (v byOrderKey) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byOrderKey) Less(i, j int) bool { return v[i].orderKey < v[j].orderKey }

func (c *Cache) makeQueue() chan *points.Points {
	c.Lock()
	writeStrategy := c.writeStrategy
	prevBuild := c.queueLastBuild
	c.Unlock()

	if !prevBuild.IsZero() {
		// @TODO: may be max (with atomic cas)
		atomic.StoreUint32(&c.stat.queueWriteoutTime, uint32(time.Since(prevBuild)/time.Second))
	}

	start := time.Now()

	defer func() {
		atomic.AddUint32(&c.stat.queueBuildTimeMs, uint32(time.Since(start)/time.Millisecond))
		atomic.AddUint32(&c.stat.queueBuildCnt, 1)

		c.Lock()
		c.queueLastBuild = time.Now()
		c.Unlock()
	}()

	orderKey := func(p *points.Points) int64 {
		return 0
	}

	switch writeStrategy {
	case MaximumLength:
		orderKey = func(p *points.Points) int64 {
			return int64(len(p.Data))
		}
	case TimestampOrder:
		orderKey = func(p *points.Points) int64 {
			return p.Data[0].Timestamp
		}
	}

	size := c.Len() * 2
	q := make(queue, size)
	index := int32(0)

	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shard.Lock()

		for _, p := range shard.items {
			if index < size {
				q[index].points = p
				q[index].orderKey = orderKey(p)
			} else {
				q = append(q, queueItem{p, orderKey(p)})
			}
			index++
		}

		shard.Unlock()
	}

	q = q[:index]

	switch writeStrategy {
	case MaximumLength:
		sort.Sort(sort.Reverse(byOrderKey(q)))
	case TimestampOrder:
		sort.Sort(byOrderKey(q))
	}

	l := len(q)
	if l == 0 {
		return nil
	}

	ch := make(chan *points.Points, l)
	for _, i := range q {
		ch <- i.points
	}

	return ch
}
