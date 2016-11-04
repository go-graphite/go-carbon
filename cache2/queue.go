package cache

import (
	"sort"

	"github.com/lomik/go-carbon/points"
)

type queueItem struct {
	point    *points.Points
	orderKey int64
}

type queue []queueItem

type byOrderKey queue

func (v byOrderKey) Len() int           { return len(v) }
func (v byOrderKey) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byOrderKey) Less(i, j int) bool { return v[i].orderKey < v[j].orderKey }

func (c *Cache) makeQueue() queue {

	orderKey := func(p *points.Points) int64 {
		return 0
	}

	switch c.writeStrategy {
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
				q[index].point = p
				q[index].orderKey = orderKey(p)
			} else {
				q = append(q, queueItem{p, orderKey(p)})
			}
			index++
		}

		shard.Unlock()
	}

	q = q[:index]

	switch c.writeStrategy {
	case MaximumLength:
		sort.Sort(sort.Reverse(byOrderKey(q)))
	case TimestampOrder:
		sort.Sort(byOrderKey(q))
	}

	return q
}
