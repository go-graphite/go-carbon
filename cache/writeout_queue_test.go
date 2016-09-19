package cache

import (
	"fmt"
	"github.com/lomik/go-carbon/cache/cmap"
	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
	"testing"
)

func makeQ(n int) *WriteoutQueue {
	c := cmap.New()
	q := NewQueue(&c, &cacheStats{})

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%d", i)
		c.Upsert(key, points.OnePoint(key, 42, 10), func(_ bool, _, p *points.Points) (*points.Points, bool) { return p, false })
	}
	q.Update()
	return &q
}

func TestChop(t *testing.T) {
	assert := assert.New(t)

	q := makeQ(0)
	chops := q.Chop(16)
	assert.Nil(chops)
	for c := range chops {
		assert.NotNil(c)
	}

	q = makeQ(1)
	chops = q.Chop(16)
	assert.Len(chops, 1)
	for c := range chops {
		assert.NotNil(c)
	}

	q = makeQ(16)
	chops = q.Chop(16)
	assert.Len(chops, 1)
	for c := range chops {
		assert.NotNil(c)
	}

	q = makeQ(17)
	chops = q.Chop(16)
	assert.Len(chops, 1)
	for c := range chops {
		assert.NotNil(c)
	}

	q = makeQ(MIN_POINTS_PER_BATCH + 1)
	chops = q.Chop(16)
	assert.Len(chops, 2)
	for c := range chops {
		assert.NotNil(c)
	}
	assert.Len(chops[0], MIN_POINTS_PER_BATCH)
	assert.Len(chops[1], 1)
}
