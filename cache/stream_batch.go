package cache

import (
	"weak"

	"github.com/go-graphite/go-carbon/points"
)

// AddStreamPoints admits a consecutive run from one stream dictionary slot.
// It borrows data and slot until return. Admission and logging remain per point.
func (c *Cache) AddStreamPoints(name []byte, data []points.Point, slot *any) {
	for len(data) > 0 {
		n := 0
		if slot != nil {
			if series, ok := (*slot).(*StreamSeries); ok && series.cache == c {
				n = c.tryStreamPoints(series, data[:min(64, len(data))])
			}
		}
		if n == 0 {
			p := data[0]
			c.AddStreamPoint(name, p.Value, p.Timestamp, slot)
			n = 1
		}
		data = data[n:]
	}
}

// A locked run cannot be removed or rehashed. Recheck settings/admission per row:
// a quota callback may publish diversion or change its own state mid-run.
func (c *Cache) tryStreamPoints(series *StreamSeries, data []points.Point) (n int) {
	h := series.hint.Load()
	if h == 0 {
		return 0
	}
	sh := c.data[series.hash&(shardCount-1)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	it := series.item
	if it == nil {
		slot := uint32(h&0xffffff) - 1
		if h == 0 || int(slot) >= len(sh.items.slots) {
			return 0
		}
		it = sh.items.slots[slot].item
		if it == nil || it.streamGeneration != h>>24 {
			return 0
		}
		if it.streamSeries == (weak.Pointer[StreamSeries]{}) {
			bindStreamItem(series, it)
		}
	}
	for _, p := range data {
		s := c.settings.Load().(*cacheSettings)
		if s.xlog != nil || s.tagsEnabled || !c.addPointLocked(sh, s, it, series.hash, p.Value, p.Timestamp) {
			break
		}
		n++
	}
	return n
}
