package cache

import (
	"bytes"

	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

const lookupBatchSize = 64
const lookupBatchNameLimit = 4096

type pointLookupHint struct {
	name  string
	hash  uint64
	slot  int
	valid bool
}

// AddPointBatch borrows names and data until it returns. Points are admitted in
// input order with the ordinary per-point locks, quota checks and input log.
// Read-only hints warm independent canonical names before exact comparisons;
// every hint is validated again under the exclusive lock before use.
func (c *Cache) AddPointBatch(names [][]byte, data []points.Point) {
	if len(names) != len(data) {
		panic("cache: point batch length mismatch")
	}
	for len(data) > 0 {
		n := min(len(data), lookupBatchSize)
		// Retain the inexpensive existing path for single points and runs;
		// independent-name warming only pays when there are several names.
		run := 1
		for run < n && bytes.Equal(names[0], names[run]) {
			run++
		}
		switch {
		case run > 1:
			n = run
			c.AddPoints(names[0], data[:n])
		case n == 1:
			c.AddPoint(names[0], data[0].Value, data[0].Timestamp)
		default:
			c.addPointBatch(names[:n], data[:n])
		}
		names, data = names[n:], data[n:]
	}
}

func (c *Cache) addPointBatch(names [][]byte, data []points.Point) {
	var hints [lookupBatchSize]pointLookupHint
	maxLength := 0
	for i, name := range names {
		if len(name) > lookupBatchNameLimit || i > 0 && bytes.Equal(names[i-1], name) {
			continue
		}
		h := xxh3.Hash(name)
		sh := c.data[h&(shardCount-1)]
		sh.mu.RLock()
		if len(sh.items.slots) > 0 {
			mask := len(sh.items.slots) - 1
			for slot := int(h>>shardBits) & mask; ; slot = (slot + 1) & mask {
				control := sh.items.control[slot]
				if control == 0 {
					break
				}
				if control != itemTag(h) {
					continue
				}
				it := sh.items.slots[slot].item
				if it.hash == h {
					if len(it.Metric) == len(name) {
						hints[i] = pointLookupHint{name: it.Metric, hash: h, slot: slot, valid: true}
						maxLength = max(maxLength, len(name))
					}
					break
				}
			}
		}
		sh.mu.RUnlock()
	}
	// Interleave the first byte of each cache line across independent names.
	// Rejecting mismatches makes every read observable and avoids an unsafe or
	// architecture-specific prefetch primitive. Full equality still follows.
	for offset := 0; offset < maxLength; offset += 64 {
		for i := range names {
			hint := &hints[i]
			if hint.valid && offset < len(hint.name) && hint.name[offset] != names[i][offset] {
				hint.valid = false
			}
		}
	}
	for i := 0; i < len(names); {
		name := names[i]
		end := i + 1
		for end < len(names) && bytes.Equal(names[end], name) {
			end++
		}
		if end > i+1 {
			c.AddPoints(name, data[i:end])
			i = end
			continue
		}
		hint := &hints[i]
		p := data[i]
		i++
		if !hint.valid || hint.name != string(name) {
			c.AddPoint(name, p.Value, p.Timestamp)
			continue
		}
		if !c.addPointHint(hint, p) {
			c.AddPoint(name, p.Value, p.Timestamp)
		}
	}
}

// Keep the ordinary AddPoint panic/unwind behavior: a callback that panics
// must release the write lock before a caller can recover and keep ingesting.
func (c *Cache) addPointHint(hint *pointLookupHint, p points.Point) bool {
	s := c.settings.Load().(*cacheSettings)
	if s.xlog != nil || s.tagsEnabled {
		return false
	}
	sh := c.data[hint.hash&(shardCount-1)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if hint.slot >= len(sh.items.slots) {
		return false
	}
	it := sh.items.slots[hint.slot].item
	// Full name equality also handles removal, replacement and address reuse.
	if it == nil || it.hash != hint.hash || it.Metric != hint.name || c.settings.Load().(*cacheSettings).xlog != nil {
		return false
	}
	return c.addPointLocked(sh, s, it, hint.hash, p.Value, p.Timestamp)
}
