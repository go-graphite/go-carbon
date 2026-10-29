package chunkstore

import (
	"sync"
	"sync/atomic"
)

// Pages cover 65,536 IDs in 8 KiB. Dense catalog IDs use one bit per metric;
// sparse/high IDs cannot force an allocation proportional to their maximum.
// Membership belongs to one traversal and never survives an interrupted process.
// IDSet tracks visited catalog IDs without allocating metric names or a dense
// array sized by an untrusted maximum ID. Its zero value is ready for use.
type IDSet struct {
	mu    sync.RWMutex
	pages map[uint64]*[1024]atomic.Uint64
}

// Mark records a nonzero catalog ID. It accepts a nil receiver.
func (v *IDSet) Mark(id uint64) {
	v.markBounded(id, 0)
}

// A bounded set is an expendable cache: eviction can only remove membership.
// Ordinary traversal Mark remains unlimited and never evicts visited IDs.
func (v *IDSet) markBounded(id uint64, maxPages int) {
	if v == nil || id == 0 {
		return
	}
	key := id >> 16
	v.mu.RLock()
	page := v.pages[key]
	v.mu.RUnlock()
	if page == nil {
		v.mu.Lock()
		if v.pages == nil {
			v.pages = make(map[uint64]*[1024]atomic.Uint64)
		}
		page = v.pages[key]
		if page == nil {
			if maxPages > 0 && len(v.pages) >= maxPages {
				for old := range v.pages {
					delete(v.pages, old)
					break
				}
			}
			page = &[1024]atomic.Uint64{}
			v.pages[key] = page
		}
		v.mu.Unlock()
	}
	page[(id&0xffff)>>6].Or(uint64(1) << (id & 63))
}

// Clear removes membership without allocating a page.
func (v *IDSet) Clear(id uint64) {
	if v == nil || id == 0 {
		return
	}
	v.mu.RLock()
	page := v.pages[id>>16]
	v.mu.RUnlock()
	if page != nil {
		page[(id&0xffff)>>6].And(^(uint64(1) << (id & 63)))
	}
}

// Contains reports whether this traversal marked id.
func (v *IDSet) Contains(id uint64) bool {
	if v == nil || id == 0 {
		return false
	}
	v.mu.RLock()
	page := v.pages[id>>16]
	v.mu.RUnlock()
	return page != nil && page[(id&0xffff)>>6].Load()&(uint64(1)<<(id&63)) != 0
}
