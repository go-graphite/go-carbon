package cache

import (
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

// item holds a metric's queued points. While in Shard.items they are
// compact. A Points header is allocated only at pop, or when pending recovery
// needs a stable identity before pop. Quota state stays inline on the ingest
// path; ordinary queued items have no raw slice header or separate identity.
type item struct {
	hash               uint64 // exact map hash, including forced collisions in tests
	logFirst           uint64 // first unpersisted generation while queued; see logTracked
	streamGeneration   uint64 // assigned lazily under the owning shard lock
	inputlog.StreamRef        // owns the canonical name and atomic input-log hint
	compact            compactPoints
	// spread is the metric's share of the writeout spread (see
	// SetWriteoutSpread), fixed at creation so batching checks need no hash.
	spread uint16
	// flags holds sparse-spread, startup-window and queued log ownership bits.
	// Sparse means this metric last wrote at least writeout-max-delay before
	// this item; startup holds its first batching window to the chosen phase.
	flags uint8
	// slot locates the metric in its shard's sparseLast table.
	slot            uint32
	firstArrival    time.Time
	throttle        any            // hot for every configured quota check
	pending         *points.Points // allocated only when recovery needs a queued identity
	indexGeneration uint64
}

const (
	itemSparse uint8 = 1 << iota
	itemStartup
	itemLogTracked
)

func (it *item) has(flag uint8) bool { return it.flags&flag != 0 }
func (it *item) setFlag(flag uint8, value bool) {
	if value {
		it.flags |= flag
	} else {
		it.flags &^= flag
	}
}

func newItem(metric string, capacity int) *item {
	h := writeoutHash(metric)
	it := &item{StreamRef: inputlog.StreamRef{Metric: metric}, compact: compactPoints{buf: make([]byte, 0, capacity)}, spread: uint16(h >> 48), slot: sparseSlot(h)}
	return it
}

func (it *item) count() int { return it.compact.n }

// decode moves the queued points into Data, which callers may then own.
// Called with the item unreachable from the shard, or with its shard locked.
func (it *item) decode() *points.Points {
	p := it.pending
	if p == nil {
		p = &points.Points{Metric: it.Metric}
	}
	p.Data = it.compact.appendDecoded(nil)
	it.compact = compactPoints{}
	return p
}

// Recovery alone needs a queued Points identity. The raw header is the
// on-demand side structure; a normal item creates its header only at pop.
// Quota state remains inline: moving that hot slot behind a side pointer
// exceeded the ingest CPU budget in the stateful workload.
func (it *item) identity() *points.Points {
	if it.pending == nil {
		it.pending = &points.Points{Metric: it.Metric}
	}
	return it.pending
}

func (it *item) throttleState() any { return it.throttle }

// Called only for items already in this shard's map. New replacements call
// identity directly and setHashed charges their complete state on insertion.
func (sh *Shard) itemIdentity(it *item) *points.Points {
	before := it.extraMemory()
	p := it.identity()
	sh.items.bytes += it.extraMemory() - before
	return p
}

func (sh *Shard) setItemThrottle(it *item, state any) bool {
	it.throttle = state
	return false
}

type itemSlot struct {
	item *item
}

// itemMap keeps pointers and one control byte per slot. Controls are zero for
// empty, one for deleted, or a seven-bit fingerprint with its high bit set.
// Items retain the exact hash; full names disambiguate even exact collisions.
type itemMap struct {
	slots     []itemSlot
	control   []byte
	n, used   int
	bytes     int64
	deadlines *queueDeadlines
}

func (m *itemMap) get(name string) *item {
	return m.getHashed(xxh3.HashString(name), name)
}

func (m *itemMap) getHashed(hash uint64, name string) *item {
	if len(m.slots) == 0 {
		return nil
	}
	tag := itemTag(hash)
	mask := len(m.slots) - 1
	for i := int(hash>>shardBits) & mask; ; i = (i + 1) & mask {
		slot := &m.slots[i]
		if slot.item == nil {
			if m.control[i] == 0 {
				return nil
			}
		} else if m.control[i] == tag && slot.item.hash == hash && slot.item.Metric == name {
			return slot.item
		}
	}
}

func (m *itemMap) getBytesHashed(hash uint64, name []byte) *item {
	if len(m.slots) == 0 {
		return nil
	}
	tag := itemTag(hash)
	mask := len(m.slots) - 1
	for i := int(hash>>shardBits) & mask; ; i = (i + 1) & mask {
		slot := &m.slots[i]
		if slot.item == nil {
			if m.control[i] == 0 {
				return nil
			}
		} else if m.control[i] == tag && slot.item.hash == hash && slot.item.Metric == string(name) {
			return slot.item
		}
	}
}

func (m *itemMap) set(name string, it *item) { m.setHashed(xxh3.HashString(name), it) }

func (m *itemMap) setHashed(hash uint64, it *item) {
	if len(m.slots) == 0 {
		m.slots = make([]itemSlot, 8)
		m.control = make([]byte, 8)
		m.deadlines = nil
	}
	if (m.used+1)*2 > len(m.slots) {
		n := len(m.slots)
		if (m.n+1)*2 > n {
			n *= 2
		}
		old := m.slots
		m.deadlines = nil
		m.slots, m.n, m.used, m.bytes = make([]itemSlot, n), 0, 0, 0
		m.control = make([]byte, n)
		for _, slot := range old {
			if slot.item != nil {
				m.setHashed(slot.item.hash, slot.item)
			}
		}
	}
	tag := itemTag(hash)
	mask := len(m.slots) - 1
	free := -1
	for i := int(hash>>shardBits) & mask; ; i = (i + 1) & mask {
		slot := &m.slots[i]
		if slot.item == nil {
			if free < 0 {
				free = i
			}
			if m.control[i] == 0 {
				if m.control[free] == 0 {
					m.used++
				}
				it.hash = hash
				m.slots[free] = itemSlot{item: it}
				m.control[free] = tag
				m.invalidateDeadline(free)
				m.n++
				m.bytes += it.memoryBytes()
				return
			}
		} else if m.control[i] == tag && slot.item.hash == hash && slot.item.Metric == it.Metric {
			m.bytes += it.memoryBytes() - slot.item.memoryBytes()
			it.hash = hash
			slot.item = it
			m.invalidateDeadline(i)
			return
		}
	}
}

func (m *itemMap) remove(name string) { m.removeHashed(xxh3.HashString(name), name) }

func (m *itemMap) removeHashed(hash uint64, name string) {
	if len(m.slots) == 0 {
		return
	}
	tag := itemTag(hash)
	mask := len(m.slots) - 1
	for i := int(hash>>shardBits) & mask; ; i = (i + 1) & mask {
		slot := &m.slots[i]
		if slot.item == nil {
			if m.control[i] == 0 {
				return
			}
		} else if m.control[i] == tag && slot.item.hash == hash && slot.item.Metric == name {
			m.bytes -= slot.item.memoryBytes()
			*slot = itemSlot{}
			m.control[i] = 1
			m.invalidateDeadline(i)
			m.n--
			return
		}
	}
}

func (m *itemMap) len() int { return m.n }
func (m *itemMap) all(yield func(string, *item) bool) {
	for _, slot := range m.slots {
		if slot.item != nil && !yield(slot.item.Metric, slot.item) {
			return
		}
	}
}

func itemTag(hash uint64) byte { return byte(hash>>57) | 0x80 }
