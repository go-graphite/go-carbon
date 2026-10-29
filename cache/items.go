package cache

import "github.com/zeebo/xxh3"

type itemSlot struct {
	hash uint64
	item *item
}

// itemMap uses the shard hash directly. A nil item with a nonzero hash is a
// deleted slot; a zero slot ends a probe. Full names disambiguate collisions.
type itemMap struct {
	slots   []itemSlot
	n, used int
}

func (m *itemMap) get(name string) *item {
	return m.getHashed(xxh3.HashString(name), name)
}

func (m *itemMap) getHashed(hash uint64, name string) *item {
	if len(m.slots) == 0 {
		return nil
	}
	mask := len(m.slots) - 1
	for i := int(hash>>shardBits) & mask; ; i = (i + 1) & mask {
		slot := &m.slots[i]
		if slot.item == nil {
			if slot.hash == 0 {
				return nil
			}
		} else if slot.hash == hash && slot.item.Metric == name {
			return slot.item
		}
	}
}

func (m *itemMap) getBytesHashed(hash uint64, name []byte) *item {
	if len(m.slots) == 0 {
		return nil
	}
	mask := len(m.slots) - 1
	for i := int(hash>>shardBits) & mask; ; i = (i + 1) & mask {
		slot := &m.slots[i]
		if slot.item == nil {
			if slot.hash == 0 {
				return nil
			}
		} else if slot.hash == hash && slot.item.Metric == string(name) {
			return slot.item
		}
	}
}

func (m *itemMap) set(name string, it *item) { m.setHashed(xxh3.HashString(name), it) }

func (m *itemMap) setHashed(hash uint64, it *item) {
	if len(m.slots) == 0 {
		m.slots = make([]itemSlot, 8)
	}
	if (m.used+1)*2 > len(m.slots) {
		n := len(m.slots)
		if (m.n+1)*2 > n {
			n *= 2
		}
		old := m.slots
		m.slots, m.n, m.used = make([]itemSlot, n), 0, 0
		for _, slot := range old {
			if slot.item != nil {
				m.setHashed(slot.hash, slot.item)
			}
		}
	}
	mask := len(m.slots) - 1
	free := -1
	for i := int(hash>>shardBits) & mask; ; i = (i + 1) & mask {
		slot := &m.slots[i]
		if slot.item == nil {
			if free < 0 {
				free = i
			}
			if slot.hash == 0 {
				if m.slots[free].hash == 0 {
					m.used++
				}
				m.slots[free] = itemSlot{hash: hash, item: it}
				m.n++
				return
			}
		} else if slot.hash == hash && slot.item.Metric == it.Metric {
			slot.item = it
			return
		}
	}
}

func (m *itemMap) remove(name string) { m.removeHashed(xxh3.HashString(name), name) }

func (m *itemMap) removeHashed(hash uint64, name string) {
	if len(m.slots) == 0 {
		return
	}
	mask := len(m.slots) - 1
	for i := int(hash>>shardBits) & mask; ; i = (i + 1) & mask {
		slot := &m.slots[i]
		if slot.item == nil {
			if slot.hash == 0 {
				return
			}
		} else if slot.hash == hash && slot.item.Metric == name {
			*slot = itemSlot{hash: 1}
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
