package chunkstore

import (
	"bytes"
	"encoding/binary"
)

// ContinuationStateSlots assigns a bounded cache slot to a frequently accessed
// data key. A slot keeps one exact key/value pair, never a whole data prefix.
// Slot names start with zero, outside the codec's persistent key space.
type ContinuationStateSlots interface {
	StateSlot([]byte) []byte
}

func (io *continuationIO) stateSlot(key []byte) []byte {
	if io.state == nil {
		return nil
	}
	slots, ok := io.state.keys.(ContinuationStateSlots)
	if !ok {
		return nil
	}
	return slots.StateSlot(key)
}

func (io *continuationIO) slotValue(slot, key []byte) ([]byte, bool) {
	value, ok := io.state.lookup(slot)
	if !ok || len(value) < 2 {
		return nil, false
	}
	n := int(binary.BigEndian.Uint16(value))
	if len(value) < 2+n || !bytes.Equal(value[2:2+n], key) {
		return nil, false
	}
	return bytes.Clone(value[2+n:]), true
}

func (io *continuationIO) setSlot(slot, key, value []byte) {
	data := make([]byte, 2, 2+len(key)+len(value))
	binary.BigEndian.PutUint16(data, uint16(len(key)))
	data = append(append(data, key...), value...)
	io.state.mutable().set(slot, data)
}

// Deletions and rewrites can change a cached tail's ownership. Drop slots in
// this view conservatively; no old slot survives publication of the mutation.
func (io *continuationIO) clearSlots() {
	if io.state != nil {
		state := io.state.mutable()
		state.thaw()
		for key := range state.keys {
			if len(key) > 0 && key[0] == 0 {
				delete(state.keys, key)
			}
		}
	}
}

// Tails use spare metadata-cache capacity. Refuse optional data before it
// would evict another metric's metadata; a full cache keeps the old read path.
func (c *stateCache) fitSlots(sh *stateShard, hash uint64, name string, st *metricState) *metricState {
	if st == nil || sh.bytes+st.size <= c.shardBudget {
		return st
	}
	old := int64(0)
	if _, slot := sh.find(hash, name); slot >= 0 {
		old = int64(sh.slots[slot].n) + int64(stateSlotOverhead)
	}
	if sh.bytes-old+st.size <= c.shardBudget {
		return st
	}
	keys, _ := st.walk([]byte{0})
	if len(keys) == 0 {
		return st
	}
	trimmed := st.clone(st.meta)
	trimmed.thaw()
	for _, key := range keys {
		delete(trimmed.keys, string(key))
	}
	c.slotRefusals.Add(1)
	return trimmed.seal()
}
