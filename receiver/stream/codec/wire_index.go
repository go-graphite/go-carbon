package codec

const wireBlockBits = 6
const wireBlockSize = 1 << wireBlockBits
const wireIDMask = 1<<24 - 1

// A wire slot needs 24 bytes: two predictors and a generation-checked global
// token. Its high bit records predictor validity. Blocks avoid geometric per-name capacity waste.
type wirePoint struct {
	ts    int64
	bits  uint64
	token uint64
}

const predictorValid uint64 = 1 << 63

func (p *wirePoint) global() uint32    { return uint32(p.token) & 0xffffff }
func (p *wirePoint) nameToken() uint64 { return p.token &^ predictorValid }
func (p *wirePoint) valid() bool       { return p.token&predictorValid != 0 }

type wireBlock [wireBlockSize]wirePoint
type wireStates struct {
	blocks []*wireBlock
	n      int
}

func (s *wireStates) at(id uint32) *wirePoint {
	return &s.blocks[id>>wireBlockBits][id&(wireBlockSize-1)]
}
func (s *wireStates) add(token uint64) uint32 {
	id := uint32(s.n)
	if int(id>>wireBlockBits) == len(s.blocks) {
		size := nextCapacity(len(s.blocks)+1, cap(s.blocks))
		if size > cap(s.blocks) {
			blocks := make([]*wireBlock, len(s.blocks), size)
			copy(blocks, s.blocks)
			s.blocks = blocks
		}
		s.blocks = append(s.blocks, new(wireBlock))
	}
	*s.at(id) = wirePoint{token: token}
	s.n++
	return id
}
func (s *wireStates) projectedBytes() int {
	blocks := max(len(s.blocks), (s.n+wireBlockSize)/wireBlockSize)
	return blocks*wireBlockSize*24 + nextCapacity(blocks, cap(s.blocks))*8
}
func (s *wireStates) bytes() int { return len(s.blocks)*wireBlockSize*24 + cap(s.blocks)*8 }

// Four bytes per slot: a 24-bit wire id + 1 and an 8-bit hash filter. The
// filter avoids random state-table loads on misses; a full global-id comparison
// still disambiguates every collision. Neither ids nor filters go on the wire.
type wireIndex []uint32

func (m wireIndex) find(global uint32, states *wireStates) (uint32, bool) {
	if global == 0 || len(m) == 0 {
		return 0, false
	}
	hash := mixHash(uint64(global))
	tag := uint32(hash >> 56)
	mask := len(m) - 1
	for i := int(hash) & mask; ; i = (i + 1) & mask {
		slot := m[i]
		if slot == 0 {
			return 0, false
		}
		if slot>>24 == tag {
			id := (slot & wireIDMask) - 1
			if states.at(id).global() == global {
				return id, true
			}
		}
	}
}
func (m wireIndex) insert(global, wire uint32) {
	hash := mixHash(uint64(global))
	mask := len(m) - 1
	for i := int(hash) & mask; ; i = (i + 1) & mask {
		if m[i] == 0 {
			m[i] = uint32(hash>>56)<<24 | (wire + 1)
			return
		}
	}
}
func (m *wireIndex) grow(n int, states *wireStates) {
	size := indexCapacity(n, len(*m))
	if size == len(*m) {
		return
	}
	old := *m
	*m = make(wireIndex, size)
	for _, slot := range old {
		if slot != 0 {
			id := (slot & wireIDMask) - 1
			m.insert(states.at(id).global(), id)
		}
	}
}
