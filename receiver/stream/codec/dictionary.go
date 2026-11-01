package codec

// Names and lookup metadata contain no per-name pointers. Chunks never move;
// resets reuse their allocation, so churn does not create a second live arena.
type nameRef struct{ chunk, offset, size uint32 }
type nameArena struct {
	chunks  [][]byte
	chunk   int
	bytes   int
	quantum int
}

func (a *nameArena) name(r nameRef) []byte { return a.chunks[r.chunk][r.offset : r.offset+r.size] }
func (a *nameArena) extra(n int) int {
	for i := a.chunk; i < len(a.chunks); i++ {
		if cap(a.chunks[i])-len(a.chunks[i]) >= n {
			return 0
		}
	}
	return max(a.quantum, n) + (nextCapacity(len(a.chunks)+1, cap(a.chunks))-cap(a.chunks))*24
}
func (a *nameArena) add(name []byte) nameRef {
	for a.chunk < len(a.chunks) && cap(a.chunks[a.chunk])-len(a.chunks[a.chunk]) < len(name) {
		a.chunk++
	}
	if a.chunk == len(a.chunks) {
		n := max(a.quantum, len(name))
		size := nextCapacity(len(a.chunks)+1, cap(a.chunks))
		if size > cap(a.chunks) {
			chunks := make([][]byte, len(a.chunks), size)
			copy(chunks, a.chunks)
			a.bytes += (size - cap(a.chunks)) * 24
			a.chunks = chunks
		}
		a.chunks = append(a.chunks, make([]byte, 0, n))
		a.bytes += n
	}
	b := a.chunks[a.chunk]
	r := nameRef{uint32(a.chunk), uint32(len(b)), uint32(len(name))}
	a.chunks[a.chunk] = append(b, name...)
	return r
}
func (a *nameArena) reset() {
	for i := range a.chunks {
		a.chunks[i] = a.chunks[i][:0]
	}
	a.chunk = 0
}

func nameHash(name []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, b := range name {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
}
func nextCapacity(n, current int) int {
	if n <= current {
		return current
	}
	return max(16, current*2)
}

func indexCapacity(n, current int) int {
	if n*8 <= current*7 {
		return current
	}
	return max(16, current*2)
}

func mixHash(h uint64) uint64 {
	h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
	h = (h ^ (h >> 27)) * 0x94d049bb133111eb
	return h ^ (h >> 31)
}
