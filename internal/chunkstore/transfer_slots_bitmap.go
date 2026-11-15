package chunkstore

import "sync"

// Bound scratch independently of the sender's advertised archive capacity.
// Sparse or larger rings keep the point-sized map path.
const transferBitmapSlots = 1 << 16

var transferBitmapPool = sync.Pool{New: func() any { return new([transferBitmapSlots / 64]uint64) }}

func retainTransferSlotsBitmap(points []Point, retention Retention) []Point {
	bits := transferBitmapPool.Get().(*[transferBitmapSlots / 64]uint64)
	words := bits[:(retention.Count+63)/64]
	step, count := int64(retention.Step), int64(retention.Count)
	start := len(points)
	for i := len(points) - 1; i >= 0; i-- {
		slot := points[i].Timestamp / step % count
		word, mask := slot/64, uint64(1)<<uint(slot%64)
		if words[word]&mask != 0 {
			continue
		}
		words[word] |= mask
		start--
		points[start] = points[i]
	}
	clear(words)
	transferBitmapPool.Put(bits)
	return points[start:]
}
