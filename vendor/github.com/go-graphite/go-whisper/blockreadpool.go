package whisper

import "sync"

// A source-window read owns its scratch until decoding has returned scalar
// points. Reuse bounded size classes; unusually large blocks stay unpooled.
const (
	readScratchMin     = 4 << 10
	readScratchMax     = 1 << 20
	readScratchClasses = 9
)

type blockReadScratch struct {
	data []byte
	pool *sync.Pool
}

var blockReadPools [readScratchClasses]sync.Pool

func acquireBlockReadScratch(size int) *blockReadScratch {
	if size < 0 || size > readScratchMax {
		return &blockReadScratch{data: make([]byte, size)}
	}
	class, capacity := 0, readScratchMin
	for capacity < size {
		class++
		capacity *= 2
	}
	pool := &blockReadPools[class]
	if value := pool.Get(); value != nil {
		return value.(*blockReadScratch)
	}
	return &blockReadScratch{data: make([]byte, capacity), pool: pool}
}

func (b *blockReadScratch) release() {
	if b.pool != nil {
		b.pool.Put(b)
	}
}
