package whisperio

import store "github.com/go-graphite/go-carbon/internal/chunkstore"

// Limit idle large buffers globally instead of retaining one or more per CPU in
// sync.Pool. Each holds at most 8,192 points (128 KiB) and 160 KiB decompression
// scratch: eight idle buffers retain at most 2.25 MiB of payload capacity. Active
// readers own their buffers exclusively and never wait for an idle one.
const maxIdleSealedPointBuffers = 8

var sealedPointBuffers = make(chan *store.SealedPointBuffer, maxIdleSealedPointBuffers)

func getSealedPointBuffer() *store.SealedPointBuffer {
	select {
	case scratch := <-sealedPointBuffers:
		return scratch
	default:
		return new(store.SealedPointBuffer)
	}
}

func putSealedPointBuffer(scratch *store.SealedPointBuffer) {
	select {
	case sealedPointBuffers <- scratch:
	default:
		// Surplus active readers release their storage instead of growing idle RAM.
	}
}
