package v2read

import (
	"cmp"
	"encoding/json"
	"hash/maphash"
	"slices"
	"sync"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// pendingIndex finds a request's pending batches without scanning every
// owner's whole pending head. Owners only append to the published head between
// seals, so a generation reuses its predecessor's immutable chunks and adds one
// chunk for the new suffix. Chunks hold positions and name hashes only, never
// batches, and are built once by the first reader that needs them. Candidates
// are re-checked by exact name, so hash collisions cannot change results.
type pendingIndex struct {
	chunks []*pendingChunk
}

type pendingChunk struct {
	start, end int
	once       sync.Once
	keys       []pendingKey
	err        error
}

type pendingKey struct {
	hash uint64
	pos  int32
}

// maxPendingChunks bounds lookup work for owners that publish often but rarely
// seal; past it the next generation indexes its whole head as one chunk.
const maxPendingChunks = 64

var pendingSeed = maphash.MakeSeed()

// samePendingBatch reports whether two published descriptors are the same
// owner application. Point arenas are detached and immutable per packet.
func samePendingBatch(a, b *v2core.Batch) bool {
	if a.ID != b.ID || a.Order != b.Order || a.Clock != b.Clock || a.Registered != b.Registered || len(a.Points) != len(b.Points) || len(a.Policy) != len(b.Policy) {
		return false
	}
	if len(a.Points) > 0 && &a.Points[0] != &b.Points[0] {
		return false
	}
	return len(a.Policy) == 0 || &a.Policy[0] == &b.Policy[0]
}

// nextPendingIndex reuses old's chunks when pending extends oldPending.
func nextPendingIndex(old *pendingIndex, oldPending, pending []v2core.Batch) *pendingIndex {
	if len(pending) == 0 {
		return &pendingIndex{}
	}
	n := len(oldPending)
	if old != nil && n > 0 && n <= len(pending) && len(old.chunks) < maxPendingChunks &&
		samePendingBatch(&oldPending[0], &pending[0]) && samePendingBatch(&oldPending[n-1], &pending[n-1]) {
		if n == len(pending) {
			return old
		}
		chunks := make([]*pendingChunk, len(old.chunks), len(old.chunks)+1)
		copy(chunks, old.chunks)
		return &pendingIndex{chunks: append(chunks, &pendingChunk{start: n, end: len(pending)})}
	}
	return &pendingIndex{chunks: []*pendingChunk{{start: 0, end: len(pending)}}}
}

// pendingName is the exact name predicate the linear scan used.
func pendingName(b *v2core.Batch) (string, error) {
	if b.Registered != nil {
		return b.Registered.Name(), nil
	}
	var p v2core.RollupPolicy
	if err := json.Unmarshal(b.Policy, &p); err != nil {
		return "", err
	}
	return string(p.Name), nil
}

// load indexes pending[start:end]. Every holder of the chunk publishes the
// same batches at these positions, so any holder's slice may build it.
func (c *pendingChunk) load(pending []v2core.Batch) error {
	c.once.Do(func() {
		keys := make([]pendingKey, c.end-c.start)
		for i := c.start; i < c.end; i++ {
			name, err := pendingName(&pending[i])
			if err != nil {
				c.err = err
				return
			}
			keys[i-c.start] = pendingKey{hash: maphash.String(pendingSeed, name), pos: int32(i)}
		}
		slices.SortFunc(keys, func(a, b pendingKey) int {
			if c := cmp.Compare(a.hash, b.hash); c != 0 {
				return c
			}
			return cmp.Compare(a.pos, b.pos)
		})
		c.keys = keys
	})
	return c.err
}

// candidates returns ascending pending positions whose name hash matches one
// of names. The caller still applies the exact name predicate.
func (x *pendingIndex) candidates(pending []v2core.Batch, names []string) ([]int32, error) {
	if len(x.chunks) == 0 {
		return nil, nil
	}
	hashes := make([]uint64, len(names))
	for i, n := range names {
		hashes[i] = maphash.String(pendingSeed, n)
	}
	var out []int32
	for _, c := range x.chunks {
		if err := c.load(pending); err != nil {
			return nil, err
		}
		for _, h := range hashes {
			i, _ := slices.BinarySearchFunc(c.keys, h, func(k pendingKey, h uint64) int { return cmp.Compare(k.hash, h) })
			for ; i < len(c.keys) && c.keys[i].hash == h; i++ {
				out = append(out, c.keys[i].pos)
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}
