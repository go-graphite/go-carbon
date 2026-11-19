package v2read

import (
	"encoding/json"
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
	// hashes[i] is the name hash of pending[start+i]; heads/next chain equal
	// buckets so lookups need no sort and no name bytes.
	hashes []uint64
	heads  []int32
	next   []int32
	err    error
}

// maxPendingChunks bounds lookup work for owners that publish often but rarely
// seal; past it the next generation indexes its whole head as one chunk.
const maxPendingChunks = 64

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

// pendingHash is the hash of pendingName. It reads the batch's own NameHash
// when set, then the registered identity, and only then the name bytes.
func pendingHash(b *v2core.Batch) (uint64, error) {
	if b.NameHash != 0 {
		return b.NameHash, nil
	}
	if b.Registered != nil {
		return b.Registered.NameHash(), nil
	}
	name, err := pendingName(b)
	return v2core.NameHash(name), err
}

// load indexes pending[start:end]. Every holder of the chunk publishes the
// same batches at these positions, so any holder's slice may build it.
func (c *pendingChunk) load(pending []v2core.Batch) error {
	c.once.Do(func() {
		n := c.end - c.start
		hashes := make([]uint64, n)
		for i := range hashes {
			h, err := pendingHash(&pending[c.start+i])
			if err != nil {
				c.err = err
				return
			}
			hashes[i] = h
		}
		size := 1
		for size < 2*n {
			size <<= 1
		}
		heads := make([]int32, size)
		for i := range heads {
			heads[i] = -1
		}
		next := make([]int32, n)
		mask := uint64(size - 1)
		for i := n - 1; i >= 0; i-- { // descending inserts leave each chain ascending
			b := hashes[i] & mask
			next[i] = heads[b]
			heads[b] = int32(i)
		}
		c.hashes, c.heads, c.next = hashes, heads, next
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
		hashes[i] = v2core.NameHash(n)
	}
	var out []int32
	for _, c := range x.chunks {
		if err := c.load(pending); err != nil {
			return nil, err
		}
		mask := uint64(len(c.heads) - 1)
		for _, h := range hashes {
			for j := c.heads[h&mask]; j >= 0; j = c.next[j] {
				if c.hashes[j] == h {
					out = append(out, int32(c.start)+j)
				}
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}
