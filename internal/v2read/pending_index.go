package v2read

import (
	"context"
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

// nextPendingIndex reuses old's chunks when pending extends oldPending.
func nextPendingIndex(old *pendingIndex, oldPending, pending v2core.PendingView) *pendingIndex {
	if pending.Len() == 0 {
		return &pendingIndex{}
	}
	n := oldPending.Len()
	if old != nil && n > 0 && len(old.chunks) < maxPendingChunks && pending.Extends(oldPending) {
		if n == pending.Len() {
			return old
		}
		chunks := make([]*pendingChunk, len(old.chunks), len(old.chunks)+1)
		copy(chunks, old.chunks)
		return &pendingIndex{chunks: append(chunks, &pendingChunk{start: n, end: pending.Len()})}
	}
	return &pendingIndex{chunks: []*pendingChunk{{start: 0, end: pending.Len()}}}
}

// pendingName is the exact name predicate the linear scan used.
func pendingName(registered *v2core.ApplicationPolicy, policy []byte) (string, error) {
	if registered != nil {
		return registered.Name(), nil
	}
	var p v2core.RollupPolicy
	if err := json.Unmarshal(policy, &p); err != nil {
		return "", err
	}
	return string(p.Name), nil
}

// pendingHash is the hash of pendingName without reading name bytes when the
// application carries a registered identity.
func pendingHash(registered *v2core.ApplicationPolicy, policy []byte) (uint64, error) {
	if registered != nil {
		return registered.NameHash(), nil
	}
	name, err := pendingName(registered, policy)
	return v2core.NameHash(name), err
}

// load indexes positions [start, end). Every holder of the chunk publishes
// the same applications at these positions, so any holder's view may build it.
func (c *pendingChunk) load(pending v2core.PendingView) error {
	c.once.Do(func() {
		n := c.end - c.start
		hashes := make([]uint64, n)
		pending.Refs(c.start, c.end, func(pos int, _ uint64, registered *v2core.ApplicationPolicy, policy []byte) bool {
			h, err := pendingHash(registered, policy)
			if err != nil {
				c.err = err
				return false
			}
			hashes[pos-c.start] = h
			return true
		})
		if c.err != nil {
			return
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

// selectPending returns the applications whose exact name is in names, in
// arrival order. Only matching applications are decoded.
func selectPending(ctx context.Context, x *pendingIndex, pending v2core.PendingView, names []string) ([]v2core.Batch, error) {
	positions, err := x.candidates(pending, names)
	if err != nil {
		return nil, err
	}
	wanted := make(map[string]bool, len(names))
	for _, n := range names {
		wanted[n] = true
	}
	var out []v2core.Batch
	covered := -1 // positions before this belong to an application already taken
	for _, i := range positions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if int(i) < covered {
			continue
		}
		_, registered, policy := pending.Ref(int(i))
		name, err := pendingName(registered, policy)
		if err != nil {
			return nil, err
		}
		if wanted[name] {
			b, _, end := pending.Application(int(i))
			out = append(out, b)
			covered = end
		}
	}
	return out, nil
}

// candidates returns ascending pending positions whose name hash matches one
// of names. The caller still applies the exact name predicate.
func (x *pendingIndex) candidates(pending v2core.PendingView, names []string) ([]int32, error) {
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
