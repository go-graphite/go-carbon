package whisperio

import (
	"bytes"
	"maps"
	"sync"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Scratch is leased to one update. Each block is handed out only once, including
// across failed private attempts. Flush encodes owned bytes before return. Retain
// at most 128 blocks (258KiB) per lease, even for whole-file rewrites; excess
// temporaries use the ordinary allocator. Pool eviction is always safe.
type continuationScratch struct {
	blocks [128]*store.PointBlock
	used   int
}

var continuationScratchPool = sync.Pool{New: func() any { return new(continuationScratch) }}

func (fs *continuationFiles) pointBlockScratch() *store.PointBlock {
	if fs.update != nil {
		return fs.update.scratch.block()
	}
	return new(store.PointBlock)
}

func (s *continuationScratch) block() *store.PointBlock {
	if s == nil || s.used == len(s.blocks) {
		return new(store.PointBlock)
	}
	i := s.used
	s.used++
	if s.blocks[i] == nil {
		s.blocks[i] = new(store.PointBlock)
	} else {
		*s.blocks[i] = store.PointBlock{}
	}
	return s.blocks[i]
}

// Manifests are cached only for one Update, by their complete encoded value.
// Baseline, successful writes and discarded attempts can therefore share only
// identical, validated immutable layouts. Every open still reads its own
// manifest and owns its header; allocation pages become owned on first change.
// This is separate from the store's revision-fenced writer-state cache.
type continuationUpdateState struct {
	scratch        *continuationScratch
	reuseManifests bool
	entries        map[string]*continuationFileData
	bytes          int
}

func (c *continuationUpdateState) decode(encoded []byte) (*continuationFileData, error) {
	// Single-open updates need no immutable template or manifest key copy.
	// Proof and sum-cache processing enable reuse before their repeated opens.
	if c == nil || !c.reuseManifests {
		return decodeContinuationManifest(encoded)
	}
	d := c.entries[string(encoded)]
	if d == nil {
		var err error
		d, err = decodeContinuationManifest(encoded)
		if err != nil {
			return nil, err
		}
		// Include a conservative allowance for the decoded map, plus the raw
		// key, header and archive descriptors in the operation's retention cap.
		size := len(encoded) + len(d.header) + len(d.pages)*32 + len(d.layout.Archives)*64
		if len(c.entries) >= 8 || size > (1<<20)-c.bytes {
			return d, nil
		}
		if c.entries == nil {
			c.entries = make(map[string]*continuationFileData)
		}
		c.entries[string(encoded)] = d
		c.bytes += size
	}
	view := *d
	view.header = bytes.Clone(d.header)
	view.sharedPages = true
	return &view, nil
}

func (d *continuationFileData) allocatePage(page uint32) {
	if d.pages[page] {
		return
	}
	if d.sharedPages {
		d.pages = maps.Clone(d.pages)
		d.sharedPages = false
	}
	d.pages[page] = true
	d.sortedPages = nil
}
