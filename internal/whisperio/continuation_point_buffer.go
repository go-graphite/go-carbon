package whisperio

import (
	"bytes"
	"sort"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A small rollup window can flush the same 128-point group many times in one
// update. Keep its decoded value until the update ends or a walk needs it.
// The indexed batch still sees every final value before the store commits.
type continuationPointBuffer struct {
	store.ContinuationIO
	blocks map[string]*store.PointBlock
	// reads belongs to this metric update's indexed batch, never a later
	// revision or snapshot. Keep unchanged rollup/base groups too: unlike
	// blocks, these must not be written by flush.
	reads                    map[string]*store.PointBlock
	cacheReads, writeThrough bool
}

var coalescePointWrites = true

func (b *continuationPointBuffer) TimeBuckets() bool {
	v, ok := b.ContinuationIO.(store.ContinuationTimeBuckets)
	return ok && v.TimeBuckets()
}

func (b *continuationPointBuffer) ContinuationRevision() uint64 {
	if v, ok := b.ContinuationIO.(store.ContinuationRevision); ok {
		return v.ContinuationRevision()
	}
	return 0
}

func (b *continuationPointBuffer) Probe(key []byte) ([]byte, error) {
	if block := b.blocks[string(key)]; block != nil {
		return store.EncodePointBlock(block), nil
	}
	if prober, ok := b.ContinuationIO.(store.ContinuationProber); ok {
		return prober.Probe(key)
	}
	return b.ContinuationIO.Get(key)
}

func (b *continuationPointBuffer) Get(key []byte) ([]byte, error) {
	if block := b.blocks[string(key)]; block != nil {
		return store.EncodePointBlock(block), nil
	}
	return b.ContinuationIO.Get(key)
}

func (b *continuationPointBuffer) Set(key, value []byte) error {
	delete(b.blocks, string(key))
	delete(b.reads, string(key))
	return b.ContinuationIO.Set(key, value)
}

func (b *continuationPointBuffer) Delete(key []byte) error {
	delete(b.blocks, string(key))
	delete(b.reads, string(key))
	return b.ContinuationIO.Delete(key)
}

func (b *continuationPointBuffer) DeletePrefix(prefix []byte) error {
	for key := range b.reads {
		if bytes.HasPrefix([]byte(key), prefix) {
			delete(b.reads, key)
		}
	}
	for key := range b.blocks {
		if bytes.HasPrefix([]byte(key), prefix) {
			delete(b.blocks, key)
		}
	}
	return b.ContinuationIO.DeletePrefix(prefix)
}

func (b *continuationPointBuffer) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	if err := b.flush(prefix); err != nil {
		return err
	}
	return b.ContinuationIO.Visit(prefix, visit)
}

func (b *continuationPointBuffer) set(key []byte, block *store.PointBlock) error {
	delete(b.reads, string(key))
	if b.writeThrough {
		if err := b.Set(key, store.EncodePointBlock(block)); err != nil {
			return err
		}
		b.remember(key, block)
		return nil
	}
	// Bound scratch memory even during a whole-file rewrite.
	if len(b.blocks) >= 64 {
		if err := b.flush(nil); err != nil {
			return err
		}
	}
	if b.blocks == nil {
		b.blocks = make(map[string]*store.PointBlock)
	}
	b.blocks[string(key)] = block
	return nil
}

// point returns an owned copy: callers may patch it before set succeeds.
func (b *continuationPointBuffer) point(key []byte) *store.PointBlock {
	block := b.blocks[string(key)]
	if block == nil {
		block = b.reads[string(key)]
	}
	if block == nil {
		return nil
	}
	owned := *block
	return &owned
}

func (b *continuationPointBuffer) remember(key []byte, block *store.PointBlock) {
	if !b.cacheReads {
		return
	}
	// At most 64 decoded groups (~132KiB) per active update, independent of
	// metric cardinality or the length of a rewrite. Clearing only reads
	// cannot flush or discard pending writes.
	if len(b.reads) >= 64 {
		clear(b.reads)
	}
	if b.reads == nil {
		b.reads = make(map[string]*store.PointBlock)
	}
	owned := *block
	b.reads[string(key)] = &owned
}

func (b *continuationPointBuffer) flush(prefix []byte) error {
	var keys []string
	for key := range b.blocks {
		if bytes.HasPrefix([]byte(key), prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := b.ContinuationIO.Set([]byte(key), store.EncodePointBlock(b.blocks[key])); err != nil {
			return err
		}
		delete(b.blocks, key)
	}
	return nil
}
