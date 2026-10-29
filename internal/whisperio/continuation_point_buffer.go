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
}

var coalescePointWrites = true

func (b *continuationPointBuffer) TimeBuckets() bool {
	v, ok := b.ContinuationIO.(store.ContinuationTimeBuckets)
	return ok && v.TimeBuckets()
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
	return b.ContinuationIO.Set(key, value)
}

func (b *continuationPointBuffer) Delete(key []byte) error {
	delete(b.blocks, string(key))
	return b.ContinuationIO.Delete(key)
}

func (b *continuationPointBuffer) DeletePrefix(prefix []byte) error {
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
