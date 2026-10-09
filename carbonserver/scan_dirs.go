package carbonserver

import (
	"bytes"

	"github.com/blevesearch/vellum"
)

// The directory catalogue lets a scan rebuild the listing of a directory that
// did not change since the previous scan. The metric index already names every
// metric file and every directory leading to one, so the catalogue holds only
// what else the walk acts on, keyed like the index (path separators as 0):
//
//   - "dir": a directory without a metric below it, value scanTypeDir.
//   - "dir\x00name": an entry named *.ooo that is not a directory, value its
//     scanType. Sidecars change the sizes of their metrics.
//   - "dir\x00": how many more regular *.lock files the directory holds than
//     metric files that are not directories, zigzag-encoded, when not zero.
//     Whisper keeps one lock file beside each metric file, so this is usually
//     absent; lock files left by deleted metrics show up here.
//
// Each scan range writes the records of the keys it covers. A range that sees
// only part of a directory may record it although a later range finds a metric
// below it; such records are redundant, never wrong.

// scanCatalogueWriter builds one range's catalogue shard.
type scanCatalogueWriter struct {
	builder     *vellum.Builder
	buf         bytes.Buffer
	first, last []byte
	count       int
}

func (c *scanCatalogueWriter) reset() error {
	c.buf.Reset()
	c.first, c.last, c.count = c.first[:0], c.last[:0], 0
	if c.builder == nil {
		var err error
		c.builder, err = vellum.New(&c.buf, nil)
		return err
	}
	return c.builder.Reset(&c.buf)
}

func (c *scanCatalogueWriter) add(key []byte, value uint64) error {
	if err := c.builder.Insert(key, value); err != nil {
		return err
	}
	if c.count == 0 {
		c.first = append(c.first[:0], key...)
	}
	c.last = append(c.last[:0], key...)
	c.count++
	return nil
}

// finish closes the shard and spools its node bytes; ranges without records
// have no shard.
func (c *scanCatalogueWriter) finish(r *scanRange, spool scanSpoolWriter) (*fstShard, error) {
	if err := c.builder.Close(); err != nil {
		return nil, err
	}
	if c.count == 0 {
		return nil, nil
	}
	left, right := scanCommonPrefix(c.first, r.start), scanCommonPrefix(c.last, r.end)
	data := c.buf.Bytes()
	shard, err := newFSTShard(data, bytes.Clone(c.first), bytes.Clone(c.last), uint64(c.count), left, right)
	if err != nil {
		return nil, err
	}
	_, err = spool.Write(fstShardBody(data))
	return shard, err
}
