package chunkstore

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble"
)

// SetTransferRereadMetadataForTest forces commit to re-read every catalog
// entry, the behaviour before the unchanged-epoch shortcut.
func (s *Store) SetTransferRereadMetadataForTest(on bool) { s.transferRereadMetadata = on }

// SetTransferSplitSinglesForTest restores the single-name publication of
// contended groups, the behaviour before partitioned publication.
func (s *Store) SetTransferSplitSinglesForTest(on bool) { s.transferSplitSingles = on }

// FlushForTest flushes the memtable so reads probe sstable filter blocks.
func (s *Store) FlushForTest() error { return s.db.Flush() }

// RawKeyValuesForTest returns every live key and value in key order.
func (s *Store) RawKeyValuesForTest() ([][2][]byte, error) {
	it, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, err
	}
	var out [][2][]byte
	for ok := it.First(); ok; ok = it.Next() {
		out = append(out, [2][]byte{bytes.Clone(it.Key()), bytes.Clone(it.Value())})
	}
	return out, errors.Join(it.Error(), it.Close())
}

// SetTransferCommitHookForTest pauses publication after releasing its stripes.
func (s *Store) SetTransferCommitHookForTest(hook func()) { s.transferCommitTestHook = hook }
func (s *Store) MetricStripeForTest(name string) uint64   { return s.metricStripe(name) }
