package chunkstore

import (
	"bytes"
)

// GenerationKeysForTest counts the keys still stored for generation m, both
// metric-prefixed and time-bucketed, so tests in other packages can prove a
// transfer retire left nothing of the replaced generation behind.
func (s *Store) GenerationKeysForTest(m Metadata) (int, error) {
	prefix := continuationPrefix(m.ID, m.Generation)
	it, err := s.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	keys := 0
	for valid := it.First(); valid; valid = it.Next() {
		key := it.Key()
		if bytes.HasPrefix(key, prefix) || len(key) >= 5+len(prefix) && key[0] == continuationBucketKeyPrefix && bytes.Equal(key[5:5+len(prefix)], prefix) {
			keys++
		}
	}
	return keys, it.Error()
}

// SeedMemtableRangeTombstonesForTest commits n disjoint range tombstones over
// IDs no store reserves, reproducing the tombstone backlog a busy memtable
// holds after many range-deleting writes.
func (s *Store) SeedMemtableRangeTombstonesForTest(n int) error {
	b := s.db.NewBatch()
	defer b.Close()
	for i := range n {
		start := continuationPrefix(1<<62+uint64(i), 1)[:9]
		if err := b.DeleteRange(start, prefixEnd(start), nil); err != nil {
			return err
		}
	}
	return b.Commit(nil)
}

// SetTransferBatchBytesForTest overrides the batch/ingest threshold of
// ImportTransfer: 0 restores the default, negative always ingests SSTs.
func (s *Store) SetTransferBatchBytesForTest(n int) { s.transferBatchBytes = n }

// PebbleMetricsForTest renders the Pebble LSM metrics (levels, files, sizes).
func (s *Store) PebbleMetricsForTest() string { return s.db.Metrics().String() }
