package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
)

func (s *Store) writeTransferTables(ctx context.Context, first uint64, records []TransferMetric, metas []Metadata) (paths []string, err error) {
	count := 0
	for _, r := range records {
		count += len(r.Keys)
	}
	keys := make([]RelayoutKV, 0, count)
	var arena transferArena
	for i, r := range records {
		prefix := chunkPrefix(metas[i].ID, metas[i].Generation)
		if r.Continuation {
			prefix = continuationPrefix(metas[i].ID, metas[i].Generation)
		}
		for _, kv := range r.Keys {
			key := arena.alloc(len(prefix) + len(kv.Key))[:0]
			if r.Continuation {
				key = appendContinuationKey(key, prefix, kv.Key)
			} else {
				key = append(append(key, prefix...), kv.Key...)
			}
			keys = append(keys, RelayoutKV{key, kv.Value})
		}
	}
	slices.SortFunc(keys, func(a, b RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	// Keep each namespace/time bucket in a separate table, so its bounds cover
	// only the reserved IDs and cannot overlap unrelated foreground writes.
	for len(keys) > 0 {
		n := 1
		prefix := keys[0].Key[:1]
		if prefix[0] == continuationBucketKeyPrefix {
			prefix = keys[0].Key[:5]
		}
		for n < len(keys) && bytes.HasPrefix(keys[n].Key, prefix) {
			n++
		}
		path := s.fs.PathJoin(s.dir, fmt.Sprintf("transfer-%020d-%d.sst", first, len(paths)))
		paths = append(paths, path)
		if err := s.writeTransferTable(ctx, path, keys[:n]); err != nil {
			return paths, err
		}
		keys = keys[n:]
	}
	return paths, nil
}

func (s *Store) writeTransferTable(ctx context.Context, path string, keys []RelayoutKV) error {
	f, err := s.fs.Create(path)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), s.pebbleOptions.MakeWriterOptions(numLevels-1, s.db.FormatMajorVersion().MaxTableFormat()))
	for _, kv := range keys {
		if err := errors.Join(ctx.Err(), w.Set(kv.Key, kv.Value)); err != nil {
			return errors.Join(err, w.Close())
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	return s.verifyTransferTable(ctx, path, keys)
}

// Verify every staged byte in table order before ingestion. Re-reading each
// metric through the live LSM repeats unrelated block reads and allocations.
// Ingest preserves these immutable files; receipts publish only after it and
// the sampled point checks/overlap merges succeed, followed by a WAL barrier.
func (s *Store) verifyTransferTable(ctx context.Context, path string, keys []RelayoutKV) (err error) {
	f, err := s.fs.Open(path)
	if err != nil {
		return err
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		return errors.Join(err, f.Close())
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{MergerName: s.pebbleOptions.Merger.Name})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	it, err := r.NewIter(nil, nil)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	key, value := it.First()
	for _, expected := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if key == nil || !bytes.Equal(key.UserKey, expected.Key) {
			return errors.New("staged transfer key verification failed")
		}
		data, _, err := value.Value(nil)
		if err != nil {
			return err
		}
		if !bytes.Equal(data, expected.Value) {
			return errors.New("staged transfer value verification failed")
		}
		key, value = it.Next()
	}
	if key != nil {
		return errors.New("unexpected staged transfer key")
	}
	return it.Error()
}
