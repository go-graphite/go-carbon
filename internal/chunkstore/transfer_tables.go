package chunkstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
)

func (s *Store) writeTransferTables(ctx context.Context, first uint64, records []TransferMetric, metas []Metadata) (paths []string, err error) {
	samples := make(map[int]*TransferMetric)
	for i, r := range records {
		if r.Continuation && r.PointDigest != "" {
			samples[i] = &TransferMetric{Config: r.Config, Continuation: true}
		}
	}
	var sampled transferArena
	capture := func(key, value []byte) error {
		offset := 0
		if key[0] == continuationBucketKeyPrefix {
			offset = 5
		}
		if len(key) < offset+17 {
			return ErrFormat
		}
		id := binary.BigEndian.Uint64(key[offset+1 : offset+9])
		if id < first || id-first >= uint64(len(records)) {
			return ErrFormat
		}
		i := int(id - first)
		if sample := samples[i]; sample != nil {
			var suffix []byte
			if offset == 5 {
				suffix = ContinuationBucketKey(binary.BigEndian.Uint32(key[1:5]), key[22:])
			} else {
				suffix = key[17:]
			}
			data := sampled.alloc(len(suffix) + len(value))
			copy(data, suffix)
			copy(data[len(suffix):], value)
			sample.Keys = append(sample.Keys, RelayoutKV{data[:len(suffix):len(suffix)], data[len(suffix):]})
		}
		return nil
	}
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
		if err := s.writeTransferTable(ctx, path, keys[:n], capture); err != nil {
			return paths, err
		}
		keys = keys[n:]
	}
	// Decode the independent SST read-back, rather than re-reading the same
	// sampled IDs through every LSM level after ingestion.
	for i, sample := range samples {
		slices.SortFunc(sample.Keys, func(a, b RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
		got, err := s.continuation.Snapshot(ctx, transferRecordIO(*sample), metas[i], s.now)
		if err != nil {
			return paths, err
		}
		digest, counts := snapshotDigest(got)
		if digest != records[i].PointDigest || !slices.Equal(counts, records[i].Counts) {
			return paths, fmt.Errorf("staged point verification %s failed", records[i].Config.Name)
		}
	}
	return paths, nil
}

func (s *Store) writeTransferTable(ctx context.Context, path string, keys []RelayoutKV, observers ...func([]byte, []byte) error) error {
	f, err := s.fs.Create(path)
	if err != nil {
		return err
	}
	bytes := 0
	for _, kv := range keys {
		bytes += len(kv.Key) + len(kv.Value) + 16
	}
	buffered := &transferWriteFile{File: f, buffer: bufio.NewWriterSize(f, min(256<<10, max(4096, bytes)))}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(buffered), s.pebbleOptions.MakeWriterOptions(numLevels-1, s.db.FormatMajorVersion().MaxTableFormat()))
	for _, kv := range keys {
		if err := errors.Join(ctx.Err(), w.Set(kv.Key, kv.Value)); err != nil {
			return errors.Join(err, w.Close())
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	return s.verifyTransferTable(ctx, path, keys, observers...)
}

// Verify every staged byte in table order before ingestion. Re-reading each
// metric through the live LSM repeats unrelated block reads and allocations.
// Ingest preserves these immutable files; receipts publish only after it and
// the sampled point checks/overlap merges succeed, followed by a WAL barrier.
func (s *Store) verifyTransferTable(ctx context.Context, path string, keys []RelayoutKV, observers ...func([]byte, []byte) error) (err error) {
	f, err := s.fs.Open(path)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return errors.Join(err, f.Close())
	}
	buffered := &transferReadFile{File: f, buffer: make([]byte, min(256<<10, info.Size())), size: info.Size()}
	readable, err := sstable.NewSimpleReadable(buffered)
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
		for _, observe := range observers {
			if err := observe(key.UserKey, data); err != nil {
				return err
			}
		}
		key, value = it.Next()
	}
	if key != nil {
		return errors.New("unexpected staged transfer key")
	}
	return it.Error()
}
