package chunkstore

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
)

func TestTransferTableVerificationRejectsMismatch(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	ctx := context.Background()
	keys := []RelayoutKV{{[]byte("c/one"), []byte("value")}, {[]byte("c/two"), []byte("other")}}
	path := s.fs.PathJoin(s.dir, "test.sst")
	if err := s.writeTransferTable(ctx, path, keys); err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][]RelayoutKV{keys[:1], {{[]byte("c/one"), []byte("wrong")}, keys[1]}, {{[]byte("c/missing"), keys[0].Value}, keys[1]}} {
		if err := s.verifyTransferTable(ctx, path, changed); err == nil {
			t.Fatal("staged mismatch accepted")
		}
	}
	f, err := s.fs.OpenReadWrite(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{255}, 16), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.verifyTransferTable(ctx, path, keys); err == nil {
		t.Fatal("corrupt table accepted")
	}
}

// Retain the previous SST staging path as a byte-for-byte differential oracle.
// A single validated key/value may exceed this payload target and is kept
// intact. Table boundaries never change the stored key/value bytes.
const transferTableBytes = 8 << 20

func (s *Store) writeTransferTables(ctx context.Context, first uint64, stage *transferStage) (paths []string, err error) {
	keys := stage.keys
	// Keep each namespace/time bucket in a separate table, so its bounds cover
	// only the reserved IDs and cannot overlap unrelated foreground writes.
	for len(keys) > 0 {
		n := 1
		size := len(keys[0].Key) + len(keys[0].Value) + 16
		prefix := keys[0].Key[:1]
		if prefix[0] == continuationBucketKeyPrefix {
			prefix = keys[0].Key[:5]
		}
		for n < len(keys) && bytes.HasPrefix(keys[n].Key, prefix) {
			next := len(keys[n].Key) + len(keys[n].Value) + 16
			if size+next > transferTableBytes {
				break
			}
			size += next
			n++
		}
		path := s.fs.PathJoin(s.dir, fmt.Sprintf("transfer-%020d-%d.sst", first, len(paths)))
		paths = append(paths, path)
		if err := s.writeTransferTable(ctx, path, keys[:n], stage.capture); err != nil {
			return paths, err
		}
		keys = keys[n:]
	}
	return paths, stage.verify(ctx)
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

// writeTransferBatch stages the keys in one batch and verifies an
// independent read-back of its encoded representation, as the SST path
// verifies its written tables. The keys sit under reserved, unpublished IDs,
// so committing them before publication exposes nothing.
func (s *Store) writeTransferBatch(ctx context.Context, stage *transferStage) (*pebble.Batch, error) {
	b := s.db.NewBatchWithSize(stage.bytes + 16*len(stage.keys) + 64)
	for _, kv := range stage.keys {
		if err := b.Set(kv.Key, kv.Value, nil); err != nil {
			return nil, errors.Join(err, b.Close())
		}
	}
	if err := errors.Join(s.verifyTransferBatch(ctx, b, stage), ctx.Err()); err != nil {
		return nil, errors.Join(err, b.Close())
	}
	return b, nil
}

// ImportTransferSSTForTest wires the previous private-SST staging into the
// unchanged publication/target-wins boundary. Its inputs are validated exports;
// it is an independent oracle for the transport change, not a production API.
func (s *Store) ImportTransferSSTForTest(ctx context.Context, records []TransferMetric) ([]TransferProof, error) {
	first, err := s.reserveFileMetricIDs(uint64(len(records)))
	if err != nil {
		return nil, err
	}
	metas := make([]Metadata, len(records))
	for i, r := range records {
		metas[i] = Metadata{MetricConfig: r.Config, ID: first + uint64(i), Generation: 1, Revision: 1, LastUpdate: s.nextActivity(time.Time{}), Continuation: r.Continuation}
		if s.incrementalSource != "" {
			metas[i].FileMigration = FileReclaimed
		}
	}
	stage := s.stageTransfer(first, records, metas)
	paths, err := s.writeTransferTables(ctx, first, stage)
	defer func() {
		for _, path := range paths {
			_ = s.fs.Remove(path)
		}
	}()
	if err != nil {
		return nil, err
	}
	remaining := paths
	for len(remaining) > 0 {
		n := min(nativeIngestFiles, len(remaining))
		if _, err := s.db.IngestWithStats(remaining[:n]); err != nil {
			return nil, err
		}
		remaining = remaining[n:]
	}
	proofs := make([]TransferProof, 0, len(records))
	for start := 0; start < len(records); start += 16 {
		end := min(start+16, len(records))
		group, err := s.publishTransfer(ctx, records[start:end], metas[start:end])
		if err != nil {
			return nil, err
		}
		proofs = append(proofs, group...)
	}
	if err := s.syncWAL(); err != nil {
		return nil, err
	}
	s.invalidateReadCache()
	return proofs, nil
}
