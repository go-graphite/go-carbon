package chunkstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
)

// Table identities bind the stream digest and reserved-ID mapping in the
// same durable receipt. Pebble supplies on-disk block checksums; no re-read.
type NativeSSTReceipt struct {
	Namespace      string
	Lower, Upper   []byte
	Bytes, Entries uint64
}

type nativeTable struct {
	writer       *sstable.Writer
	lower, upper []byte
}
type nativeTables struct {
	store            *Store
	directory        string
	paths            []string
	writers          map[string]*nativeTable
	namespaces       map[string]struct{}
	completed        []NativeSSTReceipt
	openBytes        uint64
	key              []byte
	closed           bool
	closeErr         error
	written, checked uint64
	metadataBytes    int64
}

func nativeDirectory(identity string, first uint64) string {
	return fmt.Sprintf(".native-%s-%020d", identity, first)
}
func (s *Store) newNativeTables(identity string, first uint64) (*nativeTables, error) {
	directory := s.fs.PathJoin(s.dir, nativeDirectory(identity, first))
	if err := s.fs.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	return &nativeTables{store: s, directory: directory, writers: make(map[string]*nativeTable), namespaces: make(map[string]struct{})}, nil
}
func (t *nativeTables) add(ctx context.Context, m Metadata, kv RelayoutKV) error {
	if t.closed {
		return errors.New("native tables are closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.Continuation && referenceManifest(kv) {
		if err := t.store.fenceReferences(); err != nil {
			return err
		}
	}
	prefix := chunkPrefix(m.ID, m.Generation)
	if m.Continuation {
		prefix = continuationPrefix(m.ID, m.Generation)
	}
	key := t.key[:0]
	if m.Continuation {
		key = appendContinuationKey(key, prefix, kv.Key)
	} else {
		key = append(append(key, prefix...), kv.Key...)
	}
	t.key = key
	namespace := key[:1]
	if key[0] == continuationBucketKeyPrefix {
		namespace = key[:5]
	}
	group := string(namespace)
	table := t.writers[group]
	if table == nil {
		// Closed files retain receipts, paths and cleanup bounds. Cap that
		// metadata too, independently of the number of historical buckets.
		if err := t.retainMetadata(1024 + len(t.directory) + 2*len(key)); err != nil {
			return err
		}
		if len(t.writers) == nativeTableWriters {
			if err := t.finishWriters(); err != nil {
				return err
			}
		}
		if err := t.store.nativeDiskCheck(); err != nil {
			return err
		}
		path := t.store.fs.PathJoin(t.directory, fmt.Sprintf("data-%08d-%s.sst", len(t.paths), hex.EncodeToString(namespace)))
		file, err := t.store.fs.Create(path)
		if err != nil {
			return err
		}
		buffered := &transferWriteFile{File: file, buffer: bufio.NewWriterSize(file, 256<<10)}
		writer := sstable.NewWriter(objstorageprovider.NewFileWritable(buffered), t.store.pebbleOptions.MakeWriterOptions(numLevels-1, t.store.db.FormatMajorVersion().MaxTableFormat()))
		table = &nativeTable{writer: writer, lower: bytes.Clone(key)}
		t.writers[group] = table
		t.namespaces[group] = struct{}{}
		t.paths = append(t.paths, path)
	}
	if len(key) > len(table.upper) && len(table.upper) != 0 {
		if err := t.retainMetadata(2 * (len(key) - len(table.upper))); err != nil {
			return err
		}
	}
	if err := table.writer.Set(key, kv.Value); err != nil {
		return err
	}
	table.upper = append(table.upper[:0], key...)
	size := uint64(len(key) + len(kv.Value))
	t.written += size
	t.openBytes += size
	if t.openBytes >= nativeTableBytes {
		if err := t.finishWriters(); err != nil {
			return err
		}
	}
	if t.written-t.checked >= 64<<20 {
		t.checked = t.written
		if err := t.store.nativeDiskCheck(); err != nil {
			return err
		}
	}
	return nil
}

func (t *nativeTables) retainMetadata(size int) error {
	if int64(size) > nativeRangeMetadataBytes-t.metadataBytes {
		return fmt.Errorf("native SST metadata exceeds %d bytes: %w", nativeRangeMetadataBytes, ErrTransferLimit)
	}
	t.metadataBytes += int64(size)
	return nil
}

// Close a bounded group and drop every writer reference: the SST index and
// filter builders must not retain a range's entire physical history.
func (t *nativeTables) finishWriters() error {
	var result error
	for namespace, table := range t.writers {
		err := table.writer.Close()
		result = errors.Join(result, err)
		if err == nil {
			meta, err := table.writer.Metadata()
			result = errors.Join(result, err)
			if err == nil {
				t.completed = append(t.completed, NativeSSTReceipt{Namespace: hex.EncodeToString([]byte(namespace)), Lower: table.lower, Upper: prefixEnd(table.upper), Bytes: meta.Size, Entries: meta.Properties.NumEntries})
			}
		}
		delete(t.writers, namespace)
	}
	t.openBytes = 0
	t.closeErr = errors.Join(t.closeErr, result)
	return t.closeErr
}
func (t *nativeTables) close() error {
	if !t.closed {
		t.closed = true
		t.finishWriters()
	}
	return t.closeErr
}
func (t *nativeTables) spans() []nativeSpan {
	spans := make([]nativeSpan, 0, len(t.completed))
	for _, table := range t.completed {
		spans = append(spans, nativeSpan{table.Lower, table.Upper})
	}
	return spans
}

// Deferred live-create races leave their reserved IDs invisible. Retire only
// those IDs from the just-ingested namespaces in the same publication batch.
func (t *nativeTables) unusedSpans(m Metadata) []nativeSpan {
	spans := make([]nativeSpan, 0, len(t.namespaces))
	for namespace := range t.namespaces {
		prefix := chunkPrefix(m.ID, m.Generation)
		if namespace[0] == continuationBucketKeyPrefix {
			prefix = continuationPrefix(m.ID, m.Generation)
			prefix = append([]byte(namespace), prefix...)
		} else {
			prefix[0] = namespace[0]
		}
		spans = append(spans, nativeSpan{prefix, prefixEnd(prefix)})
	}
	return spans
}

func (t *nativeTables) receipts() ([]NativeSSTReceipt, error) {
	if !t.closed {
		return nil, errors.New("native tables are not closed")
	}
	return t.completed, t.closeErr
}
