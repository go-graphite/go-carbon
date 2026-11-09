package chunkstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

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
	key              []byte
	closed           bool
	closeErr         error
	written, checked uint64
}

func nativeDirectory(identity string, first uint64) string {
	return fmt.Sprintf(".native-%s-%020d", identity, first)
}
func (s *Store) newNativeTables(identity string, first uint64) (*nativeTables, error) {
	directory := s.fs.PathJoin(s.dir, nativeDirectory(identity, first))
	if err := s.fs.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	return &nativeTables{store: s, directory: directory, writers: make(map[string]*nativeTable)}, nil
}
func (t *nativeTables) add(ctx context.Context, m Metadata, kv RelayoutKV) error {
	if t.closed {
		return errors.New("native tables are closed")
	}
	if err := ctx.Err(); err != nil {
		return err
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
		if err := t.store.nativeDiskCheck(); err != nil {
			return err
		}
		path := t.store.fs.PathJoin(t.directory, "data-"+hex.EncodeToString(namespace)+".sst")
		file, err := t.store.fs.Create(path)
		if err != nil {
			return err
		}
		buffered := &transferWriteFile{File: file, buffer: bufio.NewWriterSize(file, 256<<10)}
		writer := sstable.NewWriter(objstorageprovider.NewFileWritable(buffered), t.store.pebbleOptions.MakeWriterOptions(numLevels-1, t.store.db.FormatMajorVersion().MaxTableFormat()))
		table = &nativeTable{writer: writer, lower: bytes.Clone(key)}
		t.writers[group] = table
		t.paths = append(t.paths, path)
	}
	if err := table.writer.Set(key, kv.Value); err != nil {
		return err
	}
	table.upper = append(table.upper[:0], key...)
	t.written += uint64(len(key) + len(kv.Value))
	if t.written-t.checked >= 64<<20 {
		t.checked = t.written
		if err := t.store.nativeDiskCheck(); err != nil {
			return err
		}
	}
	return nil
}
func (t *nativeTables) close() error {
	if t.closed {
		return t.closeErr
	}
	t.closed = true
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, table := range t.writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := table.writer.Close(); err != nil {
				mu.Lock()
				t.closeErr = errors.Join(t.closeErr, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return t.closeErr
}
func (t *nativeTables) spans() []nativeSpan {
	spans := make([]nativeSpan, 0, len(t.writers))
	for _, table := range t.writers {
		spans = append(spans, nativeSpan{table.lower, prefixEnd(table.upper)})
	}
	return spans
}

// Deferred live-create races leave their reserved IDs invisible. Retire only
// those IDs from the just-ingested namespaces in the same publication batch.
func (t *nativeTables) unusedSpans(m Metadata) []nativeSpan {
	spans := make([]nativeSpan, 0, len(t.writers))
	for namespace := range t.writers {
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
	result := make([]NativeSSTReceipt, 0, len(t.writers))
	for namespace, table := range t.writers {
		meta, err := table.writer.Metadata()
		if err != nil {
			return nil, err
		}
		result = append(result, NativeSSTReceipt{Namespace: hex.EncodeToString([]byte(namespace)), Lower: bytes.Clone(table.lower), Upper: prefixEnd(table.upper), Bytes: meta.Size, Entries: meta.Properties.NumEntries})
	}
	return result, nil
}
