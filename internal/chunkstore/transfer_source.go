package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// TransferSource reads an exclusively owned checkpoint. It deliberately exposes
// no mutations and does not run Store.Open's migrations or background workers.
// Never point it at a live store: Pebble's normal exclusive LOCK is retained.
type TransferSource struct {
	store *Store
	once  sync.Once
	err   error
}

type TransferSourceOptions struct {
	Continuation ContinuationCodec
	CacheBytes   int64
	Now          func() time.Time
}

func OpenTransferSource(directory string, options TransferSourceOptions) (*TransferSource, error) {
	codec, cacheBytes := options.Continuation, options.CacheBytes
	if cacheBytes < 0 {
		return nil, errors.New("negative transfer source cache")
	}
	format, err := checkFormatMarker(vfs.Default, directory)
	if err != nil {
		return nil, err
	}
	if format < 4 || format == 5 || (format >= 6 && codec == nil) || (format == 4 && codec != nil) {
		return nil, fmt.Errorf("unsupported transfer source format/codec: %d", format)
	}
	if format == 12 {
		reader, ok := codec.(ContinuationReferenceReader)
		if !ok || !reader.NativeReferenceReader() {
			return nil, fmt.Errorf("%w: continuation codec lacks v12 reference reader", ErrFormat)
		}
	}
	cache := pebble.NewCache(max(cacheBytes, 1<<20))
	opts := &pebble.Options{ReadOnly: true, ErrorIfNotExists: true, Cache: cache,
		Comparer: storeComparer, Merger: exactChunkMerger, MemTableSize: 64 << 20}
	db, err := pebble.Open(directory, opts)
	if err != nil {
		cache.Unref()
		return nil, err
	}
	s := &Store{db: db, dir: directory, fs: vfs.Default, now: time.Now,
		exactBlocks: true, continuation: codec, timeBuckets: format >= 7, cache: cache}
	if options.Now != nil {
		s.now = options.Now
	}
	if _, err := os.Stat(filepath.Join(directory, incrementalMarker)); err == nil {
		// Preserve the ownership checks without rebinding or opening originals.
		s.incrementalSource = "checkpoint-owned-only"
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = db.Close()
		cache.Unref()
		return nil, err
	}
	s.coldPack.format.Store(format >= 9)
	s.coldPack.floatFormat.Store(format >= 11)
	s.referenceFormat.Store(format >= 12)
	// Offline catalog discovery uses the same read admission as a live store.
	s.readCtx, s.readCancel = context.WithCancel(context.Background())
	s.readDone = make(chan struct{})
	return &TransferSource{store: s}, nil
}

func (s *TransferSource) Close() error {
	s.once.Do(func() {
		s.store.readMu.Lock()
		s.store.readCancel()
		if s.store.readCount == 0 {
			close(s.store.readDone)
		}
		s.store.readMu.Unlock()
		<-s.store.readDone
		s.err = s.store.db.Close()
		s.store.cache.Unref()
	})
	return s.err
}

func (s *TransferSource) ExportTransferLimit(ctx context.Context, names []string, budget int64) ([]TransferMetric, error) {
	return s.store.ExportTransferLimit(ctx, names, budget)
}

func (s *TransferSource) Snapshot(ctx context.Context, name string) (Snapshot, error) {
	return s.store.Snapshot(ctx, name)
}

// ListCatalogPage exposes the same discovery catalogue as the live read API.
// The source retains the exclusive database lock for the entire enumeration.
func (s *TransferSource) ListCatalogPage(ctx context.Context, prefix, after string, limit int) ([]Metadata, error) {
	return s.store.ListCatalogPage(ctx, prefix, after, limit)
}

// PinNativeMergeSource reuses the committed-history native export path on an
// exclusively owned read-only source. Close the pin before closing the source.
func (s *TransferSource) PinNativeMergeSource(session string) (*NativeSource, error) {
	return s.store.PinNativeMergeSource(session)
}
