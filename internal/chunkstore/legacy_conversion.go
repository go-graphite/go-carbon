package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// StoredArchiveCodec exports physical archive values without recomputing
// coarse rollups or folding an OOO overlay into the main archive. This differs
// intentionally from the transfer Snapshot contract.
type StoredArchiveCodec interface {
	StoredSnapshot(context.Context, ContinuationIO, Metadata, func() time.Time) (Snapshot, error)
}

// StoredSnapshot reads every existing archive of an immutable offline copy.
// The codec preserves already stored coarse values. A caller must separately
// qualify render semantics for buffered tails/OOO before publishing these slots
// as a complete replacement for a continuation reader.
func (s *TransferSource) StoredSnapshot(ctx context.Context, name string) (Snapshot, error) {
	ctx, finish, err := s.store.beginRead(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	defer finish()
	view := s.store.db.NewSnapshot()
	defer view.Close()
	m, err := metadataFrom(view, name)
	if err != nil {
		return Snapshot{}, err
	}
	if s.store.incrementalSource != "" {
		if err := s.store.checkFileOwnership(m); err != nil {
			return Snapshot{}, err
		}
	}
	if !m.Continuation {
		return snapshotFrom(view, name)
	}
	codec, ok := s.store.continuation.(StoredArchiveCodec)
	if !ok {
		return Snapshot{}, fmt.Errorf("%w: codec lacks stored archive export", ErrFormat)
	}
	return codec.StoredSnapshot(ctx, s.store.newContinuationIO(ctx, view, nil, m), m, s.store.now)
}

// ConversionView borrows one frozen read snapshot for catalog/range planning.
// The callback must close every iterator before returning and must not retain
// the view. It is covered by the same close/read admission as ordinary reads.
func (s *TransferSource) ConversionView(ctx context.Context, fn func(pebble.Reader) error) error {
	ctx, finish, err := s.store.beginRead(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if err := ctx.Err(); err != nil {
		return err
	}
	view := s.store.db.NewSnapshot()
	defer view.Close()
	return fn(view)
}

// OpenLegacyReclaimCopy opens only an exclusively owned offline COPY for raw
// physical cleanup, using the existing exact chunk merger and comparer. It
// starts no Store maintenance, upgrades no format markers, and refuses external
// WAL paths (a copied WALDIR must never lead back into the live source).
// The caller must drain and close all read-only handles before invoking it.
func OpenLegacyReclaimCopy(directory string) (*pebble.DB, error) {
	format, err := checkFormatMarker(vfs.Default, directory)
	if err != nil {
		return nil, err
	}
	if format < 4 || format == 5 {
		return nil, fmt.Errorf("%w: unsupported reclaim copy format %d", ErrFormat, format)
	}
	walDir, err := readWALDir(vfs.Default, directory)
	if err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	walAbs, err := filepath.Abs(walDir)
	if err != nil || walAbs != abs {
		return nil, errors.New("reclaim copy must contain its own WAL directory")
	}
	opts := &pebble.Options{ErrorIfNotExists: true, Comparer: storeComparer, Merger: exactChunkMerger, DisableAutomaticCompactions: true, MaxConcurrentCompactions: func() int { return 1 }, MemTableSize: 8 << 20, Cleaner: pebble.DeleteCleaner{}}
	return pebble.Open(directory, opts)
}
