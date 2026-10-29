package chunkstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/helper"
)

const incrementalMarker = "INCREMENTAL-FILE-MIGRATION"
const incrementalMarkerVersion = "go-carbon-incremental-v1\n"

type ownershipError struct{ err error }

func incrementalMarkerContents(fs vfs.FS, source string) (string, error) {
	info, err := fs.Stat(source)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("incremental source is not a directory")
	}
	identity := "virtual"
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		identity = fmt.Sprintf("%d:%d", uint64(stat.Dev), uint64(stat.Ino))
	}
	return incrementalMarkerVersion + source + "\n" + identity + "\n", nil
}

func checkIncrementalMarker(fs vfs.FS, dir, source string) error {
	f, err := fs.Open(fs.PathJoin(dir, incrementalMarker))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, 16384))
	if err := errors.Join(readErr, f.Close()); err != nil {
		return err
	}
	expected, identityErr := incrementalMarkerContents(fs, source)
	if source == "" || identityErr != nil || string(data) != expected {
		return fmt.Errorf("%w: incremental migration requires its original source directory and mode", ErrFormat)
	}
	return nil
}

func (s *Store) configureIncrementalSource(fs vfs.FS, dir, source string) error {
	if source == "" {
		return nil
	}
	if !s.exactBlocks || !filepath.IsAbs(source) || filepath.Clean(source) != source || strings.ContainsAny(source, "\r\n") {
		return errors.New("incremental migration requires exact encoding and a canonical absolute source directory")
	}
	for _, pair := range [][2]string{{source, dir}, {dir, source}} {
		relative, err := filepath.Rel(pair[0], pair[1])
		if err != nil || filepath.IsLocal(relative) {
			return errors.New("incremental source and destination must be separate directory trees")
		}
	}
	info, err := fs.Stat(source)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("incremental source is not a directory: %w", errors.Join(err, ErrFormat))
	}
	marker := fs.PathJoin(dir, incrementalMarker)
	if _, err := fs.Stat(marker); errors.Is(err, os.ErrNotExist) {
		watermark, err := s.CatalogHighWatermark(context.Background())
		if err != nil {
			return err
		}
		if watermark != 0 {
			return errors.New("incremental migration requires an empty destination, not an unverified baseline")
		}
		for _, name := range []string{"NEEDS-CATCHUP", "MIRROR-RECONCILE-REQUIRED"} {
			if _, err := fs.Stat(fs.PathJoin(dir, name)); !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("incremental migration cannot adopt %s", name)
			}
		}
		temporary := marker + ".next"
		f, err := fs.Create(temporary)
		if err != nil {
			return err
		}
		contents, identityErr := incrementalMarkerContents(fs, source)
		if identityErr != nil {
			_ = f.Close()
			return identityErr
		}
		_, writeErr := f.Write([]byte(contents))
		if err := errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
			return err
		}
		if err := fs.Rename(temporary, marker); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	f, err := fs.Open(marker)
	if err != nil {
		return err
	}
	if err := errors.Join(f.Sync(), f.Close(), syncStoreDirectory(fs, dir)); err != nil {
		return err
	}
	// The database lock is held and no readers/converters have started. These
	// reserved directories contain only disposable captures and name-sort runs;
	// a process crash must not accumulate a second working set on every restart.
	for _, name := range []string{".incremental-capture", ".incremental-index"} {
		if err := fs.RemoveAll(fs.PathJoin(dir, name)); err != nil {
			return err
		}
	}
	s.incrementalSource = source
	return nil
}

func (s *Store) IncrementalSource() string { return s.incrementalSource }

// OriginalReadGuard spans persistent and cache reads. Publication drains these
// readers before reclaiming originals. SharedWriteGuard prevents a writer from
// confirming a cache batch after a read took its persistent snapshot but before
// that read captured the cache. Long conversion capture/verification stays out
// of this guard.
func (s *Store) OriginalReadGuard(name string) func() {
	if s.fileReadMu == nil {
		return func() {}
	}
	mu := &s.fileReadMu[(helper.HashString(name)>>32)%uint64(len(s.fileReadMu))]
	mu.RLock()
	return mu.RUnlock
}

// SharedWriteGuard spans a persister's shared update and cache confirmation.
// Readers can run together; unrelated metrics use independent stripes.
func (s *Store) SharedWriteGuard(name string) func() {
	mu := &s.fileReadMu[(helper.HashString(name)>>32)%uint64(len(s.fileReadMu))]
	mu.Lock()
	return mu.Unlock
}

func (s *Store) lockFilePublication(ctx context.Context, name string) (func(), error) {
	mu := &s.fileReadMu[(helper.HashString(name)>>32)%uint64(len(s.fileReadMu))]
	for !mu.TryLock() {
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return mu.Unlock, nil
}

func (s *Store) fileOwnershipError() error {
	if failure := s.ownershipFailure.Load(); failure != nil {
		return failure.err
	}
	return nil
}

func (s *Store) checkFileOwnership(m Metadata) error {
	if s.incrementalSource == "" {
		return nil
	}
	if err := s.fileOwnershipError(); err != nil {
		return err
	}
	if m.FileMigration == FileUnowned {
		return ErrNotFound
	}
	return nil
}

// StageFileSnapshot installs a complete but invisible candidate. The caller
// serializes this with every original writer for this metric until publication.
func (s *Store) StageFileSnapshot(ctx context.Context, snapshot Snapshot) (Metadata, error) {
	if s.incrementalSource == "" {
		return Metadata{}, errors.New("incremental migration is disabled")
	}
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return Metadata{}, err
	}
	if err := validateSnapshot(snapshot); err != nil {
		return Metadata{}, err
	}
	if snapshot.Metadata.LastUpdate.IsZero() && snapshot.SourceState != "" {
		info, err := os.Stat(snapshot.SourceState)
		if err != nil {
			return Metadata{}, fmt.Errorf("stat conversion activity: %w", err)
		}
		snapshot.Metadata.LastUpdate = info.ModTime()
	}
	// Encoding and batch construction must not hold a store stripe or the
	// catalog allocator: ordinary writers sharing those locks remain live.
	var chunks []map[int][]byte
	var err error
	if s.continuation == nil {
		chunks, err = encodeSnapshotChunksWith(snapshot.Metadata.Retentions, snapshot.Archives, s.encodeChunk)
		if err != nil {
			return Metadata{}, err
		}
	} else if snapshot.SourceState == "" {
		return Metadata{}, errors.New("continuation migration requires the fenced physical source")
	}
	unlock := s.lockMetricMutation(snapshot.Metadata.Name)
	old, oldErr := s.metadataLockedOrAbsent(snapshot.Metadata.Name)
	if oldErr == nil && old.FileMigration != FileUnowned {
		unlock()
		return Metadata{}, ErrExists
	}
	if oldErr != nil && !errors.Is(oldErr, ErrNotFound) {
		unlock()
		return Metadata{}, oldErr
	}
	m := snapshot.Metadata
	m.Continuation = s.continuation != nil
	m.FileMigration = FileUnowned
	if s.ingest != nil && m.Continuation && errors.Is(oldErr, ErrNotFound) {
		// The ingester assigns IDs from its own durable block reservation.
		unlock()
		return s.stageFileIngest(ctx, snapshot, m)
	}
	if errors.Is(oldErr, ErrNotFound) {
		m.ID, err = s.reserveFileMetricID()
		m.Generation, m.Revision = 1, 1
		if m.LastUpdate.IsZero() {
			m.LastUpdate = s.nextActivity(time.Time{})
		}
	} else {
		if old.Generation == math.MaxUint64 || old.Revision == math.MaxUint64 {
			unlock()
			return Metadata{}, errors.New("staged metric revision exhausted")
		}
		m.ID, m.Generation, m.Revision = old.ID, old.Generation+1, old.Revision+1
		if m.LastUpdate.IsZero() {
			m.LastUpdate = s.nextActivity(old.LastUpdate)
		}
	}
	unlock()
	if err != nil {
		return Metadata{}, err
	}
	batch := s.newStateBatch()
	defer batch.Close()
	if oldErr == nil {
		prefix := chunkPrefix(old.ID, old.Generation)
		if err := batch.DeleteRange(prefix, prefixEnd(prefix), nil); err != nil {
			return Metadata{}, err
		}
		if old.Continuation {
			if err := s.dropContinuation(ctx, batch, batch, old); err != nil {
				return Metadata{}, err
			}
			if err := s.newContinuationIO(ctx, batch, batch, old).DeletePrefix(nil); err != nil {
				return Metadata{}, err
			}
		}
		if err := batch.Delete(revisionKey(old), nil); err != nil {
			return Metadata{}, err
		}
	}
	var sidecar *SidecarSummary
	if m.Continuation {
		io := s.newFreshContinuationIO(ctx, batch, m)
		if err := s.continuation.Import(ctx, io, snapshot.SourceState, m, s.now); err != nil {
			return Metadata{}, err
		}
		sidecar = &SidecarSummary{}
		if io.sidecar != nil {
			sidecar = io.sidecar
		}
	}
	for archive, encoded := range chunks {
		for index, value := range encoded {
			if err := batch.Set(chunkKey(m, archive, index), value, nil); err != nil {
				return Metadata{}, err
			}
		}
	}
	encoded, err := s.encodeCatalog(m)
	if err != nil {
		return Metadata{}, err
	}
	if err := batch.Set(catalogKey(m.Name), encoded, nil); err != nil {
		return Metadata{}, err
	}
	if err := batch.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
		return Metadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	unlock = s.lockMetricMutation(m.Name)
	defer unlock()
	current, currentErr := s.metadataLocked(m.Name)
	switch {
	case errors.Is(oldErr, ErrNotFound):
		if currentErr == nil {
			return Metadata{}, ErrConflict
		}
		if !errors.Is(currentErr, ErrNotFound) {
			return Metadata{}, currentErr
		}
	case currentErr != nil:
		return Metadata{}, currentErr
	case current.ID != old.ID || current.Generation != old.Generation || current.Revision != old.Revision || current.FileMigration != FileUnowned:
		return Metadata{}, ErrConflict
	}
	if err := s.commitSidecars(batch, m.Name, sidecar, s.commitFileStage); err != nil {
		return Metadata{}, err
	}
	s.catalogChanged(m.Name)
	return m, nil
}

// Reserve IDs in a tiny transactional record before constructing large history
// batches. Gaps after a crash are safe; every later ownership sync covers its
// earlier reservation, so no published ID can be allocated again on recovery.
func (s *Store) reserveFileMetricID() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, err := s.CatalogHighWatermark(context.Background())
	if err != nil {
		return 0, err
	}
	if id == math.MaxUint64 {
		return 0, errors.New("metric IDs exhausted")
	}
	id++
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(sequenceKey(), uint64Bytes(id), nil); err != nil {
		return 0, err
	}
	return id, s.commitFileStage(batch)
}

func (s *Store) commitFileStage(batch *pebble.Batch) error {
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	s.pendingSync.Store(true)
	return nil
}

func fileCleanupKey(id uint64) []byte {
	return append([]byte("f/cleanup/"), uint64Bytes(id)...)
}

// PublishFileOwnership verifies actual stored values, then syncs data, ownership
// and the cleanup queue together. Normal ingestion retains its existing sync
// policy. Concurrent durable commits use Pebble's WAL commit pipeline.
func (s *Store) PublishFileOwnership(ctx context.Context, want Snapshot, expected Metadata) (Metadata, error) {
	if s.incrementalSource == "" {
		return Metadata{}, errors.New("incremental migration is disabled")
	}
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return Metadata{}, err
	}
	view := s.db.NewSnapshot()
	m, err := metadataFrom(view, want.Metadata.Name)
	var got Snapshot
	if err == nil && m.Continuation {
		if s.continuation == nil || want.SourceState == "" {
			err = ErrFormat
		} else if verifier, ok := s.continuation.(ContinuationVerifier); ok {
			got, err = verifier.VerifySnapshot(ctx, s.newContinuationIO(ctx, view, nil, m), want.SourceState, m, s.now)
		} else {
			err = s.continuation.Verify(ctx, s.newContinuationIO(ctx, view, nil, m), want.SourceState, m)
			if err == nil {
				got, err = s.continuationSnapshot(ctx, view, m)
			}
		}
	} else if err == nil {
		got, err = snapshotFrom(view, want.Metadata.Name)
	}
	// Closing takes the DB mutex and manifest lock; thousands of publications
	// finishing together must not queue on it.
	s.releaseSnapshot(view)
	if err != nil {
		return Metadata{}, err
	}
	m = got.Metadata
	if m.ID != expected.ID || m.Generation != expected.Generation || m.Revision != expected.Revision || m.FileMigration != FileUnowned {
		return Metadata{}, ErrConflict
	}
	if err := EqualSnapshots(want, got); err != nil {
		return Metadata{}, fmt.Errorf("verify file conversion %s: %w", m.Name, err)
	}
	m, err = s.publishFileOwnershipLocked(ctx, m)
	if err != nil {
		return Metadata{}, err
	}
	// Ownership must be durable before the caller reclaims originals. The sync
	// runs after the guards are released: concurrent publications share one WAL
	// sync, and readers and writers of the stripes never wait on it.
	if err := s.syncWAL(); err != nil {
		failure := fmt.Errorf("file ownership sync failed; storage must restart: %w", err)
		s.ownershipFailure.CompareAndSwap(nil, &ownershipError{failure})
		return Metadata{}, failure
	}
	// Exact conversion changes ownership, not visible values. Keep Graphite's
	// normal render-cache TTL rather than invalidating every metric per import.
	return m, nil
}

// publishFileOwnershipLocked drains original readers, then commits ownership
// and the cleanup queue together without waiting for the WAL sync.
func (s *Store) publishFileOwnershipLocked(ctx context.Context, m Metadata) (Metadata, error) {
	release, err := s.lockFilePublication(ctx, m.Name)
	if err != nil {
		return Metadata{}, err
	}
	defer release()
	unlock := s.lockMetricMutation(m.Name)
	defer unlock()
	current, err := s.metadataLocked(m.Name)
	if err != nil {
		return Metadata{}, err
	}
	if current.ID != m.ID || current.Generation != m.Generation || current.Revision != m.Revision || current.FileMigration != FileUnowned {
		return Metadata{}, ErrConflict
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if m.Revision == math.MaxUint64 {
		return Metadata{}, errors.New("metric revision exhausted")
	}
	m.FileMigration = FileCleanupPending
	m.Revision++
	encoded, err := s.encodeCatalog(m)
	if err != nil {
		return Metadata{}, err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, kv := range []struct{ k, v []byte }{{catalogKey(m.Name), encoded}, {revisionKey(m), revisionBytes(m)}, {fileCleanupKey(m.ID), []byte(m.Name)}} {
		if err := batch.Set(kv.k, kv.v, nil); err != nil {
			return Metadata{}, err
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		failure := fmt.Errorf("file ownership commit failed; storage must restart: %w", err)
		s.ownershipFailure.CompareAndSwap(nil, &ownershipError{failure})
		return Metadata{}, failure
	}
	s.catalogChanged(m.Name)
	return m, nil
}

// PendingFileCleanup reads only the small durable cleanup queue, not the whole
// catalog. A crash after publication never causes a source re-import.
func (s *Store) PendingFileCleanup(ctx context.Context, limit int) ([]Metadata, error) {
	if limit < 1 || limit > 10000 {
		return nil, errors.New("cleanup page limit must be 1..10000")
	}
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return nil, err
	}
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	prefix := []byte("f/cleanup/")
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var result []Metadata
	for it.First(); it.Valid() && len(result) < limit; it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(it.Key()) != len(prefix)+8 {
			return nil, errors.New("invalid file-cleanup key")
		}
		m, err := metadataFrom(snapshot, string(it.Value()))
		if err != nil {
			return nil, err
		}
		if m.ID != binary.BigEndian.Uint64(it.Key()[len(prefix):]) || m.FileMigration != FileCleanupPending {
			return nil, errors.New("file-cleanup ownership mismatch")
		}
		result = append(result, m)
	}
	return result, it.Error()
}

// VisitPendingFileCleanup calls fn with the name of every metric whose original
// files still need removal, in one pass over a snapshot. Unlike paging with
// PendingFileCleanup it neither restarts at the first key, past the tombstones
// of completed cleanups, nor reads each metric's catalog record first: the
// cleanup itself rereads and checks it.
func (s *Store) VisitPendingFileCleanup(ctx context.Context, fn func(name string) error) error {
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return err
	}
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	prefix := []byte("f/cleanup/")
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(it.Key()) != len(prefix)+8 {
			return errors.New("invalid file-cleanup key")
		}
		if err := fn(string(it.Value())); err != nil {
			return err
		}
	}
	return it.Error()
}

// CompleteFileCleanup is called only after both original files are absent and
// their directory is synced. A lost completion is safe: cleanup repeats.
func (s *Store) CompleteFileCleanup(ctx context.Context, expected Metadata) error {
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return err
	}
	unlock := s.lockMetricMutation(expected.Name)
	defer unlock()
	m, err := s.metadataLocked(expected.Name)
	if err != nil {
		return err
	}
	if m.ID != expected.ID || m.Generation != expected.Generation || m.FileMigration == FileUnowned {
		return ErrConflict
	}
	if m.FileMigration == FileReclaimed {
		return nil
	}
	if m.Revision == math.MaxUint64 {
		return errors.New("metric revision exhausted")
	}
	m.FileMigration = FileReclaimed
	m.Revision++
	encoded, err := s.encodeCatalog(m)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(catalogKey(m.Name), encoded, nil); err != nil {
		return err
	}
	if err := batch.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
		return err
	}
	if err := batch.Delete(fileCleanupKey(m.ID), nil); err != nil {
		return err
	}
	if err := s.commit(batch); err != nil {
		return err
	}
	s.catalogChanged(m.Name)
	return nil
}

// DiscardStagedFile removes an unowned candidate when its original was deleted
// before conversion. It cannot remove an owned metric or race publication.
func (s *Store) DiscardStagedFile(ctx context.Context, name string) error {
	m, err := metadataFrom(s.db, name)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if m.FileMigration != FileUnowned {
		return ErrConflict
	}
	return s.DeleteIfUnchanged(ctx, name, m)
}
