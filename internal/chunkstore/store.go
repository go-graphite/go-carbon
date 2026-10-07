package chunkstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/helper"
)

var (
	ErrNotFound = errors.New("metric not found")
	ErrExists   = errors.New("metric already exists")
	ErrFormat   = errors.New("unsupported chunk store format")
)

const (
	formatMarkerV1     = "go-carbon-chunks-v1\n"
	formatMarkerV2     = "go-carbon-chunks-v2\n"
	legacyRevisionSize = 8
	revisionSize       = 16
)

type Options struct {
	CacheSize    int64
	MemTableSize uint64
	// SyncInterval controls WAL syncing for successful store mutations. Zero
	// syncs each mutation; a positive interval permits loss of mutations since
	// the last sync.
	SyncInterval time.Duration
	Now          func() time.Time
	fs           vfs.FS // Fault-injection tests use the same storage path as production.
}

type Store struct {
	db           *pebble.DB
	cache        *pebble.Cache
	now          func() time.Time
	writeOptions *pebble.WriteOptions
	pendingSync  atomic.Bool
	syncStop     chan struct{}
	syncDone     chan struct{}
	// mu serializes metric ID allocation from the catalog sequence. Everything
	// else that touches one metric is covered by its metricMu stripe.
	mu               sync.Mutex
	metricMu         [256]sync.Mutex
	materializations atomic.Uint64
	operands         atomic.Uint64
}

// Open accepts only a new directory or an explicitly identified chunk store.
// A legacy Pebble directory is rejected before Pebble can modify its manifest.
func Open(dir string, options Options) (*Store, error) {
	if dir == "" {
		return nil, errors.New("store directory is empty")
	}
	if options.SyncInterval < 0 {
		return nil, errors.New("store sync interval must be non-negative")
	}
	fs := options.fs
	if fs == nil {
		fs = vfs.Default
	}
	format, created, err := checkFormat(fs, dir)
	if err != nil {
		return nil, err
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.MemTableSize == 0 {
		options.MemTableSize = 64 << 20
	}
	opts := &pebble.Options{MemTableSize: options.MemTableSize, Merger: chunkMerger, FS: fs}
	s := &Store{now: options.Now, writeOptions: pebble.Sync}
	if options.CacheSize > 0 {
		s.cache = pebble.NewCache(options.CacheSize)
		opts.Cache = s.cache
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		if s.cache != nil {
			s.cache.Unref()
		}
		return nil, fmt.Errorf("open chunk store: %w", err)
	}
	s.db = db
	if format == 1 {
		if err := upgradeFormatMarker(fs, dir); err != nil {
			return nil, s.openFailure(db, fmt.Errorf("upgrade chunk store format: %w", err))
		}
	}
	// An interrupted upgrade may have completed the rename while its directory
	// sync failed. Re-sync an accepted v2 marker while Pebble owns the database
	// lock, before allowing any v2 revision record to be written. A marker
	// created by this Open was already synced by createFormatMarker.
	if !created {
		if err := syncFormatMarker(fs, dir); err != nil {
			return nil, s.openFailure(db, fmt.Errorf("sync chunk store marker: %w", err))
		}
	}
	if options.SyncInterval > 0 {
		s.writeOptions = pebble.NoSync
		s.syncStop = make(chan struct{})
		s.syncDone = make(chan struct{})
		go s.syncLoop(options.SyncInterval)
	}
	return s, nil
}

func (s *Store) openFailure(db *pebble.DB, err error) error {
	closeErr := db.Close()
	if s.cache != nil {
		s.cache.Unref()
	}
	return errors.Join(err, closeErr)
}

func (s *Store) syncLoop(interval time.Duration) {
	defer close(s.syncDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.syncStop:
			return
		case <-ticker.C:
			if !s.pendingSync.Swap(false) {
				continue
			}
			// LogData flushes Pebble's userspace WAL buffer and syncs all prior
			// records, without forcing a memtable flush or growing an idle WAL.
			if err := s.db.LogData(nil, pebble.Sync); err != nil {
				// Pebble already fails fatally on WAL I/O errors. Do not continue
				// accepting mutations if another sync error is ever returned.
				panic(fmt.Errorf("periodic store WAL sync: %w", err))
			}
		}
	}
}

// checkFormat returns the on-disk format version and whether this call
// created (and synced) the marker for a new store.
func checkFormat(fs vfs.FS, dir string) (format int, created bool, err error) {
	names, err := fs.List(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, false, fmt.Errorf("list store directory: %w", err)
	}
	if len(names) > 0 {
		format, err = checkFormatMarker(fs, dir)
		return format, false, err
	}
	if err := createFormatMarker(fs, dir); err != nil {
		return 0, false, err
	}
	return 2, true, nil
}

func checkFormatMarker(fs vfs.FS, dir string) (int, error) {
	f, err := fs.Open(fs.PathJoin(dir, "CHUNKSTORE"))
	if err != nil {
		return 0, fmt.Errorf("%w: missing marker; migrate legacy data through buckyd", ErrFormat)
	}
	data, readErr := io.ReadAll(io.LimitReader(f, int64(len(formatMarkerV2)+1)))
	closeErr := f.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return 0, fmt.Errorf("read store marker: %w", err)
	}
	switch string(data) {
	case formatMarkerV1:
		return 1, nil
	case formatMarkerV2:
		return 2, nil
	default:
		return 0, ErrFormat
	}
}

func createFormatMarker(fs vfs.FS, dir string) error {
	// Sync newly created directory entries too: syncing the WAL and store
	// directory alone cannot make a missing ancestor survive a crash.
	var parents []string
	for path := dir; ; path = fs.PathDir(path) {
		if _, err := fs.Stat(path); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		parent := fs.PathDir(path)
		if parent == path {
			return fmt.Errorf("store directory has no existing ancestor: %s", dir)
		}
		parents = append(parents, parent)
	}
	if err := fs.MkdirAll(dir, 0755); err != nil {
		return err
	}
	f, err := fs.Create(fs.PathJoin(dir, "CHUNKSTORE"))
	if err != nil {
		return err
	}
	_, writeErr := f.Write([]byte(formatMarkerV2))
	syncErr := f.Sync()
	if err := errors.Join(writeErr, syncErr, f.Close()); err != nil {
		return fmt.Errorf("write store marker: %w", err)
	}
	if err := syncStoreDirectory(fs, dir); err != nil {
		return err
	}
	for _, path := range parents {
		if err := syncStoreDirectory(fs, path); err != nil {
			return err
		}
	}
	return nil
}

func upgradeFormatMarker(fs vfs.FS, dir string) error {
	temporary := fs.PathJoin(dir, "CHUNKSTORE.upgrade")
	f, err := fs.Create(temporary)
	if err != nil {
		return err
	}
	_, writeErr := f.Write([]byte(formatMarkerV2))
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("write upgraded store marker: %w", err)
	}
	if err := fs.Rename(temporary, fs.PathJoin(dir, "CHUNKSTORE")); err != nil {
		return fmt.Errorf("replace store marker: %w", err)
	}
	if err := syncStoreDirectory(fs, dir); err != nil {
		return fmt.Errorf("sync upgraded store marker: %w", err)
	}
	return nil
}

func syncStoreDirectory(fs vfs.FS, dir string) error {
	d, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func syncFormatMarker(fs vfs.FS, dir string) error {
	f, err := fs.Open(fs.PathJoin(dir, "CHUNKSTORE"))
	if err != nil {
		return err
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return err
	}
	return syncStoreDirectory(fs, dir)
}

func (s *Store) Close() error {
	if s.syncStop != nil {
		close(s.syncStop)
		<-s.syncDone
	}
	// Writers must be stopped before Close. Pebble flushes and syncs the WAL
	// on close, including mutations made after the final periodic sync.
	err := s.db.Close()
	if s.cache != nil {
		s.cache.Unref()
	}
	return err
}
func (s *Store) Flush() error                         { return s.db.Flush() }
func (s *Store) AsyncFlush() (<-chan struct{}, error) { return s.db.AsyncFlush() }
func (s *Store) Compact() error {
	if err := s.db.Flush(); err != nil {
		return err
	}
	return s.db.Compact([]byte{0}, []byte{255}, true)
}

type Stats struct {
	DiskBytes, WALBytes, MemTableBytes, Materializations, Operands uint64
	CacheBytes, CacheHits, CacheMisses                             uint64
}

func (s *Store) Stats() Stats {
	m := s.db.Metrics()
	return Stats{
		DiskBytes: uint64(m.DiskSpaceUsage()), WALBytes: m.WAL.Size, MemTableBytes: m.MemTable.Size,
		Materializations: s.materializations.Load(), Operands: s.operands.Load(),
		CacheBytes: uint64(m.BlockCache.Size), CacheHits: uint64(m.BlockCache.Hits), CacheMisses: uint64(m.BlockCache.Misses),
	}
}

func (s *Store) lockMetric(name string) func() {
	mu := &s.metricMu[helper.HashString(name)%uint64(len(s.metricMu))]
	mu.Lock()
	return mu.Unlock
}

func (s *Store) commit(batch *pebble.Batch) error {
	if err := batch.Commit(s.writeOptions); err != nil {
		return err
	}
	if !s.writeOptions.Sync {
		s.pendingSync.Store(true)
	}
	return nil
}

func catalogKey(name string) []byte { return append([]byte("m/"), name...) }
func sequenceKey() []byte           { return []byte("z/sequence") }
func uint64Bytes(v uint64) []byte   { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }
func revisionBytes(m Metadata) []byte {
	b := make([]byte, revisionSize)
	binary.BigEndian.PutUint64(b[:8], m.Revision)
	if !m.LastUpdate.IsZero() {
		binary.BigEndian.PutUint64(b[8:], uint64(m.LastUpdate.UnixNano()))
	}
	return b
}
func decodeRevision(value []byte, m *Metadata) error {
	switch len(value) {
	case legacyRevisionSize:
		m.Revision = binary.BigEndian.Uint64(value)
		m.LastUpdate = time.Time{}
	case revisionSize:
		m.Revision = binary.BigEndian.Uint64(value[:8])
		nanos := int64(binary.BigEndian.Uint64(value[8:]))
		if nanos == 0 {
			m.LastUpdate = time.Time{}
		} else {
			m.LastUpdate = time.Unix(0, nanos)
		}
	default:
		return errors.New("invalid metric revision")
	}
	return nil
}
func (s *Store) nextActivity(previous time.Time) time.Time {
	now := time.Unix(0, s.now().UnixNano())
	if !previous.IsZero() && now.Before(previous) {
		return previous
	}
	// An all-zero encoded timestamp means "activity unknown"; a clock that
	// reads exactly the Unix epoch must not collide with that sentinel.
	if now.UnixNano() == 0 {
		return time.Unix(0, 1)
	}
	return now
}
func revisionKey(m Metadata) []byte {
	b := make([]byte, 17)
	b[0] = 'r'
	binary.BigEndian.PutUint64(b[1:], m.ID)
	binary.BigEndian.PutUint64(b[9:], m.Generation)
	return b
}

func (s *Store) Create(ctx context.Context, config MetricConfig) (Metadata, error) {
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := validate(config); err != nil {
		return Metadata{}, err
	}
	unlock := s.lockMetric(config.Name)
	defer unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, closer, err := s.db.Get(catalogKey(config.Name)); err == nil {
		closer.Close()
		return Metadata{}, ErrExists
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return Metadata{}, err
	}
	id := uint64(1)
	if v, closer, err := s.db.Get(sequenceKey()); err == nil {
		if len(v) != 8 {
			closer.Close()
			return Metadata{}, errors.New("invalid catalog sequence")
		}
		previous := binary.BigEndian.Uint64(v)
		closer.Close()
		if previous == math.MaxUint64 {
			return Metadata{}, errors.New("metric IDs exhausted")
		}
		id = previous + 1
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return Metadata{}, err
	}
	m := Metadata{MetricConfig: cloneConfig(config), ID: id, Generation: 1, Revision: 1, LastUpdate: s.nextActivity(time.Time{})}
	encoded, err := json.Marshal(m)
	if err != nil {
		return Metadata{}, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	for _, kv := range []struct{ k, v []byte }{{catalogKey(config.Name), encoded}, {sequenceKey(), uint64Bytes(id)}, {revisionKey(m), revisionBytes(m)}} {
		if err := b.Set(kv.k, kv.v, nil); err != nil {
			return Metadata{}, err
		}
	}
	if err := s.commit(b); err != nil {
		return Metadata{}, fmt.Errorf("commit create: %w", err)
	}
	return m, nil
}

func (s *Store) Metadata(ctx context.Context, name string) (Metadata, error) {
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	return metadataFrom(snapshot, name)
}
func metadataFrom(reader pebble.Reader, name string) (Metadata, error) {
	v, closer, err := reader.Get(catalogKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return Metadata{}, ErrNotFound
	}
	if err != nil {
		return Metadata{}, fmt.Errorf("read catalog: %w", err)
	}
	var m Metadata
	err = json.Unmarshal(v, &m)
	closer.Close()
	if err != nil {
		return Metadata{}, fmt.Errorf("decode catalog: %w", err)
	}
	v, closer, err = reader.Get(revisionKey(m))
	if err != nil {
		return Metadata{}, fmt.Errorf("read revision: %w", err)
	}
	defer closer.Close()
	if err := decodeRevision(v, &m); err != nil {
		return Metadata{}, err
	}
	return m, nil
}
