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

const formatMarker = "go-carbon-chunks-v1\n"

type Options struct {
	CacheSize    int64
	MemTableSize uint64
	Now          func() time.Time
	fs           vfs.FS // Fault-injection tests use the same storage path as production.
}

type Store struct {
	db    *pebble.DB
	cache *pebble.Cache
	now   func() time.Time
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
	fs := options.fs
	if fs == nil {
		fs = vfs.Default
	}
	if err := checkFormat(fs, dir); err != nil {
		return nil, err
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.MemTableSize == 0 {
		options.MemTableSize = 64 << 20
	}
	opts := &pebble.Options{MemTableSize: options.MemTableSize, Merger: chunkMerger, FS: fs}
	s := &Store{now: options.Now}
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
	return s, nil
}

func checkFormat(fs vfs.FS, dir string) error {
	names, err := fs.List(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("list store directory: %w", err)
	}
	if len(names) > 0 {
		f, err := fs.Open(fs.PathJoin(dir, "CHUNKSTORE"))
		if err != nil {
			return fmt.Errorf("%w: missing marker; migrate legacy data through buckyd", ErrFormat)
		}
		data, readErr := io.ReadAll(io.LimitReader(f, int64(len(formatMarker)+1)))
		closeErr := f.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return fmt.Errorf("read store marker: %w", err)
		}
		if string(data) != formatMarker {
			return ErrFormat
		}
		return nil
	}
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
	_, writeErr := f.Write([]byte(formatMarker))
	syncErr := f.Sync()
	if err := errors.Join(writeErr, syncErr, f.Close()); err != nil {
		return fmt.Errorf("write store marker: %w", err)
	}
	for _, path := range append([]string{dir}, parents...) {
		d, err := fs.OpenDir(path)
		if err != nil {
			return err
		}
		if err := errors.Join(d.Sync(), d.Close()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Close() error {
	err := s.db.Close()
	if s.cache != nil {
		s.cache.Unref()
	}
	return err
}
func (s *Store) Flush() error { return s.db.Flush() }
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
func catalogKey(name string) []byte { return append([]byte("m/"), name...) }
func sequenceKey() []byte           { return []byte("z/sequence") }
func uint64Bytes(v uint64) []byte   { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }
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
	m := Metadata{MetricConfig: cloneConfig(config), ID: id, Generation: 1, Revision: 1}
	encoded, err := json.Marshal(m)
	if err != nil {
		return Metadata{}, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	for _, kv := range []struct{ k, v []byte }{{catalogKey(config.Name), encoded}, {sequenceKey(), uint64Bytes(id)}, {revisionKey(m), uint64Bytes(1)}} {
		if err := b.Set(kv.k, kv.v, nil); err != nil {
			return Metadata{}, err
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return Metadata{}, fmt.Errorf("sync create: %w", err)
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
	if len(v) != 8 {
		return Metadata{}, errors.New("invalid metric revision")
	}
	m.Revision = binary.BigEndian.Uint64(v)
	return m, nil
}
