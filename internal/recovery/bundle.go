package recovery

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/blevesearch/mmap-go"
	"github.com/go-graphite/go-carbon/points"
)

const (
	manifestName = ".pending-points.json"
	// Chunk digests grow with checkpoint size. Bound metadata independently of
	// source bytes while allowing large production dumps to use the fast path.
	maxManifestSize = 1 << 20
)

type File struct {
	Name      string
	Size      int64
	SHA256    [sha256.Size]byte
	ChunkSize int                 `json:",omitempty"`
	Chunks    [][sha256.Size]byte `json:",omitempty"`
}
type Manifest struct {
	Version           int
	ReadIndexID       string
	Root              string
	Device, Inode     uint64
	Cache, WAL, Index File
}

func rootIdentity(root string) (string, uint64, uint64, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", 0, 0, err
	}
	info, err := os.Stat(root)
	if err != nil {
		return "", 0, 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() {
		return "", 0, 0, fmt.Errorf("recovery data root identity unavailable")
	}
	return root, uint64(stat.Dev), uint64(stat.Ino), nil
}

// Describe hashes a closed/synchronized legacy source or index file. The live
// writers may instead compute this digest while writing, before publication.
func Describe(path string) (File, error) {
	f, err := os.Open(path)
	if err != nil {
		return File{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return File{}, err
	}
	if !info.Mode().IsRegular() {
		return File{}, fmt.Errorf("recovery source is not a regular file")
	}
	hash := newFileDigester()
	n, err := io.Copy(hash, f)
	if err != nil {
		return File{}, err
	}
	if n != info.Size() {
		return File{}, fmt.Errorf("recovery source changed while hashing")
	}
	return hash.descriptor(filepath.Base(path), n), nil
}

func validFile(file File, prefix, suffix string) bool {
	return file.Size >= 0 && validChecksumShape(file) && filepath.Base(file.Name) == file.Name && strings.HasPrefix(file.Name, prefix) && strings.HasSuffix(file.Name, suffix)
}
func validManifest(m *Manifest) bool {
	return m.Version == 1 && validFile(m.Cache, "cache.", ".bin") && validFile(m.WAL, "input.", ".bin") && validFile(m.Index, ".pending-index-", ".bin")
}

// Publish must run after both legacy files and the index have been synchronized.
// If publishing the accelerator fails, callers retain the ordinary dump files.
func Publish(dir, root string, cache, wal, index File, readIndexID string) error {
	identity, device, inode, err := rootIdentity(root)
	if err != nil {
		return err
	}
	manifest := Manifest{Version: 1, ReadIndexID: readIndexID, Root: identity, Device: device, Inode: inode, Cache: cache, WAL: wal, Index: index}
	if !validManifest(&manifest) {
		return fmt.Errorf("invalid recovery manifest")
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if len(data) > maxManifestSize {
		return fmt.Errorf("recovery manifest oversized")
	}
	file, err := os.CreateTemp(dir, ".pending-manifest-*")
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	if _, err = file.Write(data); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(file.Name(), filepath.Join(dir, manifestName)); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

type mappings []mmap.MMap

func (m mappings) close() error {
	var err error
	for _, data := range m {
		if data != nil {
			err = errors.Join(err, data.Unmap())
		}
	}
	return err
}

// Bundle owns immutable mappings. Read returns detached points; the reachability
// cleanup cannot release a mapping while a cache reader still owns this bundle.
type Bundle struct {
	manifest Manifest
	index    *Index
	maps     mappings
	cleanup  runtime.Cleanup
	dir      string
}

func mapFile(dir string, file File) (mmap.MMap, error) {
	f, err := os.Open(filepath.Join(dir, file.Name))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != file.Size {
		return nil, fmt.Errorf("recovery source size/type changed")
	}
	if file.Size == 0 {
		if !verifyFileChecksum(nil, file) {
			return nil, fmt.Errorf("empty recovery source checksum mismatch")
		}
		return nil, nil
	}
	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	if !verifyFileChecksum(data, file) {
		_ = data.Unmap()
		return nil, fmt.Errorf("recovery source checksum mismatch")
	}
	return data, nil
}

func OpenBundle(dir, root string) (_ *Bundle, err error) {
	f, err := os.Open(filepath.Join(dir, manifestName))
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(f, maxManifestSize+1))
	closeErr := f.Close()
	if err = errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if len(raw) > maxManifestSize {
		return nil, fmt.Errorf("recovery manifest oversized")
	}
	var manifest Manifest
	if err = json.Unmarshal(raw, &manifest); err != nil {
		return nil, err
	}
	if !validManifest(&manifest) {
		return nil, fmt.Errorf("unsupported recovery manifest")
	}
	root, device, inode, err := rootIdentity(root)
	if err != nil {
		return nil, err
	}
	if manifest.Root != root || manifest.Device != device || manifest.Inode != inode {
		return nil, fmt.Errorf("recovery data root changed")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	// An extra generation or live journal requires ordered legacy replay. A crash
	// during background recovery therefore cannot reuse an incomplete checkpoint.
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == manifest.Cache.Name || name == manifest.WAL.Name {
			continue
		}
		if strings.HasPrefix(name, "cache.") || strings.HasPrefix(name, "input.") {
			return nil, fmt.Errorf("unindexed recovery generation exists")
		}
	}
	b := &Bundle{manifest: manifest, dir: dir}
	defer func() {
		if err != nil {
			_ = b.Close()
		}
	}()
	// These three immutable files are independent. Keep every checksum and
	// structural check, but overlap validation instead of hashing serially.
	b.maps = make(mappings, 3)
	checks := make([]error, len(b.maps))
	var wg sync.WaitGroup
	for i, file := range []File{manifest.Cache, manifest.WAL, manifest.Index} {
		wg.Go(func() { b.maps[i], checks[i] = mapFile(dir, file) })
	}
	wg.Wait()
	if err = errors.Join(checks...); err != nil {
		return nil, err
	}
	b.index, err = Open(b.maps[2], b.maps[0], b.maps[1])
	if err != nil {
		return nil, err
	}
	b.cleanup = runtime.AddCleanup(b, func(m mappings) { _ = m.close() }, b.maps)
	return b, nil
}

func (b *Bundle) Find(metric string) (uint64, bool, error) {
	defer runtime.KeepAlive(b)
	return b.index.Find(metric)
}
func (b *Bundle) Read(slot uint64) (*points.Points, error) {
	defer runtime.KeepAlive(b)
	return b.index.Read(slot)
}
func (b *Bundle) Name(slot uint64) (string, bool, error) {
	defer runtime.KeepAlive(b)
	return b.index.Name(slot)
}
func (b *Bundle) NewNames(visit func(string) error) error {
	defer runtime.KeepAlive(b)
	return b.index.NewNames(visit)
}
func (b *Bundle) Slots() uint64   { return b.index.Slots() }
func (b *Bundle) Points() uint64  { return b.index.Points() }
func (b *Bundle) Metrics() uint64 { return b.index.Metrics() }

// Close is for an unpublished bundle or an owner which has drained every reader.
func (b *Bundle) Close() error {
	b.cleanup.Stop()
	err := b.maps.close()
	b.maps = nil
	b.index = nil
	return err
}

// Retire removes source files only after every old point has been persisted.
// The caller keeps input receivers gated until this directory sync succeeds.
// Mappings held by readers remain valid after unlink.
func (b *Bundle) Retire() error {
	for _, file := range []File{b.manifest.Cache, b.manifest.WAL, b.manifest.Index} {
		if err := os.Remove(filepath.Join(b.dir, file.Name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	// Leave the manifest: missing source files force fallback, and a subsequent
	// shutdown atomically replaces it. This never removes another generation.
	return syncDirectory(b.dir)
}

func (b *Bundle) Count(slot uint64) uint64 { defer runtime.KeepAlive(b); return b.index.Count(slot) }

func (b *Bundle) ReadIndexID() string { return b.manifest.ReadIndexID }
