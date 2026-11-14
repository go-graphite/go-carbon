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
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/points"
)

// ManifestName names a published checkpoint's manifest in its directory.
const ManifestName = manifestName

// manifestName holds one SHA-256 per 16 MiB chunk of every checkpoint file,
// so the manifest grows with the cache (about 1.2 MiB for a 174 GB dump). It
// has no size limit: a fixed one turns a large cache into a restart without a
// checkpoint, which stops input until the whole dump is restored.
const manifestName = ".pending-points.json"

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

// validManifest accepts version 1, over a legacy cache dump, which every
// release with checkpoints reads, and version 2, over a compact one, which
// older releases reject and restore without, ignoring the compact dump.
//
// Version 3 goes with a compact cache dump, a compact input file and a
// GCPOINT2 index; releases before it reject it and ignore both files.
//
// Version 4 goes with a cache dump of FormatBlocks, with a compact input file
// and a GCPOINT3 (or GCPOINT2) index, or a legacy input file and a GCPOINT1
// index. Releases before it reject it and ignore the dump.
func validManifest(m *Manifest) bool {
	cachePrefix, walPrefix := "cache.", "input."
	switch m.Version {
	case 1:
	case 2:
		cachePrefix = CompactPrefix
	case 3:
		cachePrefix, walPrefix = CompactPrefix, CompactInputPrefix
	case 4:
		cachePrefix = BlocksPrefix
		if InputFormat(m.WAL.Name) == FormatCompact {
			walPrefix = CompactInputPrefix
		}
	default:
		return false
	}
	return validFile(m.Cache, cachePrefix, ".bin") && validFile(m.WAL, walPrefix, ".bin") && validFile(m.Index, ".pending-index-", ".bin")
}

func manifestVersion(cache, wal File) int {
	switch {
	case CacheFormat(cache.Name) == FormatBlocks:
		return 4
	case InputFormat(wal.Name) == FormatCompact:
		return 3
	case CacheFormat(cache.Name) == FormatCompact:
		return 2
	}
	return 1
}

// TailName names the input log a stopping instance appends to after its
// checkpoint while it hands input over to its successor. It sorts right after
// wal, so an ordered replay, by older binaries too, applies it after wal.
// The tail holds legacy records, so a compact input file's tail drops the
// compact prefix: input-compact.<x>.bin has tail input.<x>.tail.bin.
func TailName(wal string) string {
	if rest, ok := strings.CutPrefix(wal, CompactInputPrefix); ok {
		wal = "input." + rest
	}
	return strings.TrimSuffix(wal, ".bin") + ".tail.bin"
}

// IsGenerationTail includes the successor's independent capture shards. Their
// names sort after the predecessor tail. Epoch segments use independent frames.
func IsGenerationTail(name, wal string) bool {
	tail := TailName(wal)
	if name == tail {
		return true
	}
	if suffix, ok := strings.CutPrefix(name, tail+".capture-epoch-"); ok {
		return IsCaptureSegment(name) && len(suffix) == len("00000000000000000000.000000.000000000000.tail.bin")
	}
	suffix, ok := strings.CutPrefix(name, tail+".capture-")
	return ok && len(suffix) == len("00.tail.bin") && suffix[0] >= '0' && suffix[0] <= '9' && suffix[1] >= '0' && suffix[1] <= '9' && suffix[2:] == ".tail.bin"
}

// IsTail reports whether name is such an input log.
func IsTail(name string) bool {
	return strings.HasPrefix(name, "input.") && strings.HasSuffix(name, ".tail.bin")
}

// Publish must run after both legacy files and the index have been synchronized.
// If publishing the accelerator fails, callers retain the ordinary dump files.
func Publish(dir, root string, cache, wal, index File, readIndexID string) error {
	identity, device, inode, err := rootIdentity(root)
	if err != nil {
		return err
	}
	manifest := Manifest{Version: manifestVersion(cache, wal), ReadIndexID: readIndexID, Root: identity, Device: device, Inode: inode, Cache: cache, WAL: wal, Index: index}
	if !validManifest(&manifest) {
		return fmt.Errorf("invalid recovery manifest")
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
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
	arrivals map[string]int64
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

// loadManifest reads and validates dir's manifest, whatever its size.
func loadManifest(dir string) (Manifest, error) {
	var manifest Manifest
	raw, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		return manifest, err
	}
	if err = json.Unmarshal(raw, &manifest); err != nil {
		return manifest, err
	}
	if !validManifest(&manifest) {
		return manifest, fmt.Errorf("unsupported recovery manifest")
	}
	return manifest, nil
}

func OpenBundle(dir, root string) (_ *Bundle, err error) {
	manifest, err := loadManifest(dir)
	if err != nil {
		return nil, err
	}
	root, device, inode, err := rootIdentity(root)
	if err != nil {
		return nil, err
	}
	if manifest.Root != root || manifest.Device != device || manifest.Inode != inode {
		return nil, fmt.Errorf("recovery data root changed: saved path=%q device=%d inode=%d; current path=%q device=%d inode=%d", manifest.Root, manifest.Device, manifest.Inode, root, device, inode)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	// An extra generation or live journal requires ordered legacy replay. A crash
	// during background recovery therefore cannot reuse an incomplete checkpoint.
	// This generation's tail is input that arrived after the checkpoint; the
	// caller replays it before opening input.
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == manifest.Cache.Name || name == manifest.WAL.Name || IsGenerationTail(name, manifest.WAL.Name) {
			continue
		}
		if isGeneration(name) {
			return nil, fmt.Errorf("unindexed recovery generation exists")
		}
	}
	b := &Bundle{manifest: manifest, dir: dir}
	b.arrivals, err = ReadArrivals(filepath.Join(dir, manifest.Cache.Name))
	if err != nil {
		return nil, err
	}
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
	b.index, err = OpenFormat(b.maps[2], b.maps[0], b.maps[1], CacheFormat(manifest.Cache.Name))
	if err != nil {
		return nil, err
	}
	// Validation has touched every source byte. Do not retain a resident second
	// copy of the checkpoint while restore builds the live cache. These remain
	// valid read-only mappings: pending readers fault unchanged bytes back in.
	for _, data := range b.maps {
		discardMappedPages(data)
	}
	b.index.mappedCache = true
	b.cleanup = runtime.AddCleanup(b, func(m mappings) { _ = m.close() }, b.maps)
	return b, nil
}

func (b *Bundle) Find(metric string) (uint64, bool, error) {
	defer runtime.KeepAlive(b)
	return b.index.Find(metric)
}
func (b *Bundle) Read(slot uint64) (*points.Points, error) {
	defer runtime.KeepAlive(b)
	p, err := b.index.Read(slot)
	if p != nil {
		p.FirstArrival = b.FirstArrival(p.Metric)
	}
	return p, err
}

func (b *Bundle) ReadForWrite(slot uint64, extra int) (*points.Points, error) {
	defer runtime.KeepAlive(b)
	p, err := b.index.ReadForWrite(slot, extra)
	if p != nil {
		p.FirstArrival = b.FirstArrival(p.Metric)
	}
	return p, err
}

// RawRecords visits slot's encoded records in replay order; see Index.RawRecords.
func (b *Bundle) RawRecords(slot uint64, visit func(raw []byte, count int) error) error {
	defer runtime.KeepAlive(b)
	return b.index.RawRecords(slot, visit)
}

// ReadCompact reads slot for a compact encoder; see Index.ReadCompact. The
// borrowed name and head are valid only during the call that receives them
// or while the caller keeps b reachable.
func (b *Bundle) ReadCompact(slot uint64, tail []points.Point, head func(CompactRecord)) ([]byte, []points.Point, error) {
	defer runtime.KeepAlive(b)
	return b.index.ReadCompact(slot, tail, head)
}

// ReadCompactAs reads a slot known to hold metric name; see
// Index.ReadCompactAs. The head's data is borrowed like ReadCompact's.
func (b *Bundle) ReadCompactAs(slot uint64, name string, tail []points.Point, head func(CompactRecord)) ([]points.Point, error) {
	defer runtime.KeepAlive(b)
	return b.index.ReadCompactAs(slot, name, tail, head)
}

// VisitMetrics visits every saved metric once; see Index.VisitMetrics.
func (b *Bundle) VisitMetrics(visit func(slot uint64, name string) error) error {
	defer runtime.KeepAlive(b)
	defer discardMappedPages(b.maps[0]) // also on cancellation or callback error
	return b.index.VisitMetrics(visit)
}

// CacheFormat reports the record format of the checkpoint's cache dump.
func (b *Bundle) CacheFormat() Format { return b.index.CacheFormat() }

// Bytes reports the size of the checkpoint's cache dump and input file.
func (b *Bundle) Bytes() int64 { return b.manifest.Cache.Size + b.manifest.WAL.Size }

// CacheName reports the file name of the checkpoint's cache dump.
func (b *Bundle) CacheName() string { return b.manifest.Cache.Name }

func (b *Bundle) Name(slot uint64) (string, bool, error) {
	defer runtime.KeepAlive(b)
	return b.index.Name(slot)
}
func (b *Bundle) NewNames(visit func(string) error) error {
	defer runtime.KeepAlive(b)
	return b.index.NewNames(visit)
}

// VisitNewNames borrows each name only during visit; see Index.VisitNewNames.
func (b *Bundle) VisitNewNames(visit func([]byte) error) error {
	defer runtime.KeepAlive(b)
	return b.index.VisitNewNames(visit)
}

// FilterNewNames keeps mappings alive until all predicate workers and serial
// visitors finish; see Index.FilterNewNames for ownership and ordering.
func (b *Bundle) FilterNewNames(keep func([]byte) bool, visit func(string) error) error {
	defer runtime.KeepAlive(b)
	return b.index.FilterNewNames(keep, visit)
}
func (b *Bundle) NewNameCount() uint64 { return b.index.NewNameCount() }

// VisitNewSlots calls visit with the slot of every metric absent from the
// saved read index when the dump was written.
func (b *Bundle) VisitNewSlots(visit func(slot uint64)) {
	defer runtime.KeepAlive(b)
	b.index.VisitNewSlots(visit)
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
	for _, file := range []File{b.manifest.Cache, b.manifest.WAL, b.manifest.Index, {Name: TailName(b.manifest.WAL.Name)}} {
		if IsTail(file.Name) && handoff.Locked(filepath.Join(b.dir, file.Name)) {
			continue
		}
		if err := os.Remove(filepath.Join(b.dir, file.Name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && IsGenerationTail(entry.Name(), b.manifest.WAL.Name) {
			if handoff.Locked(filepath.Join(b.dir, entry.Name())) {
				continue
			}
			if err := os.Remove(filepath.Join(b.dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	_ = os.Remove(ArrivalPath(filepath.Join(b.dir, b.manifest.Cache.Name)))
	// Leave the manifest: missing source files force fallback, and a subsequent
	// shutdown atomically replaces it. This never removes another generation.
	return syncDirectory(b.dir)
}

func (b *Bundle) Count(slot uint64) uint64 { defer runtime.KeepAlive(b); return b.index.Count(slot) }

func (b *Bundle) ReadIndexID() string { return b.manifest.ReadIndexID }

// IsCaptureSegment reports independently compressed startup spool segments.
func IsCaptureSegment(name string) bool {
	_, suffix, ok := strings.Cut(name, ".tail.bin.capture-epoch-")
	if !ok {
		return false
	}
	fields := strings.Split(suffix, ".")
	if len(fields) != 5 || len(fields[0]) != 20 || len(fields[1]) != 6 || len(fields[2]) != 12 || fields[3] != "tail" || fields[4] != "bin" {
		return false
	}
	for _, field := range fields[:3] {
		for _, c := range field {
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return true
}
