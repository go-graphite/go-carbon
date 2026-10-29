package whisperio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	whisper "github.com/go-graphite/go-whisper"
)

var continuationNames atomic.Uint64

// A filesystem is one metric operation over one snapshot or indexed batch.
// Only headers and file identities stay in memory. Historical values live in
// shared exact blocks; no raw Whisper body or per-metric OS file is retained.
type continuationFiles struct {
	sidecarDirty bool
	kv           store.ContinuationIO
	readOnly     bool
	// buckets writes new compressed blocks to the store's time-partitioned
	// key space, bucketed by the day clock reports when each is written.
	buckets bool
	// prober, when kv has one, reads keys that are usually absent or long
	// unchanged (see probe).
	prober store.ContinuationProber
	clock  func() time.Time
	dir    string
	// A nil entry records an absent name in this operation's snapshot/batch.
	files map[string]*continuationFileData
	byID  map[uint64]*continuationFileData
	// The initial namespace is local to this operation. files overlays later
	// namespace mutations, including absent names, until the batch commits.
	names map[string]uint64
	// timer, non-nil for a sampled persister update, times the reads and
	// encodes whisper issues while merging.
	timer *stagetime.Update
	// reuseBlockKeys lets a block rewritten under the same keys overwrite its
	// old groups instead of deleting them first.
	reuseBlockKeys bool
	// verifiedBlocks is local to physical verification and its visible
	// snapshot over the same immutable Pebble reader. It is never used by
	// imports, updates or writable OOO snapshot overlays.
	verifiedBlocks map[verifiedBlockKey][]whisper.TimeSeriesPoint
}

type verifiedBlockKey struct {
	file  uint64
	block whisper.ContinuationBlock
}

type continuationFileData struct {
	sidecarCount *uint64
	id           uint64
	layout       whisper.StorageFileLayout
	header       []byte
	// pages is the virtual classic-file allocation (4096-byte pages), used by
	// the OOO policy and to skip reading classic groups (see classicGroup).
	// Invariant: every stored classic point group lies on allocated pages.
	// Both writers of classic groups, writeAt and Import, allocate the pages of
	// every slot they store, in the same operation and batch; pages are never
	// cleared except with the file's keys (O_TRUNC); and the manifest, which
	// every format version encodes pages into, is rewritten whenever pages
	// change (allocation sets dirty). Relayout moves only compressed blocks.
	pages       map[uint32]bool
	sortedPages []uint32
	dirty       bool
	refs        int
	// A paged header (format v7) is stored as continuationHeaderPage-byte
	// keys, of which an update rewrites only those it changed; dirty then
	// covers only layout and allocation. Otherwise it is part of the manifest.
	headerPaged bool
	dirtyPages  map[int]bool
}

// Each update changes a few hundred bytes of a header of up to a few KiB
// (buffers, encoder state, the current block's range); rewriting it whole
// made it the largest hot value in the store.
const continuationHeaderPage = 128

func continuationHeaderPrefix(id uint64) []byte { return append(continuationFilePrefix(id), 'H') }

// writeHeader stores data at offset and records which pages changed. CWhisper
// rewrites the whole header on each update, so compare page by page.
func (d *continuationFileData) writeHeader(offset int, data []byte) {
	for len(data) > 0 {
		page := offset / continuationHeaderPage
		n := min(len(data), (page+1)*continuationHeaderPage-offset)
		if !bytes.Equal(d.header[offset:offset+n], data[:n]) {
			copy(d.header[offset:offset+n], data[:n])
			if !d.headerPaged {
				d.dirty = true
			} else {
				if d.dirtyPages == nil {
					d.dirtyPages = make(map[int]bool)
				}
				d.dirtyPages[page] = true
			}
		}
		offset, data = offset+n, data[n:]
	}
}

type continuationFile struct {
	fs       *continuationFiles
	data     *continuationFileData
	name     string
	position int64
	readOnly bool
	closed   bool
	// sidecar marks the OOO sidecar, whose point groups are probed (see
	// pointBlock).
	sidecar bool
}

func newContinuationFiles(kv store.ContinuationIO, readOnly bool) *continuationFiles {
	fs := &continuationFiles{kv: kv, readOnly: readOnly, clock: time.Now,
		dir:   fmt.Sprintf("/shared-exact/%d", continuationNames.Add(1)),
		files: make(map[string]*continuationFileData), byID: make(map[uint64]*continuationFileData)}
	if b, ok := kv.(store.ContinuationTimeBuckets); ok {
		fs.buckets = b.TimeBuckets()
	}
	fs.prober, _ = kv.(store.ContinuationProber)
	return fs
}

// probe reads a key that is usually absent or long unchanged, which the store
// may serve through the operation's shared iterator. Get serves keys that
// most updates rewrite: a fresh lookup stops at their newest version.
func (fs *continuationFiles) probe(key []byte) ([]byte, error) {
	if fs.prober == nil {
		return fs.kv.Get(key)
	}
	return fs.prober.Probe(key)
}

// manifest reads the manifest of file id, named name. Most are rarely
// rewritten, so they are probed: a sidecar's changes only when late points
// arrive, and in a v7 store, whose headers are paged, any file's only when its
// layout or allocation does. A v6 main file's holds the header, which every
// update rewrites.
func (fs *continuationFiles) manifest(name string, id uint64) ([]byte, error) {
	if fs.buckets || fs.sidecar(name) {
		return fs.probe(continuationManifestKey(id))
	}
	return fs.kv.Get(continuationManifestKey(id))
}

// sidecar reports whether name is the OOO sidecar of the main file.
func (fs *continuationFiles) sidecar(name string) bool {
	return name == filepath.Base(whisper.OutOfOrderSidecarPath(fs.main()))
}

// withClock sets the clock that buckets newly written blocks.
func (fs *continuationFiles) withClock(clock func() time.Time) *continuationFiles {
	if clock != nil {
		fs.clock = clock
	}
	return fs
}

// deleteFile removes every key of file id, including blocks held in the
// time-partitioned space, which only its descriptors locate.
func (fs *continuationFiles) deleteFile(id uint64) error {
	if fs.buckets {
		buckets, err := continuationBuckets(fs.kv, append(continuationFilePrefix(id), 'b'))
		if err != nil {
			return err
		}
		for _, bucket := range buckets {
			if err := fs.kv.DeletePrefix(store.ContinuationBucketKey(bucket, continuationFilePrefix(id))); err != nil {
				return err
			}
		}
	}
	return fs.kv.DeletePrefix(continuationFilePrefix(id))
}

// continuationBuckets lists, in order, the buckets that block descriptors
// under prefix point to.
func continuationBuckets(kv store.ContinuationIO, prefix []byte) ([]uint32, error) {
	seen := make(map[uint32]bool)
	err := kv.Visit(prefix, func(key, value []byte) error {
		if len(key) == 16 && key[9] == 'b' {
			if d, err := decodeBlockDescriptor(value); err != nil {
				return err
			} else if d.bucketed {
				seen[d.bucket] = true
			}
		}
		return nil
	})
	buckets := make([]uint32, 0, len(seen))
	for bucket := range seen {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
	return buckets, err
}

// AtomicStorageCommit reports that every change one update makes lands in the
// store's single batch for that update (see flush), so go-whisper need not
// order publishing a rewrite and removing a sidecar for crash safety.
func (fs *continuationFiles) AtomicStorageCommit() bool { return !fs.readOnly }

func (fs *continuationFiles) main() string { return filepath.Join(fs.dir, "main.wsp") }

func (fs *continuationFiles) basename(path string) (string, error) {
	if filepath.Dir(path) != fs.dir || filepath.Base(path) == "." || len(filepath.Base(path)) > 255 {
		return "", errors.New("continuation path escapes its metric")
	}
	return filepath.Base(path), nil
}

func continuationNameKey(name string) []byte { return append([]byte("n/"), name...) }
func continuationFilePrefix(id uint64) []byte {
	b := make([]byte, 9)
	b[0] = 'f'
	binary.BigEndian.PutUint64(b[1:], id)
	return b
}
func continuationManifestKey(id uint64) []byte { return append(continuationFilePrefix(id), 'h') }

func continuationBodyKey(id uint64, kind byte, archive, block, group int) []byte {
	b := append(continuationFilePrefix(id), kind)
	b = binary.BigEndian.AppendUint16(b, uint16(archive))
	b = binary.BigEndian.AppendUint32(b, uint32(block))
	return binary.BigEndian.AppendUint32(b, uint32(group))
}
func continuationBlockPrefix(id uint64, kind byte, archive, block int) []byte {
	return continuationBodyKey(id, kind, archive, block, 0)[:16]
}

var errContinuationNamespaceLarge = errors.New("continuation namespace exceeds prefetch bound")

// Share the namespace walk already required by flush with initial file opens.
// Call before opening files, and retain no decoded header across operations.
func (fs *continuationFiles) prefetchNames() error {
	names := make(map[string]uint64)
	err := fs.kv.Visit([]byte("n/"), func(key, value []byte) error {
		if len(names) == 64 || len(key) > 2+255 {
			return errContinuationNamespaceLarge
		}
		if len(key) < 2 || len(value) != 8 {
			return errors.New("invalid continuation file pointer")
		}
		names[string(key[2:])] = binary.BigEndian.Uint64(value)
		return nil
	})
	if errors.Is(err, errContinuationNamespaceLarge) {
		return nil
	}
	if err != nil {
		return err
	}
	fs.names = names
	return nil
}

func (fs *continuationFiles) load(name string) (*continuationFileData, error) {
	if d, ok := fs.files[name]; ok {
		if d == nil {
			return nil, os.ErrNotExist
		}
		return d, nil
	}
	id, found := fs.names[name]
	if fs.names == nil {
		value, err := fs.probe(continuationNameKey(name))
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		if err == nil {
			if len(value) != 8 {
				return nil, errors.New("invalid continuation file identity")
			}
			id, found = binary.BigEndian.Uint64(value), true
		}
	}
	if !found {
		// Every namespace mutation below updates this operation-local cache.
		// Repeated absent OOO probes need not reopen an LSM point iterator.
		fs.files[name] = nil
		return nil, os.ErrNotExist
	}
	d := fs.byID[id]
	if d == nil {
		encoded, err := fs.manifest(name, id)
		if err != nil {
			return nil, err
		}
		d, err = decodeContinuationManifest(encoded)
		if err != nil {
			return nil, err
		}
		if d.headerPaged {
			if d.header, err = fs.loadHeader(id, d.layout.HeaderSize); err != nil {
				return nil, err
			}
		}
		d.id = id
		fs.byID[id] = d
	}
	fs.files[name] = d
	return d, nil
}

func (fs *continuationFiles) loadHeader(id uint64, size int) ([]byte, error) {
	header := make([]byte, size)
	prefix := continuationHeaderPrefix(id)
	pages := 0
	err := fs.kv.Visit(prefix, func(key, value []byte) error {
		if len(key) != len(prefix)+2 {
			return errors.New("invalid continuation header page key")
		}
		page := int(binary.BigEndian.Uint16(key[len(prefix):]))
		start := page * continuationHeaderPage
		if page != pages || start >= size || len(value) != min(continuationHeaderPage, size-start) {
			return errors.New("invalid continuation header page")
		}
		copy(header[start:], value)
		pages++
		return nil
	})
	if err != nil {
		return nil, err
	}
	if pages != (size+continuationHeaderPage-1)/continuationHeaderPage {
		return nil, errors.New("missing continuation header page")
	}
	return header, nil
}

func (fs *continuationFiles) setHeaderPage(d *continuationFileData, page int) error {
	start := page * continuationHeaderPage
	key := binary.BigEndian.AppendUint16(continuationHeaderPrefix(d.id), uint16(page))
	return fs.kv.Set(key, d.header[start:min(start+continuationHeaderPage, len(d.header))])
}

func (fs *continuationFiles) create(name string) (*continuationFileData, error) {
	fs.sidecarDirty = fs.sidecarDirty || fs.sidecar(name)
	if fs.readOnly {
		return nil, os.ErrPermission
	}
	previous := uint64(0)
	if value, err := fs.probe([]byte("sequence")); err == nil {
		if len(value) != 8 {
			return nil, errors.New("invalid continuation sequence")
		}
		previous = binary.BigEndian.Uint64(value)
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	if previous == math.MaxUint64 {
		return nil, errors.New("continuation file identities exhausted")
	}
	id := previous + 1
	value := binary.BigEndian.AppendUint64(nil, id)
	if err := fs.kv.Set([]byte("sequence"), value); err != nil {
		return nil, err
	}
	if err := fs.kv.Set(continuationNameKey(name), value); err != nil {
		return nil, err
	}
	d := &continuationFileData{id: id, pages: make(map[uint32]bool), dirty: true}
	fs.files[name], fs.byID[id] = d, d
	return d, nil
}

func (fs *continuationFiles) OpenFile(path string, flags int, mode os.FileMode) (whisper.StorageFile, error) {
	name, err := fs.basename(path)
	if err != nil {
		return nil, err
	}
	readOnly := flags&(os.O_RDWR|os.O_WRONLY) == 0
	if fs.readOnly && (!readOnly || flags&(os.O_CREATE|os.O_TRUNC) != 0) {
		return nil, os.ErrPermission
	}
	d, err := fs.load(name)
	if err == nil && flags&os.O_CREATE != 0 && flags&os.O_EXCL != 0 {
		return nil, os.ErrExist
	}
	if errors.Is(err, os.ErrNotExist) && flags&os.O_CREATE != 0 {
		d, err = fs.create(name)
	}
	if err != nil {
		return nil, err
	}
	if flags&os.O_TRUNC != 0 {
		if readOnly {
			return nil, os.ErrPermission
		}
		if err := fs.deleteFile(d.id); err != nil {
			return nil, err
		}
		d.layout, d.header, d.pages, d.dirty = whisper.StorageFileLayout{}, nil, make(map[uint32]bool), true
		d.sidecarCount = nil
	}
	d.refs++
	return &continuationFile{fs: fs, data: d, name: path, readOnly: readOnly, sidecar: fs.sidecar(name)}, nil
}

func (fs *continuationFiles) Stat(path string) (os.FileInfo, error) {
	name, err := fs.basename(path)
	if err != nil {
		return nil, err
	}
	d, err := fs.load(name)
	if err != nil {
		return nil, err
	}
	blocks := int64((d.layout.Size + 4095) / 4096 * 8)
	if !d.layout.Compressed {
		blocks = int64(len(d.pages) * 8)
	}
	return continuationFileInfo{name: name, size: int64(d.layout.Size), blocks: blocks}, nil
}

func (fs *continuationFiles) Remove(path string) error {
	fs.sidecarDirty = fs.sidecarDirty || fs.sidecar(filepath.Base(path))
	if fs.readOnly {
		return os.ErrPermission
	}
	name, err := fs.basename(path)
	if err != nil {
		return err
	}
	if _, err := fs.load(name); err != nil {
		return err
	}
	if err := fs.kv.Delete(continuationNameKey(name)); err != nil {
		return err
	}
	fs.files[name] = nil
	return nil
}

func (fs *continuationFiles) Rename(source, destination string) error {
	if fs.readOnly {
		return os.ErrPermission
	}
	a, err := fs.basename(source)
	if err != nil {
		return err
	}
	b, err := fs.basename(destination)
	if err != nil {
		return err
	}
	d, err := fs.load(a)
	if err != nil || a == b {
		return err
	}
	if _, err := fs.load(b); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := fs.kv.Set(continuationNameKey(b), binary.BigEndian.AppendUint64(nil, d.id)); err != nil {
		return err
	}
	if err := fs.kv.Delete(continuationNameKey(a)); err != nil {
		return err
	}
	fs.files[b] = d
	fs.files[a] = nil
	return nil
}

func (fs *continuationFiles) flush() error {
	if fs.readOnly {
		return nil
	}
	linked := make(map[uint64]bool)
	if fs.names != nil {
		for name, id := range fs.names {
			if _, changed := fs.files[name]; !changed {
				linked[id] = true
			}
		}
		for _, d := range fs.files {
			if d != nil {
				linked[d.id] = true
			}
		}
	} else {
		if err := fs.kv.Visit([]byte("n/"), func(key, value []byte) error {
			if len(value) != 8 {
				return errors.New("invalid continuation file pointer")
			}
			linked[binary.BigEndian.Uint64(value)] = true
			return nil
		}); err != nil {
			return err
		}
	}
	for id, d := range fs.byID {
		if !linked[id] && d.refs == 0 {
			if err := fs.deleteFile(id); err != nil {
				return err
			}
			continue
		}
		if fs.buckets && !d.headerPaged {
			// A v7 store moves each file to a paged header on its next write.
			d.headerPaged, d.dirty = true, true
		}
		if d.dirty {
			encoded, err := encodeContinuationManifest(d)
			if err != nil {
				return err
			}
			if err := fs.kv.Set(continuationManifestKey(id), encoded); err != nil {
				return err
			}
			if d.headerPaged {
				for page := 0; page*continuationHeaderPage < len(d.header); page++ {
					if err := fs.setHeaderPage(d, page); err != nil {
						return err
					}
				}
			}
			d.dirty, d.dirtyPages = false, nil
		} else if len(d.dirtyPages) > 0 {
			pages := make([]int, 0, len(d.dirtyPages))
			for page := range d.dirtyPages {
				pages = append(pages, page)
			}
			sort.Ints(pages)
			for _, page := range pages {
				if err := fs.setHeaderPage(d, page); err != nil {
					return err
				}
			}
			d.dirtyPages = nil
		}
	}
	return fs.flushSidecarSummary()
}

func (f *continuationFile) Name() string { return f.name }
func (f *continuationFile) Fd() uintptr  { return ^uintptr(0) }
func (f *continuationFile) Close() error {
	if f.closed {
		return os.ErrClosed
	}
	f.closed = true
	f.data.refs--
	return nil
}
func (f *continuationFile) writable() error {
	if f.closed {
		return os.ErrClosed
	}
	if f.readOnly || f.fs.readOnly {
		return os.ErrPermission
	}
	return nil
}
func (f *continuationFile) NeedsStorageLayout() bool { return f.data.layout.HeaderSize == 0 }
func (f *continuationFile) ConfigureStorage(layout whisper.StorageFileLayout) error {
	if err := f.writable(); err != nil {
		return err
	}
	if err := validateContinuationLayout(layout); err != nil {
		return err
	}
	f.data.layout = layout
	f.data.layout.Archives = append([]whisper.StorageArchiveLayout(nil), layout.Archives...)
	f.data.header = make([]byte, layout.HeaderSize)
	f.data.dirty = true
	return nil
}
func (f *continuationFile) Truncate(size int64) error {
	if err := f.writable(); err != nil {
		return err
	}
	if size != int64(f.data.layout.Size) {
		return errors.New("continuation truncate must match its configured layout")
	}
	if !f.data.layout.Compressed && size > 0 {
		// Ordinary sparse CWhisper sidecars allocate their final page at create.
		f.data.pages[uint32((size-1)/4096)] = true
		f.data.sortedPages = nil
	}
	f.data.dirty = true
	return nil
}
func (f *continuationFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	position := offset
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		position += f.position
	case io.SeekEnd:
		position += int64(f.data.layout.Size)
	default:
		return 0, errors.New("invalid continuation seek")
	}
	if position < 0 {
		return 0, errors.New("negative continuation seek")
	}
	f.position = position
	return position, nil
}
func (f *continuationFile) Read(data []byte) (int, error) {
	n, err := f.ReadAt(data, f.position)
	f.position += int64(n)
	return n, err
}
func (f *continuationFile) Write(data []byte) (int, error) {
	n, err := f.WriteAt(data, f.position)
	f.position += int64(n)
	return n, err
}

func validateContinuationLayout(layout whisper.StorageFileLayout) error {
	if layout.HeaderSize <= 0 || layout.Size < layout.HeaderSize || len(layout.Archives) == 0 || len(layout.Archives) > 64 {
		return errors.New("invalid continuation file layout")
	}
	end := int64(layout.HeaderSize)
	for _, a := range layout.Archives {
		if int64(a.Offset) != end || a.Step <= 0 || a.Count <= 0 {
			return errors.New("invalid continuation archive layout")
		}
		size := int64(a.Count) * 12
		if layout.Compressed {
			if a.BlockSize < 17 || a.BlockCount <= 0 {
				return errors.New("invalid continuation block layout")
			}
			if int64(a.BlockCount) > (int64(layout.Size)-end)/int64(a.BlockSize) {
				return errors.New("continuation blocks exceed file size")
			}
			size = int64(a.BlockSize) * int64(a.BlockCount)
		} else if int64(a.Count) > (int64(layout.Size)-end)/12 {
			return errors.New("continuation classic slots exceed file size")
		}
		end += size
		if end > int64(layout.Size) {
			return errors.New("continuation archive exceeds its file")
		}
	}
	if end != int64(layout.Size) {
		return errors.New("continuation file size mismatch")
	}
	return nil
}

func encodeContinuationManifest(d *continuationFileData) ([]byte, error) {
	if err := validateContinuationLayout(d.layout); err != nil {
		return nil, err
	}
	if len(d.header) != d.layout.HeaderSize {
		return nil, errors.New("continuation header size mismatch")
	}
	b := []byte{1, 0}
	if d.headerPaged {
		b[0] = 2
	}
	if d.layout.Compressed {
		b[1] = 1
	}
	for _, n := range []int{d.layout.Size, d.layout.HeaderSize, len(d.layout.Archives)} {
		b = binary.AppendUvarint(b, uint64(n))
	}
	for _, a := range d.layout.Archives {
		for _, n := range []int{a.Offset, a.Step, a.Count, a.BlockSize, a.BlockCount} {
			b = binary.AppendUvarint(b, uint64(n))
		}
	}
	pages := make([]uint32, 0, len(d.pages))
	for page := range d.pages {
		pages = append(pages, page)
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i] < pages[j] })
	b = binary.AppendUvarint(b, uint64(len(pages)))
	var previous uint32
	for _, page := range pages {
		b = binary.AppendUvarint(b, uint64(page-previous))
		previous = page
	}
	if d.headerPaged {
		return b, nil
	}
	return append(b, d.header...), nil
}

// Takes ownership of data, as returned by ContinuationIO.Get. No returned
// header is shared with the database or with another metric operation.
func decodeContinuationManifest(data []byte) (*continuationFileData, error) {
	capacity := cap(data)
	if len(data) < 2 || data[0] < 1 || data[0] > 2 || data[1] > 1 {
		return nil, errors.New("invalid continuation manifest")
	}
	d := &continuationFileData{layout: whisper.StorageFileLayout{Compressed: data[1] == 1}, pages: make(map[uint32]bool), headerPaged: data[0] == 2}
	data = data[2:]
	read := func() (int, error) {
		value, size := binary.Uvarint(data)
		if size <= 0 || value > uint64(^uint(0)>>1) {
			return 0, errors.New("invalid continuation manifest integer")
		}
		data = data[size:]
		return int(value), nil
	}
	var err error
	if d.layout.Size, err = read(); err != nil {
		return nil, err
	}
	if d.layout.HeaderSize, err = read(); err != nil {
		return nil, err
	}
	count, err := read()
	if err != nil || count <= 0 || count > 64 {
		return nil, errors.New("invalid continuation archive count")
	}
	d.layout.Archives = make([]whisper.StorageArchiveLayout, count)
	for i := range d.layout.Archives {
		a := &d.layout.Archives[i]
		for _, field := range []*int{&a.Offset, &a.Step, &a.Count, &a.BlockSize, &a.BlockCount} {
			if *field, err = read(); err != nil {
				return nil, err
			}
		}
	}
	if err := validateContinuationLayout(d.layout); err != nil {
		return nil, err
	}
	count, err = read()
	if err != nil || count > len(data) || count > (d.layout.Size+4095)/4096 {
		return nil, errors.New("invalid continuation allocation pages")
	}
	page := uint64(0)
	for i := 0; i < count; i++ {
		delta, err := read()
		if err != nil || (i > 0 && delta == 0) {
			return nil, errors.New("invalid continuation page delta")
		}
		page += uint64(delta)
		if page >= uint64((d.layout.Size+4095)/4096) {
			return nil, errors.New("continuation allocation outside file")
		}
		d.pages[uint32(page)] = true
	}
	if d.headerPaged {
		if len(data) != 0 {
			return nil, errors.New("invalid paged continuation manifest")
		}
		return d, nil
	}
	if len(data) != d.layout.HeaderSize {
		return nil, errors.New("invalid continuation header length")
	}
	// Compressed manifests usually have only a short layout before the header.
	// Reuse their already-owned storage, but bound the extra retained capacity.
	// Classic OOO manifests can have large allocation-page lists; keep copying
	// those so a small header cannot retain the whole manifest. Paged (v7)
	// headers are not in the manifest and returned above.
	if d.layout.Compressed && capacity-len(data) <= 256 {
		d.header = data[:len(data):len(data)]
	} else {
		d.header = append([]byte(nil), data...)
	}
	return d, nil
}

type continuationFileInfo struct {
	name         string
	size, blocks int64
}

func (i continuationFileInfo) Name() string       { return i.name }
func (i continuationFileInfo) Size() int64        { return i.size }
func (i continuationFileInfo) Mode() os.FileMode  { return 0644 }
func (i continuationFileInfo) ModTime() time.Time { return time.Time{} }
func (i continuationFileInfo) IsDir() bool        { return false }
func (i continuationFileInfo) Sys() interface{}   { return &syscall.Stat_t{Blocks: i.blocks} }

var _ whisper.StorageFileSystem = (*continuationFiles)(nil)
