package partlog

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/klauspost/compress/zstd"
)

type catalogEntry struct {
	key        string
	definition []byte
	split      bool
}

// CatalogBinding resolves producer-local IDs in an existing immutable catalog.
// Bind must own and validate its inputs before returning. Bindings stay valid
// for the NameCatalog's lifetime, including checkpoint and WAL recovery.
// A bound NameCatalog retains only its numeric index, not per-name headers.
type CatalogBinding interface {
	Bind(uint64, string, []byte) error
	Key(uint64) string
	Definition(uint64) []byte
	Matches(uint64, string, []byte) bool
}

// NameCatalog is single-owner, including its codecs. Each journal stream owns a
// separate catalog. IDs are local to the random persisted epoch, never routing
// or replica metric IDs. Sync the catalog BEFORE syncing referencing segments.
// Checkpoint durably replaces the catalog prefix before reclaiming its WAL.
// Predictor epochs must start at every independently retained segment boundary.
type NameCatalog struct {
	binding    CatalogBinding
	boundCount uint64

	definitionArena    []byte
	definitionSuffixes map[string][]byte

	buffer         *bufio.Writer
	dirty          bool
	checkpointHook func(string) // test-only fault injection; nil in normal operation
	dir            string
	epoch          [16]byte
	entries        []catalogEntry
	ids            catalogIndex
	batchHashes    []uint64
	batchIDs       []uint64
	wal            *os.File
	failed         error
	pending        []uint64
	pendingBytes   int
	groupEncoder   *zstd.Encoder
	groupDecoder   *zstd.Decoder
}

var errCatalog = errors.New("invalid name catalog")

const catalogLimit = 64 << 20

var catalogCRC = crc32.MakeTable(crc32.Castagnoli)

func catalogHeader(epoch [16]byte) []byte {
	b := append([]byte("GCNCAT02"), epoch[:]...)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, catalogCRC))
}
func parseCatalogHeader(b []byte) ([16]byte, error) {
	var e [16]byte
	if len(b) != 28 || string(b[:8]) != "GCNCAT02" || crc32.Checksum(b[:24], catalogCRC) != binary.LittleEndian.Uint32(b[24:]) {
		return e, errCatalog
	}
	copy(e[:], b[8:24])
	return e, nil
}
func catalogRecord(id uint64, key string, definition []byte) []byte {
	b := binary.AppendUvarint(nil, id)
	b = binary.AppendUvarint(b, uint64(len(key)))
	b = append(b, key...)
	b = binary.AppendUvarint(b, uint64(len(definition)))
	b = append(b, definition...)
	out := binary.LittleEndian.AppendUint32(nil, uint32(len(b)))
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, catalogCRC))
	out = append(out, b...)
	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, catalogCRC))
}
func syncCatalogDir(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}

// Publish each new directory entry before a referencing journal may become
// durable. Syncing only the new directory does not persist its parent's entry.
func makeCatalogDir(dir string) error {
	st, err := os.Stat(dir)
	if err == nil {
		if !st.IsDir() {
			return fmt.Errorf("%w: catalog path is not a directory", errCatalog)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(dir)
	if err = makeCatalogDir(parent); err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	return syncCatalogDir(parent)
}
func writeCatalogBytes(f *os.File, b []byte) error {
	n, e := f.Write(b)
	if e == nil && n != len(b) {
		e = io.ErrShortWrite
	}
	return e
}

// OpenNameCatalog recovers a durable checkpoint plus its append-only suffix.
// Only a physically incomplete final WAL record is discarded. CRC failures,
// ID holes, conflicting definitions and epoch mismatches fail closed.
func OpenNameCatalog(dir string, bind ...CatalogBinding) (*NameCatalog, error) {
	if e := makeCatalogDir(filepath.Clean(dir)); e != nil {
		return nil, e
	}
	c := &NameCatalog{dir: dir, entries: []catalogEntry{{}}}
	if len(bind) > 0 {
		c.binding = bind[0]
		if c.binding != nil {
			c.entries = nil
		}
	}
	checkpoint, err := os.Open(filepath.Join(dir, "catalog.checkpoint"))
	hasCheckpoint := err == nil
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if hasCheckpoint {
		err = c.recoverCheckpoint(checkpoint)
		closeErr := checkpoint.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	path := filepath.Join(dir, "catalog.wal")
	c.wal, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*NameCatalog, error) { _ = c.Close(); return nil, e }
	st, err := c.wal.Stat()
	if err != nil {
		return fail(err)
	}
	if st.Size() == 0 {
		if !hasCheckpoint {
			if _, err = rand.Read(c.epoch[:]); err != nil {
				return fail(err)
			}
		}
		if err = writeCatalogBytes(c.wal, catalogHeader(c.epoch)); err != nil {
			return fail(err)
		}
		if err = c.wal.Sync(); err != nil {
			return fail(err)
		}
		if err = syncCatalogDir(dir); err != nil {
			return fail(err)
		}
	} else {
		h := make([]byte, 28)
		if _, err = io.ReadFull(c.wal, h); err != nil {
			return fail(err)
		}
		epoch, e := parseCatalogHeader(h)
		if e != nil {
			return fail(e)
		}
		if hasCheckpoint && epoch != c.epoch {
			return fail(errCatalog)
		}
		c.epoch = epoch
		good := int64(28)
		for {
			n, e := c.readRecord(c.wal)
			if errors.Is(e, io.EOF) {
				break
			}
			if errors.Is(e, io.ErrUnexpectedEOF) {
				if e = c.wal.Truncate(good); e != nil {
					return fail(e)
				}
				if e = c.wal.Sync(); e != nil {
					return fail(e)
				}
				break
			}
			if e != nil {
				return fail(e)
			}
			good += n
		}
	}
	if _, err = c.wal.Seek(0, io.SeekEnd); err != nil {
		return fail(err)
	}
	// Recovery knows the cardinality; release geometric growth slack.
	c.entries = slices.Clone(c.entries)
	c.buffer = bufio.NewWriterSize(c.wal, 1<<20)
	return c, nil
}

// Recovery streams both compressed and decoded bytes. Memory beyond the live
// catalog is bounded by the zstd window and one catalog record, not the total
// checkpoint size. Check the whole-file CRC before publishing any entries.
func (c *NameCatalog) recoverCheckpoint(f *os.File) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() < 40 {
		return errCatalog
	}
	crc := crc32.New(catalogCRC)
	if _, err = io.Copy(crc, io.NewSectionReader(f, 0, st.Size()-4)); err != nil {
		return err
	}
	var tail [4]byte
	if _, err = f.ReadAt(tail[:], st.Size()-4); err != nil {
		return err
	}
	if crc.Sum32() != binary.LittleEndian.Uint32(tail[:]) {
		return errCatalog
	}
	var h [36]byte
	if _, err = f.ReadAt(h[:], 0); err != nil {
		return err
	}
	if c.epoch, err = parseCatalogHeader(h[:28]); err != nil {
		return err
	}
	decoder, err := zstd.NewReader(io.NewSectionReader(f, 36, st.Size()-40), zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return err
	}
	defer decoder.Close()
	for range binary.LittleEndian.Uint64(h[28:]) {
		if _, err = c.readRecord(decoder); err != nil {
			return err
		}
	}
	var extra [1]byte
	if n, err := decoder.Read(extra[:]); n != 0 || !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errCatalog
	}
	return nil
}
func (c *NameCatalog) readRecord(r io.Reader) (int64, error) {
	var h [8]byte
	n, e := io.ReadFull(r, h[:])
	if e != nil {
		return int64(n), e
	}
	if crc32.Checksum(h[:4], catalogCRC) != binary.LittleEndian.Uint32(h[4:]) {
		return 0, errCatalog
	}
	size := binary.LittleEndian.Uint32(h[:])
	grouped := size&catalogGroupFlag != 0
	size &^= catalogGroupFlag
	if size == 0 || size > catalogLimit {
		return 4, errCatalog
	}
	b := make([]byte, int(size)+4)
	n, e = io.ReadFull(r, b)
	if e != nil {
		return int64(n) + 4, e
	}
	crc := crc32.Update(0, catalogCRC, h[:])
	crc = crc32.Update(crc, catalogCRC, b[:size])
	if crc != binary.LittleEndian.Uint32(b[size:]) {
		return 0, errCatalog
	}
	if grouped {
		return int64(size) + 12, c.readGroup(b[:size])
	}
	d := dictionaryReader{b: b[:size]}
	id := d.u()
	key := string(d.bytes())
	def := d.bytes()
	if d.err != nil || len(d.b) != 0 || key == "" || id == 0 || id > c.Count()+1 {
		return 0, errCatalog
	}
	if id <= c.Count() {
		if !c.Matches(id, key, def) {
			return 0, errCatalog
		}
	} else {
		hash := NameHash(key)
		if c.ids.lookup(key, hash, c) != 0 {
			return 0, errCatalog
		}
		c.ids.insert(hash, id)
		if err := c.bindEntry(id, key, def); err != nil {
			return 0, err
		}
	}
	return int64(size) + 12, nil
}
func (c *NameCatalog) Intern(key string, definition []byte) (uint64, error) {
	return c.InternHashed(key, definition, NameHash(key))
}

// InternHashed reuses NameHash(key) from routing without a second name index.
// Callers must supply that exact hash. Hash collisions retain distinct IDs.
func (c *NameCatalog) InternHashed(key string, definition []byte, hash uint64) (uint64, error) {
	if c.failed != nil {
		return 0, c.failed
	}
	if id := c.ids.lookup(key, hash, c); id != 0 {
		if !c.Matches(id, key, definition) {
			return 0, fmt.Errorf("%w: changed definition", errCatalog)
		}
		return id, nil
	}
	if key == "" || len(key)+len(definition) > catalogLimit-32 {
		return 0, errCatalog
	}
	id := c.Count() + 1
	// Bound transient sorting/compression memory independently of cardinality.
	if c.pendingBytes+len(key)+len(definition)+64 > catalogGroupLimit {
		if e := c.flushGroup(); e != nil {
			c.failed = e
			return 0, e
		}
	}
	if len(key)+len(definition)+64 > catalogGroupLimit {
		// A single unusually large definition retains the original record format.
		if _, e := c.buffer.Write(catalogRecord(id, key, definition)); e != nil {
			c.failed = e
			return 0, e
		}
	} else {
		c.pending = append(c.pending, id)
		c.pendingBytes += len(key) + len(definition) + 64
	}
	c.dirty = true
	c.ids.insert(hash, id)
	if err := c.bindEntry(id, key, definition); err != nil {
		c.failed = err
		return 0, err
	}
	return id, nil
}

// Find returns the ID of an existing exact name, or zero when it is absent.
// It uses the catalog's existing index and never admits or changes a definition.
func (c *NameCatalog) Find(key string) uint64 {
	return c.ids.lookup(key, NameHash(key), c)
}

// Lookup returns catalog-owned immutable metadata; callers must not modify it.
func (c *NameCatalog) Lookup(id uint64) (string, []byte, bool) {
	if id == 0 || id > c.Count() {
		return "", nil, false
	}
	return c.key(id), c.definition(id), true
}

// Key returns the catalog-owned name for id without rebuilding its definition.
// With a bind hook it is the shared identity name, so callers can retain or key
// maps by it instead of keeping another copy of the name.
func (c *NameCatalog) Key(id uint64) (string, bool) {
	if id == 0 || id > c.Count() {
		return "", false
	}
	return c.key(id), true
}

// Matches reports whether id is bound to exactly name and definition, without
// reconstructing the definition bytes.
func (c *NameCatalog) Matches(id uint64, name string, definition []byte) bool {
	if id == 0 || id > c.Count() {
		return false
	}
	if c.binding != nil {
		return c.binding.Matches(id, name, definition)
	}
	e := c.entries[id]
	return e.key == name && e.definitionEqual(definition)
}
func (c *NameCatalog) Epoch() [16]byte { return c.epoch }
func (c *NameCatalog) Count() uint64 {
	if c.binding != nil {
		return c.boundCount
	}
	return uint64(len(c.entries) - 1)
}

func (c *NameCatalog) key(id uint64) string {
	if c.binding != nil {
		return c.binding.Key(id)
	}
	return c.entries[id].key
}

func (c *NameCatalog) definition(id uint64) []byte {
	if c.binding != nil {
		return c.binding.Definition(id)
	}
	return c.entries[id].definitionBytes()
}
func (c *NameCatalog) Sync() error {
	if c.failed != nil {
		return c.failed
	}
	if !c.dirty {
		return nil
	}
	c.failed = c.flushGroup()
	if c.failed != nil {
		return c.failed
	}
	c.failed = c.buffer.Flush()
	if c.failed != nil {
		return c.failed
	}
	c.failed = c.wal.Sync()
	if c.failed == nil {
		c.dirty = false
	}
	return c.failed
}

// Checkpoint is synchronous and requires the stream owner to be quiescent.
// Failure poisons the catalog; close and recover it before any further append.
func (c *NameCatalog) Checkpoint() error {
	if c.failed != nil {
		return c.failed
	}
	c.failed = c.checkpoint()
	if c.failed == nil {
		c.dirty = false
	}
	return c.failed
}
func (c *NameCatalog) checkpoint() error {
	hook := func(stage string) {
		if c.checkpointHook != nil {
			c.checkpointHook(stage)
		}
	}
	path := filepath.Join(c.dir, "catalog.checkpoint.tmp")
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if e != nil {
		return e
	}
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(path)
		}
	}()
	b := catalogHeader(c.epoch)
	b = binary.LittleEndian.AppendUint64(b, c.Count())
	crc := crc32.New(catalogCRC)
	write := func(b []byte) error { _, _ = crc.Write(b); return writeCatalogBytes(f, b) }
	if e = write(b); e != nil {
		return e
	}
	compressed, e := zstd.NewWriter(io.MultiWriter(f, crc), zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(true))
	if e != nil {
		return e
	}
	for id := uint64(1); id <= c.Count(); id++ {
		if _, e = compressed.Write(catalogRecord(id, c.key(id), c.definition(id))); e != nil {
			_ = compressed.Close()
			return e
		}
	}
	if e = compressed.Close(); e != nil {
		return e
	}
	if e = writeCatalogBytes(f, binary.LittleEndian.AppendUint32(nil, crc.Sum32())); e != nil {
		return e
	}
	hook("written")
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	hook("synced")
	if e = os.Rename(path, filepath.Join(c.dir, "catalog.checkpoint")); e != nil {
		return e
	}
	hook("renamed")
	if e = syncCatalogDir(c.dir); e != nil {
		return e
	}
	ok = true
	hook("published")
	// Keep the epoch header. Durable checkpoint precedes WAL reclamation.
	c.buffer.Reset(c.wal)
	c.pending = c.pending[:0]
	c.pendingBytes = 0
	if e = c.wal.Truncate(28); e != nil {
		return e
	}
	hook("truncated")
	if _, e = c.wal.Seek(28, io.SeekStart); e != nil {
		return e
	}
	return c.wal.Sync()
}
func (c *NameCatalog) Close() error {
	if c.groupEncoder != nil {
		_ = c.groupEncoder.Close()
		c.groupEncoder = nil
	}
	if c.groupDecoder != nil {
		c.groupDecoder.Close()
		c.groupDecoder = nil
	}
	if c.wal == nil {
		return nil
	}
	e := c.wal.Close()
	c.wal = nil
	return e
}
