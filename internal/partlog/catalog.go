package partlog

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// NameCatalog is single-owner, including its codecs. Each journal stream owns a
// separate catalog. IDs are local to the random persisted epoch, never routing
// or replica metric IDs. Sync the catalog BEFORE syncing referencing segments.
// Checkpoint durably replaces the catalog prefix before reclaiming its WAL.
// Predictor epochs must start at every independently retained segment boundary.
type NameCatalog struct {
	buffer         *bufio.Writer
	dirty          bool
	checkpointHook func(string) // test-only fault injection; nil in normal operation
	dir            string
	epoch          [16]byte
	entries        []predictorV3Entry
	ids            map[string]uint64
	wal            *os.File
	failed         error
}

var errCatalog = errors.New("invalid name catalog")

const catalogLimit = 64 << 20

func catalogHeader(epoch [16]byte) []byte {
	b := append([]byte("GCNCAT02"), epoch[:]...)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, predictorV3CRC))
}
func parseCatalogHeader(b []byte) ([16]byte, error) {
	var e [16]byte
	if len(b) != 28 || string(b[:8]) != "GCNCAT02" || crc32.Checksum(b[:24], predictorV3CRC) != binary.LittleEndian.Uint32(b[24:]) {
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
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, predictorV3CRC))
	out = append(out, b...)
	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, predictorV3CRC))
}
func syncCatalogDir(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
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
func OpenNameCatalog(dir string) (*NameCatalog, error) {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	c := &NameCatalog{dir: dir, ids: make(map[string]uint64), entries: []predictorV3Entry{{}}}
	checkpoint, err := os.ReadFile(filepath.Join(dir, "catalog.checkpoint"))
	hasCheckpoint := err == nil
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if hasCheckpoint {
		if len(checkpoint) < 40 {
			return nil, errCatalog
		}
		c.epoch, err = parseCatalogHeader(checkpoint[:28])
		if err != nil {
			return nil, err
		}
		// A committed checkpoint has an explicit entry count and a whole-file CRC.
		if crc32.Checksum(checkpoint[:len(checkpoint)-4], predictorV3CRC) != binary.LittleEndian.Uint32(checkpoint[len(checkpoint)-4:]) {
			return nil, errCatalog
		}
		count := binary.LittleEndian.Uint64(checkpoint[28:36])
		decoder, e := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(1<<30))
		if e != nil {
			return nil, e
		}
		raw, e := decoder.DecodeAll(checkpoint[36:len(checkpoint)-4], nil)
		decoder.Close()
		if e != nil {
			return nil, e
		}
		r := bytes.NewReader(raw)
		for range count {
			if _, err = c.readRecord(r); err != nil {
				return nil, err
			}
		}
		if r.Len() != 0 {
			return nil, errCatalog
		}
	}
	path := filepath.Join(dir, "catalog.wal")
	c.wal, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*NameCatalog, error) { _ = c.wal.Close(); return nil, e }
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
			if e == io.EOF {
				break
			}
			if e == io.ErrUnexpectedEOF {
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
	c.buffer = bufio.NewWriterSize(c.wal, 1<<20)
	return c, nil
}
func (c *NameCatalog) readRecord(r io.Reader) (int64, error) {
	var h [8]byte
	n, e := io.ReadFull(r, h[:])
	if e != nil {
		return int64(n), e
	}
	if crc32.Checksum(h[:4], predictorV3CRC) != binary.LittleEndian.Uint32(h[4:]) {
		return 0, errCatalog
	}
	size := binary.LittleEndian.Uint32(h[:])
	if size == 0 || size > catalogLimit {
		return 4, errCatalog
	}
	b := make([]byte, int(size)+4)
	n, e = io.ReadFull(r, b)
	if e != nil {
		return int64(n) + 4, e
	}
	crc := crc32.Update(0, predictorV3CRC, h[:])
	crc = crc32.Update(crc, predictorV3CRC, b[:size])
	if crc != binary.LittleEndian.Uint32(b[size:]) {
		return 0, errCatalog
	}
	d := dictionaryReader{b: b[:size]}
	id := d.u()
	key := string(d.bytes())
	def := d.bytes()
	if d.err != nil || len(d.b) != 0 || key == "" || id == 0 || id > uint64(len(c.entries)) {
		return 0, errCatalog
	}
	if id < uint64(len(c.entries)) {
		old := c.entries[id]
		if old.key != key || !bytes.Equal(old.definition, def) {
			return 0, errCatalog
		}
	} else {
		if _, ok := c.ids[key]; ok {
			return 0, errCatalog
		}
		c.ids[key] = id
		c.entries = append(c.entries, predictorV3Entry{key: key, definition: bytes.Clone(def)})
	}
	return int64(size) + 12, nil
}
func (c *NameCatalog) Intern(key string, definition []byte) (uint64, error) {
	if c.failed != nil {
		return 0, c.failed
	}
	if id, ok := c.ids[key]; ok {
		if !bytes.Equal(c.entries[id].definition, definition) {
			return 0, fmt.Errorf("%w: changed definition", errCatalog)
		}
		return id, nil
	}
	if key == "" || len(key)+len(definition) > catalogLimit-32 {
		return 0, errCatalog
	}
	id := uint64(len(c.entries))
	b := catalogRecord(id, key, definition)
	if _, e := c.buffer.Write(b); e != nil {
		c.failed = e
		return 0, e
	}
	c.dirty = true
	c.ids[key] = id
	c.entries = append(c.entries, predictorV3Entry{key: key, definition: bytes.Clone(definition)})
	return id, nil
}

// Lookup returns catalog-owned immutable metadata; callers must not modify it.
func (c *NameCatalog) Lookup(id uint64) (string, []byte, bool) {
	if id == 0 || id >= uint64(len(c.entries)) {
		return "", nil, false
	}
	e := c.entries[id]
	return e.key, e.definition, true
}
func (c *NameCatalog) Epoch() [16]byte { return c.epoch }
func (c *NameCatalog) Count() uint64   { return uint64(len(c.entries) - 1) }
func (c *NameCatalog) Sync() error {
	if c.failed != nil {
		return c.failed
	}
	if !c.dirty {
		return nil
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
	crc := crc32.New(predictorV3CRC)
	write := func(b []byte) error { _, _ = crc.Write(b); return writeCatalogBytes(f, b) }
	if e = write(b); e != nil {
		return e
	}
	compressed, e := zstd.NewWriter(io.MultiWriter(f, crc), zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(true))
	if e != nil {
		return e
	}
	for id := uint64(1); id <= c.Count(); id++ {
		r := c.entries[id]
		if _, e = compressed.Write(catalogRecord(id, r.key, r.definition)); e != nil {
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
	if c.wal == nil {
		return nil
	}
	e := c.wal.Close()
	c.wal = nil
	return e
}
