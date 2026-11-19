package v2core

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// An owner's rollup checkpoint is derived state: one sequential file holding,
// per installed name, its identity, committed scalars and KeepRecent
// residency, written at clean close. Open loads it instead of scanRollups only
// while rollupCheckpointKey still holds the file's token and coreMetaKey still
// holds the bytes the file was stamped with. Open deletes the token (synced)
// before the owner can change anything, so after any later seal, a kill, a
// partial write or a corrupt file, the next open rebuilds from parts.
const (
	rollupCheckpointFile  = "rollup-state.ckpt"
	rollupCheckpointMagic = "GCRSCKP1"
)

var rollupCheckpointKey = []byte{0xff, 'k'}

var rollupCheckpointCRC = crc32.MakeTable(crc32.Castagnoli)

// writeCheckpoint runs after the owner stopped and before its DB closes. The
// file is durable before the token is, so a kill at any point leaves either no
// token or a complete file.
func (r *coreRollups) writeCheckpoint(dir string) error {
	stamp, closer, err := r.db.Get(coreMetaKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	buf, token, err := r.encodeCheckpoint(stamp)
	if err = errors.Join(err, closer.Close()); err != nil {
		return err
	}
	path := filepath.Join(dir, rollupCheckpointFile)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(buf)
	err = errors.Join(err, f.Sync(), f.Close())
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err != nil {
		return errors.Join(err, os.Remove(tmp))
	}
	return r.db.Set(rollupCheckpointKey, token[:], pebble.Sync)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

// Layout: magic, token[16], stamp, suffix table, names in owner-slot order,
// CRC32-C of everything before it. Each name: id, suffix index, the name part
// of its canonical definition, scalars, resident samples.
func (r *coreRollups) encodeCheckpoint(stamp []byte) ([]byte, [16]byte, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, token, err
	}
	suffixes := map[string]uint64{}
	var table [][]byte
	var body []byte
	var resident []sealrollup.Delta
	names := uint64(0)
	for _, id := range r.ids {
		m := r.metrics[id]
		if m == nil {
			continue
		}
		suffix := m.policy.suffix
		index, ok := suffixes[string(suffix)]
		if !ok {
			index = uint64(len(table))
			suffixes[string(suffix)] = index
			table = append(table, suffix)
		}
		def := m.definitionBytes()
		meta := r.meta[r.slotOf(id, m)]
		body = binary.AppendUvarint(body, id)
		body = binary.AppendUvarint(body, index)
		name := def[len(`{"Name":"`) : len(def)-len(suffix)]
		body = binary.AppendUvarint(body, uint64(len(name)))
		body = append(body, name...)
		body = binary.AppendVarint(body, meta.ClosedThrough)
		body = binary.AppendUvarint(body, meta.BaselineOrder)
		body = binary.AppendUvarint(body, meta.ResetOrder)
		body = binary.AppendVarint(body, meta.LastUpdate)
		body = binary.AppendUvarint(body, meta.Revision)
		body = binary.AppendUvarint(body, meta.Frontier.Order)
		body = binary.AppendVarint(body, meta.Frontier.MaxTime)
		expired := byte(0)
		if meta.Expired {
			expired = 1
		}
		body = append(body, meta.Version, expired)
		resident = m.slots.AppendResident(resident[:0])
		body = binary.AppendUvarint(body, uint64(len(resident)))
		for _, d := range resident {
			body = binary.AppendUvarint(body, uint64(d.Archive))
			body = binary.AppendVarint(body, d.Slot)
			body = binary.AppendVarint(body, d.Sample.Time)
			body = binary.LittleEndian.AppendUint64(body, d.Sample.Bits)
			body = binary.AppendUvarint(body, d.Sample.Sequence)
		}
		names++
	}
	out := make([]byte, 0, len(body)+len(stamp)+64)
	out = append(out, rollupCheckpointMagic...)
	out = append(out, token[:]...)
	out = binary.AppendUvarint(out, uint64(len(stamp)))
	out = append(out, stamp...)
	out = binary.AppendUvarint(out, uint64(len(table)))
	for _, s := range table {
		out = binary.AppendUvarint(out, uint64(len(s)))
		out = append(out, s...)
	}
	out = binary.AppendUvarint(out, names)
	out = append(out, body...)
	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, rollupCheckpointCRC)), token, nil
}

// takeCheckpoint consumes the owner's checkpoint: it returns the file bytes
// when the token in Pebble matches, and always deletes token and file first,
// so a checkpoint is loaded at most once and never after a later mutation.
// An empty dir (validation-only recoveries) leaves the checkpoint alone.
func takeCheckpoint(db *pebble.DB, dir string) ([]byte, error) {
	if dir == "" {
		return nil, nil
	}
	token, closer, err := db.Get(rollupCheckpointKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	want := bytes.Clone(token)
	if err = closer.Close(); err != nil {
		return nil, err
	}
	if err = db.Delete(rollupCheckpointKey, pebble.Sync); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, rollupCheckpointFile)
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, nil
	}
	if len(buf) < len(rollupCheckpointMagic)+16 || !bytes.Equal(buf[len(rollupCheckpointMagic):len(rollupCheckpointMagic)+16], want) {
		return nil, nil
	}
	return buf, nil
}

var errCheckpoint = errors.New("invalid rollup checkpoint")

// loadCheckpoint rebuilds the owner's rollups from a taken checkpoint, exactly
// as recoverCoreRollups would install them from a scan. Any mismatch returns
// errCheckpoint; the caller then scans parts.
func loadCheckpoint(db *pebble.DB, buf, stamp []byte, identities *PolicyCatalog) (*coreRollups, error) {
	n := len(buf) - 4
	if n < len(rollupCheckpointMagic)+16 || string(buf[:len(rollupCheckpointMagic)]) != rollupCheckpointMagic || binary.LittleEndian.Uint32(buf[n:]) != crc32.Checksum(buf[:n], rollupCheckpointCRC) {
		return nil, errCheckpoint
	}
	d := checkpointDecoder{buf: buf[len(rollupCheckpointMagic)+16 : n]}
	if !bytes.Equal(d.bytes(), stamp) {
		return nil, errCheckpoint
	}
	table := make([][]byte, d.uvarint())
	for i := range table {
		table[i] = d.bytes()
	}
	r := newCoreRollups()
	r.db = db
	if identities != nil {
		r.identities = identities
	}
	count := d.uvarint()
	for i := uint64(0); i < count && d.err == nil; i++ {
		id := d.uvarint()
		index := d.uvarint()
		name := d.bytes()
		var meta metricMetadata
		meta.ClosedThrough = d.varint()
		meta.BaselineOrder = d.uvarint()
		meta.ResetOrder = d.uvarint()
		meta.LastUpdate = d.varint()
		meta.Revision = d.uvarint()
		meta.Frontier.Order = d.uvarint()
		meta.Frontier.MaxTime = d.varint()
		meta.Version = d.byte()
		meta.Expired = d.byte() == 1
		if d.err != nil || index >= uint64(len(table)) {
			return nil, errCheckpoint
		}
		def := make([]byte, 0, len(`{"Name":"`)+len(name)+len(table[index]))
		def = append(append(append(def, `{"Name":"`...), name...), table[index]...)
		m, err := r.metric(id, def)
		if err != nil || r.metrics[id] != nil {
			return nil, errCheckpoint
		}
		m.keepRecent(meta.Frontier.MaxTime)
		archives := len(m.schema().Archives)
		for range d.uvarint() {
			var x sealrollup.Delta
			x.Archive = int(d.uvarint())
			x.Slot = d.varint()
			x.Sample.Time = d.varint()
			x.Sample.Bits = d.uint64()
			x.Sample.Sequence = d.uvarint()
			if d.err != nil || x.Archive >= archives {
				return nil, errCheckpoint
			}
			m.installRecent(x)
		}
		m.keepRecent(meta.Frontier.MaxTime)
		r.install(id, m, meta)
	}
	if d.err != nil || len(d.buf) != 0 {
		return nil, errCheckpoint
	}
	r.fromCheckpoint = true
	return r, nil
}

type checkpointDecoder struct {
	buf []byte
	err error
}

func (d *checkpointDecoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.buf)
	if n <= 0 {
		d.err, d.buf = errCheckpoint, nil
		return 0
	}
	d.buf = d.buf[n:]
	return v
}
func (d *checkpointDecoder) varint() int64 {
	v, n := binary.Varint(d.buf)
	if n <= 0 {
		d.err, d.buf = errCheckpoint, nil
		return 0
	}
	d.buf = d.buf[n:]
	return v
}
func (d *checkpointDecoder) byte() byte {
	if len(d.buf) < 1 {
		d.err, d.buf = errCheckpoint, nil
		return 0
	}
	v := d.buf[0]
	d.buf = d.buf[1:]
	return v
}
func (d *checkpointDecoder) uint64() uint64 {
	if len(d.buf) < 8 {
		d.err, d.buf = errCheckpoint, nil
		return 0
	}
	v := binary.LittleEndian.Uint64(d.buf)
	d.buf = d.buf[8:]
	return v
}
func (d *checkpointDecoder) bytes() []byte {
	n := d.uvarint()
	if d.err != nil || n > uint64(len(d.buf)) {
		d.err, d.buf = errCheckpoint, nil
		return nil
	}
	v := d.buf[:n:n]
	d.buf = d.buf[n:]
	return v
}
