package v2core

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/klauspost/compress/zstd"
)

// An owner's rollup checkpoint is derived state: one sequential file holding,
// per installed name, its identity, committed scalars and KeepRecent
// residency, stamped with the live part bindings the state reflects. Owners
// write it after seals and at close. Open loads it, folds in only the parts
// published since (absent from the stamp) with the same rules scanRollups
// uses, and rebuilds from parts when the file is missing, corrupt or torn.
// The file is one zstd frame (fastest level), 2.6-9x smaller than raw.
const (
	rollupCheckpointFile  = "rollup-state.ckpt"
	rollupCheckpointMagic = "GCRSCKP3"
)

var (
	rollupCheckpointCRC       = crc32.MakeTable(crc32.Castagnoli)
	rollupCheckpointZstd, _   = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(16))
	rollupCheckpointUnzstd, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(16), zstd.WithDecoderMaxMemory(1<<32))
)

// rollupCheckpointEvery spaces at-seal checkpoints; close always writes one.
// A kill restart folds at most this much of newer parts; the rewrite costs
// file size / interval of disk writes per owner.
var rollupCheckpointEvery = 2 * time.Minute

// livePartBindings lists the sealed part bindings visible now. Seals publish
// bindings with their parts; a merge replaces inputs by an output holding the
// same rows, so the in-memory state reflects every listed part.
func livePartBindings(db *pebble.DB) ([][]byte, error) {
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for valid := it.First(); valid; valid = it.Next() {
		out = append(out, bytes.Clone(it.Key()))
	}
	return out, errors.Join(it.Error(), it.Close())
}

// encodeOwnerCheckpoint runs on the owner goroutine with no seal in flight.
func (r *coreRollups) encodeOwnerCheckpoint() ([]byte, error) {
	parts, err := livePartBindings(r.db)
	if err != nil {
		return nil, err
	}
	return r.encodeCheckpoint(parts), nil
}

// checkpointAfterSeal runs on the owner goroutine right after a seal commits,
// with no seal in flight. It encodes the per-name state (about 0.3 µs/name)
// and hands the file write to one goroutine; a write still running skips this
// round.
func (o *coreOwner) checkpointAfterSeal() {
	if o.rollups == nil || o.rollups.db == nil || time.Since(o.ckptAt) < rollupCheckpointEvery {
		return
	}
	if o.ckptWriting != nil {
		select {
		case <-o.ckptWriting:
			o.ckptWriting = nil
		default:
			return
		}
	}
	o.ckptAt = time.Now()
	buf, err := o.rollups.encodeOwnerCheckpoint()
	if err != nil {
		o.checkpointFailed(err)
		return
	}
	done := make(chan struct{})
	o.ckptWriting = done
	go func() {
		_ = o.saveCheckpoint(buf)
		close(done)
	}()
}

// saveCheckpoint writes buf as the owner's checkpoint. A failure is loud
// (ERROR log, v2_rollup_checkpoint_failures_total) and keeps the previous
// file, so the next open folds more parts, or rebuilds when there is none.
func (o *coreOwner) saveCheckpoint(raw []byte) error {
	buf := rollupCheckpointZstd.EncodeAll(raw, make([]byte, 0, len(raw)/4))
	if err := writeCheckpointFile(o.dir, buf); err != nil {
		o.checkpointFailed(err)
		return err
	}
	o.ckptBytes.Add(uint64(len(buf)))
	return nil
}

func (o *coreOwner) checkpointFailed(err error) {
	o.ckptFailures.Add(1)
	slog.Error("v2 rollup checkpoint failed; next open folds more parts", "dir", o.dir, "error", err)
}

// writeCheckpointFile makes buf the owner's checkpoint: tmp, fsync, rename,
// dir fsync. A kill at any point leaves the previous file or the new one.
func writeCheckpointFile(dir string, buf []byte) error {
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
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

// Layout: magic, stamp (part binding keys), suffix table, names in owner-slot
// order, CRC32-C of everything before it. Each name: id, suffix index, the
// name part of its canonical definition, scalars, resident samples.
func (r *coreRollups) encodeCheckpoint(parts [][]byte) []byte {
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
	size := len(body) + 64
	for _, p := range parts {
		size += len(p) + 2
	}
	out := make([]byte, 0, size)
	out = append(out, rollupCheckpointMagic...)
	out = binary.AppendUvarint(out, uint64(len(parts)))
	for _, p := range parts {
		out = binary.AppendUvarint(out, uint64(len(p)))
		out = append(out, p...)
	}
	out = binary.AppendUvarint(out, uint64(len(table)))
	for _, s := range table {
		out = binary.AppendUvarint(out, uint64(len(s)))
		out = append(out, s...)
	}
	out = binary.AppendUvarint(out, names)
	out = append(out, body...)
	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, rollupCheckpointCRC))
}

var errCheckpoint = errors.New("invalid rollup checkpoint")

// loadCheckpoint rebuilds the owner's rollups from its checkpoint plus the
// parts published after it, exactly as recoverCoreRollups would install them
// from a scan. Any mismatch returns errCheckpoint; the caller then scans.
func loadCheckpoint(db *pebble.DB, snapshot *pebble.Snapshot, file []byte, identities *PolicyCatalog) (*coreRollups, error) {
	buf, err := rollupCheckpointUnzstd.DecodeAll(file, nil)
	if err != nil {
		return nil, errCheckpoint
	}
	n := len(buf) - 4
	if n < len(rollupCheckpointMagic) || string(buf[:len(rollupCheckpointMagic)]) != rollupCheckpointMagic || binary.LittleEndian.Uint32(buf[n:]) != crc32.Checksum(buf[:n], rollupCheckpointCRC) {
		return nil, errCheckpoint
	}
	d := checkpointDecoder{buf: buf[len(rollupCheckpointMagic):n]}
	stamp := make(map[string]bool, 64)
	for range d.uvarint() {
		stamp[string(d.bytes())] = true
	}
	table := make([][]byte, d.uvarint())
	for i := range table {
		table[i] = d.bytes()
	}
	if d.err != nil {
		return nil, errCheckpoint
	}
	// Fold the parts published after the stamp. Re-folding rows the state
	// already reflects (a merge output) is idempotent under after/newerOrigin.
	series := map[v2part.ID]*rollupScanEntry{}
	selected := map[archivePartIdentity]bool{}
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		return nil, err
	}
	for valid := it.First(); valid; valid = it.Next() {
		if stamp[string(it.Key())] {
			continue
		}
		desc, e := v2part.ParseDescriptor(it.Value())
		if e != nil {
			return nil, errors.Join(e, it.Close())
		}
		selected[archivePartIdentity{desc.Scope.Group, desc.Scope.Part}] = true
	}
	if err = errors.Join(it.Error(), it.Close()); err != nil {
		return nil, err
	}
	if len(selected) > 0 {
		iter, err := newArchiveIterSelected(context.Background(), snapshot, rollupScanBounds(), selected)
		if err != nil {
			return nil, err
		}
		err = foldRollupKeys(iter, identities, series)
		if err = errors.Join(err, iter.Close()); err != nil {
			return nil, errCheckpoint
		}
	}
	r := newCoreRollups()
	r.db = db
	if identities != nil {
		r.identities = identities
	}
	var resident []sealrollup.Delta
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
		resident = resident[:0]
		for range d.uvarint() {
			var x sealrollup.Delta
			x.Archive = int(d.uvarint())
			x.Slot = d.varint()
			x.Sample.Time = d.varint()
			x.Sample.Bits = d.uint64()
			x.Sample.Sequence = d.uvarint()
			resident = append(resident, x)
		}
		if d.err != nil || index >= uint64(len(table)) {
			return nil, errCheckpoint
		}
		def := make([]byte, 0, len(`{"Name":"`)+len(name)+len(table[index]))
		def = append(append(append(def, `{"Name":"`...), name...), table[index]...)
		if e := series[v2part.ID(name)]; e != nil {
			// Touched since the stamp: merge the reflected state into the fold.
			delete(series, v2part.ID(name))
			if meta.after(e.meta) {
				h, err := scanIdentity(r.identities, id, string(name), def)
				if err != nil {
					return nil, errCheckpoint
				}
				e.identity, e.meta = h, meta
			}
			for _, x := range resident {
				// A record's Sequence is its Order.
				if err := e.addRecord(sealrollup.Record{Delta: sealrollup.Delta{Archive: x.Archive, Slot: x.Slot, Order: x.Sample.Sequence, Sample: x.Sample}}); err != nil {
					return nil, errCheckpoint
				}
			}
			policy, m, records, err := e.finish(v2part.ID(name))
			if err != nil || m.ID != id {
				return nil, errCheckpoint
			}
			if err = r.installRecovered(policy, m, records); err != nil {
				return nil, errCheckpoint
			}
			continue
		}
		m, err := r.metric(id, def)
		if err != nil || r.metrics[id] != nil {
			return nil, errCheckpoint
		}
		archives := len(m.schema().Archives)
		m.keepRecent(meta.Frontier.MaxTime)
		for _, x := range resident {
			if x.Archive >= archives {
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
	// Names first published after the stamp.
	for name, e := range series {
		policy, m, records, err := e.finish(name)
		if err != nil {
			return nil, errCheckpoint
		}
		if err = r.installRecovered(policy, m, records); err != nil {
			return nil, errCheckpoint
		}
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
