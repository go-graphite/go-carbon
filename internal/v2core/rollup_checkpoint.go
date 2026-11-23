package v2core

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/klauspost/compress/zstd"
)

// An owner's rollup checkpoint is derived state: one sequential file holding,
// per installed name, its identity and committed scalars, stamped with the live part bindings the state reflects. Owners
// write it after seals and at close. Open loads it, folds in only the parts
// published since (absent from the stamp) with the same rules scanRollups
// uses, and rebuilds from parts when the file is missing, corrupt or torn.
// Record blocks stay compressed in a read-only mapping. Open decodes only
// the pointer-free lookup indexes and validates the checksum and part bindings;
// unchanged names are not materialized.
const (
	rollupCheckpointFile  = "rollup-state.ckpt"
	rollupCheckpointMagic = "GCRSCKP5"
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
	if err := r.loadAllCheckpoint(); err != nil {
		return nil, err
	}
	parts, err := livePartBindings(r.db)
	if err != nil {
		return nil, err
	}
	return r.encodeCheckpoint(parts), nil
}

// checkpointAfterSeal runs on the owner goroutine right after a seal commits,
// with no seal in flight. It encodes the records and their mapped indexes
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
func (o *coreOwner) saveCheckpoint(buf []byte) error {
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

// Layout: magic, part stamp, suffix table, count, compressed record blocks,
// compressed indexes, index offset/count, CRC32-C. Decoded indexes hold ID,
// block offset and packed record range (24 B), name-order ordinals (8 B),
// and closure deadlines/IDs (16 B). Each block contains up to 128 records.
// Each name: id, suffix index, the name part of its canonical definition,
// scalars, and a reserved zero residency count.
func (r *coreRollups) encodeCheckpoint(parts [][]byte) []byte {
	suffixes := map[string]uint64{}
	var table [][]byte
	var body []byte
	names := uint64(0)
	entries := make([]checkpointIndexEntry, 0, len(r.ids))
	ids := slices.Clone(r.ids)
	slices.Sort(ids)
	for _, id := range ids {
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
		start := len(body)
		body = binary.AppendUvarint(body, id)
		body = binary.AppendUvarint(body, index)
		name := def[len(`{"Name":"`) : len(def)-len(suffix)]
		body = binary.AppendUvarint(body, uint64(len(name)))
		nameOffset := uint64(len(body))
		body = append(body, name...)
		body = binary.AppendVarint(body, 0)
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
		body = binary.AppendUvarint(body, 0) // retired residency count
		entries = append(entries, checkpointIndexEntry{id: id, offset: uint64(start), size: uint64(len(body) - start), nameOffset: nameOffset, nameSize: uint64(len(name))})
		names++
	}
	// Compress independently addressable groups. A single owner-local scratch
	// block is enough to decode a first touch; there is no decoded state cache.
	var compressed []byte
	for first := 0; first < len(entries); first += 128 {
		last := min(first+128, len(entries))
		start := entries[first].offset
		end := entries[last-1].offset + entries[last-1].size
		offset := uint64(len(compressed))
		compressed = binary.LittleEndian.AppendUint64(compressed, 0)
		compressed = rollupCheckpointZstd.EncodeAll(body[start:end], compressed)
		binary.LittleEndian.PutUint64(compressed[offset:], uint64(len(compressed))-offset-8)
		for i := first; i < last; i++ {
			entries[i].size |= (entries[i].offset - start) << 32
			entries[i].offset = offset
		}
	}
	size := len(compressed) + len(entries)*12 + 80
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
	bodyOffset := uint64(len(out))
	out = append(out, compressed...)
	indexes := make([]byte, 0, len(entries)*48)
	indexOffset := uint64(len(out))

	// The sorted IDs are no longer needed. Reuse their storage to sort
	// ordinals instead of moving the much larger index records.
	order := ids[:len(entries)]
	for i := range entries {
		e := &entries[i]
		order[i] = uint64(i)
		indexes = binary.LittleEndian.AppendUint64(indexes, e.id)
		indexes = binary.LittleEndian.AppendUint64(indexes, e.offset+bodyOffset)
		indexes = binary.LittleEndian.AppendUint64(indexes, e.size)
	}
	slices.SortFunc(order, func(i, j uint64) int {
		a, b := &entries[i], &entries[j]
		return bytes.Compare(body[a.nameOffset:a.nameOffset+a.nameSize], body[b.nameOffset:b.nameOffset+b.nameSize])
	})
	for _, ordinal := range order {
		indexes = binary.LittleEndian.AppendUint64(indexes, ordinal)
	}
	for _, ordinal := range order {
		e := &entries[ordinal]
		indexes = binary.LittleEndian.AppendUint64(indexes, uint64(math.MaxInt64)) // retired closure deadline
		indexes = binary.LittleEndian.AppendUint64(indexes, e.id)
	}
	out = rollupCheckpointZstd.EncodeAll(indexes, out)
	out = binary.LittleEndian.AppendUint64(out, indexOffset)
	out = binary.LittleEndian.AppendUint64(out, names)
	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, rollupCheckpointCRC))
}

var errCheckpoint = errors.New("invalid rollup checkpoint")

// loadCheckpoint validates a mapped checkpoint and folds the newer parts.
// Sparse deltas decode only touched IDs; dense deltas restore sequentially.
// Untouched records stay mapped. Invalid derived state falls back to parts.
func loadCheckpoint(db *pebble.DB, snapshot *pebble.Snapshot, file []byte, identities *PolicyCatalog) (*coreRollups, error) {
	buf := file
	n := len(buf) - 4
	if n < len(rollupCheckpointMagic) || string(buf[:len(rollupCheckpointMagic)]) != rollupCheckpointMagic || binary.LittleEndian.Uint32(buf[n:]) != crc32.Checksum(buf[:n], rollupCheckpointCRC) {
		return nil, errCheckpoint
	}
	d := checkpointDecoder{buf: buf[len(rollupCheckpointMagic):n]}
	stamp := make(map[string]bool, 64)
	for range d.uvarint() {
		stamp[string(d.bytes())] = true
	}
	tableCount := d.uvarint()
	if d.err != nil || tableCount > uint64(len(d.buf)) {
		return nil, errCheckpoint
	}
	table := make([][]byte, tableCount)
	for i := range table {
		table[i] = d.bytes()
	}
	if d.err != nil {
		return nil, errCheckpoint
	}
	if n < 16 {
		return nil, errCheckpoint
	}
	indexOffset := binary.LittleEndian.Uint64(buf[n-16 : n-8])
	countIndex := binary.LittleEndian.Uint64(buf[n-8 : n])
	if indexOffset > uint64(n-16) || countIndex > uint64(math.MaxInt)/48 {
		return nil, errCheckpoint
	}
	indexes, err := rollupCheckpointUnzstd.DecodeAll(buf[indexOffset:n-16], nil)
	if err != nil || uint64(len(indexes)) != countIndex*48 {
		return nil, errCheckpoint
	}
	// Fold the parts published after the stamp. Re-folding rows the state
	// already reflects (a merge output) is idempotent under after/newerOrigin.
	series := map[v2part.ID]*rollupScanEntry{}
	selected := map[archivePartIdentity]bool{}
	reflected := map[archivePartIdentity]bool{}
	it, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		return nil, err
	}
	for valid := it.First(); valid; valid = it.Next() {
		desc, e := v2part.ParseDescriptor(it.Value())
		if e != nil {
			return nil, errors.Join(e, it.Close())
		}
		if stamp[string(it.Key())] {
			reflected[archivePartIdentity{desc.Scope.Group, desc.Scope.Part}] = true
		} else {
			selected[archivePartIdentity{desc.Scope.Group, desc.Scope.Part}] = true
		}
	}
	if err = errors.Join(it.Error(), it.Close()); err != nil {
		return nil, err
	}
	// Open the reflected parts too, without reading their rows: a missing,
	// resized or unreadable original (footer, index, properties) fails open
	// exactly as a rebuild from parts would.
	if len(reflected) > 0 {
		iter, err := newArchiveIterSelected(context.Background(), snapshot, rollupScanBounds(), reflected)
		if err != nil {
			return nil, err
		}
		if err = iter.Close(); err != nil {
			return nil, err
		}
	}
	// New parts may introduce IDs, reset barriers and activity changes.
	// Their part inventory does not expose the latest per-name metadata. Fold
	// those rows before publishing the owner so idle maintenance and duplicate
	// checks cannot consult a stale checkpoint. Deferring this needs a separate
	// per-name reconciliation/scheduling mechanism, not just deferred decoding.
	if len(selected) > 0 {
		iter, err := newArchiveIterSelected(context.Background(), snapshot, rollupScanBounds(), selected)
		if err != nil {
			return nil, err
		}
		err = foldRollupKeys(iter, identities, series, true, false)
		if err = errors.Join(err, iter.Close()); err != nil {
			return nil, errCheckpoint
		}
	}
	r := newCoreRollups()
	r.db = db
	if identities != nil {
		r.identities = identities
	}
	count := d.uvarint()
	if d.err != nil || count != countIndex || uint64(len(buf)-4-len(d.buf)) > indexOffset {
		return nil, errCheckpoint
	}
	r.checkpoint = &mappedRollupCheckpoint{buf: buf, table: table, index: indexes[:count*24], names: indexes[count*24 : count*32]}
	// A large delta already needs most names. Decode in file order instead
	// of turning recovery into millions of random ID-index probes.
	if count > 0 && uint64(len(series)) >= (count+1)/2 {
		for i := 0; i < int(count); i++ {
			_, record, err := r.checkpoint.record(i)
			if err != nil {
				return nil, err
			}
			rd := checkpointDecoder{buf: record}
			if err := r.decodeCheckpointRecord(&rd, table, series); err != nil {
				return nil, err
			}
			if rd.err != nil || len(rd.buf) != 0 {
				return nil, errCheckpoint
			}
		}
		r.checkpoint = nil
	} else {
		for _, e := range series {
			if err := r.loadCheckpointID(e.identity.id, series); err != nil {
				return nil, err
			}
		}
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

func (r *coreRollups) decodeCheckpointRecord(d *checkpointDecoder, table [][]byte, series map[v2part.ID]*rollupScanEntry) error {
	id := d.uvarint()
	index := d.uvarint()
	name := d.bytes()
	var meta metricMetadata
	_ = d.varint() // retired closure watermark
	meta.BaselineOrder = d.uvarint()
	meta.ResetOrder = d.uvarint()
	meta.LastUpdate = d.varint()
	meta.Revision = d.uvarint()
	meta.Frontier.Order = d.uvarint()
	meta.Frontier.MaxTime = d.varint()
	meta.Version = d.byte()
	meta.Expired = d.byte() == 1
	for range d.uvarint() { // consume legacy residency without retaining it
		_ = d.uvarint()
		_ = d.varint()
		_ = d.varint()
		_ = d.uint64()
		_ = d.uvarint()
	}
	if d.err != nil || index >= uint64(len(table)) {
		return errCheckpoint
	}
	e := series[v2part.ID(name)]
	var def []byte
	// A newer part already supplied and validated this identity. Rebuild the
	// checkpoint definition only when its metadata can win, or it is untouched.
	if e == nil || meta.after(e.meta) {
		def = make([]byte, 0, len(`{"Name":"`)+len(name)+len(table[index]))
		def = append(append(append(def, `{"Name":"`...), name...), table[index]...)
	}
	if e != nil {
		// Touched since the stamp: merge the reflected state into the fold.
		delete(series, v2part.ID(name))
		if meta.after(e.meta) {
			h, err := scanIdentity(r.identities, id, string(name), def)
			if err != nil {
				return errCheckpoint
			}
			e.identity, e.meta = h, meta
		}
		policy, m, records, err := e.finish(v2part.ID(name))
		if err != nil || m.ID != id {
			return errCheckpoint
		}
		if err = r.installRecovered(policy, m, records); err != nil {
			return errCheckpoint
		}
		return nil
	}
	m, err := r.metric(id, def)
	if err != nil || r.metrics[id] != nil {
		return errCheckpoint
	}
	r.install(id, m, meta)
	return d.err
}
