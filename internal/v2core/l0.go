package v2core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// L0 run files hold an owner's published head between seals (Config.L0).
// Each publication writes the applications it adds as one file, through a
// temporary name and a rename, before readers can see them, so a render never
// shows a point a kill -9 would lose. The seal that covers a file unlinks it
// once the part and frontier are durable. Restart loads every file present:
// each row keeps its application Order and Clock, so loading rows a seal
// already covers converges to the same records. Files are written, never
// fsynced: a kill -9 keeps them in the page cache, a power loss may lose them.
const l0Magic = "V2L0\x01"

var l0Table = crc32.MakeTable(crc32.Castagnoli)

// encodeL0 appends one file image of v's applications: magic, application
// count, then per application ID, Order, Clock, policy bytes and points (ID
// delta from the application, time, value bits, sequence delta from Order); a
// CRC-32C of everything before it ends the file. Row segments encode straight
// from their packed arrays: no row objects and no catalog lookups (LoadL0
// resolves handles).
func encodeL0(dst []byte, v PendingView) []byte {
	dst = append(dst[:0], l0Magic...)
	apps := 0
	for si := range v.segs {
		s := &v.segs[si]
		if s.cold != nil {
			apps += len(s.cold)
			continue
		}
		for pi := range s.packets {
			pk := &s.packets[pi]
			if pk.Flags&pendingMulti == 0 {
				apps += int(pk.Rows)
				continue
			}
			d := s.deltas[pk.Deltas : pk.Deltas+pk.Rows]
			apps++
			for k := 1; k < len(d); k++ {
				if d[k] != d[k-1] {
					apps++
				}
			}
		}
	}
	dst = binary.AppendUvarint(dst, uint64(apps))
	for si := range v.segs {
		s := &v.segs[si]
		if s.cold != nil {
			for i := range s.cold {
				dst = appendL0Batch(dst, &s.cold[i])
			}
			continue
		}
		dst = s.appendL0(dst)
	}
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst, l0Table))
}

func appendL0Batch(dst []byte, b *coreBatch) []byte {
	dst = binary.AppendUvarint(dst, b.ID)
	dst = binary.AppendUvarint(dst, b.Order)
	dst = binary.AppendVarint(dst, b.Clock)
	dst = binary.AppendUvarint(dst, uint64(len(b.Policy)))
	dst = append(dst, b.Policy...)
	dst = binary.AppendUvarint(dst, uint64(len(b.Points)))
	for _, p := range b.Points {
		dst = binary.AppendVarint(dst, int64(p.ID-b.ID))
		dst = binary.AppendVarint(dst, p.Time)
		dst = binary.LittleEndian.AppendUint64(dst, p.Bits)
		dst = binary.AppendVarint(dst, int64(p.Seq-b.Order))
	}
	return dst
}

// appendL0 encodes a row segment's applications as appendL0Batch would their
// decoded batches. Rows ascend, so one cursor walks the escapes.
func (s *pendingSegment) appendL0(dst []byte) []byte {
	first := s.firstRow()
	escape := sort.Search(len(s.escapes), func(i int) bool { return s.escapes[i].Row >= first })
	idTime := func(pk *pendingPacket, k uint32) (uint64, int64) {
		if escape < len(s.escapes) && s.escapes[escape].Row == k {
			e := &s.escapes[escape]
			escape++
			return e.ID, e.Time
		}
		r := &s.rows[k]
		return uint64(r.ID), pk.Base + int64(r.DT)
	}
	for pi := range s.packets {
		pk := &s.packets[pi]
		end := pk.FirstRow + pk.Rows
		for lo := pk.FirstRow; lo < end; {
			d := s.delta(pk, lo)
			hi := lo + 1
			for hi < end && s.delta(pk, hi) == d {
				hi++
			}
			order := pk.FirstOrder + uint64(d)
			app, t := idTime(pk, lo)
			dst = binary.AppendUvarint(dst, app)
			dst = binary.AppendUvarint(dst, order)
			dst = binary.AppendVarint(dst, pk.Clock)
			dst = append(dst, 0) // row applications carry no policy bytes
			dst = binary.AppendUvarint(dst, uint64(hi-lo))
			for k, id := lo, app; k < hi; k++ {
				if k > lo {
					id, t = idTime(pk, k)
				}
				seq := order
				if pk.Flags&pendingSeq != 0 {
					seq = s.seqs[pk.Seqs+k-pk.FirstRow]
				}
				dst = binary.AppendVarint(dst, int64(id-app))
				dst = binary.AppendVarint(dst, t)
				dst = binary.LittleEndian.AppendUint64(dst, s.rows[k].Bits)
				dst = binary.AppendVarint(dst, int64(seq-order))
			}
			lo = hi
		}
	}
	return dst
}

var errL0Corrupt = errors.New("corrupt L0 run file")

// decodeL0 returns the applications of one file image. Handles are left nil;
// the owner resolves them through its catalog when it loads the rows.
func decodeL0(b []byte) ([]coreBatch, error) {
	if len(b) < len(l0Magic)+4 || string(b[:len(l0Magic)]) != l0Magic {
		return nil, errL0Corrupt
	}
	body := b[:len(b)-4]
	if crc32.Checksum(body, l0Table) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return nil, errL0Corrupt
	}
	r := body[len(l0Magic):]
	bad := false
	uv := func() uint64 {
		v, n := binary.Uvarint(r)
		if n <= 0 {
			bad = true
			return 0
		}
		r = r[n:]
		return v
	}
	sv := func() int64 {
		v, n := binary.Varint(r)
		if n <= 0 {
			bad = true
			return 0
		}
		r = r[n:]
		return v
	}
	count := uv()
	if bad || count > uint64(len(r)) {
		return nil, errL0Corrupt
	}
	out := make([]coreBatch, count)
	for i := range out {
		b := &out[i]
		b.ID, b.Order, b.Clock = uv(), uv(), sv()
		if n := uv(); !bad && n > 0 {
			if n > uint64(len(r)) {
				return nil, errL0Corrupt
			}
			b.Policy, r = slices.Clone(r[:n]), r[n:]
		}
		n := uv()
		if bad || n > uint64(len(r)) {
			return nil, errL0Corrupt
		}
		b.Points = make([]row, n)
		for j := range b.Points {
			p := &b.Points[j]
			p.ID = b.ID + uint64(sv())
			p.Time = sv()
			if bad || len(r) < 8 {
				return nil, errL0Corrupt
			}
			p.Bits, r = binary.LittleEndian.Uint64(r), r[8:]
			p.Seq = b.Order + uint64(sv())
		}
		if bad {
			return nil, errL0Corrupt
		}
	}
	if len(r) != 0 {
		return nil, errL0Corrupt
	}
	return out, nil
}

func (o *coreOwner) l0Dir() string { return filepath.Join(o.dir, "l0") }

// openL0 creates the owner's L0 directory, removes temporaries a kill left
// before their rename and returns the files present, oldest first, and the
// next file number.
func (o *coreOwner) openL0() ([]string, uint64, error) {
	dir := o.l0Dir()
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, 0, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	var files []string
	next := uint64(1)
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".tmp") {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, 0, err
			}
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimPrefix(name, "l0-"), 10, 64)
		if err != nil || !strings.HasPrefix(name, "l0-") {
			return nil, 0, fmt.Errorf("unexpected L0 file %s", name)
		}
		files = append(files, filepath.Join(dir, name))
		next = max(next, seq+1)
	}
	// Zero-padded names sort by number.
	slices.Sort(files)
	return files, next, nil
}

// writeL0 publishes one run file through a temporary name and a rename.
func (o *coreOwner) writeL0(v PendingView) (string, error) {
	o.l0Buf = encodeL0(o.l0Buf, v)
	path := filepath.Join(o.l0Dir(), fmt.Sprintf("l0-%020d", o.l0Next))
	o.l0Next++
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, o.l0Buf, 0600); err != nil {
		return "", err
	}
	if o.stage != nil {
		o.stage("l0-written")
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", err
	}
	if o.stage != nil {
		o.stage("l0-renamed")
	}
	o.l0Bytes.Add(uint64(len(o.l0Buf)))
	// A seal can leave one small suffix between normal publications. Keep
	// headroom for both recent sizes, then shrink after two smaller writes.
	keep := max(64<<10, 2*max(len(o.l0Buf), o.l0LastBytes))
	o.l0LastBytes = len(o.l0Buf)
	if cap(o.l0Buf) > keep {
		o.l0Buf = make([]byte, 0, keep)
	}
	return path, nil
}

// unlinkL0 removes run files a durable seal covers.
func unlinkL0(files []string) error {
	for _, f := range files {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// readL0 decodes run files in order into one application sequence.
func readL0(files []string) ([]coreBatch, error) {
	var batches []coreBatch
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		decoded, err := decodeL0(b)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		batches = append(batches, decoded...)
	}
	return batches, nil
}

// LoadL0 reads every L0 run file present into its owner's head, as published
// rows the next seal covers and whose files it then unlinks. Call it once,
// before intake. The callback may open the catalog resolving each kept
// application; an error leaves this owner's files untouched. keep sees each
// loaded application with its owner: callers continue
// each producer's order above them and drop the ones a durable seal already
// covers (a kill between a seal's commit and its unlink), which the rollup
// reducer would refuse as not after its frontier.
func (e *coreEngine) LoadL0(keep func(owner int, b *Batch) (bool, error)) error {
	if !e.cfg.L0 {
		return nil
	}
	for owner, o := range e.owners {
		files := o.l0Files
		o.l0Files = nil
		if len(files) == 0 {
			continue
		}
		batches, err := readL0(files)
		if err != nil {
			return err
		}
		if keep != nil {
			kept := batches[:0]
			for i := range batches {
				ok, err := keep(owner, &batches[i])
				if err != nil {
					return err
				}
				if ok {
					kept = append(kept, batches[i])
				}
			}
			batches = kept
		}
		// Like producer packets, rows carry every point and batches share them.
		n := 0
		for i := range batches {
			n += len(batches[i].Points)
		}
		rows := make([]row, 0, n)
		for i := range batches {
			b := &batches[i]
			b.Registered = o.lookup(b.ID)
			if e.cfg.Rollups && b.Registered == nil && len(b.Policy) == 0 {
				return fmt.Errorf("%s: L0 application %d has no catalog entry", files[0], b.ID)
			}
			start := len(rows)
			rows = append(rows, b.Points...)
			b.Points = rows[start:len(rows):len(rows)]
		}
		ack := make(chan error, 1)
		if err := o.queues[0].push(&corePacket{rows: rows, batches: batches, l0: files, ack: ack}, o.dead); err != nil {
			return err
		}
		select {
		case err := <-ack:
			if err != nil {
				return err
			}
		case <-o.dead:
			return errors.New("owner stopped while loading L0")
		}
	}
	return nil
}
