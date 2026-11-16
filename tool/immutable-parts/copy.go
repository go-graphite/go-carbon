package main

// Offline copy experiment. A sealed, pinned manifest is the transfer unit.
// It deliberately does not use the daemon's name-at-a-time copy API.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

type copyFile struct {
	Part partsformat.Part
	Path string // sender-local; never supplied by the receiver
}

// Optional owned-file cache advice for the offline Linux campaign only.
// Installed before accepting connections; never mutates daemon/global caches.
var copyBeforeFileRead func(*os.File) error

type copyPartition struct {
	Manifest partsformat.Manifest
	Files    []copyFile
	Blocks   *copyBlockManifest `json:",omitempty"` // persisted once at seal
	Additive bool               `json:",omitempty"` // hole repair preserves target-only keys
}

// copyPlan compares persisted partition inventories, never names or points.
// A different physical sealing history conservatively requests the partition.
// Target-only partitions are outside this donor's authority and stay untouched.
func copyPlan(source []copyPartition, target []partsformat.Manifest) ([]int, error) {
	type key struct {
		group     partsformat.Group
		shard     uint16
		partition int64
	}
	digests := make(map[key][32]byte, len(target))
	for _, m := range target {
		k := key{m.Group, m.VShard, m.Partition}
		if _, ok := digests[k]; ok {
			return nil, errors.New("duplicate target partition")
		}
		d, err := m.Digest()
		if err != nil {
			return nil, err
		}
		digests[k] = d
	}
	var plan []int
	seen := make(map[key]bool, len(source))
	for i, p := range source {
		m := p.Manifest
		k := key{m.Group, m.VShard, m.Partition}
		if seen[k] {
			return nil, errors.New("duplicate source partition")
		}
		seen[k] = true
		if len(m.Parts) != len(p.Files) {
			return nil, errors.New("manifest/file count mismatch")
		}
		for j, f := range p.Files {
			if f.Part != m.Parts[j] {
				return nil, errors.New("manifest/file identity mismatch")
			}
		}
		d, err := m.Digest()
		if err != nil {
			return nil, err
		}
		if t, ok := digests[k]; !ok || d != t {
			plan = append(plan, i)
		}
	}
	return plan, nil
}

// copyExport is the compatibility path for an SST crossing partition bounds.
// It visits a bounded immutable Pebble snapshot ONCE and builds a portable SST.
// This cost belongs to export, not to the zero-rewrite transport measurement.
// Fast-path writers retain their already partition-bounded seal SST instead.
func copyExport(snap *pebble.Snapshot, dir string, m partsformat.Manifest, id partsformat.PartID) (copyPartition, error) {
	lo, err := partsformat.PartitionPrefixForGroup(m.Group, m.VShard, m.Partition)
	if err != nil {
		return copyPartition{}, err
	}
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: partsformat.PrefixEnd(lo)})
	if err != nil {
		return copyPartition{}, err
	}
	defer it.Close()
	f, err := os.CreateTemp(dir, "export-*.sst")
	if err != nil {
		return copyPartition{}, err
	}
	path := f.Name()
	_ = f.Close()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(path)
		}
	}()
	vf, err := vfs.Default.Create(path)
	if err != nil {
		return copyPartition{}, err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(vf), sstable.WriterOptions{Compression: sstable.NoCompression, TableFormat: sstable.TableFormatPebblev4})
	n := 0
	for valid := it.First(); valid; valid = it.Next() {
		v, e := it.ValueAndErr()
		if e != nil {
			_ = w.Close()
			return copyPartition{}, e
		}
		if e = w.Set(it.Key(), v); e != nil {
			_ = w.Close()
			return copyPartition{}, e
		}
		n++
	}
	err = errors.Join(it.Error(), w.Close())
	if err != nil {
		return copyPartition{}, err
	}
	m.Parts = nil
	p := copyPartition{Manifest: m}
	if n == 0 {
		_ = os.Remove(path)
		ok = true
		return p, nil
	}
	fdesc, err := copyDescribe(path, id)
	if err != nil {
		return copyPartition{}, err
	}
	p.Files = []copyFile{fdesc}
	p.Manifest.Parts = []partsformat.Part{fdesc.Part}
	ok = true
	return p, nil
}

func copyDescribe(path string, id partsformat.PartID) (copyFile, error) {
	f, e := os.Open(path)
	if e != nil {
		return copyFile{}, e
	}
	defer f.Close()
	h := sha256.New()
	n, e := io.Copy(h, f)
	if e != nil {
		return copyFile{}, e
	}
	p := partsformat.Part{ID: id, Bytes: uint64(n)}
	copy(p.SHA256[:], h.Sum(nil))
	return copyFile{Part: p, Path: path}, nil
}

// Sender serves only indices into a frozen, trusted inventory. The caller pins
// these immutable files until all connections have completed. No remote paths.
func copyServeConn(c net.Conn, source []copyPartition) error {
	defer c.Close()
	var req [8]byte
	for {
		_ = c.SetDeadline(time.Now().Add(2 * time.Minute))
		_, e := io.ReadFull(c, req[:])
		if errors.Is(e, io.EOF) {
			return nil
		}
		if e != nil {
			return e
		}
		pi, fi := binary.BigEndian.Uint32(req[:4]), binary.BigEndian.Uint32(req[4:])
		if pi == ^uint32(0) && fi == 2 {
			// Trusted offline discovery for the same frozen inventory. Paths are
			// sender-local descriptors, never opened or chosen by the receiver.
			b, err := json.Marshal(source)
			if err != nil {
				return err
			}
			if len(b) > 64<<20 {
				return errors.New("copy inventory too large")
			}
			binary.BigEndian.PutUint64(req[:], uint64(len(b)))
			if _, err = c.Write(req[:]); err != nil {
				return err
			}
			if _, err = c.Write(b); err != nil {
				return err
			}
			continue
		}
		if pi == ^uint32(0) && fi == 0 {
			if e = copyServeCompare(c, source); e != nil {
				return e
			}
			continue
		}
		if uint64(pi) >= uint64(len(source)) || uint64(fi) >= uint64(len(source[pi].Files)) {
			return errors.New("invalid file index")
		}
		fdesc := source[pi].Files[fi]
		f, e := os.Open(fdesc.Path)
		if e != nil {
			return e
		}
		if copyBeforeFileRead != nil {
			if e = copyBeforeFileRead(f); e != nil {
				_ = f.Close()
				return e
			}
		}
		st, e := f.Stat()
		if e != nil || uint64(st.Size()) != fdesc.Part.Bytes {
			_ = f.Close()
			return errors.New("pinned part changed")
		}
		binary.BigEndian.PutUint64(req[:], fdesc.Part.Bytes)
		_, e = c.Write(req[:])
		if e == nil {
			_, e = io.CopyN(c, f, st.Size())
		}
		e = errors.Join(e, f.Close())
		if e != nil {
			return e
		}
	}
}

// Digests are keyed by the exact portable group/vshard/partition prefix.
// Metadata grows with partitions and placement labels, never metric names.
// Source descriptors are supplied once for the same pinned snapshot.
func copyServeCompare(c net.Conn, source []copyPartition) error {
	var header [8]byte
	if _, e := io.ReadFull(c, header[:]); e != nil {
		return e
	}
	n, size := binary.BigEndian.Uint32(header[:4]), binary.BigEndian.Uint32(header[4:])
	if n > 1<<20 || size > 64<<20 {
		return errors.New("partition inventory too large")
	}
	b := make([]byte, int(size))
	if _, e := io.ReadFull(c, b); e != nil {
		return e
	}
	target := make(map[string][32]byte, n)
	for range n {
		if len(b) < 4 {
			return io.ErrUnexpectedEOF
		}
		klen := int(binary.BigEndian.Uint32(b[:4]))
		b = b[4:]
		if klen < 18 || klen > partsformat.MaxNameBytes+32 || len(b) < klen+32 {
			return errors.New("invalid partition digest prefix")
		}
		k := string(b[:klen])
		var d [32]byte
		copy(d[:], b[klen:klen+32])
		b = b[klen+32:]
		if _, ok := target[k]; ok {
			return errors.New("duplicate digest partition")
		}
		target[k] = d
	}
	if len(b) != 0 {
		return errors.New("trailing inventory bytes")
	}
	var plan []int
	for i, p := range source {
		k, e := partsformat.PartitionPrefixForGroup(p.Manifest.Group, p.Manifest.VShard, p.Manifest.Partition)
		if e != nil {
			return e
		}
		d, e := p.Manifest.Digest()
		if e != nil {
			return e
		}
		if got, ok := target[string(k)]; !ok || got != d {
			plan = append(plan, i)
		}
	}
	out := make([]byte, 4+4*len(plan))
	binary.BigEndian.PutUint32(out[:4], uint32(len(plan)))
	for i, pi := range plan {
		binary.BigEndian.PutUint32(out[4+i*4:], uint32(pi))
	}
	_, e := c.Write(out)
	return e
}
func copyCompare(ctx context.Context, address string, target []partsformat.Manifest) ([]int, uint64, error) {
	c, e := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if e != nil {
		return nil, 0, e
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	_ = c.SetDeadline(time.Now().Add(2 * time.Minute))
	if len(target) > 1<<20 {
		return nil, 0, errors.New("partition inventory too large")
	}
	var payload bytes.Buffer
	for _, m := range target {
		prefix, e := partsformat.PartitionPrefixForGroup(m.Group, m.VShard, m.Partition)
		if e != nil {
			return nil, 0, e
		}
		d, e := m.Digest()
		if e != nil {
			return nil, 0, e
		}
		if payload.Len()+4+len(prefix)+32 > 64<<20 {
			return nil, 0, errors.New("partition inventory too large")
		}
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(prefix)))
		payload.Write(n[:])
		payload.Write(prefix)
		payload.Write(d[:])
	}
	var header [16]byte
	binary.BigEndian.PutUint32(header[:4], ^uint32(0))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(target)))
	binary.BigEndian.PutUint32(header[12:], uint32(payload.Len()))
	if _, e = c.Write(header[:]); e != nil {
		return nil, 0, e
	}
	if _, e = c.Write(payload.Bytes()); e != nil {
		return nil, 0, e
	}
	var count [4]byte
	if _, e = io.ReadFull(c, count[:]); e != nil {
		return nil, 0, e
	}
	n := binary.BigEndian.Uint32(count[:])
	if n > 1<<20 {
		return nil, 0, errors.New("copy plan too large")
	}
	out := make([]byte, int(n)*4)
	if _, e = io.ReadFull(c, out); e != nil {
		return nil, 0, e
	}
	plan := make([]int, n)
	for i := range plan {
		plan[i] = int(binary.BigEndian.Uint32(out[i*4:]))
	}
	return plan, uint64(20 + payload.Len() + len(out)), nil
}

func copySelectGroup(source []copyPartition, plan []int, group partsformat.Group) []int {
	var selected []int
	for _, i := range plan {
		if source[i].Manifest.Group == group {
			selected = append(selected, i)
		}
	}
	return selected
}

// Receive verifies and fsyncs every SST before atomically replacing a partition
// with IngestAndExcise. The caller must fence local writes for this partition:
// replacing a snapshot over concurrent local writes would erase the catch-up.
// A durable owner frontier and READY publication are intentionally not implied.
func copyReceivePartition(ctx context.Context, c net.Conn, source []copyPartition, pi int, db *pebble.DB, stage string) (uint64, error) {
	p := source[pi]
	lo, e := partsformat.PartitionPrefixForGroup(p.Manifest.Group, p.Manifest.VShard, p.Manifest.Partition)
	if e != nil {
		return 0, e
	}
	dir, e := os.MkdirTemp(stage, "receive-")
	if e != nil {
		return 0, e
	}
	defer os.RemoveAll(dir)
	var paths []string
	var total uint64
	buf := make([]byte, 1<<20)
	for fi, fdesc := range p.Files {
		if e = ctx.Err(); e != nil {
			return total, e
		}
		var req [8]byte
		binary.BigEndian.PutUint32(req[:4], uint32(pi))
		binary.BigEndian.PutUint32(req[4:], uint32(fi))
		_ = c.SetDeadline(time.Now().Add(2 * time.Minute))
		if _, e = c.Write(req[:]); e != nil {
			return total, e
		}
		if _, e = io.ReadFull(c, req[:]); e != nil {
			return total, e
		}
		n := binary.BigEndian.Uint64(req[:])
		if n != fdesc.Part.Bytes || n > 1<<40 {
			return total, errors.New("part length mismatch")
		}
		path := filepath.Join(dir, fmt.Sprintf("%06d.sst", fi))
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return total, e
		}
		h := sha256.New()
		written, e := io.CopyBuffer(io.MultiWriter(f, h), io.LimitReader(c, int64(n)), buf)
		if e == nil && uint64(written) != n {
			e = io.ErrUnexpectedEOF
		}
		if e == nil && !bytes.Equal(h.Sum(nil), fdesc.Part.SHA256[:]) {
			e = errors.New("part checksum mismatch")
		}
		if e == nil {
			e = f.Sync()
		}
		e = errors.Join(e, f.Close())
		if e != nil {
			return total, e
		}
		if e = copyValidateFile(p.Manifest, path); e != nil {
			return total, e
		}
		total += n
		paths = append(paths, path)
	}
	if e = ctx.Err(); e != nil {
		return total, e
	}
	if p.Additive {
		if len(paths) == 0 {
			return total, nil
		}
		return total, db.Ingest(paths)
	}
	// Pebble's empty IngestAndExcise is a no-op. A blind, synced range
	// tombstone is required when the authoritative partition is empty.
	if len(paths) == 0 {
		return total, db.DeleteRange(lo, partsformat.PrefixEnd(lo), pebble.Sync)
	}
	_, e = db.IngestAndExcise(paths, nil, pebble.KeyRange{Start: lo, End: partsformat.PrefixEnd(lo)})
	return total, e
}

// Bound checks read table metadata and its first/last index entries, not names
// across the catalogue. Range tombstones are forbidden in portable seal files.
func copyValidateFile(m partsformat.Manifest, path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	rd, e := sstable.NewSimpleReadable(f)
	if e != nil {
		_ = f.Close()
		return e
	}
	r, e := sstable.NewReader(rd, sstable.ReaderOptions{})
	if e != nil {
		return e
	}
	defer r.Close()
	if r.Properties.NumRangeDeletions != 0 || r.Properties.NumRangeKeys() != 0 {
		return errors.New("portable SST contains range mutations")
	}
	it, e := r.NewIter(nil, nil)
	if e != nil {
		return e
	}
	defer it.Close()
	first, _ := it.First()
	if first == nil {
		return errors.New("empty portable SST")
	}
	lo := bytes.Clone(first.UserKey)
	last, _ := it.Last()
	if last == nil {
		return errors.New("invalid portable SST")
	}
	if e = it.Error(); e != nil {
		return e
	}
	return partsformat.ValidateFileRange(m, lo, last.UserKey)
}

type copyStats struct {
	Bytes      uint64
	Partitions int
	Seconds    float64
}

// One TCP connection and bounded receive buffer per worker; Pebble ownership is
// independent of stream count. Independent receiver DBs avoid a shared writer.
func copyTransfer(ctx context.Context, address string, source []copyPartition, plan []int, streams int, targets []*pebble.DB, stage string) (copyStats, error) {
	return copyTransferResumable(ctx, address, source, plan, streams, targets, stage, nil)
}

func copyTransferResumable(ctx context.Context, address string, source []copyPartition, plan []int, streams int, targets []*pebble.DB, stage string, ledger *copyDoneLedger) (copyStats, error) {
	start := time.Now()
	stats := copyStats{Partitions: len(plan)}
	if streams < 1 || streams > 64 || len(targets) == 0 {
		return stats, os.ErrInvalid
	}
	seenPlan := make(map[int]bool, len(plan))
	for _, pi := range plan {
		if seenPlan[pi] {
			return stats, errors.New("duplicate partition task")
		}
		seenPlan[pi] = true
		if pi < 0 || pi >= len(source) {
			return stats, errors.New("invalid partition index")
		}
	}
	if len(plan) == 0 {
		stats.Seconds = time.Since(start).Seconds()
		return stats, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failure error
	for range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, e := (&net.Dialer{}).DialContext(ctx, "tcp", address)
			if e != nil {
				mu.Lock()
				failure = errors.Join(failure, e)
				mu.Unlock()
				cancel()
				return
			}
			defer c.Close()
			stop := context.AfterFunc(ctx, func() { _ = c.Close() })
			defer stop()
			for pi := range jobs {
				var n uint64
				var skipped bool
				if ledger != nil {
					skipped, e = ledger.begin(pi, source[pi])
				}
				if e == nil && !skipped {
					n, e = copyReceivePartition(ctx, c, source, pi, targets[int(source[pi].Manifest.VShard)%len(targets)], stage)
					if e == nil && ledger != nil {
						e = ledger.finish(pi, source[pi])
					}
				}
				mu.Lock()
				stats.Bytes += n
				failure = errors.Join(failure, e)
				mu.Unlock()
				if e != nil {
					cancel()
					return
				}
			}
		}()
	}
loop:
	for _, pi := range plan {
		select {
		case jobs <- pi:
		case <-ctx.Done():
			break loop
		}
	}
	close(jobs)
	wg.Wait()
	stats.Seconds = time.Since(start).Seconds()
	if failure == nil {
		failure = ctx.Err()
	}
	return stats, failure
}
