//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"golang.org/x/sys/unix"
)

// Packing changes the physical offline Pebble representation only. Recovery
// receipts retain their exact logical keys, prefixes and digests. No online
// publisher or reader is allowed to open this exclusively locked copy.
type packingInventory struct {
	Records, Points, KeyBytes, ValueBytes, Boundaries uint64
	SSTs, SSTBytes, SSTMedianBytes, AllocatedBytes    uint64
	MinimumOutputFiles, OutputBoundBytes              uint64
	PlannedSSTBytes, PlannedSSTAllocatedBytes         uint64
	Digest                                            string
}

// Dry-run the identical SST encoder without allocating files. This permits
// admission to charge compressed bytes and each file's allocation rounding.
type packingSizeSink struct{ bytes uint64 }

func (s *packingSizeSink) Write(p []byte) error { s.bytes += uint64(len(p)); return nil }
func (*packingSizeSink) Finish() error          { return nil }
func (*packingSizeSink) Abort()                 {}

type packingResult struct {
	Before, After packingInventory
	TargetBytes   uint64
	BitExact      bool
	// Allocation admission is conservative, but does not certify an entire
	// conversion's source-compaction scratch or filesystem metadata maximum.
	DiskCapQualified bool
}

type packingJournal struct {
	Version     int
	Destination string
	State       string
	Expected    packingInventory
	IndexDigest string `json:",omitempty"`
}

func packingOptions(readOnly bool) *pebble.Options {
	return &pebble.Options{ReadOnly: readOnly, DisableAutomaticCompactions: true,
		MemTableSize: 1 << 20, MaxConcurrentCompactions: func() int { return 1 },
		Levels: []pebble.LevelOptions{{TargetFileSize: 32 << 20}}}
}

func packingPrefix(key []byte) ([]byte, error) {
	k, err := partsformat.Decode(key)
	if err != nil {
		return nil, err
	}
	return partsformat.PartitionPrefixForGroup(k.Group, k.VShard, k.Partition)
}

func packingDigest(h hash.Hash, key, value []byte) {
	var sizes [16]byte
	binary.BigEndian.PutUint64(sizes[:8], uint64(len(key)))
	binary.BigEndian.PutUint64(sizes[8:], uint64(len(value)))
	_, _ = h.Write(sizes[:])
	_, _ = h.Write(key)
	_, _ = h.Write(value)
}

func inventoryPacking(ctx context.Context, db *pebble.DB, path string, target uint64) (packingInventory, error) {
	var out packingInventory
	it, err := db.NewIter(nil)
	if err != nil {
		return out, err
	}
	h := sha256.New()
	var prefix, shardPrefix []byte
	var used uint64
	var writer *sstable.Writer
	var sink *packingSizeSink
	flush := func() error {
		if writer == nil {
			return nil
		}
		err := writer.Close()
		writer = nil
		if err == nil {
			out.PlannedSSTBytes += sink.bytes
			out.PlannedSSTAllocatedBytes += (sink.bytes + 4095) / 4096 * 4096
		}
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		if err = ctx.Err(); err != nil {
			break
		}
		p, e := packingPrefix(it.Key())
		if e != nil {
			err = e
			break
		}
		if !bytes.Equal(prefix, p) {
			out.Boundaries++
			prefix = bytes.Clone(p)
		}
		shard := p[:len(p)-8]
		if !bytes.Equal(shardPrefix, shard) {
			if err = flush(); err != nil {
				break
			}
			shardPrefix = bytes.Clone(shard)
			used = 0
		}
		n := uint64(len(it.Key()) + len(it.Value()) + 64)
		if used == 0 || used+n > target {
			if err = flush(); err != nil {
				break
			}
			out.MinimumOutputFiles++
			used = 0
			sink = new(packingSizeSink)
			writer = sstable.NewWriter(sink, sstable.WriterOptions{Compression: sstable.SnappyCompression})
		}
		if err = writer.Set(it.Key(), it.Value()); err != nil {
			break
		}
		used += n
		out.Records++
		out.OutputBoundBytes += uint64(2*len(p) + 128)
		out.KeyBytes += uint64(len(it.Key()))
		out.ValueBytes += uint64(len(it.Value()))
		_, _, points, e := partsconvert.DecodeArchive(it.Value())
		if e != nil {
			err = e
			break
		}
		out.Points += uint64(len(points))
		packingDigest(h, it.Key(), it.Value())
	}
	if err = errors.Join(err, it.Error(), it.Close(), flush()); err != nil {
		return out, err
	}
	out.Digest = hex.EncodeToString(h.Sum(nil))
	// Ingest links these same-filesystem SSTs. Charge dry-encoded lengths
	// rounded to 4KiB plus a metadata/WAL allowance. Directory blocks and
	// filesystem-specific metadata are not certified by this estimate.
	out.OutputBoundBytes += out.PlannedSSTAllocatedBytes + (16 << 20)
	levels, err := db.SSTables()
	if err != nil {
		return out, err
	}
	var sizes []uint64
	for _, level := range levels {
		for _, table := range level {
			sizes = append(sizes, table.Size)
			out.SSTBytes += table.Size
		}
	}
	slices.Sort(sizes)
	out.SSTs = uint64(len(sizes))
	if len(sizes) > 0 {
		out.SSTMedianBytes = sizes[len(sizes)/2]
	}
	out.AllocatedBytes, err = allocatedBytes(path)
	return out, err
}

func samePacking(a, b packingInventory) bool {
	return a.Digest == b.Digest && a.Records == b.Records && a.Points == b.Points && a.KeyBytes == b.KeyBytes && a.ValueBytes == b.ValueBytes && a.Boundaries == b.Boundaries
}

func syncPackingDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func packingLock(target string) (func(), error) {
	info, err := os.Lstat(target)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("packing target must be a real offline conversion directory")
	}
	id, err := os.ReadFile(filepath.Join(target, "CONVERSION-TARGET"))
	if err != nil {
		return nil, err
	}
	raw, err := hex.DecodeString(string(bytes.TrimSpace(id)))
	if err != nil || len(raw) != 16 {
		return nil, errors.New("invalid packing destination identity")
	}
	f, err := os.OpenFile(filepath.Join(target, "PACKING.LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, errors.New("offline copy packing/conversion already active")
	}
	return func() { _ = unix.Flock(int(f.Fd()), unix.LOCK_UN); _ = f.Close() }, nil
}

// Called while holding PACKING.LOCK, before any consumer opens parts. Fixed
// child names and destination identity prevent journals from naming other data.
func recoverPacking(ctx context.Context, target string) error {
	return recoverPackingAt(ctx, target, nil)
}

func recoverPackingAt(ctx context.Context, target string, checkpoint func(string) error) error {
	check := func(stage string) error {
		if checkpoint != nil {
			return checkpoint(stage)
		}
		return nil
	}
	path := filepath.Join(target, "PACKING.json")
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var j packingJournal
	if err = json.Unmarshal(b, &j); err != nil {
		return err
	}
	id, err := os.ReadFile(filepath.Join(target, "CONVERSION-TARGET"))
	if err != nil {
		return err
	}
	if j.Version != 1 || j.Destination != string(bytes.TrimSpace(id)) {
		return errors.New("packing journal destination changed")
	}
	resumedInstalled := j.State == "installed"
	live, next, old := filepath.Join(target, "parts"), filepath.Join(target, "packing-next"), filepath.Join(target, "packing-old")
	for _, p := range []string{live, next, old} {
		info, e := os.Lstat(p)
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if e == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			return errors.New("packing generation is not a real directory")
		}
	}
	exists := func(p string) bool { _, e := os.Stat(p); return e == nil }
	if j.State == "building" {
		if !exists(live) || exists(old) {
			return errors.New("invalid building packing layout")
		}
		if err = os.RemoveAll(next); err != nil {
			return err
		}
	} else if j.State == "verified" {
		// A lost rename acknowledgement is recovered from the fixed directory
		// layout. Recheck the complete immutable replacement before any delete.
		candidate := next
		if !exists(next) {
			if !exists(old) || !exists(live) {
				return errors.New("verified packing lost its replacement")
			}
			candidate = live
		}
		db, e := pebble.Open(candidate, packingOptions(true))
		if e != nil {
			return e
		}
		got, e := inventoryPacking(ctx, db, candidate, 32<<20)
		e = errors.Join(e, checkPackingIndex(candidate, j.IndexDigest), db.Close())
		if e != nil {
			return e
		}
		if !samePacking(j.Expected, got) {
			return errors.New("packing replacement digest changed")
		}
		if candidate == next {
			if exists(live) {
				if exists(old) {
					return errors.New("packing has three live generations")
				}
				if err = os.Rename(live, old); err != nil {
					return err
				}
				if err = syncPackingDirectory(target); err != nil {
					return err
				}
				if err = check("packing-old-renamed"); err != nil {
					return err
				}
			} else if !exists(old) {
				return errors.New("packing source generation missing")
			}
			if err = os.Rename(next, live); err != nil {
				return err
			}
			if err = syncPackingDirectory(target); err != nil {
				return err
			}
			if err = check("packing-new-renamed"); err != nil {
				return err
			}
		}
		// Mark installation durable before deleting old. If removal is
		// interrupted, recovery no longer needs the old directory to exist.
		j.State = "installed"
		if err = saveRangeJSON(path, j); err != nil {
			return err
		}
		if err = check("packing-installed"); err != nil {
			return err
		}
	} else if j.State != "installed" {
		return errors.New("invalid packing journal state")
	}
	if j.State == "installed" {
		if !exists(live) || exists(next) {
			return errors.New("invalid installed packing layout")
		}
		// On a resumed installed journal, verify before removing any remaining
		// old files. The just-verified path above need not scan a second time.
		if resumedInstalled {
			db, e := pebble.Open(live, packingOptions(true))
			if e != nil {
				return e
			}
			got, e := inventoryPacking(ctx, db, live, 32<<20)
			e = errors.Join(e, checkPackingIndex(live, j.IndexDigest), db.Close())
			if e != nil {
				return e
			}
			if !samePacking(j.Expected, got) {
				return errors.New("installed packing digest changed")
			}
		}
		if err = os.RemoveAll(old); err != nil {
			return err
		}
	}
	if err = os.Remove(path); err != nil {
		return err
	}
	return syncPackingDirectory(target)
}

// packCopy is an exclusively owned OFFLINE target rewrite, never an online
// publisher. A source-independent maintenance operation leaves the source and
// all active/global conversion receipts untouched.
func packCopy(ctx context.Context, source, target string, original, targetBytes uint64, apply bool, checkpoint func(string) error) (packingResult, error) {
	out := packingResult{TargetBytes: targetBytes}
	if targetBytes < 1<<20 || targetBytes > 64<<20 {
		return out, errors.New("packing target must be between 1 and 64 MiB")
	}
	unlock, err := packingLock(target)
	if err != nil {
		return out, err
	}
	defer unlock()
	if err = recoverPacking(ctx, target); err != nil {
		return out, err
	}
	live := filepath.Join(target, "parts")
	db, err := pebble.Open(live, packingOptions(true))
	if err != nil {
		return out, err
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	out.Before, err = inventoryPacking(ctx, db, live, targetBytes)
	if err != nil || !apply {
		return out, err
	}
	if original > 0 {
		if err = admitCopyBatch(source, target, original, out.Before.OutputBoundBytes); err != nil {
			return out, err
		}
	}
	id, err := os.ReadFile(filepath.Join(target, "CONVERSION-TARGET"))
	if err != nil {
		return out, err
	}
	next := filepath.Join(target, "packing-next")
	if _, err = os.Stat(next); !errors.Is(err, os.ErrNotExist) {
		return out, errors.New("unowned packing-next exists")
	}
	j := packingJournal{Version: 1, Destination: string(bytes.TrimSpace(id)), State: "building", Expected: out.Before}
	if err = saveRangeJSON(filepath.Join(target, "PACKING.json"), j); err != nil {
		return out, err
	}
	newDB, err := pebble.Open(next, packingOptions(false))
	if err != nil {
		return out, err
	}
	defer func() {
		if newDB != nil {
			_ = newDB.Close()
		}
	}()
	it, err := db.NewIter(nil)
	if err != nil {
		return out, err
	}
	var w *sstable.Writer
	var shardPrefix []byte
	var used, number uint64
	var stage string
	flush := func() error {
		if w == nil {
			return nil
		}
		err := w.Close()
		w = nil
		if err != nil {
			return err
		}
		f, err := os.OpenFile(stage, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		if err = errors.Join(f.Sync(), f.Close()); err != nil {
			return err
		}
		if err = newDB.Ingest([]string{stage}); err != nil {
			return err
		}
		used = 0
		return nil
	}
	for ok := it.First(); ok; ok = it.Next() {
		if err = ctx.Err(); err != nil {
			break
		}
		prefix, e := packingPrefix(it.Key())
		if e != nil {
			err = e
			break
		}
		shard := prefix[:len(prefix)-8]
		n := uint64(len(it.Key()) + len(it.Value()) + 64)
		if w != nil && (!bytes.Equal(shardPrefix, shard) || used+n > targetBytes) {
			if err = flush(); err != nil {
				break
			}
		}
		if w == nil {
			stage = filepath.Join(next, fmt.Sprintf("packing-%08d.sst", number))
			number++
			f, e := vfs.Default.Create(stage)
			if e != nil {
				err = e
				break
			}
			w = sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.SnappyCompression})
			shardPrefix = bytes.Clone(shard)
		}
		if err = w.Set(it.Key(), it.Value()); err != nil {
			break
		}
		used += n
	}
	err = errors.Join(err, it.Error(), it.Close())
	if err == nil {
		err = flush()
	} else if w != nil {
		_ = w.Close()
		w = nil
	}
	if err != nil {
		return out, err
	}
	out.After, err = inventoryPacking(ctx, newDB, next, targetBytes)
	if err != nil {
		return out, err
	}
	if !samePacking(out.Before, out.After) {
		return out, errors.New("packed target changed logical data")
	}
	// Key-range transfer replaces the old one-file-per-partition contract.
	// Index names actual ingested SSTs; it is fsynced before generation swap.
	if err = writePackingIndex(ctx, newDB, next); err != nil {
		return out, err
	}
	indexBytes, err := os.ReadFile(filepath.Join(next, "PARTITION-INDEX.json"))
	if err != nil {
		return out, err
	}
	indexSum := sha256.Sum256(indexBytes)
	j.IndexDigest = hex.EncodeToString(indexSum[:])
	if err = newDB.Close(); err != nil {
		return out, err
	}
	newDB = nil
	if err = db.Close(); err != nil {
		return out, err
	}
	db = nil
	j.State = "verified"
	if err = saveRangeJSON(filepath.Join(target, "PACKING.json"), j); err != nil {
		return out, err
	}
	if checkpoint != nil {
		if err = checkpoint("packing-verified"); err != nil {
			return out, err
		}
	}
	if err = recoverPackingAt(ctx, target, checkpoint); err != nil {
		return out, err
	}
	out.After.AllocatedBytes, err = allocatedBytes(filepath.Join(target, "parts"))
	out.BitExact = err == nil
	return out, err
}

// A range is the intersection of [Prefix, PrefixEnd(Prefix)) and the indexed
// table's inclusive [Smallest, Largest]. This avoids repeating long metric names
// in every partition entry. Prefix includes placement group and routing vshard.
type packingRange struct {
	Prefix []byte `json:"prefix"`
	Table  int    `json:"table"`
}

type packingTable struct {
	SST      string `json:"sst"`
	Smallest []byte `json:"smallest"`
	Largest  []byte `json:"largest"`
}

type packingIndex struct {
	Version int            `json:"version"`
	Tables  []packingTable `json:"tables"`
	Ranges  []packingRange `json:"ranges"`
}

func writePackingIndex(ctx context.Context, db *pebble.DB, directory string) error {
	levels, err := db.SSTables()
	if err != nil {
		return err
	}
	type span struct {
		name   string
		lo, hi []byte
	}
	var tables []span
	for _, level := range levels {
		for _, table := range level {
			a, e := packingPrefix(table.Smallest.UserKey)
			b, e2 := packingPrefix(table.Largest.UserKey)
			if e != nil || e2 != nil || !bytes.Equal(a[:len(a)-8], b[:len(b)-8]) {
				return errors.New("packed SST crosses PG/vshard boundary")
			}
			tables = append(tables, span{fmt.Sprintf("%06d.sst", table.FileNum), bytes.Clone(table.Smallest.UserKey), bytes.Clone(table.Largest.UserKey)})
		}
	}
	slices.SortFunc(tables, func(a, b span) int { return bytes.Compare(a.lo, b.lo) })
	for i := 1; i < len(tables); i++ {
		if bytes.Compare(tables[i-1].hi, tables[i].lo) >= 0 {
			return errors.New("packed SST ranges overlap")
		}
	}
	it, err := db.NewIter(nil)
	if err != nil {
		return err
	}
	index := packingIndex{Version: 1}
	for _, t := range tables {
		index.Tables = append(index.Tables, packingTable{t.name, t.lo, t.hi})
	}
	table := 0
	for ok := it.First(); ok; ok = it.Next() {
		if err = ctx.Err(); err != nil {
			break
		}
		for table < len(tables) && bytes.Compare(it.Key(), tables[table].hi) > 0 {
			table++
		}
		if table == len(tables) || bytes.Compare(it.Key(), tables[table].lo) < 0 {
			err = errors.New("packed key is outside SST ranges")
			break
		}
		prefix, e := packingPrefix(it.Key())
		if e != nil {
			err = e
			break
		}
		n := len(index.Ranges)
		if n == 0 || index.Ranges[n-1].Table != table || !bytes.Equal(index.Ranges[n-1].Prefix, prefix) {
			index.Ranges = append(index.Ranges, packingRange{Prefix: prefix, Table: table})
		}
	}
	if err = errors.Join(err, it.Error(), it.Close()); err != nil {
		return err
	}
	return saveRangeJSON(filepath.Join(directory, "PARTITION-INDEX.json"), index)
}

func checkPackingIndex(directory, want string) error {
	// Old version-1 packing journals predate the optional range index.
	if want == "" {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(directory, "PARTITION-INDEX.json"))
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != want {
		return errors.New("packing partition index changed")
	}
	return nil
}
