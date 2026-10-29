package cache

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/xxh3"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// A dump sample holds records of a store's cache in the record format of
// recovery.FormatCompact (varint name length, name, varint count, uvarint
// length, encoded points), zstd compressed: a compact cache dump compressed
// with zstd is one. DUMP_NAMES_SAMPLE names the file. Its records are
// regrouped by cache shard, as dump segments group them, so a sample of
// whole cache shards measures each segment as production writes it.
type sampleRecord struct {
	name string
	data []byte
	n    int
}

func loadDumpSample(t *testing.T, path string) [][]sampleRecord {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := zstd.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	br := bufio.NewReaderSize(zr, 1<<20)
	byShard := map[uint64][]sampleRecord{}
	for {
		l, err := binary.ReadVarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		name := make([]byte, l)
		if _, err = io.ReadFull(br, name); err != nil {
			t.Fatal(err)
		}
		n, _ := binary.ReadVarint(br)
		size, _ := binary.ReadUvarint(br)
		data := make([]byte, size)
		if _, err = io.ReadFull(br, data); err != nil {
			t.Fatal(err)
		}
		s := xxh3.Hash(name) & (shardCount - 1)
		byShard[s] = append(byShard[s], sampleRecord{string(name), data, int(n)})
	}
	keys := slices.Sorted(func(yield func(uint64) bool) {
		for k := range byShard {
			if !yield(k) {
				return
			}
		}
	})
	shards := make([][]sampleRecord, 0, len(keys))
	for _, k := range keys {
		shards = append(shards, byShard[k])
	}
	return shards
}

// copyName makes copy c of a sample name distinct, keeping the structure of
// its shard: every name of a copy gets the same first component.
func copyName(name string, c int) string {
	if c == 0 {
		return name
	}
	return "c" + strconv.Itoa(c) + name
}

// TestMeasureDumpNameOptions measures name column encodings per dump
// segment on a sample: bytes per name and encode CPU per name.
func TestMeasureDumpNameOptions(t *testing.T) {
	path := os.Getenv("DUMP_NAMES_SAMPLE")
	if path == "" {
		t.Skip("set DUMP_NAMES_SAMPLE to a dump sample")
	}
	shards := loadDumpSample(t, path)
	var names, npoints, nameBytes, pointBytes, headerBytes int
	for _, s := range shards {
		for _, r := range s {
			names++
			npoints += r.n
			nameBytes += len(r.name)
			pointBytes += len(r.data)
			headerBytes += varintLen(int64(len(r.name))) + varintLen(int64(r.n)) + uvarintLen(uint64(len(r.data)))
		}
	}
	total := nameBytes + pointBytes + headerBytes
	t.Logf("sample: %d shards, %d metrics, %d points (%.1f/metric); legacy bytes %d: names %.1f%% (%.1f B/name), headers %.1f%%, points %.1f%% (%.2f B/pt)",
		len(shards), names, npoints, float64(npoints)/float64(names), total, 100*float64(nameBytes)/float64(total), float64(nameBytes)/float64(names),
		100*float64(headerBytes)/float64(total), 100*float64(pointBytes)/float64(total), float64(pointBytes)/float64(npoints))
	fast, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(false), zstd.WithLowerEncoderMem(true))
	def, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(false), zstd.WithLowerEncoderMem(true))
	dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	type option struct {
		name   string
		encode func(refs []sampleRecord, scratch *[]byte, z *[]byte) int // bytes for names
	}
	column := func(refs []sampleRecord, dst []byte) []byte {
		for _, r := range refs {
			dst = binary.AppendUvarint(dst, uint64(len(r.name)))
			dst = append(dst, r.name...)
		}
		return dst
	}
	sorted := func(refs []sampleRecord) {
		slices.SortFunc(refs, func(a, b sampleRecord) int { return strings.Compare(a.name, b.name) })
	}
	front := func(refs []sampleRecord, dst []byte, restart int) []byte {
		prev := ""
		for i, r := range refs {
			l := 0
			if restart == 0 || i%restart != 0 {
				n := min(len(prev), len(r.name))
				for l < n && prev[l] == r.name[l] {
					l++
				}
			}
			dst = binary.AppendUvarint(dst, uint64(l))
			dst = binary.AppendUvarint(dst, uint64(len(r.name)-l))
			dst = append(dst, r.name[l:]...)
			prev = r.name
		}
		return dst
	}
	options := []option{
		{"legacy raw names", func(refs []sampleRecord, s, z *[]byte) int { *s = column(refs, (*s)[:0]); return len(*s) }},
		{"(a) zstd-1 unsorted", func(refs []sampleRecord, s, z *[]byte) int {
			*s = column(refs, (*s)[:0])
			*z = fast.EncodeAll(*s, (*z)[:0])
			return len(*z)
		}},
		{"(a) zstd-3 unsorted", func(refs []sampleRecord, s, z *[]byte) int {
			*s = column(refs, (*s)[:0])
			*z = def.EncodeAll(*s, (*z)[:0])
			return len(*z)
		}},
		{"(a) sort+zstd-1", func(refs []sampleRecord, s, z *[]byte) int {
			sorted(refs)
			*s = column(refs, (*s)[:0])
			*z = fast.EncodeAll(*s, (*z)[:0])
			return len(*z)
		}},
		{"(b) sort+front", func(refs []sampleRecord, s, z *[]byte) int {
			sorted(refs)
			*s = front(refs, (*s)[:0], 0)
			return len(*s)
		}},
		{"(b) sort+front/16", func(refs []sampleRecord, s, z *[]byte) int {
			sorted(refs)
			*s = front(refs, (*s)[:0], 16)
			return len(*s)
		}},
		{"(a+b) sort+front+zstd-1", func(refs []sampleRecord, s, z *[]byte) int {
			sorted(refs)
			*s = front(refs, (*s)[:0], 0)
			*z = fast.EncodeAll(*s, (*z)[:0])
			return len(*z)
		}},
		{"(a+b) sort+front+zstd-3", func(refs []sampleRecord, s, z *[]byte) int {
			sorted(refs)
			*s = front(refs, (*s)[:0], 0)
			*z = def.EncodeAll(*s, (*z)[:0])
			return len(*z)
		}},
	}
	for _, o := range options {
		var bytes int
		var cpu time.Duration
		var decode time.Duration
		var s, z, d []byte
		for _, shard := range shards {
			refs := slices.Clone(shard)
			// Shuffle as the cache's map order would.
			slices.SortFunc(refs, func(a, b sampleRecord) int {
				return cmp.Compare(xxh3.HashString(a.name)*0x9e3779b97f4a7c15, xxh3.HashString(b.name)*0x9e3779b97f4a7c15)
			})
			runtime.GC()
			start := time.Now()
			bytes += o.encode(refs, &s, &z)
			cpu += time.Since(start)
			if strings.Contains(o.name, "zstd") {
				start = time.Now()
				d, _ = dec.DecodeAll(z, d[:0])
				decode += time.Since(start)
			}
		}
		t.Logf("%-26s %7.1f B/name  encode %6.0f ns/name  decode %5.0f ns/name", o.name, float64(bytes)/float64(names),
			float64(cpu)/float64(names), float64(decode)/float64(names))
	}
	t.Logf("(c) ids                    %7.1f B/name (uint32 id), %.1f B/name (sorted id deltas, est.)", 4.0, 2.5)
}

func varintLen(v int64) int   { return uvarintLen(uint64(v<<1) ^ uint64(v>>63)) }
func uvarintLen(v uint64) int { var b [10]byte; return binary.PutUvarint(b[:], v) }

// TestMeasureDumpNamesRestart runs a stop's dump and checkpoint index, the
// successor's checkpoint open, lookups, RecoverPending and an ordered restore
// on copies of a dump sample, in the compact and blocks formats.
// DUMP_NAMES_COPIES copies of the sample's shards make the dump's segments,
// each copy's names under another first component; DUMP_NAMES_WORKERS sets
// the dump workers. DUMP_NAMES_FORMATS=blocks skips the compact format,
// DUMP_NAMES_INDEX=v2 indexes a blocks dump with GCPOINT2, and
// DUMP_NAMES_OPEN_ONLY stops after the checkpoint open. DUMP_NAMES_DUMP_ONLY
// stops after each dump and DUMP_NAMES_PROFILE names the prefix of a CPU
// profile per format, captured only while writing and syncing the dump.
func TestMeasureDumpNamesRestart(t *testing.T) {
	path := os.Getenv("DUMP_NAMES_SAMPLE")
	if path == "" {
		t.Skip("set DUMP_NAMES_SAMPLE to a dump sample")
	}
	shards := loadDumpSample(t, path)
	copies, _ := strconv.Atoi(os.Getenv("DUMP_NAMES_COPIES"))
	copies = max(copies, 1)
	workers, _ := strconv.Atoi(os.Getenv("DUMP_NAMES_WORKERS"))
	if workers == 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	segments := copies * len(shards)
	var metrics, pts int
	for _, s := range shards {
		for _, r := range s {
			metrics++
			pts += r.n
		}
	}
	metrics *= copies
	pts *= copies
	names := make([][]string, segments)
	for seg := range segments {
		for _, r := range shards[seg%len(shards)] {
			names[seg] = append(names[seg], copyName(r.name, seg/len(shards)))
		}
	}
	t.Logf("%d segments, %d metrics, %d points, %d workers", segments, metrics, pts, workers)
	formats := []recovery.Format{recovery.FormatCompact, recovery.FormatBlocks}
	if v := os.Getenv("DUMP_NAMES_FORMATS"); v == "blocks" {
		formats = formats[1:]
	}
	for _, f := range formats {
		dir, root := t.TempDir(), t.TempDir()
		t.Logf("%s:", f)
		builder := recovery.NewConcurrentBuilder(nil, workers)
		builder.Reserve(metrics)
		// Warm names as the stop does with input active.
		var wg sync.WaitGroup
		for w := range workers {
			wg.Go(func() {
				for seg := w; seg < segments; seg += workers {
					for _, name := range names[seg] {
						_, _ = builder.ID(name)
					}
				}
			})
		}
		wg.Wait()
		builder.Prepare()
		var cacheFile, walFile recovery.File
		var encodeCPU atomic.Int64
		phase(t, "dump+sync", func() {
			if prefix := os.Getenv("DUMP_NAMES_PROFILE"); prefix != "" {
				file, err := os.Create(prefix + "-" + f.String() + ".cpu")
				if err != nil {
					t.Fatal(err)
				}
				if err = pprof.StartCPUProfile(file); err != nil {
					t.Fatal(err)
				}
				defer func() { pprof.StopCPUProfile(); _ = file.Close() }()
			}
			dump, err := recovery.NewWriterFormat(filepath.Join(dir, recovery.CacheName(f, "1.2.bin")), 0, 1<<20, builder, f)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			err = dump.WriteSegments(segments, workers, func(seg int, out *recovery.Segment) error {
				for i, r := range shards[seg%len(shards)] {
					// Captured sample bytes are immutable for the whole run,
					// as queued cache bytes are during DumpStop.
					if err := out.WriteCompactBorrowed(names[seg][i], r.data, r.n); err != nil {
						return err
					}
				}
				return nil
			})
			encodeCPU.Store(int64(time.Since(start)))
			if err == nil {
				cacheFile, err = dump.Flush()
			}
			if err == nil {
				err = dump.Sync()
			}
			if err != nil {
				t.Fatal(err)
			}
		})
		t.Logf("  dump %d bytes (%.1f B/metric), encode+append wall %v", cacheFile.Size, float64(cacheFile.Size)/float64(metrics), time.Duration(encodeCPU.Load()).Round(time.Millisecond))
		if os.Getenv("DUMP_NAMES_DUMP_ONLY") != "" {
			continue
		}
		wal, err := recovery.NewWriterFormat(filepath.Join(dir, recovery.InputName(recovery.FormatCompact, "1.2.bin")), 1, recovery.InputFileBuffer, builder, recovery.FormatCompact)
		if err != nil {
			t.Fatal(err)
		}
		var index recovery.File
		writeIndex := recovery.WriteCompactIndex
		if f == recovery.FormatBlocks && os.Getenv("DUMP_NAMES_INDEX") != "v2" {
			writeIndex = recovery.WriteBlocksIndex
		}
		phase(t, "index write", func() {
			if index, err = writeIndex(dir, builder); err != nil {
				t.Fatal(err)
			}
		})
		if walFile, err = wal.Close(); err != nil {
			t.Fatal(err)
		}
		if err = recovery.Publish(dir, root, cacheFile, walFile, index, "id"); err != nil {
			t.Fatal(err)
		}
		t.Logf("  index %d bytes (%.1f B/metric)", index.Size, float64(index.Size)/float64(metrics))
		builder = nil
		var bundle *recovery.Bundle
		phase(t, "checkpoint open", func() {
			if bundle, err = recovery.OpenBundle(dir, root); err != nil {
				t.Fatal(err)
			}
		})
		if os.Getenv("DUMP_NAMES_OPEN_ONLY") != "" {
			_ = bundle.Close()
			continue
		}
		phase(t, "find+read (parallel)", func() {
			var wg sync.WaitGroup
			for w := range workers {
				wg.Go(func() {
					for seg := w; seg < segments; seg += workers {
						for _, name := range names[seg] {
							slot, ok, err := bundle.Find(name)
							if err == nil && ok {
								_, err = bundle.Read(slot)
							}
							if err != nil || !ok {
								t.Error("read", name, ok, err)
								return
							}
						}
					}
				})
			}
			wg.Wait()
		})
		phase(t, "name+read (serial)", func() {
			var tail []points.Point
			for slot := range bundle.Slots() {
				name, found, err := bundle.Name(slot)
				if err == nil && found {
					tail, err = bundle.ReadCompactAs(slot, name, tail[:0], func(recovery.CompactRecord) {})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
		})
		var slotNames []string
		var slotIDs []uint64
		for slot := range bundle.Slots() {
			if name, found, _ := bundle.Name(slot); found {
				slotNames, slotIDs = append(slotNames, name), append(slotIDs, slot)
			}
		}
		phase(t, "read-as only (serial)", func() {
			var tail []points.Point
			for i, slot := range slotIDs {
				var err error
				if tail, err = bundle.ReadCompactAs(slot, slotNames[i], tail[:0], func(recovery.CompactRecord) {}); err != nil {
					t.Fatal(err)
				}
			}
		})
		phase(t, "find only (serial)", func() {
			for _, name := range slotNames {
				if _, ok, err := bundle.Find(name); !ok || err != nil {
					t.Fatal(name, err)
				}
			}
		})
		slotNames, slotIDs = nil, nil
		phase(t, "name only (serial)", func() {
			for slot := range bundle.Slots() {
				if _, _, err := bundle.Name(slot); err != nil {
					t.Fatal(err)
				}
			}
		})
		next := New()
		next.SetMaxSize(0)
		if err := next.AttachPendingRecovery(bundle); err != nil {
			t.Fatal(err)
		}
		phase(t, "RecoverPending", func() {
			if err := next.RecoverPending(make(chan struct{}), 0); err != nil {
				t.Fatal(err)
			}
		})
		if next.Size() != int64(pts) {
			t.Fatal("claimed points", next.Size(), pts)
		}
		next = nil
		ordered := New()
		ordered.SetMaxSize(0)
		phase(t, "ordered restore", func() {
			p := filepath.Join(dir, cacheFile.Name)
			if f == recovery.FormatBlocks {
				err = recovery.ReadBlocksFile(p, ordered.AddRestored)
			} else {
				err = recovery.ReadCompactFile(p, ordered.AddRestored)
			}
			if err != nil {
				t.Fatal(err)
			}
		})
		if ordered.Size() != int64(pts) {
			t.Fatal("restored points", ordered.Size(), pts)
		}
		ordered = nil
		_ = bundle.Close()
	}
}
