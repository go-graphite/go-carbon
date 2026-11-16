package mixedread

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsread"
)

// The physical-cold fixture uses flushed Pebble SSTs for the parts and keeps
// their immutable SlotIndex resident. It does not claim production codec coverage.
type latencySST struct {
	db    *pebble.DB
	index *partsread.SlotIndex
	fs    *latencySSTFS
}
type latencySSTFS struct {
	vfs.FS
	reads, bytes atomic.Uint64
}
type latencySSTFile struct {
	vfs.File
	fs *latencySSTFS
}

func (f *latencySSTFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.fs.reads.Add(1)
	f.fs.bytes.Add(uint64(n))
	return n, err
}
func (f *latencySSTFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	if filepath.Ext(name) == ".sst" {
		return &latencySSTFile{file, f}, nil
	}
	return file, nil
}
func latencySSTBlocks(m partsread.Metric, records []partsread.Record) ([]partsread.SlotBlock, map[string][]byte) {
	var blocks []partsread.SlotBlock
	values := map[string][]byte{}
	for ai, a := range m.Archives {
		var samples []partsread.Sample
		for _, r := range records {
			if r.Step == a.Step {
				samples = append(samples, r.Sample)
			}
		}
		limit := partsread.ArchiveBlockBytes(a) / 24
		for offset := 0; offset < len(samples); offset += limit {
			key := []byte(fmt.Sprintf("parts/%04d/%08d", ai, offset/limit))
			var data []byte
			for _, p := range samples[offset:min(offset+limit, len(samples))] {
				data = binary.LittleEndian.AppendUint64(data, uint64(p.Time))
				data = binary.LittleEndian.AppendUint64(data, p.Bits)
				data = binary.LittleEndian.AppendUint64(data, p.Sequence)
			}
			values[string(key)] = data
			blocks = append(blocks, partsread.SlotBlock{ID: m.ID, Archive: a, Slots: partsread.SlotRange{From: 0, Until: a.Points}, Key: key})
		}
	}
	return blocks, values
}
func latencyPrepareSST(t *testing.T, path string, m partsread.Metric, records []partsread.Record) {
	t.Helper()
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, values := latencySSTBlocks(m, records)
	for k, v := range values {
		if err := db.Set([]byte(k), v, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
func latencyOpenSST(t *testing.T, path string, m partsread.Metric, records []partsread.Record) *latencySST {
	t.Helper()
	fs := &latencySSTFS{FS: vfs.Default}
	db, err := pebble.Open(path, &pebble.Options{ReadOnly: true, FS: fs})
	if err != nil {
		t.Fatal(err)
	}
	blocks, _ := latencySSTBlocks(m, records)
	s := &latencySST{db: db, fs: fs}
	s.index, err = partsread.NewSlotIndex(blocks, func(ctx context.Context, key []byte, _ []partsread.SlotRange, yield func(partsread.Sample) error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		value, closer, err := db.Get(key)
		if err != nil {
			return err
		}
		defer closer.Close()
		if len(value)%24 != 0 {
			return fmt.Errorf("invalid fixture SST block")
		}
		for len(value) > 0 {
			p := partsread.Sample{Time: int64(binary.LittleEndian.Uint64(value)), Bits: binary.LittleEndian.Uint64(value[8:]), Sequence: binary.LittleEndian.Uint64(value[16:])}
			if err := yield(p); err != nil {
				return err
			}
			value = value[24:]
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func (s *latencySST) counts() (uint64, uint64) { return s.fs.reads.Load(), s.fs.bytes.Load() }
func (s *latencySST) close(t *testing.T) {
	t.Helper()
	if err := s.db.Close(); err != nil {
		t.Error(err)
	}
}
func legacyPathForProbe(paths []string, comparison string) string {
	if comparison == "native" {
		return paths[2]
	}
	return paths[0]
}

// These diagnostics run separately from latency samples. VFS table reads count
// actual read requests below Pebble's block cache, not logical codec bytes.
// Linux read_bytes counts device IO; it can be zero with a warm OS page cache.
type latencyProcessIO struct {
	Requests, Bytes, DeviceBytes uint64
	probeBytes                   uint64
}

func latencyReadProcessIO(t *testing.T) latencyProcessIO {
	t.Helper()
	f, err := os.Open("/proc/self/io")
	if err != nil {
		t.Skip("Linux process IO accounting required: ", err)
	}
	defer f.Close()
	var buf [4096]byte
	n, err := f.Read(buf[:]) // One read, so the next delta can exclude this probe.
	if err != nil {
		t.Fatal(err)
	}
	var out latencyProcessIO
	out.probeBytes = uint64(n)
	for _, line := range strings.Split(string(buf[:n]), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		switch fields[0] {
		case "syscr:":
			out.Requests = value
		case "rchar:":
			out.Bytes = value
		case "read_bytes:":
			out.DeviceBytes = value
		}
	}
	return out
}

func latencyColdIO(t *testing.T, class string, sample int, native, legacy *chunkstore.Store, evict func(string), parts func() (uint64, uint64), fetchNative, fetchMixed func() error, nativeFirst bool) {
	t.Helper()
	measure := func(arm string, store *chunkstore.Store, fetch func() error) {
		if evict != nil {
			evict(arm)
		}
		var partReads, partBytes uint64
		if parts != nil {
			partReads, partBytes = parts()
		}
		before := store.Stats().FS
		procBefore := latencyReadProcessIO(t)
		if err := fetch(); err != nil {
			t.Fatal(err)
		}
		procAfter := latencyReadProcessIO(t)
		after := store.Stats().FS
		row := struct {
			Class, Arm              string
			Sample                  int
			TableReads, TableBytes  uint64
			ReadRequests, ReadBytes uint64
			DeviceBytes             uint64
		}{class, arm, sample,
			after.Ops[chunkstore.FSRead][chunkstore.FSTable] - before.Ops[chunkstore.FSRead][chunkstore.FSTable],
			after.Bytes[chunkstore.FSRead][chunkstore.FSTable] - before.Bytes[chunkstore.FSRead][chunkstore.FSTable],
			procAfter.Requests - procBefore.Requests - 1,
			procAfter.Bytes - procBefore.Bytes - procBefore.probeBytes,
			procAfter.DeviceBytes - procBefore.DeviceBytes}
		if arm == "mixed" && parts != nil {
			reads, bytes := parts()
			row.TableReads += reads - partReads
			row.TableBytes += bytes - partBytes
		}
		data, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("COLD_IO %s\n", data)
	}
	if nativeFirst {
		measure("native", native, fetchNative)
		measure("mixed", legacy, fetchMixed)
	} else {
		measure("mixed", legacy, fetchMixed)
		measure("native", native, fetchNative)
	}
}
