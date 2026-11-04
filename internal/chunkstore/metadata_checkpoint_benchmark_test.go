package chunkstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
)

// Opt in to disk-sized measurements. TMPDIR and CHECKPOINT_BENCH_DIR must point
// at scratch storage; the fixture is removed on completion. Profiles cover only
// prepare/load, not synthetic cache construction or database open/close.
// CHECKPOINT_BENCH_DISK bypasses the filesystem data cache on macOS (Linux
// drops clean pages before load). EXPORT and INPUT split prepare/load into
// separate processes and can cross-check predecessor/rollback binaries. INPUT
// consumes and deletes the exported fixture.
func TestMetadataCheckpointPerformance(t *testing.T) {
	root := os.Getenv("CHECKPOINT_BENCH_DIR")
	if root == "" {
		t.Skip("set CHECKPOINT_BENCH_DIR for checkpoint throughput measurements")
	}
	value := func(key string, fallback int) int {
		if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
			return n
		}
		return fallback
	}
	count := value("CHECKPOINT_BENCH_ENTRIES", 1_000_000)
	size := value("CHECKPOINT_BENCH_SIZE", 1000)
	offset := value("CHECKPOINT_BENCH_OFFSET", 0)
	workers := value("CHECKPOINT_BENCH_WORKERS", 1)
	cores, _ := strconv.ParseFloat(os.Getenv("CHECKPOINT_BENCH_CORES"), 64)
	options := func() PrewarmOptions {
		options := PrewarmOptions{Workers: workers}
		if cores > 0 {
			budget := restartcpu.New(cores)
			options.Workers = budget.CheckpointWorkers(workers)
			options.CheckpointWorker = func() CheckpointPacer {
				return budget.NewCheckpoint(nil)
			}
		}
		return options
	}
	if int64(count)*int64(size+stateSlotOverhead) > 12<<30 {
		t.Fatal("fixture exceeds 12 GiB safety limit")
	}
	dir, err := os.MkdirTemp(root, "checkpoint-bench-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	input := os.Getenv("CHECKPOINT_BENCH_INPUT")
	if input != "" {
		dir = input
		defer os.RemoveAll(dir)
	}
	opts := checkpointOptions()
	fs := &checkpointMeasureFS{FS: vfs.Default}
	opts = opts.WithFS(fs)
	opts.MetadataCacheSize = int64(count)*int64(size+stateSlotOverhead)*12/10 + 64<<20
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	name := []byte("checkpoint.synthetic.000000000000")
	st := newMetricState(Metadata{MetricConfig: MetricConfig{Name: string(name), Retentions: []Retention{{Step: 1, Count: 3600}}, AggregationMethod: Average}, ID: 1, Generation: 1, Revision: 1})
	// A continuation header with changing contents. Keep the raw key encoding:
	// the measured sizes are already the packed record sizes seen by the saver.
	st.keys = map[string][]byte{"header": make([]byte, size-100)}
	st.kenc = encodeStateKeys(st.keys, nil)
	st.keys = nil
	record := appendStateRecord(nil, string(name), st)
	for i := 0; input == "" && i < count; i++ {
		n := i + offset
		for p := len(name) - 1; p >= len(name)-12; p-- {
			name[p] = byte('0' + n%10)
			n /= 10
		}
		copy(record[1:], name)
		binary.LittleEndian.PutUint64(record[1+len(name):], uint64(i+offset+1))
		// Avoid a trivially compressible fixture and identical per-entry values.
		x := uint64(i + offset + 1)
		for p := len(record) - (size - 100); p < len(record); p++ {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			record[p] = byte(x)
		}
		key := unsafeString(name)
		c := db.stateCache
		sh := &c.shards[db.metricStripe(key)]
		slot := c.newSlot(sh)
		chunk, off, dst := c.reserve(&sh.arena, len(record))
		copy(dst[:len(record)], record)
		sh.slots[slot] = stateSlot{hash: uint32(c.hash(key)), chunk: chunk, off: off, n: uint32(len(record))}
		c.index(sh, slot)
		sh.pushFront(slot)
		c.account(sh, int64(len(record)+stateSlotOverhead))
		c.entries.Add(1)
	}
	measure := func(phase string, run func() error) time.Duration {
		t.Helper()
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		var profile *os.File
		if prefix := os.Getenv("CHECKPOINT_BENCH_PROFILE"); prefix != "" {
			profile, err = os.Create(prefix + "-" + phase + ".cpu")
			if err != nil {
				t.Fatal(err)
			}
			if err := pprof.StartCPUProfile(profile); err != nil {
				t.Fatal(err)
			}
		}
		start := time.Now()
		cpu := restartcpu.CPU()
		read, write, sync := fs.read.Load(), fs.write.Load(), fs.sync.Load()
		err := run()
		elapsed := time.Since(start)
		cpu = restartcpu.CPU() - cpu
		if profile != nil {
			pprof.StopCPUProfile()
			profile.Close()
		}
		runtime.ReadMemStats(&after)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("phase=%s workers=%d entries=%d record_bytes=%d seconds=%.6f logical_MBps=%.1f alloc_bytes=%d allocs=%d gc=%d pause_ms=%.2f", phase, workers, count, len(record), elapsed.Seconds(), float64(count*len(record))/elapsed.Seconds()/1e6, after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs, after.NumGC-before.NumGC, float64(after.PauseTotalNs-before.PauseTotalNs)/1e6)
		t.Logf("phase=%s cpu_seconds=%.4f average_cores=%.2f read_seconds=%.4f write_seconds=%.4f sync_seconds=%.4f", phase, cpu, cpu/elapsed.Seconds(), float64(fs.read.Load()-read)/1e9, float64(fs.write.Load()-write)/1e9, float64(fs.sync.Load()-sync)/1e9)
		return elapsed
	}
	if input == "" {
		prepare := measure("prepare", func() error {
			return db.PrepareMetadataCheckpointWithOptions(context.Background(), options())
		})
		info, err := os.Stat(filepath.Join(dir, checkpointFile))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("file_bytes=%d prepare_disk_MBps=%.1f", info.Size(), float64(info.Size())/prepare.Seconds()/1e6)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if export := os.Getenv("CHECKPOINT_BENCH_EXPORT"); export != "" {
			if err := os.Rename(dir, export); err != nil {
				t.Fatal(err)
			}
			return
		}
		db = nil
		runtime.GC()
		db, err = Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.Stat(filepath.Join(dir, checkpointFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.checkpoint.enabled = false; db.Close() }()
	load := measure("load", func() error {
		stats, err := db.LoadMetadataCheckpointAfter(context.Background(), options())
		if err == nil && (!stats.Valid || stats.Entries != int64(count) || stats.Fenced != int64(count)) {
			return fmt.Errorf("unexpected load: %+v", stats)
		}
		return err
	})
	t.Logf("load_disk_MBps=%.1f", float64(info.Size())/load.Seconds()/1e6)
}

type checkpointMeasureFS struct {
	vfs.FS
	read, write, sync atomic.Int64
}

func (fs *checkpointMeasureFS) Open(name string, options ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, options...)
	if err != nil {
		return nil, err
	}
	if err := checkpointBenchmarkDisk(name, f); err != nil {
		f.Close()
		return nil, err
	}
	return &checkpointMeasureFile{File: f, fs: fs}, nil
}

func (fs *checkpointMeasureFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	if err := checkpointBenchmarkDisk(name, f); err != nil {
		f.Close()
		return nil, err
	}
	return &checkpointMeasureFile{File: f, fs: fs}, nil
}

func checkpointBenchmarkDisk(name string, f vfs.File) error {
	base := filepath.Base(name)
	if os.Getenv("CHECKPOINT_BENCH_DISK") != "" && (base == checkpointFile || base == checkpointFile+".tmp") {
		return checkpointBenchmarkNoCache(f)
	}
	return nil
}

type checkpointMeasureFile struct {
	vfs.File
	fs *checkpointMeasureFS
}

func (f *checkpointMeasureFile) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := f.File.Read(p)
	f.fs.read.Add(int64(time.Since(start)))
	return n, err
}

func (f *checkpointMeasureFile) Write(p []byte) (int, error) {
	start := time.Now()
	n, err := f.File.Write(p)
	f.fs.write.Add(int64(time.Since(start)))
	return n, err
}

func (f *checkpointMeasureFile) Sync() error {
	start := time.Now()
	err := f.File.Sync()
	f.fs.sync.Add(int64(time.Since(start)))
	return err
}
