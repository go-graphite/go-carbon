package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/helper/malloc"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
)

type pausedIndexFS struct {
	vfs.FS
	offset       atomic.Int64
	active, peak atomic.Int64
	release      chan struct{}
}
type pausedIndexFile struct {
	vfs.File
	fs *pausedIndexFS
}

func (f *pausedIndexFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return &pausedIndexFile{file, f}, nil
}
func (f *pausedIndexFile) ReadAt(p []byte, off int64) (int, error) {
	n, e := f.File.ReadAt(p, off)
	// The top-level index has already been decoded and is pinned by the
	// iterator while its first partition is loaded. Pause real reads here.
	if off == f.fs.offset.Load() {
		active := f.fs.active.Add(1)
		for old := f.fs.peak.Load(); active > old; old = f.fs.peak.Load() {
			if f.fs.peak.CompareAndSwap(old, active) {
				break
			}
		}
		<-f.fs.release
		f.fs.active.Add(-1)
	}
	return n, e
}
func nativeReadInUse(t *testing.T) uint64 {
	t.Helper()
	if !malloc.Supported() {
		return 0
	}
	tr := malloc.Start(time.Millisecond, true)
	defer tr.Stop()
	c := malloc.NewCollector()
	c.SetTrimmer(tr)
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ms, e := reg.Gather()
		if e != nil {
			t.Fatal(e)
		}
		var trims float64
		for _, m := range ms {
			if m.GetName() == "malloc_inuse_bytes" {
				return uint64(m.GetMetric()[0].GetGauge().GetValue())
			}
			if m.GetName() == "malloc_trims_total" {
				trims = m.GetMetric()[0].GetCounter().GetValue()
			}
		}
		// Linux cgo also covers musl and older glibc, which lack mallinfo2.
		// Two trim iterations guarantee the first stats read has completed.
		// Keep the RSS and admission assertions on those platforms.
		if trims >= 2 {
			t.Log("mallinfo2 unavailable; checking RSS and reader admission")
			return 0
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("malloc stats did not arrive")
	return 0
}
func nativeReadRSS() uint64 {
	data, _ := os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			v, _ := strconv.ParseUint(strings.Fields(line)[1], 10, 64)
			return v * 1024
		}
	}
	return 0
}

// Exercise the native allocator, not just Go HeapAlloc: concurrent cold point
// lookups pin private copies of a valid Snappy top-level index that is too big
// for a cache shard. More request fan-out must not multiply those copies.
func TestColdReadsBoundNativeIndexMemory(t *testing.T) {
	const names = 16000
	fs := &pausedIndexFS{FS: vfs.NewMem(), release: make(chan struct{})}
	fs.offset.Store(-1)
	f, err := fs.Create("/index.sst")
	if err != nil {
		t.Fatal(err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		Comparer: storeComparer, MergerName: exactChunkMerger.Name, Compression: sstable.SnappyCompression, BlockSize: 128, IndexBlockSize: 128,
	})
	// Long shared prefixes survive separator shortening at most boundaries.
	prefix := strings.Repeat("long_metric_component.", 12)
	for i := range names {
		key := []byte(fmt.Sprintf("m/%s%08d", prefix, i))
		if err := w.Set(key, []byte("unused")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := fs.Open("/index.sst")
	if err != nil {
		t.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(file)
	if err != nil {
		t.Fatal(err)
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{Comparer: storeComparer, MergerName: exactChunkMerger.Name})
	if err != nil {
		t.Fatal(err)
	}
	layout, err := r.Layout()
	if err != nil {
		t.Fatal(err)
	}
	if len(layout.Index) < 2 || layout.TopIndex.Length == 0 {
		t.Fatal("fixture needs two-level index")
	}
	compressed := make([]byte, layout.TopIndex.Length)
	if _, err := file.ReadAt(compressed, int64(layout.TopIndex.Offset)); err != nil {
		t.Fatal(err)
	}
	decoded, err := snappy.DecodedLen(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	const cacheBytes = 1 << 20
	if decoded <= cacheBytes/4 {
		t.Fatalf("index %d must exceed cache shard capacity", decoded)
	}
	s, now := openTestStore(t, "/store", Options{fs: fs, ExactBlocks: true, CacheSize: cacheBytes, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	if err := s.db.Ingest([]string{"/index.sst"}); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	beforeMalloc, beforeRSS := nativeReadInUse(t), nativeReadRSS()
	fs.offset.Store(int64(layout.Index[1].Offset))
	var wg sync.WaitGroup
	resume := sync.OnceFunc(func() { close(fs.release) })
	defer func() { resume(); wg.Wait() }()
	const requests = 256
	for range requests {
		wg.Go(func() {
			_, err := s.Fetch(context.Background(), prefix+"00000000.absent", now-60, now)
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("cold lookup: %v", err)
			}
		})
	}
	// Wait until admission is saturated and block loads have reached the FS.
	deadline := time.Now().Add(10 * time.Second)
	for (s.readSlots.Stats().Outstanding < maxConcurrentStoreReads || fs.active.Load() < maxConcurrentBlockLoads) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Give any incorrectly admitted readers a chance to arrive as well.
	time.Sleep(50 * time.Millisecond)
	duringMalloc, duringRSS := nativeReadInUse(t), nativeReadRSS()
	outstanding := s.readSlots.Stats().Outstanding
	peak := fs.peak.Load()
	nativeBound := uint64(maxConcurrentStoreReads) * (uint64(decoded) + layout.TopIndex.Length + 1<<20)
	t.Logf("names=%d requests=%d decoded_index=%d native_delta=%d RSS_delta=%d native_bound=%d admitted=%d loaders=%d", names, requests, decoded, int64(duringMalloc)-int64(beforeMalloc), int64(duringRSS)-int64(beforeRSS), nativeBound, outstanding, peak)
	if outstanding != maxConcurrentStoreReads || peak > maxConcurrentBlockLoads || peak == 0 {
		t.Fatalf("unbounded reads: admitted=%d partition loads=%d", outstanding, peak)
	}
	// Cancellation must release a queued request without opening another view.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, cancelErr := s.Metadata(ctx, prefix+"00000000.absent")
	cancel()
	resume()
	wg.Wait()
	if !errors.Is(cancelErr, context.DeadlineExceeded) {
		t.Errorf("queued metadata cancellation: %v", cancelErr)
	}
	if duringMalloc > beforeMalloc+nativeBound {
		t.Errorf("native index allocation %d exceeds %d", duringMalloc-beforeMalloc, nativeBound)
	}
	// Includes queued Go stacks and runtime growth. This catches native RSS that
	// a HeapAlloc-only regression test would miss; the exact allocation bound
	// above is independent of page residency and allocator fragmentation.
	if beforeRSS > 0 && duringRSS > beforeRSS+nativeBound+64<<20 {
		t.Errorf("RSS growth %d exceeds native bound plus 64 MiB Go allowance", duringRSS-beforeRSS)
	}
}
