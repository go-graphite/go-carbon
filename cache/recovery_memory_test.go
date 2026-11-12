package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

func writeMemoryCheckpoint(t *testing.T, dir, root string, names int) {
	t.Helper()
	data := make([]points.Point, 11)
	for i := range data {
		data[i] = points.Point{Timestamp: 1700000000 + int64(i), Value: float64(i)}
	}
	writeMemoryCheckpointData(t, dir, root, names, data)
}

func writeMemoryCheckpointData(t *testing.T, dir, root string, names int, data []points.Point) {
	t.Helper()
	builder := recovery.NewBuilder(func(string) bool { return true })
	writer, err := recovery.NewWriterFormat(filepath.Join(dir, recovery.CacheName(recovery.FormatBlocks, "1.2.bin")), 0, 1<<20, builder, recovery.FormatBlocks)
	if err != nil {
		t.Fatal(err)
	}
	prefix := strings.Repeat("long_prefix.", 30)
	if err := writer.WriteSegments(1024, 4, func(part int, segment *recovery.Segment) error {
		for i := part; i < names; i += 1024 {
			name := fmt.Sprintf("synthetic.restore.%s%012d", prefix, i)
			if err := segment.WritePoints(&points.Points{Metric: name, Data: data}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	saved, err := writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	index, err := recovery.WriteBlocksIndex(dir, builder)
	if err != nil {
		t.Fatal(err)
	}
	input, err := recovery.NewWriterFormat(filepath.Join(dir, recovery.InputName(recovery.FormatCompact, "1.2.bin")), 1, 65536, builder, recovery.FormatCompact)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := input.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.Publish(dir, root, saved, empty, index, "memory-test"); err != nil {
		t.Fatal(err)
	}
}
func recoveryRSS() uint64 {
	raw, _ := os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			rss, _ := strconv.ParseUint(strings.Fields(line)[1], 10, 64)
			return rss * 1024
		}
	}
	return 0
}

// Run the production GCPOINT3 open/attach/claim path, including real mmap'd
// files. GO_CARBON_RESTORE_MEMORY_NAMES=2000000 runs the same assertion with
// two million names / 22 million points; the default stays inexpensive in CI.
func TestPendingRestoreMemoryPerPoint(t *testing.T) {
	names := 20000
	if value := os.Getenv("GO_CARBON_RESTORE_MEMORY_NAMES"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < names {
			t.Fatal("GO_CARBON_RESTORE_MEMORY_NAMES must be >= 20000")
		}
		names = n
	}
	dir, root := t.TempDir(), t.TempDir()
	writeMemoryCheckpoint(t, dir, root, names)
	// Drop generation buffers, including the previous cycle of sync.Pools.
	runtime.GC()
	debug.FreeOSMemory()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	rssBefore := recoveryRSS()
	bundle, err := recovery.OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	c := New()
	// Without a memory sampler, restore admits only half the cache.
	c.SetMaxSize(uint64(names*44 + 1))
	if err := c.AttachPendingRecoveryWithPreparedNames(bundle); err != nil {
		t.Fatal(err)
	}
	if err := c.RecoverPending(make(chan struct{}), 0); err != nil {
		t.Fatal(err)
	}
	if c.Size() != int64(names*11) || c.Len() != int32(names) {
		t.Fatalf("incomplete restore: names=%d points=%d", c.Len(), c.Size())
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	rssAfter := recoveryRSS()
	points := uint64(names) * 11
	const allowance = 64 << 20
	if after.HeapAlloc > before.HeapAlloc+128*points+allowance {
		t.Errorf("heap growth %d exceeds 128 bytes/point plus 64 MiB", after.HeapAlloc-before.HeapAlloc)
	}
	if rssBefore > 0 && rssAfter > rssBefore+192*points+allowance {
		t.Errorf("RSS growth %d exceeds 192 bytes/point plus 64 MiB", rssAfter-rssBefore)
	}
	t.Logf("names=%d points=%d heap_delta=%d RSS_delta=%d heap_bound=128B/point RSS_bound=192B/point allowance=64MiB", names, points, int64(after.HeapAlloc)-int64(before.HeapAlloc), int64(rssAfter)-int64(rssBefore))
	runtime.KeepAlive(c)
}
