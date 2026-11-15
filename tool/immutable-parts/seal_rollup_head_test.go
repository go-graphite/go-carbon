package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"unsafe"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func TestCompactRollupHeadExact(t *testing.T) {
	rng := rand.New(rand.NewSource(34))
	var h compactRollupHead
	var want []coreBatch
	for i := range 30 {
		b := coreBatch{ID: uint64(i % 3), Order: uint64(i + 1), Clock: int64(rng.Uint64()), Policy: []byte(fmt.Sprintf("policy%d", i%2))}
		for range 118 {
			b.Points = append(b.Points, row{ID: b.ID, Time: int64(rng.Uint64()), Bits: rng.Uint64(), Seq: rng.Uint64()})
		}
		if i == 0 {
			for j, bits := range []uint64{0, 1 << 63, 1, 0x7ff80000000000ab, 0x7ff0000000000000, 0xfff0000000000000} {
				b.Points[j].Bits = bits
			}
		}
		if err := h.append(b); err != nil {
			t.Fatal(err)
		}
		want = append(want, b)
	}
	got, err := h.decode()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("lossless compact roundtrip", err)
	}
	before := append([]byte(nil), h.data...)
	if err = h.append(coreBatch{ID: 1, Points: []row{{ID: 2}}}); err == nil || !reflect.DeepEqual(before, h.data) {
		t.Fatal("invalid append changed head")
	}
	h.reset()
	if h.points != 0 || len(h.data) != 0 || len(h.batches) != 0 {
		t.Fatal("reset")
	}
}
func TestCompactRollupHeadMemory(t *testing.T) {
	s := os.Getenv("SEAL_COMPACT_MEMORY_POINTS")
	if s == "" {
		t.Skip("opt-in resident head measurement")
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		t.Fatal("invalid point count")
	}
	density := rollupMemoryDensity(t)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	h := compactRollupHead{batches: make([]compactRollupBatch, 0, (n+density-1)/density)}
	rows := make([]row, density)
	for added := 0; added < n; {
		count := min(density, n-added)
		id := uint64(added / density)
		for j := range rows[:count] {
			rows[j] = row{ID: id, Time: 1794700800 + int64(j), Bits: math.Float64bits(float64(j % 97)), Seq: uint64(j + 1)}
		}
		if err = h.append(coreBatch{ID: id, Order: 1, Clock: 1794701000, Policy: []byte("shared-policy"), Points: rows[:count]}); err != nil {
			t.Fatal(err)
		}
		added += count
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	fmt.Printf("COMPACT_HEAD points=%d encoded=%d resident=%d live_heap_delta=%d total_heap=%d head_B_pt=%.6f live_B_pt=%.6f raw_row_B_pt=%d\n", n, len(h.data), h.residentBytes(), after.HeapAlloc-before.HeapAlloc, after.HeapAlloc, float64(h.residentBytes())/float64(n), float64(after.HeapAlloc-before.HeapAlloc)/float64(n), unsafe.Sizeof(row{}))
	if float64(after.HeapAlloc-before.HeapAlloc)/float64(n) > 16 {
		t.Fatal("head exceeds16B/point")
	}
	runtime.KeepAlive(h)
}

// The incumbent cache and the v2 fixture receive the same point count/density.
// This measures actual cache residency, distinct from whole-daemon RSS/heap.
func TestClassicRollupHeadMemory(t *testing.T) {
	s := os.Getenv("SEAL_CLASSIC_MEMORY_POINTS")
	if s == "" {
		t.Skip("opt-in matched incumbent cache measurement")
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		t.Fatal("invalid point count")
	}
	density := rollupMemoryDensity(t)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	c := cache.New()
	c.SetMaxSize(uint64(n + 100000))
	rows := make([]points.Point, density)
	for added := 0; added < n; {
		count := min(density, n-added)
		for j := range rows[:count] {
			rows[j] = points.Point{Value: float64(j % 97), Timestamp: 1794700800 + int64(j)}
		}
		c.AddPoints([]byte(fmt.Sprintf("fixture.metric.%d", added/density)), rows[:count])
		added += count
	}
	if c.Size() != int64(n) {
		t.Fatal("classic cache count", c.Size(), n)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	fmt.Printf("CLASSIC_HEAD points=%d estimated=%d live_heap_delta=%d total_heap=%d estimated_B_pt=%.6f live_B_pt=%.6f\n", n, c.EstimatedBytes(), after.HeapAlloc-before.HeapAlloc, after.HeapAlloc, float64(c.EstimatedBytes())/float64(n), float64(after.HeapAlloc-before.HeapAlloc)/float64(n))
	runtime.KeepAlive(c)
}

func rollupMemoryDensity(t *testing.T) int {
	t.Helper()
	s := os.Getenv("SEAL_MEMORY_DENSITY")
	if s == "" {
		return 118
	}
	n, err := strconv.Atoi(s)
	if err != nil || (n != 24 && n != 118 && n != 513) {
		t.Fatal("invalid memory density")
	}
	return n
}
