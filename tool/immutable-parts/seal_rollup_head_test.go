package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"unsafe"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/partsrun"
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
	for i, m := range h.batches {
		b := want[i]
		encoded, e := partsrun.Append(nil, b.Clock, len(b.Points), func(j int) partsrun.Sample {
			p := b.Points[j]
			return partsrun.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}
		})
		if e != nil || !reflect.DeepEqual(encoded, compactRunValue(&h, m)) {
			t.Fatal("shared persisted codec differs from admission codec", i, e)
		}
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

// Compression preserves complete original boundaries and every physical slot
// across corrections, fine-ring wrap, replay and a cold owner reconstruction.
func TestCompactEngineDifferentialReopen(t *testing.T) {
	cfg := coreConfig{Shards: 1, Producers: 1, MaxRows: 100000, PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "compact-exact"}
	root := t.TempDir()
	e, states, err := openCoreRollups(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	reference, classic, err := openCoreRollups(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reference.close(); err != nil {
			t.Error(err)
		}
	}()
	for seal := 1; seal <= 3; seal++ {
		var h compactRollupHead
		var all []coreBatch
		id := uint64(0)
		for _, density := range []int{118, 513} {
			for _, wrap := range []bool{false, true} {
				for method := sealrollup.Average; method <= sealrollup.First; method++ {
					for _, xff := range []float32{0, .5, 1} {
						id++
						c := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 600}, {Step: 60, Count: 120}, {Step: 3600, Count: 48}}, Method: method, XFilesFactor: xff}
						policy, _ := json.Marshal(c)
						advance := 20
						if wrap {
							advance = 600
						}
						now := int64(1794700800 + seal*advance)
						b := coreBatch{ID: id, Order: uint64(seal), Clock: now, Policy: policy, Points: make([]row, density)}
						for j := range b.Points {
							bits := math.Float64bits(float64(j%17) - 8)
							if j == 3 {
								bits = 1 << 63
							}
							if j == 5 {
								bits = 0x7ff80000000000ab
							}
							b.Points[j] = row{ID: id, Time: now - int64(density) + int64(j), Bits: bits, Seq: uint64(j + 1)}
						}
						if err = h.append(b); err != nil {
							t.Fatal(err)
						}
						all = append(all, b)
					}
				}
			}
		}
		if err = e.submitCompactRollupGroup(0, &h, uint64(seal)); err != nil {
			t.Fatal(err)
		}
		if err = reference.submitBatches(0, all, uint64(seal), true); err != nil {
			t.Fatal(err)
		}
		// A duplicate source cursor must not invoke its plan or publish another part.
		if err = e.submitCompactRollupGroup(0, &h, uint64(seal)); err != nil {
			t.Fatal(err)
		}
		h.reset()
		clear(h.data[:cap(h.data)])
		if e.owners[0].parts.Load() != uint64(seal) {
			t.Fatal("duplicate cursor committed")
		}
		for id := uint64(1); id <= 72; id++ {
			if !reflect.DeepEqual(states[0].state[id].Slots, classic[0].state[id].Slots) || states[0].state[id].Frontier != classic[0].state[id].Frontier {
				t.Fatalf("seal%d metric%d archive mismatch", seal, id)
			}
			a := e.owners[0].db.NewSnapshot()
			z := reference.owners[0].db.NewSnapshot()
			got, gerr := coreRead(a, id)
			want, werr := coreRead(z, id)
			a.Close()
			z.Close()
			if gerr != nil || werr != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("seal%d metric%d raw mismatch %v/%v", seal, id, gerr, werr)
			}
		}
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	e, states, err = openCoreRollups(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := e.close(); err != nil {
			t.Error(err)
		}
	}()
	for id := uint64(1); id <= 72; id++ {
		if !reflect.DeepEqual(states[0].state[id].Slots, classic[0].state[id].Slots) || states[0].state[id].Frontier != classic[0].state[id].Frontier {
			t.Fatalf("reopen metric%d mismatch", id)
		}
	}
}

func TestCompactRunRejectCorruption(t *testing.T) {
	for _, v := range [][]byte{nil, {1}, {2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, {1, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0}} {
		if _, err := decodeCompactRun(v, 1, 1, nil); err == nil {
			t.Fatal("accepted corrupt compact record")
		}
	}
}

func TestCompactEngineCrashReplay(t *testing.T) {
	cfg := coreRollupTestConfig()
	cfg.Shards = 1
	if root := os.Getenv("COMPACT_CRASH_ROOT"); root != "" {
		e, _, err := openCoreRollups(root, cfg)
		if err != nil {
			t.Fatal(err)
		}
		submit := func(order uint64, value float64) {
			var h compactRollupHead
			for _, b := range coreRollupBatches(t, order, value) {
				if err := h.append(b); err != nil {
					t.Fatal(err)
				}
			}
			if err := e.submitCompactRollupGroup(0, &h, order); err != nil {
				t.Fatal(err)
			}
		}
		submit(1, 2)
		kill := func() {
			if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
				os.Exit(99)
			}
			select {}
		}
		prior := e.owners[0].plan
		switch os.Getenv("COMPACT_CRASH_STAGE") {
		case "before":
			e.owners[0].plan = func(in coreSealInput) (coreSealPlan, error) { kill(); return prior(in) }
		case "after-ingest":
			e.owners[0].plan = func(in coreSealInput) (coreSealPlan, error) {
				p, err := prior(in)
				if err == nil {
					p.Commit = kill
				}
				return p, err
			}
		}
		submit(2, 7)
		kill()
		return
	}
	for _, stage := range []string{"before", "after-ingest", "after-publish"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCompactEngineCrashReplay$")
			cmd.Env = append(os.Environ(), "COMPACT_CRASH_ROOT="+root, "COMPACT_CRASH_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			var killed *exec.ExitError
			if !errors.As(err, &killed) {
				t.Fatalf("expected crash: %v %s", err, out)
			}
			status, ok := killed.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("unexpected exit %v %s", err, out)
			}
			e, states, err := openCoreRollups(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(2)
			if stage == "before" {
				want = 1
			}
			if states[0].state[42].Frontier.Order != want || e.durableFrontiers()[0][0] != want {
				t.Fatal("torn compact archive/frontier")
			}
			var h compactRollupHead
			for _, b := range coreRollupBatches(t, 2, 7) {
				if err = h.append(b); err != nil {
					t.Fatal(err)
				}
			}
			if err = e.submitCompactRollupGroup(0, &h, 2); err != nil {
				t.Fatal(err)
			}
			if states[0].state[42].Frontier.Order != 2 || states[0].state[42].Slots[[2]int64{1, 480}].Bits != math.Float64bits(10) {
				t.Fatal("compact crash replay changed archive")
			}
			snap := e.owners[0].db.NewSnapshot()
			rows, err := coreRead(snap, 42)
			snap.Close()
			if err != nil || len(rows) != 4 {
				t.Fatal("raw receipt lost/duplicated", len(rows), err)
			}
			if err = e.close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
