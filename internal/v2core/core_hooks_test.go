package v2core

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestCoreOriginalBatchesAtomicExtension(t *testing.T) {
	t.Parallel()
	for _, blind := range []bool{false, true} {
		cfg := coreTestConfig()
		cfg.Shards = 1
		cfg.Producers = 1
		cfg.BlindBatch = blind
		var got []coreBatch
		var committed int
		e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{PlanSeal: func(owner int, in coreSealInput) (coreSealPlan, error) {
			if owner != 0 {
				t.Error(owner)
			}
			for _, b := range in.Batches {
				b.Policy = slices.Clone(b.Policy)
				b.Points = slices.Clone(b.Points)
				got = append(got, b)
			}
			return coreSealPlan{Files: [][]coreKV{{{[]byte{'R', byte(in.Next.NextPart)}, []byte("archive"), nil, ""}}}, Commit: func() { committed++ }}, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		want := []coreBatch{{ID: 7, Order: 11, Clock: 1234, Policy: []byte("exact-policy"), Points: []row{{7, -1, 0x7ff8000000000042, 11}, {7, 19, 0x8000000000000000, 12}}}, {ID: 7, Order: 12, Clock: 9876, Policy: []byte("exact-policy"), Points: []row{{7, 0, 1, 13}}}}
		if err = e.submitBatches(0, want, 101, true); err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Clock != 1234 || got[1].Clock != 9876 || !slices.Equal(got[0].Points, want[0].Points) || !slices.Equal(got[1].Points, want[1].Points) || string(got[0].Policy) != "exact-policy" || committed != 1 {
			t.Fatalf("lost batch metadata: %+v commit%d", got, committed)
		}
		if g := e.durableFrontiers(); g[0][0] != 101 {
			t.Fatal(g)
		}
		snap := e.owners[0].db.NewSnapshot()
		v, c, err := snap.Get([]byte{'R', 1})
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != "archive" {
			t.Fatal(string(v))
		}
		c.Close()
		rows, err := coreRead(snap, 7)
		if err != nil || len(rows) != 3 {
			t.Fatalf("raw rows %v %v", rows, err)
		}
		snap.Close()
		if err = e.close(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestCorePlanFailureDoesNotCommit(t *testing.T) {
	t.Parallel()
	cfg := coreTestConfig()
	cfg.Shards = 1
	cfg.Producers = 1
	sentinel := errors.New("planner failed")
	e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{PlanSeal: func(int, coreSealInput) (coreSealPlan, error) { return coreSealPlan{}, sentinel }})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.submitBatches(0, []coreBatch{{ID: 1, Points: []row{{1, 1, 1, 1}}}}, 1, true); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if e.durableFrontiers()[0][0] != 0 {
		t.Fatal("advanced failed frontier")
	}
	if _, c, err := e.owners[0].db.Get(coreMetaKey); !errors.Is(err, pebble.ErrNotFound) {
		if c != nil {
			c.Close()
		}
		t.Fatal(err)
	}
	if err = e.close(); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}
func TestCorePlanValidation(t *testing.T) {
	for _, files := range [][][]coreKV{{{}}, {{{[]byte{1}, nil, nil, ""}}}, {{{[]byte{255}, nil, nil, ""}}}, {{{[]byte{'R', 2}, nil, nil, ""}, {[]byte{'R', 1}, nil, nil, ""}}}, {{{[]byte{'R', 1}, nil, nil, ""}}, {{[]byte{'R', 1}, nil, nil, ""}}}} {
		if validateCoreSealPlan(coreSealPlan{Files: files}) == nil {
			t.Fatal(files)
		}
	}
}

// Spec-less GCP3 parts (baseline bootstrap) are retained, never LSM-ingested:
// month partitions may interleave across vshards; scopes must stay distinct.
func TestCorePlanValidationSpeclessRollupParts(t *testing.T) {
	// Two metrics on distinct vshards, ordered by vshard as keys sort.
	names := []v2part.ID{"a"}
	for i := 0; len(names) < 2; i++ {
		if name := v2part.ID(fmt.Sprintf("m%d", i)); v2part.Route(name) != v2part.Route(names[0]) {
			names = append(names, name)
		}
	}
	slices.SortFunc(names, func(a, b v2part.ID) int { return int(v2part.Route(a)) - int(v2part.Route(b)) })
	key := func(name v2part.ID, partition int64, part byte) []byte {
		t.Helper()
		k, err := v2part.Encode(v2part.Key{Group: "g", VShard: v2part.Route(name), Partition: partition, Part: v2part.PartID{part}, Metric: name, Archive: v2part.MetadataArchive})
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	file := func(partition int64, part byte) []coreKV {
		return []coreKV{{Key: key(names[0], partition, part)}, {Key: key(names[1], partition, part)}}
	}
	if err := validateCoreSealPlan(coreSealPlan{Files: [][]coreKV{file(1, 1), file(2, 1)}}); err != nil {
		t.Fatalf("interleaved month parts: %v", err)
	}
	if err := validateCoreSealPlan(coreSealPlan{Files: [][]coreKV{file(1, 1), file(1, 1)}}); err == nil {
		t.Fatal("accepted duplicate spec-less part scope")
	}
	// A generic extension file keeps the strict LSM range ordering.
	if err := validateCoreSealPlan(coreSealPlan{Files: [][]coreKV{file(1, 1), file(2, 1), {{Key: []byte{'R', 1}}}}}); err == nil {
		t.Fatal("accepted unordered LSM extension files")
	}
}
func TestCoreConcurrentDurableFrontier(t *testing.T) {
	t.Parallel()
	cfg := coreTestConfig()
	e, err := openCore(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		last := make([]uint64, cfg.Producers)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
			}
			f := e.durableFrontiers()
			for p := range last {
				var low uint64 = ^uint64(0)
				for _, v := range f {
					low = min(low, v[p])
				}
				if low < last[p] {
					t.Error("frontier reversed")
				}
				last[p] = low
			}
		}
	}()
	for n := 1; n <= 12; n++ {
		for p := 0; p < cfg.Producers; p++ {
			if err = e.submit(p, coreRows(p, n), uint64(n), true); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(done)
	wg.Wait()
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
}

func TestCoreSealExtensionCrash(t *testing.T) {
	// The child performs real durable writes before signaling readiness. Keep
	// subprocess crash tests outside the parallel disk-heavy fixture pool.
	cfg := coreTestConfig()
	cfg.Shards = 1
	cfg.Producers = 1
	root, phase := os.Getenv("CORE_SEAL_CRASH_ROOT"), os.Getenv("CORE_SEAL_CRASH_PHASE")
	batch := []coreBatch{{ID: 7, Order: 11, Clock: 1234, Policy: []byte("policy"), Points: []row{{7, -1, 0x7ff8000000000042, 11}, {7, 19, 0x8000000000000000, 12}}}}
	hooks := func(pause string) coreHooks {
		return coreHooks{PlanSeal: func(int, coreSealInput) (coreSealPlan, error) {
			stop := func() {
				if err := os.WriteFile(filepath.Join(root, "ready"), []byte(pause), 0600); err != nil {
					panic(err)
				}
				select {}
			}
			if pause == "before" {
				stop()
			}
			return coreSealPlan{Files: [][]coreKV{{{[]byte{'R', 1}, []byte("same-transaction"), nil, ""}}}, Commit: func() {
				if pause == "after" {
					stop()
				}
			}}, nil
		}}
	}
	if root != "" {
		e, err := openCoreWithHooks(root, cfg, hooks(phase))
		if err != nil {
			panic(err)
		}
		if err = e.submitBatches(0, batch, 1, true); err != nil {
			panic(err)
		}
		panic("crash point not reached")
	}
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			root = t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCoreSealExtensionCrash$")
			cmd.Env = append(os.Environ(), "CORE_SEAL_CRASH_ROOT="+root, "CORE_SEAL_CRASH_PHASE="+phase)
			log, err := os.Create(filepath.Join(t.TempDir(), "child.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			cmd.Stdout = log
			cmd.Stderr = log
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			until := time.Now().Add(30 * time.Second)
			for {
				if _, err = os.Stat(filepath.Join(root, "ready")); err == nil {
					break
				}
				if time.Now().After(until) {
					t.Fatal("child crash point timeout")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			e, err := openCore(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(0)
			if phase == "after" {
				want = 1
			}
			if e.durableFrontiers()[0][0] != want {
				t.Fatal("frontier disagrees with commit")
			}
			snap := e.owners[0].db.NewSnapshot()
			rs, err := coreRead(snap, 7)
			if err != nil {
				t.Fatal(err)
			}
			if len(rs) != int(want)*2 {
				t.Fatal("raw rows disagree with frontier", rs)
			}
			if want == 1 && (rs[0].Bits != 0x7ff8000000000042 || rs[1].Bits != 0x8000000000000000) {
				t.Fatal("float bits changed", rs)
			}
			v, c, err := snap.Get([]byte{'R', 1})
			if want == 0 {
				if !errors.Is(err, pebble.ErrNotFound) {
					t.Fatal("extension committed without frontier", err)
				}
			} else {
				if err != nil || string(v) != "same-transaction" {
					t.Fatal("missing extension", err)
				}
				c.Close()
			}
			snap.Close()
			if err = e.close(); err != nil {
				t.Fatal(err)
			}
			e, err = openCoreWithHooks(root, cfg, hooks(""))
			if err != nil {
				t.Fatal(err)
			}
			if err = e.submitBatches(0, batch, 1, true); err != nil {
				t.Fatal(err)
			}
			snap = e.owners[0].db.NewSnapshot()
			rs, err = coreRead(snap, 7)
			if err != nil || len(rs) != 2 {
				t.Fatal("replay changed raw count", rs, err)
			}
			snap.Close()
			if err = e.close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
