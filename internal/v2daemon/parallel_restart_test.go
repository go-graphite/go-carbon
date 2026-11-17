package v2daemon

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

func parallelReplayConfig() Config {
	return Config{Layer: 3, Producers: 8, Shards: 4, MaxCoreRows: 32, GroupInterval: time.Hour}
}

// Journals contain corrections, backward admission clocks, out-of-order samples,
// signed zero, infinities and NaN payloads. Each name retains one producer.
func parallelReplayFixture(t *testing.T, root string) {
	t.Helper()
	s, err := Open(root, parallelReplayConfig())
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 16; round++ {
		for i, p := range s.Producers {
			apps := restartApps(t, round*2, round*2+2)
			bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000}
			for j := range apps {
				a := &apps[j]
				a.Name = fmt.Sprintf("parallel.%d.%d", i, j)
				a.Policy, err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(a.Name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 120}}}})
				if err != nil {
					t.Fatal(err)
				}
				a.Clock = 10040 - int64(round%3)
				a.Points = []points.Point{{Timestamp: 10000 + int64(round%7), Value: float64(round+i) / 4}, {Timestamp: 10000 + int64(round%7), Value: math.Float64frombits(bits[round%len(bits)])}}
			}
			if err = p.AppendApplications(apps); err != nil {
				t.Fatal(err)
			}
			if err = p.Drain(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// Keep exactly the same journals but force the next open to rebuild all parts.
	if err = os.RemoveAll(filepath.Join(root, "placement-group-00000001", "core")); err != nil {
		t.Fatal(err)
	}
}

// This canonical image includes every stored raw winner, every rollup slot and
// its metadata; physical part boundaries, UUIDs and merge scheduling are omitted.
func parallelReplayDigest(t *testing.T, s *Store) [32]byte {
	t.Helper()
	type metric struct {
		Policy   v2core.RollupPolicy
		Metadata v2core.RollupMetadata
		Raw      []v2core.Row
		Slots    []sealrollup.Record
	}
	image := map[string]metric{}
	for _, db := range s.Core.DBs() {
		snap := db.NewSnapshot()
		err := v2core.ScanRollups(snap, func(p v2core.RollupPolicy, m v2core.RollupMetadata, records []sealrollup.Record) error {
			rows, err := v2core.Read(snap, m.ID)
			if err != nil {
				return err
			}
			slices.SortFunc(records, func(a, b sealrollup.Record) int {
				if a.Archive != b.Archive {
					return a.Archive - b.Archive
				}
				if a.Slot < b.Slot {
					return -1
				}
				if a.Slot > b.Slot {
					return 1
				}
				return 0
			})
			// Correction is diagnostic and may change with seal scheduling.
			for i := range records {
				records[i].Correction = false
			}
			// Raw storage may retain overwritten tuples until its next merge.
			winners := map[int64]v2core.Row{}
			for _, row := range rows {
				if old, ok := winners[row.Time]; !ok || row.Seq > old.Seq {
					winners[row.Time] = row
				}
			}
			rows = rows[:0]
			for _, row := range winners {
				rows = append(rows, row)
			}
			slices.SortFunc(rows, func(a, b v2core.Row) int {
				if a.Time < b.Time {
					return -1
				}
				if a.Time > b.Time {
					return 1
				}
				return 0
			})
			image[string(p.Name)] = metric{p, m, rows, records}
			return nil
		})
		snap.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(image) != parallelReplayConfig().Producers*2 {
		t.Fatalf("missing metrics: %d", len(image))
	}
	data, err := json.Marshal(image)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(data)
}

func TestParallelReplayExact(t *testing.T) {
	root := t.TempDir()
	parallelReplayFixture(t, root)
	cfg := parallelReplayConfig()
	s, err := openWithRecoveryWorkers(root, cfg, v2core.Hooks{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := parallelReplayDigest(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	for replay := 0; replay < 3; replay++ {
		if replay < 2 {
			if err = os.RemoveAll(filepath.Join(root, "placement-group-00000001", "core")); err != nil {
				t.Fatal(err)
			}
		}
		var mu sync.Mutex
		seen := map[int]bool{}
		ready := make(chan struct{})
		// Four callbacks must be inside recovery at once. A serial implementation
		// fails this barrier; scheduling speed is never used as a correctness oracle.
		cfg.DurableApplications = func(producer int, _ []v2core.Batch) error {
			mu.Lock()
			seen[producer] = true
			if len(seen) == 4 {
				select {
				case <-ready:
				default:
					close(ready)
				}
			}
			mu.Unlock()
			select {
			case <-ready:
				return nil
			case <-time.After(5 * time.Second):
				return fmt.Errorf("producer recovery did not overlap")
			}
		}
		s, err = openWithRecoveryWorkers(root, cfg, v2core.Hooks{}, 4)
		if err != nil {
			t.Fatal(err)
		}
		got := parallelReplayDigest(t, s)
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("replay %d changed logical digest: %x != %x", replay, got, want)
		}
		t.Logf("replay=%d sha256=%x", replay, got)
	}
}

func TestParallelReplayCorruptProducer(t *testing.T) {
	root := t.TempDir()
	parallelReplayFixture(t, root)
	path := filepath.Join(root, "placement-group-00000001", "journal", "producer-003", "segment.pdc4")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[12] ^= 1
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	_, err = Open(root, parallelReplayConfig())
	if err == nil || !strings.Contains(err.Error(), "producer 3") {
		t.Fatalf("corrupt producer accepted or unidentified: %v", err)
	}
	b[12] ^= 1
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	// A failed parallel open must release every catalog, owner and root lock.
	s, err := Open(root, parallelReplayConfig())
	if err != nil {
		t.Fatal(err)
	}
	parallelReplayDigest(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParallelReplaySIGKILL(t *testing.T) {
	if root := os.Getenv("V2_PARALLEL_REPLAY_CHILD"); root != "" {
		var once sync.Once
		_, err := Open(root, Config{Layer: 3, Producers: 8, Shards: 4, MaxCoreRows: 32, GroupInterval: time.Hour, Hooks: v2core.Hooks{Stage: func(stage string) {
			if stage == "commit" {
				once.Do(func() { fmt.Println("CUT"); select {} })
			}
		}}})
		t.Fatal("missed replay barrier", err)
	}
	root := t.TempDir()
	parallelReplayFixture(t, root)
	s, err := openWithRecoveryWorkers(root, parallelReplayConfig(), v2core.Hooks{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := parallelReplayDigest(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(filepath.Join(root, "placement-group-00000001", "core")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestParallelReplaySIGKILL$", "-test.timeout=18s")
	cmd.Env = append(os.Environ(), "V2_PARALLEL_REPLAY_CHILD="+root)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr synchronizedBuffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	scan := bufio.NewScanner(out)
	cut := false
	for scan.Scan() {
		if scan.Text() == "CUT" {
			cut = true
			break
		}
	}
	if !cut {
		_ = cmd.Wait()
		t.Fatalf("no replay crash barrier: %s", stderr.String())
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("child survived SIGKILL")
	}
	for replay := 0; replay < 2; replay++ {
		s, err = Open(root, parallelReplayConfig())
		if err != nil {
			t.Fatal(err)
		}
		got := parallelReplayDigest(t, s)
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("crash replay %d changed digest %x != %x", replay, got, want)
		}
	}
}
