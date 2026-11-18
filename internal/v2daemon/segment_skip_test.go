package v2daemon

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

func segmentSkipConfig() Config {
	c := parallelReplayConfig()
	// Rotate after every group fsync, so every Drain starts a new segment.
	c.JournalSegmentBytes = 1
	return c
}

// segmentSkipWrite appends rounds with seals every third round. Owners also
// seal on their 32-row bound, so their frontiers differ per producer.
func segmentSkipWrite(t *testing.T, s *Store, rounds int, after func(round int)) {
	t.Helper()
	for round := 0; round < rounds; round++ {
		for i, p := range s.Producers {
			apps := restartApps(t, round*2, round*2+2)
			bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000}
			var err error
			for j := range apps {
				a := &apps[j]
				a.Name = fmt.Sprintf("parallel.%d.%d", i, j)
				a.Policy, err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(a.Name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 120}}}})
				if err != nil {
					t.Fatal(err)
				}
				a.Clock = 10040 - int64(round%3)
				a.Points = []points.Point{{Timestamp: 10000 + int64(round%7), Value: float64(round+i) / 4}, {Timestamp: 10000 + int64(round%11), Value: math.Float64frombits(bits[round%len(bits)])}}
			}
			if err = p.AppendApplications(apps); err != nil {
				t.Fatal(err)
			}
			if round%3 == 2 {
				if err = p.Seal(); err != nil {
					t.Fatal(err)
				}
			}
			if err = p.Drain(); err != nil {
				t.Fatal(err)
			}
		}
		if after != nil {
			after(round)
		}
	}
}

func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.Walk(from, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(to, rel), 0700)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(to, rel), b, 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// segmentSkipOpen opens a copy of root and returns its digest and skip count.
func segmentSkipOpen(t *testing.T, root string, full, rebuild bool) ([32]byte, int, int) {
	t.Helper()
	dir := t.TempDir()
	copyTree(t, root, dir)
	if rebuild {
		if err := os.RemoveAll(filepath.Join(dir, "placement-group-00000001", "core")); err != nil {
			t.Fatal(err)
		}
	}
	c := segmentSkipConfig()
	c.fullReplay = full
	s, err := Open(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	skipped, replayed := 0, 0
	for _, p := range s.Producers {
		skipped += p.RecoverySkippedSegments
		replayed += p.RecoveryReplayedSegments
	}
	got := parallelReplayDigest(t, s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	return got, skipped, replayed
}

// TestJournalSegmentSkipKill SIGKILLs a writer at several rounds, including
// inside a seal commit, then compares three recoveries of the same files:
// segment skip, the full journal replay, and a full rebuild from the journal
// alone. Skipping must restore the identical logical image.
func TestJournalSegmentSkipKill(t *testing.T) {
	if root := os.Getenv("V2_SEGMENT_SKIP_CHILD"); root != "" {
		cut, _ := strconv.Atoi(os.Getenv("V2_SEGMENT_SKIP_CUT"))
		commits, _ := strconv.Atoi(os.Getenv("V2_SEGMENT_SKIP_COMMITS"))
		var mu sync.Mutex
		seen := 0
		c := segmentSkipConfig()
		c.Hooks = v2core.Hooks{Stage: func(stage string) {
			if stage != "commit" || commits == 0 {
				return
			}
			mu.Lock()
			seen++
			n := seen
			mu.Unlock()
			if n == commits {
				fmt.Println("CUT")
				select {}
			}
		}}
		s, err := Open(root, c)
		if err != nil {
			t.Fatal(err)
		}
		segmentSkipWrite(t, s, 24, func(round int) {
			if round == cut {
				fmt.Println("CUT")
				select {}
			}
		})
		t.Fatal("missed cut")
	}
	cases := []struct{ cut, commits int }{{11, 0}, {23, 0}, {99, 23}}
	for _, k := range cases {
		t.Run(fmt.Sprintf("cut%d-commit%d", k.cut, k.commits), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestJournalSegmentSkipKill$", "-test.timeout=55s")
			cmd.Env = append(os.Environ(), "V2_SEGMENT_SKIP_CHILD="+root, fmt.Sprintf("V2_SEGMENT_SKIP_CUT=%d", k.cut), fmt.Sprintf("V2_SEGMENT_SKIP_COMMITS=%d", k.commits))
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
				t.Fatalf("no crash barrier: %s", stderr.String())
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			want, _, _ := segmentSkipOpen(t, root, true, true)
			full, fullSkipped, _ := segmentSkipOpen(t, root, true, false)
			got, skipped, replayed := segmentSkipOpen(t, root, false, false)
			if fullSkipped != 0 {
				t.Fatalf("oracle skipped %d segments", fullSkipped)
			}
			if full != want {
				t.Fatalf("full replay %x != journal rebuild %x", full, want)
			}
			if got != want {
				t.Fatalf("segment skip %x != journal rebuild %x (skipped %d)", got, want, skipped)
			}
			if k.cut >= 11 && skipped == 0 {
				t.Fatalf("no segment skipped (replayed %d)", replayed)
			}
			t.Logf("skipped=%d replayed=%d sha256=%x", skipped, replayed, got)
		})
	}
}
