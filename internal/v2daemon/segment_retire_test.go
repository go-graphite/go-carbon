package v2daemon

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// retireKillConfig rotates after every Drain, or, idle, keeps several records
// per segment so the idle cut and the covered open segment's rotation run.
func retireKillConfig(idle bool) Config {
	c := segmentSkipConfig()
	if idle {
		c.JournalSegmentBytes = 4 << 10
	}
	return c
}

// The archive is an independent, test-only copy of every segment before unlink
// or cut. Rebuilding core from it proves the retained store preserves the full
// journal's raw winners, rollup slots, policies, order and special float bits.
// The idle variant seals all input, then cuts each journal to its last record.
func TestJournalRetireKill(t *testing.T) {
	if root := os.Getenv("V2_RETIRE_CHILD"); root != "" {
		target := os.Getenv("V2_RETIRE_STAGE")
		idle := os.Getenv("V2_RETIRE_IDLE") != ""
		var mu sync.Mutex
		seen := map[string]int{}
		quiet := false
		cut := func(stage string) {
			mu.Lock()
			seen[stage]++
			n := seen[stage]
			mu.Unlock()
			limit := 23
			if stage == "retire" || stage == "retired" {
				limit = 8
			}
			if idle {
				limit = 1
			}
			if stage == target && n == limit && (!idle || quiet) {
				fmt.Println("CUT")
				select {}
			}
		}
		c := retireKillConfig(idle)
		c.Hooks = v2core.Hooks{Stage: func(stage string) {
			if !idle {
				cut(stage)
			}
		}}
		s, err := Open(root, c)
		if err != nil {
			t.Fatal(err)
		}
		archive := func(path string) {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				t.Fatal(err)
			}
			dst := filepath.Join(root+"-archive", rel)
			if _, err = os.Stat(dst); err == nil {
				return // an earlier cut archived the whole segment
			}
			if err = os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			// A kill at another goroutine's cut point must not leave a partial copy.
			if err = os.WriteFile(dst+".tmp", b, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.Rename(dst+".tmp", dst); err != nil {
				t.Fatal(err)
			}
		}
		for _, p := range s.Producers {
			p.stage = func(stage string) {
				if name, path, ok := strings.Cut(stage, " "); ok {
					archive(path)
					stage = name
				}
				if !idle || quiet {
					cut(stage)
				}
			}
		}
		segmentSkipWrite(t, s, 24, nil)
		if idle {
			mu.Lock()
			quiet = true
			mu.Unlock()
			// A closed segment beyond the frontier, then an open segment of two
			// records: the idle Drain rotates it, cuts it and retires the rest.
			for _, p := range s.Producers {
				if err = p.rotateJournal(); err != nil {
					t.Fatal(err)
				}
			}
			segmentSkipWriteFrom(t, s, 24, 2, nil)
			for _, p := range s.Producers {
				if err = s.Core.AppendBatches(p.index, nil, p.cursor, true); err != nil {
					t.Fatal(err)
				}
				if err = p.Drain(); err != nil {
					t.Fatal(err)
				}
			}
		}
		t.Fatal("missed crash boundary")
	}
	for _, tc := range []struct {
		stage string
		idle  bool
	}{{"seal", false}, {"commit", false}, {"retire", false}, {"retired", false}, {"seed", true}, {"seeded", true}, {"retire", true}, {"retired", true}} {
		name := tc.stage
		if tc.idle {
			name = "idle-" + name
		}
		stage := tc.stage
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := filepath.Join(t.TempDir(), "store")
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestJournalRetireKill$", "-test.timeout=85s")
			cmd.Env = append(os.Environ(), "V2_RETIRE_CHILD="+root, "V2_RETIRE_STAGE="+stage)
			if tc.idle {
				cmd.Env = append(cmd.Env, "V2_RETIRE_IDLE=1")
			}
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
			archived, _ := filepath.Glob(filepath.Join(root+"-archive", "placement-group-*", "journal", "producer-*", "segment*.pdc4"))
			if len(archived) == 0 {
				t.Fatal("no segment retired before crash")
			}
			oracle := t.TempDir()
			// Archives are whole (written by rename before the unlink or cut),
			// so they replace a retained segment cut down to its last record.
			copyTree(t, root, oracle)
			copyTree(t, root+"-archive", oracle)
			want, _, _ := segmentSkipOpen(t, oracle, true, true)
			got, _, _ := segmentSkipOpen(t, root, false, false)
			if got != want {
				t.Fatalf("retained %x != full journal rebuild %x", got, want)
			}
			if stage == "retired" || stage == "seeded" {
				for _, damage := range []string{"missing", "truncated"} {
					dir := t.TempDir()
					copyTree(t, root, dir)
					// Close writes a current checkpoint even when the kill
					// raced the first asynchronous at-seal checkpoint.
					recovered, err := Open(dir, retireKillConfig(tc.idle))
					if err != nil {
						t.Fatal(err)
					}
					if err = recovered.Close(); err != nil {
						t.Fatal(err)
					}
					checkpoints, _ := filepath.Glob(filepath.Join(dir, "placement-group-*", "core", "owner-*", "rollup-state.ckpt"))
					if len(checkpoints) == 0 {
						t.Fatal("no checkpoint to damage")
					}
					for _, path := range checkpoints {
						if damage == "missing" {
							err = os.Remove(path)
						} else {
							err = os.Truncate(path, 7)
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					image, _, _ := segmentSkipOpen(t, dir, false, false)
					if image != want {
						t.Fatalf("%s checkpoint after retirement: %x != %x", damage, image, want)
					}
				}
			}
			// Actual repeated writable restarts exercise generation allocation after
			// deleting the original prefix, including newly appended predictor resets.
			for i := 0; i < 3; i++ {
				s, err := Open(root, retireKillConfig(tc.idle))
				if err != nil {
					t.Fatal(err)
				}
				if image := parallelReplayDigest(t, s); image != want {
					t.Fatalf("restart %d: %x != %x", i, image, want)
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			// Continue the shortened and full journals with identical new
			// applications, then seal synchronously before comparing the image.
			continued := make([][32]byte, 0, 2)
			for i, dir := range []string{root, oracle} {
				c := retireKillConfig(tc.idle)
				c.fullReplay = i == 1
				s, err := Open(dir, c)
				if err != nil {
					t.Fatal(err)
				}
				segmentSkipWriteFrom(t, s, 24, 3, nil)
				for _, p := range s.Producers {
					if err = s.Core.AppendBatches(p.index, nil, p.cursor, true); err != nil {
						t.Fatal(err)
					}
				}
				continued = append(continued, parallelReplayDigest(t, s))
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if continued[0] != continued[1] {
				t.Fatal("continued intake differs after retirement")
			}
			t.Logf("retired=%d sha256=%x", len(archived), got)
		})
	}
}

func TestJournalRetirePreservesLiveCursor(t *testing.T) {
	s, err := Open(t.TempDir(), segmentSkipConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	segmentSkipWrite(t, s, 12, nil)
	for _, p := range s.Producers {
		paths, err := journalSegments(p.journalDir)
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) >= 12 {
			t.Fatalf("journal did not shrink: %d segments", len(paths))
		}
		if filepath.Base(paths[0]) == "segment.pdc4" {
			t.Fatal("original segment not retired")
		}
		cursor, order := p.cursor, p.applyOrder
		if _, err = p.durablePrefix(paths); err != nil {
			t.Fatal(err)
		}
		if cursor != p.cursor || order != p.applyOrder {
			t.Fatal("retirement changed live cursor/order")
		}
	}
}

// A lone retained segment still seeds its nonzero starting cursor. Removing
// core is unsafe once the journal prefix is gone and must fail startup.
func TestJournalRetireSingleSegment(t *testing.T) {
	root := t.TempDir()
	c := segmentSkipConfig()
	s, err := Open(root, c)
	if err != nil {
		t.Fatal(err)
	}
	segmentSkipWrite(t, s, 12, nil)

	for _, p := range s.Producers {
		if err = s.Core.AppendBatches(p.index, nil, p.cursor, true); err != nil {
			t.Fatal(err)
		}
	}
	want := parallelReplayDigest(t, s)
	dirs := make([]string, len(s.Producers))
	for i, p := range s.Producers {
		dirs[i] = p.journalDir
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, dir := range dirs {
		paths, err := journalSegments(dir)
		if err != nil {
			t.Fatal(err)
		}
		keep := ""
		for _, path := range paths {
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if st.Size() > 0 {
				keep = path
			}
		}
		if keep == "" || filepath.Base(keep) == "segment.pdc4" {
			t.Fatal("no rotated segment")
		}
		for _, path := range paths {
			if path != keep {
				if err = os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	damaged := t.TempDir()
	copyTree(t, root, damaged)
	if err = os.RemoveAll(filepath.Join(damaged, "placement-group-00000001", "core")); err != nil {
		t.Fatal(err)
	}
	if bad, err := Open(damaged, c); err == nil {
		_ = bad.Close()
		t.Fatal("accepted a retired prefix without its durable core")
	}
	s, err = Open(root, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range s.Producers {
		if p.RecoveryReplayedSegments != 0 || p.RecoverySkippedSegments != 1 {
			t.Fatalf("sealed retained anchor: skipped=%d replayed=%d", p.RecoverySkippedSegments, p.RecoveryReplayedSegments)
		}
		if p.catalog != nil {
			t.Fatal("sealed retained anchor opened its catalog")
		}
	}
	if got := s.Core.Observe().CheckpointBytes; got != 0 {
		t.Fatalf("sealed retained anchor rewrote %d checkpoint bytes", got)
	}
	if got := parallelReplayDigest(t, s); got != want {
		t.Fatalf("single segment: %x != %x", got, want)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

// Once the frontiers cover all input, an idle Drain leaves each journal one
// record, byte for byte its former last record, beside an empty open segment.
// Restart seeds cursor and order from it without opening the catalog.
func TestJournalRetireIdleKeepsLastRecord(t *testing.T) {
	root := t.TempDir()
	c := retireKillConfig(true)
	s, err := Open(root, c)
	if err != nil {
		t.Fatal(err)
	}
	segmentSkipWrite(t, s, 24, nil)
	lasts := make([][]byte, len(s.Producers))
	for i, p := range s.Producers {
		if p.segmentLast <= 0 {
			t.Fatalf("producer %d: open segment holds %d bytes, last at %d", i, p.segmentBytes, p.segmentLast)
		}
		b, err := os.ReadFile(p.segmentPath)
		if err != nil {
			t.Fatal(err)
		}
		lasts[i] = b[p.segmentLast:]
		if err = s.Core.AppendBatches(p.index, nil, p.cursor, true); err != nil {
			t.Fatal(err)
		}
		if err = p.Drain(); err != nil {
			t.Fatal(err)
		}
	}
	want := parallelReplayDigest(t, s)
	dirs := make([]string, len(s.Producers))
	for i, p := range s.Producers {
		dirs[i] = p.journalDir
		paths, err := journalSegments(p.journalDir)
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) != 2 || paths[1] != p.segmentPath || p.segmentBytes != 0 || len(p.closed) != 1 || p.closed[0].last != 0 {
			t.Fatalf("producer %d retained %v, open %s holds %d bytes, closed %+v", i, paths, p.segmentPath, p.segmentBytes, p.closed)
		}
		b, err := os.ReadFile(paths[0])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, lasts[i]) {
			t.Fatalf("producer %d: seed is not the former last record", i)
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		s, err = Open(root, c)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range s.Producers {
			if p.RecoveryReplayedSegments != 0 || p.catalog != nil {
				t.Fatalf("restart %d replayed %d segments, catalog opened %v", round, p.RecoveryReplayedSegments, p.catalog != nil)
			}
		}
		if got := parallelReplayDigest(t, s); got != want {
			t.Fatalf("restart %d: %x != %x", round, got, want)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// The restarts' empty segments retire; the seed stays the only record.
	for _, dir := range dirs {
		paths, err := journalSegments(dir)
		if err != nil {
			t.Fatal(err)
		}
		var sizes []int64
		for _, path := range paths {
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			sizes = append(sizes, st.Size())
		}
		if len(paths) != 2 || sizes[1] != 0 {
			t.Fatalf("%s after restarts: %v %v", dir, paths, sizes)
		}
	}
}
