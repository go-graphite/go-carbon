package v2daemon

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// The archive is an independent, test-only copy of every segment before unlink.
// Rebuilding core from it proves the retained store preserves the full journal's
// raw winners, rollup slots, policies, order and special floating point bits.
func TestJournalRetireKill(t *testing.T) {
	if root := os.Getenv("V2_RETIRE_CHILD"); root != "" {
		target := os.Getenv("V2_RETIRE_STAGE")
		var mu sync.Mutex
		seen := map[string]int{}
		cut := func(stage string) {
			mu.Lock()
			seen[stage]++
			n := seen[stage]
			mu.Unlock()
			limit := 23
			if stage == "retire" || stage == "retired" {
				limit = 8
			}
			if stage == target && n == limit {
				fmt.Println("CUT")
				select {}
			}
		}
		c := segmentSkipConfig()
		c.Hooks = v2core.Hooks{Stage: cut}
		s, err := Open(root, c)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range s.Producers {
			p := p
			p.stage = func(stage string) {
				if stage == "retire" {
					paths, err := journalSegments(p.journalDir)
					if err != nil || len(paths) < 2 {
						t.Fatalf("retire paths: %v %v", paths, err)
					}
					rel, err := filepath.Rel(root, paths[0])
					if err != nil {
						t.Fatal(err)
					}
					dst := filepath.Join(root+"-archive", rel)
					if err = os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
						t.Fatal(err)
					}
					b, err := os.ReadFile(paths[0])
					if err != nil {
						t.Fatal(err)
					}
					if err = os.WriteFile(dst, b, 0600); err != nil {
						t.Fatal(err)
					}
				}
				cut(stage)
			}
		}
		segmentSkipWrite(t, s, 24, nil)
		t.Fatal("missed crash boundary")
	}
	for _, stage := range []string{"seal", "commit", "retire", "retired"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			root := filepath.Join(t.TempDir(), "store")
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestJournalRetireKill$", "-test.timeout=85s")
			cmd.Env = append(os.Environ(), "V2_RETIRE_CHILD="+root, "V2_RETIRE_STAGE="+stage)
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
			copyTree(t, root, oracle)
			copyTree(t, root+"-archive", oracle)
			want, _, _ := segmentSkipOpen(t, oracle, true, true)
			got, _, _ := segmentSkipOpen(t, root, false, false)
			if got != want {
				t.Fatalf("retained %x != full journal rebuild %x", got, want)
			}
			if stage == "retired" {
				for _, damage := range []string{"missing", "truncated"} {
					dir := t.TempDir()
					copyTree(t, root, dir)
					// Close writes a current checkpoint even when the kill
					// raced the first asynchronous at-seal checkpoint.
					recovered, err := Open(dir, segmentSkipConfig())
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
				s, err := Open(root, segmentSkipConfig())
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
				c := segmentSkipConfig()
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
	if got := parallelReplayDigest(t, s); got != want {
		t.Fatalf("single segment: %x != %x", got, want)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}
