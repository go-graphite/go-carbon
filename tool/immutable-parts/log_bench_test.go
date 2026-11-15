package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
)

func TestLogReadStageCeilings(t *testing.T) {
	root := t.TempDir()
	const total = 100003
	if err := logBacklogBenchmark(root, "write", total, 101, 3, 2); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"read", "decode"} {
		if err := logBacklogBenchmark(root, phase, total, 101, 3, 2); err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
		// The stage ceiling must reject incomplete coverage instead of reporting
		// a fast rate for fewer records than requested.
		if err := logBacklogBenchmark(root, phase, total+1, 101, 3, 2); err == nil {
			t.Fatalf("%s accepted a missing point", phase)
		}
	}
}

func TestLogSIGKILLMidReplay(t *testing.T) {
	const total, metrics = 100003, 101
	cfg := coreConfig{AppendParts: true, Shards: 2, Producers: 1, MaxRows: 1 << 20, PartitionSeconds: 3600}
	if root := os.Getenv("PARTLOG_MIDREPLAY_ROOT"); root != "" {
		l, err := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{})
		if err != nil {
			t.Fatal(err)
		}
		err = l.journal.Replay(0, 0, func(r partlog.Record) error {
			if err := l.apply(0, r, os.Getenv("PARTLOG_MIDREPLAY_SEAL") == "yes"); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(root, "ready"), []byte("first record applied"), 0600); err != nil {
				return err
			}
			select {} // Parent kills with the historical suffix still unapplied.
		})
		t.Fatal(err)
	}
	for _, seal := range []string{"no", "yes"} {
		t.Run("sealed="+seal, func(t *testing.T) {
			root := t.TempDir()
			if err := logBacklogBenchmark(root, "write", total, metrics, 1, 2); err != nil {
				t.Fatal(err)
			}
			output, err := os.Create(filepath.Join(root, "child.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer output.Close()
			cmd := exec.Command(os.Args[0], "-test.run=^TestLogSIGKILLMidReplay$")
			cmd.Env = append(os.Environ(), "PARTLOG_MIDREPLAY_ROOT="+root, "PARTLOG_MIDREPLAY_SEAL="+seal)
			cmd.Stdout, cmd.Stderr = output, output
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			deadline := time.Now().Add(15 * time.Second)
			for {
				if _, err = os.Stat(filepath.Join(root, "ready")); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("child did not reach mid-replay barrier")
				}
				time.Sleep(time.Millisecond)
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			l, err := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if err = l.replay(); err != nil {
				_ = l.close()
				t.Fatal(err)
			}
			if err = l.close(); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(root, "live-counts.json"), []byte("[0]"), 0600); err != nil {
				t.Fatal(err)
			}
			if err = verifyLogBenchmark(root, total, metrics, 1, 2, func(string, uint64, map[string]any) {}); err != nil {
				t.Fatal(err)
			}
			t.Logf("SIGKILL mid-replay sealed=%s: all %d persisted rows exact, no duplicates", seal, total)
		})
	}
}
