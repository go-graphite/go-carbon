package main

import (
	"encoding/json"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

func TestLogAppendFixtureKeepsOriginalClock(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PARTLOG_BENCH_OFFSET", "6000100")
	if err := logBacklogBenchmark(root, "write", 100, 101, 1, 2); err != nil {
		t.Fatal(err)
	}
	journal, err := partlog.Open(filepath.Join(root, "log"), partlog.Options{Streams: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if err = journal.Replay(0, 0, func(record partlog.Record) error {
		rows, err := decodeLogRows(record.Payload)
		if err != nil {
			return err
		}
		if len(rows) != 100 || rows[0].Seq != 6000100 || record.Clock != rows[0].Time {
			t.Fatalf("offset fixture lost original apply clock: clock=%d rows=%v", record.Clock, rows)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
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

// Faults are chosen by seeded journal-record offsets, so the test is repeatable
// while killing three distinct incomplete prefixes on a real child process.
func TestLogSIGKILLRandomReplay(t *testing.T) {
	total := uint64(262147)
	if raw := os.Getenv("PARTLOG_RANDOM_POINTS"); raw != "" {
		var err error
		total, err = strconv.ParseUint(raw, 10, 64)
		if err != nil || total < 131073 {
			t.Fatal("invalid random replay size")
		}
	}
	const metricCount = 100003
	cfg := coreConfig{AppendParts: true, Shards: 4, Producers: 1, MaxRows: 65536, PartitionSeconds: 3600}
	if root := os.Getenv("PARTLOG_RANDOM_ROOT"); root != "" {
		target, err := strconv.Atoi(os.Getenv("PARTLOG_RANDOM_RECORD"))
		if err != nil {
			t.Fatal(err)
		}
		l, err := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{})
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		err = l.journal.Replay(0, 0, func(r partlog.Record) error {
			seen++
			if err := l.apply(0, r, seen%3 == 0); err != nil {
				return err
			}
			if seen == target {
				if err := os.WriteFile(filepath.Join(root, "ready"), []byte(strconv.Itoa(seen)), 0600); err != nil {
					return err
				}
				select {}
			}
			return nil
		})
		t.Fatal("child unexpectedly finished", err)
	}
	rng := rand.New(rand.NewPCG(20261116, 6))
	used := map[int]bool{}
	for trial := 0; trial < 3; trial++ {
		root := t.TempDir()
		if err := logBacklogBenchmark(root, "write", total, metricCount, 1, cfg.Shards); err != nil {
			t.Fatal(err)
		}
		records := int((total + 32767) / 32768)
		target := 1 + rng.IntN(records-2)
		for used[target] {
			target = 1 + rng.IntN(records-2)
		}
		used[target] = true
		output, err := os.Create(filepath.Join(root, "child.log"))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestLogSIGKILLRandomReplay$")
		cmd.Env = append(os.Environ(), "PARTLOG_RANDOM_ROOT="+root, "PARTLOG_RANDOM_RECORD="+strconv.Itoa(target), "PARTLOG_RANDOM_POINTS="+strconv.FormatUint(total, 10))
		cmd.Stdout, cmd.Stderr = output, output
		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(45 * time.Second)
		for {
			if _, err = os.Stat(filepath.Join(root, "ready")); err == nil {
				break
			}
			if time.Now().After(deadline) {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatal("random replay barrier not reached")
			}
			time.Sleep(time.Millisecond)
		}
		if err = cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		_ = cmd.Wait()
		_ = output.Close()
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
		if err = verifyLogBenchmark(root, total, metricCount, 1, cfg.Shards, func(stage string, points uint64, extra map[string]any) {
			t.Logf("trial%d stage%s points%d %v", trial, stage, points, extra)
		}); err != nil {
			t.Fatal(err)
		}
		t.Logf("random SIGKILL trial%d record%d/%d: %d rows bit exact, no lost/duplicate points", trial, target, records, total)
	}
}

func TestFinalReplayCompactionPreservesRows(t *testing.T) {
	root := t.TempDir()
	cfg := coreConfig{AppendParts: true, Shards: 2, Producers: 1, MaxRows: 64, PartitionSeconds: 60}
	log, err := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 5; i++ {
		if err = log.append(0, int64(i), coreRows(0, i), true); err != nil {
			_ = log.close()
			t.Fatal(err)
		}
	}
	if err = log.close(); err != nil {
		t.Fatal(err)
	}
	before, err := coreDigest(filepath.Join(root, "parts"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	sample, err := compactClosedReplay(filepath.Join(root, "parts"), cfg.Shards)
	if err != nil {
		t.Fatal(err)
	}
	if sample["estimated_debt_bytes"].(uint64) != 0 || sample["compactions_in_progress"].(int64) != 0 || sample["flushes_in_progress"].(int64) != 0 {
		t.Fatal(sample)
	}
	after, err := coreDigest(filepath.Join(root, "parts"), cfg)
	if err != nil || before != after {
		t.Fatal("final compaction changed persisted rows", before, after, err)
	}
}

func TestReplayCorpusIdentityAndPolicyValidation(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PARTLOG_BENCH_METADATA", "1")
	if err := logBacklogBenchmark(root, "write", 4200, 420, 3, 2); err != nil {
		t.Fatal(err)
	}
	c, err := readReplayCorpus(root)
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(root, "corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"identity", "policy", "clock"} {
		changed := c
		switch mutation {
		case "identity":
			changed.Identity = "ffffffffffffffffffffffffffffffff"
		case "policy":
			changed.PolicyRule = "schema=0"
		case "clock":
			changed.ApplyRule = "clock=replay-time"
		}
		data, err := json.Marshal(changed)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, "corpus.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = readReplayCorpus(root); err == nil {
			t.Fatalf("accepted mutated%s", mutation)
		}
	}
	if err = os.WriteFile(filepath.Join(root, "corpus.json"), original, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = readReplayCorpus(root); err != nil {
		t.Fatal(err)
	}
	if err = writeReplayCorpus(root, c.Identity, c.Historical, c.Metrics, c.Producers); err == nil {
		t.Fatal("overwrote original catalog")
	}
}

func TestFixtureResume(t *testing.T) {
	root := t.TempDir()
	const total, metrics, producers = 100003, 420, 3
	journal, err := partlog.Open(filepath.Join(root, "log"), partlog.Options{Streams: producers})
	if err != nil {
		t.Fatal(err)
	}
	if err = writeReplayCorpus(root, journal.Identity(), total, metrics, producers); err != nil {
		t.Fatal(err)
	}
	for p := range producers {
		var rows []row
		for i := 0; i < 17+p; i++ {
			seq := uint64(i*producers + p)
			rows = append(rows, row{ID: seq % metrics, Time: 1800000000 + int64(seq/6000000), Bits: math.Float64bits(float64(seq % 100003)), Seq: seq})
		}
		if _, err = journal.Append(p, 1800000000, encodeLogRows(rows)); err != nil {
			t.Fatal(err)
		}
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PARTLOG_BENCH_METADATA", "1")
	t.Setenv("PARTLOG_BENCH_RESUME", "1")
	t.Setenv("PARTLOG_BENCH_WRITE_RATE", "1000000000")
	if err = logBacklogBenchmark(root, "write", total, metrics, producers, 2); err != nil {
		t.Fatal(err)
	}
	if _, err = retainedLiveCounts(root, total, producers); err != nil {
		t.Fatal(err)
	}
	if err = logBacklogBenchmark(root, "decode", total, metrics, producers, 2); err != nil {
		t.Fatal(err)
	}
	if err = logBacklogBenchmark(root, "write", total+1, metrics, producers, 2); err == nil {
		t.Fatal("resumed with a changed historical declaration")
	}
	journal, err = partlog.Open(filepath.Join(root, "log"), partlog.Options{Streams: producers})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = journal.Append(0, 1800000000, encodeLogRows([]row{{Seq: 0, Bits: 1}})); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err = logBacklogBenchmark(root, "write", total, metrics, producers, 2); err == nil {
		t.Fatal("resumed a corrupt retained prefix")
	}
}
