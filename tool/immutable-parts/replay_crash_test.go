package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
)

// retainedLiveCounts validates producer-prefix coverage from recovered journal
// contents. SIGKILL may tear unsynced tail frames; no file written after the
// crash is allowed to manufacture the acknowledged/durable frontier.
func retainedLiveCounts(root string, total uint64, producers int) ([]uint64, error) {
	log, e := partlog.Open(filepath.Join(root, "log"), partlog.Options{Streams: producers})
	if e != nil {
		return nil, e
	}
	defer log.Close()
	counts := make([]uint64, producers)
	for p := range producers {
		var seen uint64
		e = log.Replay(p, 0, func(r partlog.Record) error {
			rows, e := decodeLogRows(r.Payload)
			if e != nil {
				return e
			}
			for _, point := range rows {
				var want uint64
				historical := uint64(0)
				if total > uint64(p) {
					historical = (total - uint64(p) + uint64(producers) - 1) / uint64(producers)
				}
				if seen < historical {
					want = seen*uint64(producers) + uint64(p)
				} else {
					want = total + (seen-historical)*uint64(producers) + uint64(p)
					counts[p]++
				}
				if point.Seq != want {
					return fmt.Errorf("stream%d nonprefix sequence%d want%d", p, point.Seq, want)
				}
				seen++
			}
			return nil
		})
		if e != nil {
			return nil, e
		}
		historical := uint64(0)
		if total > uint64(p) {
			historical = (total - uint64(p) + uint64(producers) - 1) / uint64(producers)
		}
		if seen < historical {
			return nil, fmt.Errorf("stream%d lost historical prefix", p)
		}
	}
	return counts, nil
}

// The source fixture is prepared once. Each trial clones its immutable journal,
// starts replay with live acceptance, kills at a seeded committed-point prefix,
// then reopens, drains and verifies every retained row. The numeric corpus has
// no render policy; the rendered differential is a separate tagged fixture.
func TestFullScaleReplayCrashes(t *testing.T) {
	root := os.Getenv("PARTLOG_CRASH_ROOT")
	if root == "" {
		t.Skip("offline full-scale crash qualification")
	}
	total, e := strconv.ParseUint(os.Getenv("PARTLOG_BENCH_POINTS"), 10, 64)
	if e != nil {
		t.Fatal(e)
	}
	producers, e := strconv.Atoi(os.Getenv("PARTLOG_BENCH_PRODUCERS"))
	if e != nil || producers < 1 {
		t.Fatal("invalid producers", e)
	}
	owners, e := strconv.Atoi(os.Getenv("PARTLOG_BENCH_OWNERS"))
	if e != nil || owners < 1 {
		t.Fatal("invalid owners", e)
	}
	const metrics = 36000000
	cfg := coreConfig{AppendParts: true, Shards: owners, Producers: producers, MaxRows: 1 << 20, PartitionSeconds: 3600, RawCodec: os.Getenv("CORE_RAW_CODEC")}
	if os.Getenv("PARTLOG_CRASH_CHILD") == "yes" {
		start := time.Now()
		e := logReplayLive(root, cfg, partlog.Options{Streams: producers, SegmentBytes: 64 << 20, SyncInterval: time.Second, ReadAheadBytes: 4 << 20}, total, metrics, func(stage string, points uint64, extra map[string]any) {
			if extra == nil {
				extra = map[string]any{}
			}
			extra["stage"] = stage
			extra["points"] = points
			_ = json.NewEncoder(os.Stdout).Encode(extra)
		}, start)
		t.Fatal("crash child unexpectedly completed", e)
	}
	rng := rand.New(rand.NewPCG(20261116, 7))
	for trial := range 3 {
		t.Run(fmt.Sprint(trial), func(t *testing.T) {
			trialRoot := filepath.Join(root, fmt.Sprintf("crash-%d", trial))
			if e = os.Mkdir(trialRoot, 0750); e != nil {
				t.Fatal(e)
			}
			defer os.RemoveAll(trialRoot)
			cmd := exec.Command("cp", "-a", "--reflink=auto", filepath.Join(root, "fixture", "log"), filepath.Join(trialRoot, "log"))
			if out, e := cmd.CombinedOutput(); e != nil {
				t.Fatalf("journal clone: %v %s", e, out)
			}
			target := total * uint64(10+rng.IntN(65)) / 100
			cmd = exec.Command(os.Args[0], "-test.run=^TestFullScaleReplayCrashes$", "-test.timeout=600s")
			cmd.Env = append(os.Environ(), "PARTLOG_CRASH_ROOT="+trialRoot, "PARTLOG_CRASH_CHILD=yes")
			pipe, e := cmd.StdoutPipe()
			if e != nil {
				t.Fatal(e)
			}
			output, e := os.Create(filepath.Join(root, fmt.Sprintf("crash-%d-child.log", trial)))
			if e != nil {
				t.Fatal(e)
			}
			defer output.Close()
			cmd.Stderr = output
			if e = cmd.Start(); e != nil {
				t.Fatal(e)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			reached := make(chan uint64, 1)
			go func() {
				scan := bufio.NewScanner(pipe)
				scan.Buffer(make([]byte, 65536), 4<<20)
				for scan.Scan() {
					line := append([]byte(nil), scan.Bytes()...)
					_, _ = output.Write(append(line, '\n'))
					var sample map[string]any
					if json.Unmarshal(line, &sample) == nil && sample["stage"] == "concurrent_live_replay" {
						if n, ok := sample["committed"].(float64); ok && uint64(n) >= target {
							reached <- uint64(n)
							return
						}
					}
				}
				reached <- 0
			}()
			var killedAt uint64
			select {
			case killedAt = <-reached:
			case <-time.After(180 * time.Second):
				t.Fatal("crash prefix timeout")
			}
			if killedAt == 0 {
				t.Fatal("child ended before crash target")
			}
			if e = cmd.Process.Kill(); e != nil {
				t.Fatal(e)
			}
			_ = cmd.Wait()
			counts, e := retainedLiveCounts(trialRoot, total, producers)
			if e != nil {
				t.Fatal(e)
			}
			data, e := json.Marshal(counts)
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(filepath.Join(trialRoot, "live-counts.json"), data, 0600); e != nil {
				t.Fatal(e)
			}
			recovered, e := openLoggedCore(filepath.Join(trialRoot, "log"), filepath.Join(trialRoot, "parts"), cfg, partlog.Options{ReadAheadBytes: 4 << 20})
			if e != nil {
				t.Fatal(e)
			}
			if e = recovered.replay(); e != nil {
				_ = recovered.close()
				t.Fatal(e)
			}
			if e = recovered.close(); e != nil {
				t.Fatal(e)
			}
			if e = verifyLogBenchmark(trialRoot, total, metrics, producers, owners, func(stage string, points uint64, extra map[string]any) {
				t.Logf("trial%d %s points%d %v", trial, stage, points, extra)
			}); e != nil {
				t.Fatal(e)
			}
			var live uint64
			for _, n := range counts {
				live += n
			}
			t.Logf("SIGKILL trial%d target%d killed_committed%d historical%d retained_live%d raw_exact=true rendered=unrun", trial, target, killedAt, total, live)
		})
	}
}

func TestRetainedCrashCoverage(t *testing.T) {
	root := t.TempDir()
	if e := logBacklogBenchmark(root, "write", 100003, 101, 3, 2); e != nil {
		t.Fatal(e)
	}
	counts, e := retainedLiveCounts(root, 100003, 3)
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range counts {
		if n != 0 {
			t.Fatal(counts)
		}
	}
	if _, e = retainedLiveCounts(root, 100004, 3); e == nil {
		t.Fatal("accepted incomplete historical prefix")
	}
}
