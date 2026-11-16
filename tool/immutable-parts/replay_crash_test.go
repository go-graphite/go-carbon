package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
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
	catalogs, e := openReplayCatalogs(filepath.Join(root, "log"), producers, false)
	if e != nil {
		return nil, e
	}
	defer closeReplayCatalogs(catalogs)
	counts := make([]uint64, producers)
	check := func(p int) error {
		historical := uint64(0)
		if total > uint64(p) {
			historical = (total - uint64(p) + uint64(producers) - 1) / uint64(producers)
		}
		var seen uint64
		var buffer []row
		e := log.Replay(p, 0, func(r partlog.Record) error {
			var rows []row
			var e error
			if partlog.IsCatalogRecord(r.Payload) {
				if len(catalogs) != producers {
					return errors.New("missing numeric PDC4 catalog")
				}
				rows, e = catalogs[p].decode(buffer, r.Payload)
			} else {
				rows, e = decodeLogRowsInto(buffer, r.Payload)
			}
			buffer = rows
			if e != nil {
				return e
			}
			for _, point := range rows {
				var want uint64
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
			return e
		}
		if seen < historical {
			return fmt.Errorf("stream%d lost historical prefix", p)
		}
		return nil
	}
	jobs := make(chan int)
	results := make(chan error, producers)
	var workers sync.WaitGroup
	for range min(producers, max(1, min(8, runtime.GOMAXPROCS(0)))) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for p := range jobs {
				results <- check(p)
			}
		}()
	}
	go func() {
		for p := range producers {
			jobs <- p
		}
		close(jobs)
		workers.Wait()
		close(results)
	}()
	var result error
	for e := range results {
		result = errors.Join(result, e)
	}
	if result != nil {
		return nil, result
	}

	return counts, nil
}

// The source fixture is prepared once. Each trial clones its immutable journal,
// starts replay with live acceptance, kills at a seeded committed-point prefix,
// then reopens, drains and verifies every retained row. Metadata-bearing corpora
// additionally query420 sampled metrics from this actual recovered store.
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
			if data, e := os.ReadFile(filepath.Join(root, "fixture", "corpus.json")); e == nil {
				if e = os.WriteFile(filepath.Join(trialRoot, "corpus.json"), data, 0600); e != nil {
					t.Fatal(e)
				}
			} else if !os.IsNotExist(e) {
				t.Fatal(e)
			}
			stageStart := time.Now()
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
			type ackReceipt struct {
				Identity string
				Counts   []uint64
			}
			type killTarget struct {
				Committed uint64
				Ack       ackReceipt
			}
			reached := make(chan killTarget, 1)
			go func() {
				var ack ackReceipt
				scan := bufio.NewScanner(pipe)
				scan.Buffer(make([]byte, 65536), 4<<20)
				for scan.Scan() {
					line := append([]byte(nil), scan.Bytes()...)
					_, _ = output.Write(append(line, '\n'))
					var sample map[string]any
					if json.Unmarshal(line, &sample) != nil {
						continue
					}
					if sample["stage"] == "durable_ack_frontier" {
						data, _ := json.Marshal(sample["live_counts"])
						if e := json.Unmarshal(data, &ack.Counts); e != nil {
							continue
						}
						ack.Identity, _ = sample["identity"].(string)
					}
					if sample["stage"] == "concurrent_live_replay" && ack.Identity != "" && len(ack.Counts) == producers {
						if n, ok := sample["committed"].(float64); ok && uint64(n) >= target {
							reached <- killTarget{uint64(n), ack}
							return
						}
					}
				}
				reached <- killTarget{}
			}()
			var killed killTarget
			select {
			case killed = <-reached:
			case <-time.After(180 * time.Second):
				t.Fatal("crash prefix timeout")
			}
			if killed.Committed == 0 {
				t.Fatal("child ended before crash target")
			}
			// Parent persists the pre-kill receipt; recovery cannot fabricate it.
			ackPath := filepath.Join(root, fmt.Sprintf("crash-%d-ack.json", trial))
			ackFile, e := os.OpenFile(ackPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if e != nil {
				t.Fatal(e)
			}
			data, e := json.Marshal(killed)
			if e == nil {
				_, e = ackFile.Write(data)
			}
			e = errors.Join(e, ackFile.Sync(), ackFile.Close())
			if e != nil {
				t.Fatal(e)
			}
			if e = cmd.Process.Kill(); e != nil {
				t.Fatal(e)
			}
			_ = cmd.Wait()
			t.Logf("SIGKILL trial%d target%d killed_committed%d elapsed%s prekill_ack=%v", trial, target, killed.Committed, time.Since(stageStart), killed.Ack.Counts)
			prefixStart := time.Now()
			counts, e := retainedLiveCounts(trialRoot, total, producers)
			if e != nil {
				t.Fatal(e)
			}
			for p, n := range killed.Ack.Counts {
				if counts[p] < n {
					t.Fatalf("acknowledged stream%d lost%d points", p, n-counts[p])
				}
			}
			c, e := os.ReadFile(filepath.Join(trialRoot, "log", "IDENTITY"))
			if e != nil {
				t.Fatal(e)
			}
			var identity struct{ ID string }
			if e = json.Unmarshal(c, &identity); e != nil || identity.ID != killed.Ack.Identity {
				t.Fatal("ACK journal identity mismatch", e)
			}
			t.Logf("trial%d retained_prefix_seconds=%.3f durable_ack_exact=true", trial, time.Since(prefixStart).Seconds())
			data, e = json.Marshal(counts)
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
			rendered := "unrun"
			if _, e := os.Stat(filepath.Join(trialRoot, "corpus.json")); e == nil {
				render := exec.Command(os.Args[0], "-test.v", "-test.run=^TestFullScaleRenderedCorpus$", "-test.timeout=180s")
				render.Env = append(os.Environ(), "PARTLOG_RENDER_ROOT="+trialRoot)
				out, e := render.CombinedOutput()
				if e != nil || !bytes.Contains(out, []byte("full_corpus_render_exact")) {
					t.Fatalf("full corpus render: %v %s", e, out)
				}
				t.Logf("%s", out)
				rendered = "1680_queries_exact"
			}
			t.Logf("SIGKILL trial%d target%d killed_committed%d historical%d retained_live%d raw_exact=true rendered=%s durable_ack_exact=true elapsed_seconds=%.3f", trial, target, killed.Committed, total, live, rendered, time.Since(stageStart).Seconds())
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
	cs, e := openReplayCatalogs(filepath.Join(root, "log"), 3, true)
	if e != nil {
		t.Fatal(e)
	}
	j, e := partlog.Open(filepath.Join(root, "log"), partlog.Options{Streams: 3})
	if e != nil {
		t.Fatal(e)
	}
	for p := range 3 {
		payload, err := cs[p].encode([]row{{ID: uint64(p), Seq: 100003 + uint64(p)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = j.Append(p, 0, payload); err != nil {
			t.Fatal(err)
		}
	}
	if e = errors.Join(j.Close(), closeReplayCatalogs(cs)); e != nil {
		t.Fatal(e)
	}
	counts, e = retainedLiveCounts(root, 100003, 3)
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range counts {
		if n != 1 {
			t.Fatal("PDC4 retained live prefix", counts)
		}
	}
}

// Recover an externally interrupted benchmark without manufacturing its durable
// frontier. The receipt must have been emitted before the interruption.
func TestRecoverRetainedCorpus(t *testing.T) {
	root := os.Getenv("PARTLOG_RECOVER_ROOT")
	if root == "" {
		t.Skip("offline retained-corpus recovery")
	}
	total, err := strconv.ParseUint(os.Getenv("PARTLOG_BENCH_POINTS"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	producers, err := strconv.Atoi(os.Getenv("PARTLOG_BENCH_PRODUCERS"))
	if err != nil || producers < 1 {
		t.Fatal("invalid producers", err)
	}
	owners, err := strconv.Atoi(os.Getenv("PARTLOG_BENCH_OWNERS"))
	if err != nil || owners < 1 {
		t.Fatal("invalid owners", err)
	}
	data, err := os.ReadFile(os.Getenv("PARTLOG_RECOVER_ACK"))
	if err != nil {
		t.Fatal(err)
	}
	var ack struct {
		Identity string   `json:"identity"`
		Counts   []uint64 `json:"live_counts"`
	}
	if err = json.Unmarshal(data, &ack); err != nil || ack.Identity == "" || len(ack.Counts) != producers {
		t.Fatal("invalid pre-interruption receipt", err)
	}
	data, err = os.ReadFile(filepath.Join(root, "log", "IDENTITY"))
	if err != nil {
		t.Fatal(err)
	}
	var identity struct{ ID string }
	if err = json.Unmarshal(data, &identity); err != nil || identity.ID != ack.Identity {
		t.Fatal("receipt identity mismatch", err)
	}
	counts, err := retainedLiveCounts(root, total, producers)
	if err != nil {
		t.Fatal(err)
	}
	var retained uint64
	for p, n := range counts {
		if n < ack.Counts[p] {
			t.Fatalf("stream%d lost acknowledged prefix: retained%d ack%d", p, n, ack.Counts[p])
		}
		retained += n
	}
	data, err = json.Marshal(counts)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "live-counts.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := coreConfig{AppendParts: true, Shards: owners, Producers: producers, MaxRows: 1 << 20, PartitionSeconds: 3600, RawCodec: os.Getenv("CORE_RAW_CODEC")}
	l, err := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{ReadAheadBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	err = l.replay()
	err = errors.Join(err, l.close())
	if err != nil {
		t.Fatal(err)
	}
	if err = verifyLogBenchmark(root, total, 36000000, producers, owners, func(stage string, points uint64, extra map[string]any) {
		t.Logf("%s points%d %v", stage, points, extra)
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("retained_corpus_recovered historical%d live%d durable_ack_exact=true raw_exact=true", total, retained)
}
