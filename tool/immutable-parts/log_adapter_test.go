package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
)

func loggedConfig() coreConfig { c := coreTestConfig(); c.Producers = 1; c.MaxRows = 512; return c }
func TestLogNormalReplayAndTrim(t *testing.T) {
	root := t.TempDir()
	cfg := loggedConfig()
	l, e := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{SegmentBytes: 1500})
	if e != nil {
		t.Fatal(e)
	}
	for i := 1; i <= 9; i++ {
		if e = l.append(0, int64(i), coreRows(0, i), true); e != nil {
			t.Fatal(e)
		}
	}
	if e = l.trim(); e != nil {
		t.Fatal(e)
	}
	if e = l.close(); e != nil {
		t.Fatal(e)
	}
	cfg.JournalIdentity = l.journal.Identity()
	want, e := coreDigest(filepath.Join(root, "parts"), cfg)
	if e != nil {
		t.Fatal(e)
	}
	l, e = openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{SegmentBytes: 1500})
	if e != nil {
		t.Fatal(e)
	}
	if e = l.replay(); e != nil {
		t.Fatal(e)
	}
	if e = l.close(); e != nil {
		t.Fatal(e)
	}
	got, e := coreDigest(filepath.Join(root, "parts"), cfg)
	if e != nil || got != want {
		t.Fatal(want, got, e)
	}
}

func TestLogOriginalBatchReplay(t *testing.T) {
	root := t.TempDir()
	cfg := loggedConfig()
	cfg.MaxRows = 1 << 20
	want := []coreBatch{
		{ID: 1, Order: 7, Clock: -123, Policy: []byte("exact-policy-a"), Points: []row{{1, 991, 0x7ff8000000000042, 4}, {1, 991, 0x8000000000000000, 5}}},
		{ID: 1, Order: 8, Clock: 17, Policy: []byte("exact-policy-b"), Points: []row{{1, 992, 1, 6}}},
	}
	var got []coreBatch
	hooks := coreHooks{PlanSeal: func(_ int, in coreSealInput) (coreSealPlan, error) {
		if len(in.Batches) == 0 {
			return coreSealPlan{}, nil
		}
		got = append(got, in.Batches...)
		return coreSealPlan{}, nil
	}}
	l, e := openLoggedCoreWithHooks(filepath.Join(root, "log"), filepath.Join(root, "live"), cfg, partlog.Options{}, hooks)
	if e != nil {
		t.Fatal(e)
	}
	if e = l.appendBatches(0, want, true); e != nil {
		t.Fatal(e)
	}
	if e = l.close(); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("live metadata changed: %#v", got)
	}
	got = nil
	l, e = openLoggedCoreWithHooks(filepath.Join(root, "log"), filepath.Join(root, "restart"), cfg, partlog.Options{}, hooks)
	if e != nil {
		t.Fatal(e)
	}
	if e = l.replay(); e != nil {
		t.Fatal(e)
	}
	if e = l.close(); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replay metadata changed: %#v", got)
	}
	raw, e := encodeLogBatches(want)
	if e != nil {
		t.Fatal(e)
	}
	for cut := 0; cut < len(raw); cut++ {
		if _, e = decodeLogBatches(raw[:cut]); e == nil {
			t.Fatalf("truncation accepted at%d", cut)
		}
	}
}

// The child continuously appends through the same path as replay. The parent
// sends SIGKILL with seeded offsets around writes, sync and owner publication.
// This proves process-crash recovery, not power-failure behavior of a device.
func TestLogSIGKILLRecovery(t *testing.T) {
	openCrashCore := func(logDir, partDir string, cfg coreConfig, opts partlog.Options) (*loggedCore, error) {
		if os.Getenv("PARTLOG_CRASH_ADMISSION") != "1" {
			return openLoggedCore(logDir, partDir, cfg, opts)
		}
		opts.Streams = cfg.Producers
		journal, err := partlog.OpenAdmission(logDir, opts)
		if err != nil {
			return nil, err
		}
		return attachLoggedCore(journal, partDir, cfg, coreHooks{})
	}
	if root := os.Getenv("PARTLOG_CRASH_ROOT"); root != "" {
		trial, _ := strconv.Atoi(os.Getenv("PARTLOG_CRASH_TRIAL"))
		cfg := loggedConfig()
		hook := func(_ int, phase string) {
			if phase == "before-sync" {
				_ = os.WriteFile(filepath.Join(root, "sync-start"), []byte("sync"), 0600)
			}
		}
		l, e := openCrashCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{SyncInterval: 3 * time.Millisecond, SegmentBytes: 1 << 20, Hook: hook})
		if e != nil {
			panic(e)
		}
		for i := 1; ; i++ {
			// Seed a stable durable prefix; remaining callbacks race the kill.
			if e = l.append(0, int64(1800000000+i), coreRows(0, i), i%7 == 0); e != nil {
				panic(e)
			}
			if i == 8 {
				if e = l.journal.Sync(); e != nil {
					panic(e)
				}
				// Explicitly pause one owner after it has committed the next record,
				// before other owners see it: crash inside a multi-owner seal.
				if trial%3 == 1 {
					rows := coreRows(0, 9)
					cursor, e := l.journal.Append(0, 1800000009, encodeLogRows(rows))
					if e != nil {
						panic(e)
					}
					owner := l.engine.owners[0]
					var ahead []row
					for _, r := range rows {
						if coreShard(r.ID, cfg.Shards) == 0 {
							ahead = append(ahead, r)
						}
					}
					ack := make(chan error, 1)
					if e = owner.queues[0].push(&corePacket{rows: ahead, cursor: cursor, seal: true, ack: ack}, owner.dead); e != nil {
						panic(e)
					}
					if e = <-ack; e != nil {
						panic(e)
					}
					if e = os.WriteFile(filepath.Join(root, "ready"), []byte("mid-owner-seal"), 0600); e != nil {
						panic(e)
					}
					select {}
				}
				if e = os.WriteFile(filepath.Join(root, "ready"), []byte("ingesting"), 0600); e != nil {
					panic(e)
				}
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
	rng := rand.New(rand.NewPCG(42, 99))
	for trial := 0; trial < 12; trial++ {
		t.Run(fmt.Sprint(trial), func(t *testing.T) {
			root := t.TempDir()
			output, e := os.Create(filepath.Join(root, "child.log"))
			if e != nil {
				t.Fatal(e)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLogSIGKILLRecovery$")
			cmd.Env = append(os.Environ(), "PARTLOG_CRASH_ROOT="+root, fmt.Sprintf("PARTLOG_CRASH_TRIAL=%d", trial))
			cmd.Stdout = output
			cmd.Stderr = output
			if e = cmd.Start(); e != nil {
				t.Fatal(e)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = output.Close() }()
			until := time.Now().Add(15 * time.Second)
			for {
				if _, e = os.Stat(filepath.Join(root, "ready")); e == nil {
					break
				}
				if time.Now().After(until) {
					t.Fatal("child not ready")
				}
				time.Sleep(time.Millisecond)
			}
			time.Sleep(time.Duration(rng.IntN(12000)) * time.Microsecond)
			if e = cmd.Process.Kill(); e != nil {
				t.Fatal(e)
			}
			_ = cmd.Wait()
			_ = output.Close()
			cfg := loggedConfig()
			l, e := openCrashCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{})
			if e != nil {
				t.Fatal(e)
			}
			cfg.JournalIdentity = l.journal.Identity()
			var retained []partlog.Record
			if e = l.journal.Replay(0, 0, func(r partlog.Record) error {
				r.Payload = slices.Clone(r.Payload)
				retained = append(retained, r)
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			if len(retained) < 8 {
				t.Fatalf("synced prefix lost: %d", len(retained))
			}
			if e = l.replay(); e != nil {
				t.Fatal(e)
			}
			if e = l.close(); e != nil {
				t.Fatal(e)
			}
			got, e := coreDigest(filepath.Join(root, "parts"), cfg)
			if e != nil {
				t.Fatal(e)
			}
			// Uninterrupted reference gets exactly the recoverable complete log prefix.
			reference := filepath.Join(root, "reference")
			ref, e := openCore(reference, cfg)
			if e != nil {
				t.Fatal(e)
			}
			for _, r := range retained {
				rows, e := decodeLogRows(r.Payload)
				if e != nil {
					t.Fatal(e)
				}
				if e = ref.submit(0, rows, r.Cursor, false); e != nil {
					t.Fatal(e)
				}
			}
			if e = ref.close(); e != nil {
				t.Fatal(e)
			}
			want, e := coreDigest(reference, cfg)
			if e != nil || got != want {
				t.Fatalf("exact raw row hash mismatch got%s want%s err%v", got, want, e)
			}
			receipt := map[string]any{"trial": trial, "retained_batches": len(retained), "durable_prefix": 8, "raw_sha256": got, "kill_mode": []string{"random-ingest", "between-owner-seals", "random-ingest-periodic-sync"}[trial%3]}
			b, _ := json.Marshal(receipt)
			t.Log(string(b))
		})
	}
}

func TestLogBacklogBenchmark(t *testing.T) {
	root := os.Getenv("PARTLOG_BENCH_ROOT")
	if root == "" {
		t.Skip("offline native benchmark only")
	}
	total, e := strconv.ParseUint(os.Getenv("PARTLOG_BENCH_POINTS"), 10, 64)
	if e != nil {
		t.Fatal(e)
	}
	producers, e := strconv.Atoi(os.Getenv("PARTLOG_BENCH_PRODUCERS"))
	if e != nil {
		t.Fatal(e)
	}
	owners, e := strconv.Atoi(os.Getenv("PARTLOG_BENCH_OWNERS"))
	if e != nil {
		t.Fatal(e)
	}
	if e = logBacklogBenchmark(root, os.Getenv("PARTLOG_BENCH_PHASE"), total, 36000000, producers, owners); e != nil {
		t.Fatal(e)
	}
}

// This opt-in process-start probe holds historical validation until a real
// append succeeds. It measures journal admission only, without opening Pebble.
func TestLogAdmissionExecBenchmark(t *testing.T) {
	root := os.Getenv("PARTLOG_ADMISSION_BENCH_ROOT")
	if root == "" {
		t.Skip("set PARTLOG_ADMISSION_BENCH_ROOT for the process-start probe")
	}
	streams, err := strconv.Atoi(os.Getenv("PARTLOG_BENCH_PRODUCERS"))
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	start := time.Now()
	stages := make(map[string]float64)
	journal, err := partlog.OpenAdmission(filepath.Join(root, "log"), partlog.Options{Streams: streams, Hook: func(_ int, phase string) {
		switch phase {
		case "admission-frontiers-ready", "admission-fence-ready", "admission-owners-ready":
			stages[phase] = time.Since(start).Seconds() * 1000
		}
		if phase == "recover-scan" {
			<-release
		}
	}})
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	_, err = journal.Append(0, -123, encodeLogRows([]row{{1, -3, 0x7ff8000000000042, 99}}))
	accepted := time.Now()
	close(release)
	if err != nil {
		_ = journal.Close()
		t.Fatal(err)
	}
	execNS, err := strconv.ParseInt(os.Getenv("PARTLOG_BENCH_EXEC_UNIX_NS"), 10, 64)
	if err != nil || execNS <= 0 {
		_ = journal.Close()
		t.Fatal("missing wrapper exec timestamp", err)
	}
	receipt := map[string]any{
		"stage": "first_live_append_accepted", "when_cet": accepted.In(time.FixedZone("CET", 3600)).Format(time.RFC3339Nano),
		"exec_to_first_accept_ms":    float64(accepted.UnixNano()-execNS) / 1e6,
		"journal_open_and_append_ms": accepted.Sub(start).Seconds() * 1000,
		"streams":                    streams, "historical_validation_held_until_accept": true,
		"admission_stages_ms": stages,
	}
	if err = json.NewEncoder(os.Stdout).Encode(receipt); err != nil {
		t.Error(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLogBenchmarkFullOracle(t *testing.T) {
	for _, mode := range []string{"valid", "duplicate", "missing", "bits"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			cfg := coreConfig{AppendParts: true, Shards: 2, Producers: 2, MaxRows: 64, PartitionSeconds: 3600}
			l, err := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{Streams: 2})
			if err != nil {
				t.Fatal(err)
			}
			for p := range 2 {
				var rows []row
				for seq := uint64(p); seq < 100; seq += 2 {
					if mode == "missing" && seq == 99 {
						continue
					}
					r := row{seq % 11, 1800000000 + int64(seq/6000000), math.Float64bits(float64(seq % 100003)), seq}
					if mode == "bits" && seq == 99 {
						r.Bits ^= 1
					}
					rows = append(rows, r)
					if mode == "duplicate" && seq == 99 {
						rows = append(rows, r)
					}
				}
				if err = l.append(p, 1800000000, rows, true); err != nil {
					t.Fatal(err)
				}
			}
			if err = l.close(); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(root, "live-counts.json"), []byte("[0,0]"), 0600); err != nil {
				t.Fatal(err)
			}
			err = verifyLogBenchmark(root, 100, 11, 2, 2, func(string, uint64, map[string]any) {})
			if (err == nil) != (mode == "valid") {
				t.Fatalf("mode%s err%v", mode, err)
			}
		})
	}
}

func TestLogConcurrentReplayLiveExact(t *testing.T) {
	root := t.TempDir()
	cfg := loggedConfig()
	cfg.Producers = 4
	logDir := filepath.Join(root, "log")
	journal, err := partlog.Open(logDir, partlog.Options{Streams: 4, SegmentBytes: 2000})
	if err != nil {
		t.Fatal(err)
	}
	for p := range cfg.Producers {
		for n := 1; n <= 10; n++ {
			if _, err = journal.Append(p, int64(n), encodeLogRows(coreRows(p, n))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := openLoggedCore(logDir, filepath.Join(root, "concurrent"), cfg, partlog.Options{SegmentBytes: 2000})
	if err != nil {
		t.Fatal(err)
	}
	ready := l.startReplay(context.Background())
	done := make(chan error, cfg.Producers)
	for p := range cfg.Producers {
		go func() {
			for n := 11; n <= 20; n++ {
				if err := l.append(p, int64(n), coreRows(p, n), false); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}
	for range cfg.Producers {
		if err = <-done; err != nil {
			t.Fatal(err)
		}
	}
	if err = <-ready; err != nil {
		t.Fatal(err)
	}
	if err = l.close(); err != nil {
		t.Fatal(err)
	}
	cfg.JournalIdentity = l.journal.Identity()
	want, err := coreDigest(filepath.Join(root, "concurrent"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := openLoggedCore(logDir, filepath.Join(root, "oracle"), cfg, partlog.Options{SegmentBytes: 2000})
	if err != nil {
		t.Fatal(err)
	}
	if err = oracle.replay(); err != nil {
		t.Fatal(err)
	}
	if err = oracle.close(); err != nil {
		t.Fatal(err)
	}
	got, err := coreDigest(filepath.Join(root, "oracle"), cfg)
	if err != nil || got != want {
		t.Fatalf("concurrent replay/live differs: want%s got%s err%v", want, got, err)
	}
}
