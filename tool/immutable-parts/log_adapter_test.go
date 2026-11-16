package main

import (
	"context"
	"crypto/sha256"
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

	"github.com/cockroachdb/pebble"
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
		{ID: 1, Order: 7, Clock: -123, Policy: []byte("exact-policy-a"), Points: []row{{ID: 1, Time: 991, Bits: 0x7ff8000000000042, Seq: 4}, {ID: 1, Time: 991, Bits: 0x8000000000000000, Seq: 5}}},
		{ID: 1, Order: 8, Clock: 17, Policy: []byte("exact-policy-b"), Points: []row{{ID: 1, Time: 992, Bits: 1, Seq: 6}}},
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
			hooks := coreHooks{}
			if os.Getenv("PARTLOG_CRASH_ROOT") != "" && os.Getenv("PARTLOG_CRASH_TRIAL") != "" {
				trial, _ := strconv.Atoi(os.Getenv("PARTLOG_CRASH_TRIAL"))
				if trial%3 == 1 {
					hooks.PlanSeal = func(owner int, in coreSealInput) (coreSealPlan, error) {
						if in.Next.Cursors[0] >= 9 {
							if owner != 0 {
								select {}
							}
							return coreSealPlan{Commit: func() {
								_ = os.WriteFile(filepath.Join(os.Getenv("PARTLOG_CRASH_ROOT"), "ready"), []byte("mid-owner-seal"), 0600)
							}}, nil
						}
						return coreSealPlan{}, nil
					}
				}
			}
			return openLoggedCoreWithHooks(logDir, partDir, cfg, opts, hooks)
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
			if trial%3 == 1 {
				var groups []coreBatch
				for _, r := range coreRows(0, i) {
					groups = append(groups, coreBatch{ID: r.ID, Order: uint64(i), Clock: int64(1800000000 + i), Policy: []byte("crash-test"), Points: []row{r}})
				}
				e = l.appendBatches(0, groups, i%7 == 0 || i == 9)
			} else {
				e = l.append(0, int64(1800000000+i), coreRows(0, i), i%7 == 0)
			}
			if e != nil {
				panic(e)
			}
			if i == 8 {
				if e = l.journal.Sync(); e != nil {
					panic(e)
				}
				if trial%3 == 1 {
					continue
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
				if len(r.Payload) >= logEnvelopeBytes && r.Payload[8] == 1 {
					var groups []coreBatch
					groups, e = decodeLogBatches(r.Payload)
					for _, g := range groups {
						rows = append(rows, g.Points...)
					}
				}
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
	_, err = journal.Append(0, -123, encodeLogRows([]row{{ID: 1, Time: -3, Bits: 0x7ff8000000000042, Seq: 99}}))
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
					r := row{ID: seq % 11, Time: 1800000000 + int64(seq/6000000), Bits: math.Float64bits(float64(seq % 100003)), Seq: seq}
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

func coreTestConfig() coreConfig {
	return coreConfig{AppendParts: true, Shards: 4, Producers: 2, MaxRows: 64, PartitionSeconds: 60}
}
func coreRows(producer, batch int) []row {
	var out []row
	for i := 0; i < 43; i++ {
		seq := uint64(batch*1000 + producer*100 + i)
		bits := math.Float64bits(float64(seq))
		switch i % 9 {
		case 0:
			bits = 0x8000000000000000
		case 1:
			bits = 0x7ff8000000000042
		case 2:
			bits = 1
		case 3:
			bits = 0xfff0000000000000
		}
		out = append(out, row{ID: uint64(i % 11), Time: int64(-90 + batch*13 + i%7), Bits: bits, Seq: seq})
	}
	return out
}
func coreDigest(root string, cfg coreConfig) (string, error) {
	h := sha256.New()
	for s := 0; s < cfg.Shards; s++ {
		db, e := pebble.Open(filepath.Join(root, fmt.Sprintf("owner-%03d", s)), &pebble.Options{})
		if e != nil {
			return "", e
		}
		snap := db.NewSnapshot()
		for id := uint64(0); id < 11; id++ {
			if coreShard(id, cfg.Shards) != s {
				continue
			}
			rs, e := coreRead(snap, id)
			if e != nil {
				return "", e
			}
			for _, r := range rs {
				var b [32]byte
				putRow(b[:], r)
				_, _ = h.Write(b[:])
			}
		}
		_ = snap.Close()
		if e = db.Close(); e != nil {
			return "", e
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
