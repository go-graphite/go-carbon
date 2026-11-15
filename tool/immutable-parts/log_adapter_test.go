package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
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

// The child continuously appends through the same path as replay. The parent
// sends SIGKILL with seeded offsets around writes, sync and owner publication.
// This proves process-crash recovery, not power-failure behavior of a device.
func TestLogSIGKILLRecovery(t *testing.T) {
	if root := os.Getenv("PARTLOG_CRASH_ROOT"); root != "" {
		trial, _ := strconv.Atoi(os.Getenv("PARTLOG_CRASH_TRIAL"))
		cfg := loggedConfig()
		hook := func(_ int, phase string) {
			if phase == "before-sync" {
				_ = os.WriteFile(filepath.Join(root, "sync-start"), []byte("sync"), 0600)
			}
		}
		l, e := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{SyncInterval: 3 * time.Millisecond, SegmentBytes: 1 << 20, Hook: hook})
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
			l, e := openLoggedCore(filepath.Join(root, "log"), filepath.Join(root, "parts"), cfg, partlog.Options{})
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
