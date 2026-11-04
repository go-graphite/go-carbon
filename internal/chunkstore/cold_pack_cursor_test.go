package chunkstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/lomik/zapwriter"
)

type cursorRepackCall struct {
	name  string
	block uint64
}

// Three independently published ranges per metric. Resuming a descriptor
// requires earlier publications to exist, including with unsynced hot writes.
type cursorTestCodec struct {
	bucketCodec
	calls        []cursorRepackCall
	skip         bool
	allowMissing bool // budget trials permit revision conflicts, retried next pass
}

func (c *cursorTestCodec) Create(ctx context.Context, io ContinuationIO, m Metadata, now func() time.Time) error {
	if err := c.bucketCodec.Create(ctx, io, m, now); err != nil {
		return err
	}
	return io.Set([]byte("test-name"), []byte(m.Name))
}

func (c *cursorTestCodec) Repack(_ context.Context, io ContinuationIO, r ColdPackRequest) (RepackStats, uint64, bool, error) {
	name, err := io.Get([]byte("test-name"))
	if err != nil {
		return RepackStats{}, 0, false, err
	}
	c.calls = append(c.calls, cursorRepackCall{string(name), r.Cursor})
	if c.skip {
		return RepackStats{Skipped: 1}, 0, true, nil
	}
	for i := uint64(0); !c.allowMissing && i < r.Cursor; i++ {
		if _, err := io.Get([]byte(fmt.Sprintf("packed/%d", i))); err != nil {
			return RepackStats{}, 0, false, fmt.Errorf("cursor skipped unpublished block %d: %w", i, err)
		}
	}
	err = io.Set([]byte(fmt.Sprintf("packed/%d", r.Cursor)), []byte("published"))
	return RepackStats{Blocks: 1}, r.Cursor + 1, r.Cursor == 2, err
}

func cursorTestOptions(fs vfs.FS, codec *cursorTestCodec) Options {
	return Options{fs: fs, ExactBlocks: true, TimeBuckets: true, Continuation: codec,
		CacheSize: 1 << 20, MemTableSize: 1 << 20, ColdPack: ColdPackOptions{Mode: "exact"}}
}

func cursorTestSteps(t *testing.T, s *Store, n int) bool {
	t.Helper()
	for range n {
		done, err := s.ColdPackStep(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return true
		}
	}
	return false
}

func TestColdPackCheckpointCrashResume(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		strict := vfs.NewStrictMem()
		codec := &cursorTestCodec{}
		o := cursorTestOptions(strict, codec)
		o.SyncInterval = time.Hour
		s, _ := openTestStore(t, "/store", o)
		s.stopColdPack()
		for _, name := range []string{"a", "b", "c"} {
			createTestMetric(t, s, name)
		}
		cursorTestSteps(t, s, 4) // all of a, first range of b
		if st := s.ColdPackStats(); st.Checkpoints != 1 || st.PersistedMetricsVisited != 0 {
			t.Fatalf("checkpoint written before interval: %+v", st)
		}
		time.Sleep(coldPackCheckpointInterval)
		cursorTestSteps(t, s, 1) // b's second range, checkpointed
		if st := s.ColdPackStats(); st.Checkpoints != 2 || st.PersistedMetricsVisited != 1 {
			t.Fatalf("missing timed checkpoint: %+v", st)
		}
		cursorTestSteps(t, s, 2) // uncheckpointed b completion and c's first range
		closeForRecovery(t, s, strict, true)
		codec.calls = nil
		defer zapwriter.Test()()
		s, _ = openTestStore(t, "/store", o)
		defer closeTestStore(t, s)
		s.stopColdPack()
		if st := s.ColdPackStats(); st.Cursor != "b" || st.BlockCursor != 2 || st.Resumed != 1 || st.MetricsVisited != 1 || st.PersistedMetricsVisited != 1 {
			t.Fatalf("did not resume durable position: %+v", st)
		}
		logs := zapwriter.TestString()
		if strings.Count(logs, "cold pack resumed") != 1 || !strings.Contains(logs, `"metric": "b"`) || !strings.Contains(logs, `"block_cursor": 2`) {
			t.Fatalf("missing resume position log: %s", logs)
		}
		if !cursorTestSteps(t, s, 10) {
			t.Fatal("resumed pass did not finish")
		}
		want := []cursorRepackCall{{"b", 2}, {"c", 0}, {"c", 1}, {"c", 2}}
		if !reflect.DeepEqual(codec.calls, want) {
			t.Fatalf("replayed work before checkpoint: %v, want %v", codec.calls, want)
		}
	})
}

func TestColdPackCheckpointPassAcrossRestarts(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		codec := &cursorTestCodec{}
		o := cursorTestOptions(vfs.NewMem(), codec)
		s, _ := openTestStore(t, "/store", o)
		s.stopColdPack()
		const metrics = ColdPackPageSize + 6
		for i := range metrics {
			createTestMetric(t, s, fmt.Sprintf("metric.%03d", i))
		}
		for restarts := 0; ; restarts++ {
			done := cursorTestSteps(t, s, 17)
			closeTestStore(t, s) // clean close checkpoints even below 30 seconds
			s, _ = openTestStore(t, "/store", o)
			s.stopColdPack()
			if s.ColdPackStats().Resumed != 1 {
				t.Fatal("lost progress on clean restart")
			}
			if done {
				break
			}
			if restarts > metrics {
				t.Fatal("pass never completed across restarts")
			}
		}
		defer closeTestStore(t, s)
		if st := s.ColdPackStats(); st.Passes != 1 || st.Cursor != "" || st.BlockCursor != 0 || st.MetricsVisited != 0 {
			t.Fatalf("completed pass not restored: %+v", st)
		}
		if len(codec.calls) != metrics*3 {
			t.Fatalf("reprocessed metrics: %d calls, want %d", len(codec.calls), metrics*3)
		}
		for i, call := range codec.calls {
			if want := (cursorRepackCall{fmt.Sprintf("metric.%03d", i/3), uint64(i % 3)}); call != want {
				t.Fatalf("call %d = %v, want %v", i, call, want)
			}
		}
	})
}

func TestColdPackCheckpointBackgroundStop(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		codec := &cursorTestCodec{}
		o := cursorTestOptions(vfs.NewMem(), codec)
		o.ColdPack.Mode = "off"
		s, _ := openTestStore(t, "/store", o)
		createTestMetric(t, s, "a")
		o.ColdPack.Mode, o.ColdPack.BytesPerSecond = "exact", 1
		if err := s.ConfigureColdPack(o.ColdPack); err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // first publication, followed by a long pacing wait
		if st := s.ColdPackStats(); st.BlockCursor != 1 || st.Checkpoints != 1 {
			t.Fatalf("worker did not stop in its first pacing wait: %+v", st)
		}
		s.StopBackground()
		if st := s.ColdPackStats(); st.Checkpoints != 2 {
			t.Fatalf("background stop missed uncheckpointed progress: %+v", st)
		}
		closeTestStore(t, s)
		s, _ = openTestStore(t, "/store", o)
		defer closeTestStore(t, s)
		s.stopColdPack()
		if st := s.ColdPackStats(); st.Cursor != "a" || st.BlockCursor != 1 || st.Resumed != 1 {
			t.Fatalf("background stop checkpoint not restored: %+v", st)
		}
	})
}

func TestColdPackCheckpointInvalidation(t *testing.T) {
	for _, change := range []string{"decimal", "unpack", "off", "window", "format", "budgets"} {
		t.Run(change, func(t *testing.T) {
			pebbleSynctest(t, func(t *testing.T) {
				codec := &cursorTestCodec{}
				o := cursorTestOptions(vfs.NewMem(), codec)
				s, _ := openTestStore(t, "/store", o)
				s.stopColdPack()
				createTestMetric(t, s, "a")
				createTestMetric(t, s, "b")
				cursorTestSteps(t, s, 4)
				closeTestStore(t, s)
				switch change {
				case "decimal", "unpack", "off":
					o.ColdPack.Mode = change
				case "window":
					o.ColdPack.WriteWindow = 2 * time.Hour
				case "format":
					if err := replaceFormatMarker(o.fs, "/store", formatMarkerTimeBuckets); err != nil {
						t.Fatal(err)
					}
				case "budgets":
					o.ColdPack.BytesPerSecond = 2 << 20
					o.ColdPack.CPUFraction = .1
					o.ColdPack.MinFreeBytes = 2 << 30
					o.ColdPack.MaxCompactionDebt = 128 << 20
					o.ColdPack.Interval = 2 * time.Hour
				}
				s, _ = openTestStore(t, "/store", o)
				s.stopColdPack()
				if change == "off" {
					// Off must invalidate the old active checkpoint durably, even
					// if no sweep step ran before the next restart enables it.
					closeTestStore(t, s)
					o.ColdPack.Mode = "exact"
					s, _ = openTestStore(t, "/store", o)
					s.stopColdPack()
				}
				defer closeTestStore(t, s)
				st := s.ColdPackStats()
				if change == "budgets" {
					if st.Cursor != "b" || st.BlockCursor != 1 || st.MetricsVisited != 1 || st.Resumed != 1 {
						t.Fatalf("budget change discarded progress: %+v", st)
					}
				} else if st.Cursor != "" || st.BlockCursor != 0 || st.MetricsVisited != 0 || st.Resumed != 0 {
					t.Fatalf("incompatible checkpoint restored: %+v", st)
				}
				codec.calls = nil
				cursorTestSteps(t, s, 1)
				want := cursorRepackCall{"a", 0}
				if change == "budgets" {
					want = cursorRepackCall{"b", 1}
				}
				if len(codec.calls) != 1 || codec.calls[0] != want {
					t.Fatalf("first call %v, want %v", codec.calls, want)
				}
			})
		})
	}
}

func TestColdPackCheckpointCorrupt(t *testing.T) {
	for _, kind := range []string{"checksum", "truncated", "json", "version", "position", "missing"} {
		t.Run(kind, func(t *testing.T) {
			pebbleSynctest(t, func(t *testing.T) {
				codec := &cursorTestCodec{}
				o := cursorTestOptions(vfs.NewMem(), codec)
				s, _ := openTestStore(t, "/store", o)
				s.stopColdPack()
				createTestMetric(t, s, "a")
				createTestMetric(t, s, "b")
				cursorTestSteps(t, s, 4)
				if err := s.checkpointColdPackOnStop(); err != nil {
					t.Fatal(err)
				}
				saved := s.coldPack.checkpoint
				if kind == "version" {
					saved.Version++
				}
				if kind == "position" {
					saved.Cursor = ""
				}
				value, err := json.Marshal(saved)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "json" {
					value = []byte("{broken")
				}
				value = binary.LittleEndian.AppendUint32(value, crc32.ChecksumIEEE(value))
				if kind == "checksum" {
					value[0] ^= 1
				}
				if kind == "truncated" {
					value = value[:2]
				}
				if kind == "missing" {
					err = s.db.Delete(coldPackCheckpointKey, pebble.Sync)
				} else {
					err = s.db.Set(coldPackCheckpointKey, value, pebble.Sync)
				}
				if err != nil {
					t.Fatal(err)
				}
				closeTestStore(t, s)
				defer zapwriter.Test()()
				s, _ = openTestStore(t, "/store", o)
				defer closeTestStore(t, s)
				s.stopColdPack()
				if st := s.ColdPackStats(); st.Cursor != "" || st.BlockCursor != 0 || st.Resumed != 0 {
					t.Fatalf("bad checkpoint restored: %+v", st)
				}
				if kind != "missing" && !strings.Contains(zapwriter.TestString(), "ignoring cold pack checkpoint") {
					t.Fatalf("corruption ignored silently: %s", zapwriter.TestString())
				}
				codec.calls = nil
				cursorTestSteps(t, s, 1)
				if len(codec.calls) != 1 || codec.calls[0] != (cursorRepackCall{"a", 0}) {
					t.Fatalf("bad checkpoint skipped initial metric: %v", codec.calls)
				}
			})
		})
	}
}

func TestColdPackCheckpointDeletedMetric(t *testing.T) {
	for _, steps := range []int{1, 3} { // cursor within a metric, or after it
		t.Run(fmt.Sprint(steps), func(t *testing.T) {
			pebbleSynctest(t, func(t *testing.T) {
				codec := &cursorTestCodec{}
				o := cursorTestOptions(vfs.NewMem(), codec)
				s, _ := openTestStore(t, "/store", o)
				s.stopColdPack()
				createTestMetric(t, s, "a")
				createTestMetric(t, s, "b")
				cursorTestSteps(t, s, steps)
				if err := s.Delete(context.Background(), "a"); err != nil {
					t.Fatal(err)
				}
				closeTestStore(t, s)
				s, _ = openTestStore(t, "/store", o)
				defer closeTestStore(t, s)
				s.stopColdPack()
				codec.calls = nil
				if !cursorTestSteps(t, s, 10) || len(codec.calls) != 3 || codec.calls[0] != (cursorRepackCall{"b", 0}) {
					t.Fatalf("deleted cursor did not continue at next key: %v", codec.calls)
				}
			})
		})
	}
}

func TestColdPackCheckpointUnpackFailure(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		codec := &cursorTestCodec{skip: true}
		o := cursorTestOptions(vfs.NewMem(), codec)
		o.ColdPack.Mode = "unpack"
		s, _ := openTestStore(t, "/store", o)
		s.stopColdPack()
		createTestMetric(t, s, "a")
		createTestMetric(t, s, "b")
		cursorTestSteps(t, s, 1)
		closeTestStore(t, s)
		codec.skip = false
		s, _ = openTestStore(t, "/store", o)
		defer closeTestStore(t, s)
		s.stopColdPack()
		if !cursorTestSteps(t, s, 10) {
			t.Fatal("pass did not finish")
		}
		if err := s.DowngradeColdPack(context.Background()); err == nil {
			t.Fatal("restart cleared the unpack failure")
		}
	})
}
