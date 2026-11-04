package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/lomik/zapwriter"
)

// Three ranges expose both initial selection and a saved descriptor cursor.
type failingColdCodec struct {
	bucketCodec
	before func(context.Context, string, uint64) error
}

func (c *failingColdCodec) Create(ctx context.Context, io ContinuationIO, m Metadata, now func() time.Time) error {
	if err := c.bucketCodec.Create(ctx, io, m, now); err != nil {
		return err
	}
	return io.Set([]byte("test-name"), []byte(m.Name))
}

func (c *failingColdCodec) Repack(ctx context.Context, io ContinuationIO, r ColdPackRequest) (RepackStats, uint64, bool, error) {
	name, err := io.Get([]byte("test-name"))
	if err != nil {
		return RepackStats{}, 0, false, err
	}
	if c.before != nil {
		if err := c.before(ctx, string(name), r.Cursor); err != nil {
			return RepackStats{}, 0, false, err
		}
	}
	if r.Cursor == 3 {
		return RepackStats{}, 0, true, nil
	}
	if err := io.Set([]byte(fmt.Sprintf("packed.%d", r.Cursor)), []byte("complete")); err != nil {
		return RepackStats{}, 0, false, err
	}
	return RepackStats{Blocks: 1}, r.Cursor + 1, false, nil
}

func failureColdOptions(codec *failingColdCodec) Options {
	return Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true, Continuation: codec,
		CacheSize: 1 << 20, MemTableSize: 1 << 20,
		ColdPack: ColdPackOptions{Mode: "unpack", CPUFraction: 1, BytesPerSecond: 1 << 40}}
}

func TestColdPackMetricFailureContinuesAndRetries(t *testing.T) {
	defer zapwriter.Test()()
	for _, block := range []uint64{0, 1} {
		t.Run(fmt.Sprint(block), func(t *testing.T) {
			pebbleSynctest(t, func(t *testing.T) {
				codec := &failingColdCodec{}
				s, _ := openTestStore(t, "/store", failureColdOptions(codec))
				s.StopBackground()
				defer closeTestStore(t, s)
				for i := range ColdPackPageSize + 2 {
					createTestMetric(t, s, fmt.Sprintf("m.%03d", i))
				}
				failure := errors.New("broken manifest")
				codec.before = func(_ context.Context, name string, cursor uint64) error {
					if (name == "m.000" || name == "m.001") && cursor == block {
						return failure
					}
					return nil
				}
				err := s.RunColdPack(context.Background())
				if err == nil || !strings.Contains(err.Error(), failure.Error()) {
					t.Fatalf("offline run hid metric failure: %v", err)
				}
				st := s.ColdPackStats()
				if st.Passes != 1 || st.Errors != 2 || st.Skipped != 2 || st.Blocks != 2*block+3*ColdPackPageSize || st.Cursor != "" || st.BlockCursor != 0 || st.PendingMetrics != 0 || st.Error == "" {
					t.Fatalf("failed metrics blocked successors: %+v", st)
				}
				if err := s.DowngradeColdPack(context.Background()); err == nil {
					t.Fatal("failed unpack authorized downgrade")
				}
				logs := zapwriter.TestString()
				for _, want := range []string{"ERROR", "cold pack metric failed", "m.000", "m.001", "block_cursor", "broken manifest"} {
					if !strings.Contains(logs, want) {
						t.Fatalf("missing %q in zap log: %s", want, logs)
					}
				}
				codec.before = nil
				if err := s.RunColdPack(context.Background()); err != nil {
					t.Fatal(err)
				}
				if st := s.ColdPackStats(); st.Passes != 2 || st.Errors != 2 || st.Error == "" {
					t.Fatalf("failure latch/counters changed: %+v", st)
				}
				if err := s.DowngradeColdPack(context.Background()); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestColdPackBackgroundMetricFailureDoesNotDelayPass(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		codec := &failingColdCodec{before: func(_ context.Context, name string, _ uint64) error {
			if name == "a" {
				return ErrNotFound
			}
			return nil
		}}
		s, _ := openTestStore(t, "/store", failureColdOptions(codec))
		s.stopColdPack()
		defer closeTestStore(t, s)
		createTestMetric(t, s, "a")
		createTestMetric(t, s, "z")
		s.startColdPack()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if st := s.ColdPackStats(); st.Passes != 1 || st.Blocks != 3 || st.Errors != 1 {
			t.Fatalf("metric error delayed pass: %+v", st)
		}
		s.stopColdPack()
	})
}

func TestColdPackCancellationRetainsBlockCursor(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		codec := &failingColdCodec{}
		s, _ := openTestStore(t, "/store", failureColdOptions(codec))
		s.StopBackground()
		defer closeTestStore(t, s)
		createTestMetric(t, s, "cancel")
		if _, err := s.ColdPackStep(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		codec.before = func(_ context.Context, _ string, _ uint64) error { cancel(); return ctx.Err() }
		if _, err := s.ColdPackStep(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
		if st := s.ColdPackStats(); st.Cursor != "cancel" || st.BlockCursor != 1 || st.Errors != 0 || st.Skipped != 0 {
			t.Fatalf("unfinished range retired: %+v", st)
		}
		codec.before = nil
		if err := s.RunColdPack(context.Background()); err != nil {
			t.Fatal(err)
		}
		if st := s.ColdPackStats(); st.Passes != 1 || st.Blocks != 3 {
			t.Fatalf("unfinished range lost: %+v", st)
		}
		if err := s.DowngradeColdPack(context.Background()); err == nil {
			t.Fatal("cancelled pass authorized downgrade")
		}
	})
}
