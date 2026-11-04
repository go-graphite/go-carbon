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

// An unpublished migration candidate is a valid catalog entry, invisible to
// reads. Maintenance must leave its physical state for verification/publication.
func TestColdPackUnownedCandidate(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		fs := vfs.NewMem()
		if err := fs.MkdirAll("/legacy", 0o755); err != nil {
			t.Fatal(err)
		}
		codec := &cursorTestCodec{}
		o := cursorTestOptions(fs, codec)
		o.IncrementalSource = "/legacy"
		s, _ := openTestStore(t, "/store", o)
		s.StopBackground()
		defer closeTestStore(t, s)
		m := createTestMetric(t, s, "a.unowned")
		// StageFileSnapshot publishes this ownership state. Keep the fixture's
		// codec so any unexpected Repack call remains directly observable.
		m.FileMigration = FileUnowned
		encoded, err := s.encodeCatalog(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.db.Set(catalogKey(m.Name), encoded, nil); err != nil {
			t.Fatal(err)
		}
		createTestMetric(t, s, "z.owned")
		if _, err := s.Fetch(context.Background(), m.Name, 1, 2); !errors.Is(err, ErrNotFound) {
			t.Fatalf("unowned candidate exposed: %v", err)
		}
		if err := s.RunColdPack(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, call := range codec.calls {
			if call.name == m.Name {
				t.Fatal("repacked unverified candidate")
			}
		}
		if st := s.ColdPackStats(); st.Blocks != 3 || st.Errors != 0 || st.Passes != 1 {
			t.Fatalf("unowned entry poisoned pass: %+v", st)
		}
	})
}

func TestColdPackBackgroundMetricFailureDoesNotDelayPass(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		codec := &parallelCursorCodec{before: func(_ context.Context, name string, _ uint64) error {
			if name == "a" {
				return ErrNotFound
			}
			return nil
		}}
		o := parallelOptions(vfs.NewMem(), codec, 1)
		s, _ := openTestStore(t, "/store", o)
		s.stopColdPack()
		defer closeTestStore(t, s)
		createTestMetric(t, s, "a")
		createTestMetric(t, s, "z")
		s.startColdPack()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if st := s.ColdPackStats(); st.Passes != 1 || st.Blocks != 3 || st.Errors != 1 {
			t.Fatalf("metric error applied hourly retry before finishing pass: %+v", st)
		}
		s.stopColdPack()
	})
}

func TestColdPackMetricFailureContinuesAndRetries(t *testing.T) {
	defer zapwriter.Test()()
	for _, workers := range []int{1, 8} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			pebbleSynctest(t, func(t *testing.T) {
				codec := &parallelCursorCodec{}
				o := parallelOptions(vfs.NewMem(), codec, workers)
				o.ColdPack.Mode = "unpack"
				s, _ := openTestStore(t, "/store", o)
				s.StopBackground()
				defer closeTestStore(t, s)
				stripes := make(map[uint64]bool)
				for i := range ColdPackPageSize + 2 {
					name := fmt.Sprintf("m.%03d", i)
					for suffix := 0; stripes[s.metricStripe(name)]; suffix++ {
						name = fmt.Sprintf("m.%03d.%d", i, suffix)
					}
					stripes[s.metricStripe(name)] = true
					createTestMetric(t, s, name)
				}
				failure := errors.New("broken manifest")
				codec.before = func(_ context.Context, name string, block uint64) error {
					if (strings.HasPrefix(name, "m.000") || strings.HasPrefix(name, "m.001")) && block == 1 {
						return failure
					}
					return nil
				}
				// Finish both failed metrics' first range, then fail inside them.
				err := s.RunColdPack(context.Background())
				if err == nil || !strings.Contains(err.Error(), failure.Error()) {
					t.Fatalf("offline run hid metric failure: %v", err)
				}
				st := s.ColdPackStats()
				if st.Passes != 1 || st.Errors != 2 || st.Skipped != 2 || st.Blocks != 2+3*ColdPackPageSize || st.Cursor != "" || st.PendingMetrics != 0 || st.Error == "" {
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

func TestColdPackCancellationRetainsBlockCursor(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		codec := &parallelCursorCodec{}
		o := parallelOptions(vfs.NewMem(), codec, 1)
		o.ColdPack.Mode = "unpack"
		s, _ := openTestStore(t, "/store", o)
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
