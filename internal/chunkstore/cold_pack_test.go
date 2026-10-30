package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

type testColdCodec struct {
	cancel context.CancelFunc
	bucketCodec
	fail      bool
	oversized bool
}

func (c testColdCodec) Repack(ctx context.Context, io ContinuationIO, r ColdPackRequest) (RepackStats, uint64, bool, error) {
	if c.fail {
		return RepackStats{}, 0, false, errors.New("test failure")
	}
	if c.oversized {
		err := io.Set([]byte("packed"), make([]byte, coldPackBatchLimit))
		return RepackStats{Blocks: 1}, 1, false, err
	}
	if r.Cursor != 0 {
		return RepackStats{}, 0, true, nil
	}
	if err := io.Set([]byte("packed"), []byte("complete")); err != nil {
		return RepackStats{}, 0, false, err
	}
	if c.cancel != nil {
		c.cancel()
	}
	return RepackStats{Blocks: 1, Points: 200}, 1, false, nil
}
func TestColdPackPagesAndBusyStripe(t *testing.T) {
	ctx := context.Background()
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, TimeBuckets: true, Continuation: testColdCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	s.stopColdPack()
	for i := 0; i < 201; i++ {
		createTestMetric(t, s, fmt.Sprintf("page.%03d", i))
	}
	after := ""
	count := 0
	for {
		names, err := s.coldPackPage(ctx, after)
		if err != nil {
			t.Fatal(err)
		}
		if len(names) > ColdPackPageSize {
			t.Fatal("unbounded page")
		}
		if len(names) == 0 {
			break
		}
		for _, name := range names {
			if name <= after {
				t.Fatal("page regressed")
			}
			after = name
			count++
		}
	}
	if count != 201 {
		t.Fatalf("listed %d metrics", count)
	}
	if err := s.ConfigureColdPack(ColdPackOptions{Mode: "exact"}); err != nil {
		t.Fatal(err)
	}
	unlock := s.lockMetricMutation("page.000")
	_, err := s.ColdPackStep(ctx)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	if st := s.ColdPackStats(); st.Conflicts != 1 || st.Blocks != 0 {
		t.Fatalf("busy stripe waited or published: %+v", st)
	}
	for n := 0; n < 1000; n++ {
		done, err := s.ColdPackStep(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	st := s.ColdPackStats()
	if st.Passes != 1 || st.Blocks != 200 || st.Pages != 5 {
		t.Fatalf("paged sweep: %+v", st)
	}
}
func TestColdPackBudgetsAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name  string
		codec testColdCodec
		skip  bool
	}{{"oversized", testColdCodec{oversized: true}, true}, {"failure", testColdCodec{fail: true}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, TimeBuckets: true, Continuation: tc.codec, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			defer closeTestStore(t, s)
			s.stopColdPack()
			createTestMetric(t, s, "budget")
			if err := s.ConfigureColdPack(ColdPackOptions{Mode: "exact"}); err != nil {
				t.Fatal(err)
			}
			_, err := s.ColdPackStep(context.Background())
			st := s.ColdPackStats()
			if tc.skip {
				if err != nil || st.Skipped != 1 || st.Blocks != 0 || st.WriteBytes != 0 {
					t.Fatalf("budget %+v %v", st, err)
				}
			} else if err == nil || st.Errors != 1 || st.Error == "" {
				t.Fatalf("silent failure %+v %v", st, err)
			}
		})
	}
}
func TestColdPackPacerAndStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Store{}
		s.coldPack.options, _ = NormalizeColdPack(ColdPackOptions{Mode: "off", CPUFraction: .1, BytesPerSecond: 1 << 20})
		started := time.Now()
		time.Sleep(100 * time.Millisecond)
		s.coldPack.stats.ReadBytes = 1 << 20
		if delay := s.coldPackDelay(ColdPackStats{}, started); delay != 900*time.Millisecond {
			t.Fatalf("pacer delay %s", delay)
		}
		s.startColdPack()
		time.Sleep(time.Second)
		synctest.Wait()
		s.stopColdPack()
	})
}
func TestColdPackOptions(t *testing.T) {
	for _, o := range []ColdPackOptions{{Mode: "unknown"}, {BytesPerSecond: -1}, {CPUFraction: -.1}, {CPUFraction: 2}, {WriteWindow: -1}, {Interval: time.Nanosecond}} {
		if _, err := NormalizeColdPack(o); err == nil {
			t.Fatalf("invalid policy accepted: %+v", o)
		}
	}
}

func TestColdPackReloadPreservesUnpackFailure(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, TimeBuckets: true, Continuation: testColdCodec{oversized: true}, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	s.stopColdPack()
	createTestMetric(t, s, "unpack.budget")
	if err := s.ConfigureColdPack(ColdPackOptions{Mode: "unpack"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ColdPackStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfigureColdPack(ColdPackOptions{Mode: "unpack", BytesPerSecond: 2 << 20}); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 10; n++ {
		done, err := s.ColdPackStep(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if err := s.DowngradeColdPack(context.Background()); err == nil {
		t.Fatal("budget reload cleared incomplete unpack safety guard")
	}
}

func TestColdPackCancellationRetainsCandidate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, TimeBuckets: true, Continuation: testColdCodec{cancel: cancel}, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	s.stopColdPack()
	createTestMetric(t, s, "unpack.cancel")
	if err := s.ConfigureColdPack(ColdPackOptions{Mode: "unpack"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ColdPackStep(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled plan: %v", err)
	}
	if st := s.ColdPackStats(); st.PendingMetrics != 1 || st.Blocks != 0 {
		t.Fatalf("lost candidate after cancellation: %+v", st)
	}
	for n := 0; n < 10; n++ {
		done, err := s.ColdPackStep(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if s.ColdPackStats().Blocks != 1 {
		t.Fatal("cancelled candidate was not retried")
	}
	if err := s.DowngradeColdPack(context.Background()); err == nil {
		t.Fatal("interrupted sweep authorized downgrade")
	}
}
