package v2read

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

func testMetric(archives []Archive) Metric { return Metric{ID: 7, Archives: archives} }

// testRender renders into a fresh workspace and copies the result, so a test
// never holds pooled buffers.
func testRender(t testing.TB, source Source, archives []Archive, from, until, now, work int64) (*Series, error) {
	t.Helper()
	s, w, err := render(context.Background(), testMetric(archives), source, from, until, now, work)
	if w == nil {
		return s, err
	}
	defer releaseRenderWorkspace(w)
	if s != nil {
		s = &Series{Metric: s.Metric, From: s.From, Until: s.Until, Step: s.Step, Values: append([]float64(nil), s.Values...), Occupied: append([]bool(nil), s.Occupied...)}
	}
	return s, err
}

// The reader admits budgetBytes/workBytes renders and holds each admission and
// its buffers until the consumer returns.
func TestReaderAdmissionHeldUntilConsumed(t *testing.T) {
	reader, _ := New(1, 1<<20, 1<<20)
	reader.Now = func() time.Time { return time.Unix(1000, 0) }
	engine, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishBatchesWithInventory: reader.PublishWithInventory})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		reader.Close()
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	}()
	policy, _ := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "metric", Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}})
	if err = engine.AppendBatches(0, []v2core.Batch{{ID: 1, Order: 1, Clock: 1000, Policy: policy, Points: []v2core.Row{{ID: 1, Time: 990, Bits: math.Float64bits(1), Seq: 1}}}}, 1, true); err != nil {
		t.Fatal(err)
	}
	if cap(reader.renders) != 1 {
		t.Fatal("render slots", cap(reader.renders))
	}
	err = reader.FetchMany(context.Background(), []string{"metric"}, 980, 990, 1000, func(_ string, s *Series) error {
		if len(reader.renders) != 1 || len(s.Values) != 10 || s.Values[9] != 1 {
			return fmt.Errorf("render not admitted while consumed: %d %v", len(reader.renders), s.Values)
		}
		if e := reader.FetchMany(context.Background(), []string{"metric"}, 980, 990, 1000, func(string, *Series) error { return nil }); !errors.Is(e, ErrAdmission) {
			return fmt.Errorf("second render admitted: %w", e)
		}
		return nil
	})
	if err != nil || len(reader.renders) != 0 {
		t.Fatal(err, len(reader.renders))
	}
}

func TestPhysicalSlotWinnerBeforeTimestampFilter(t *testing.T) {
	s, err := testRender(t, sources{Freeze([]Record{{7, 1, Sample{10, math.Float64bits(1), 1}}}), Freeze([]Record{{7, 1, Sample{20, math.Float64bits(2), 2}}})}, []Archive{{1, 10}}, 9, 10, 10, 1<<20)
	if err != nil || !math.IsNaN(s.Values[0]) || !s.Occupied[0] {
		t.Fatal("resurrected overwritten timestamp", s, err)
	}
}
func TestHeadPartBlockMergeExactBits(t *testing.T) {
	nan := uint64(0x7ff8000000000042)
	negzero := uint64(1) << 63
	blocks := Freeze([]Record{{7, 1, Sample{10, 0, 1}}, {7, 1, Sample{11, 0, 1}}})
	parts := Freeze([]Record{{7, 1, Sample{10, nan, 2}}})
	head := Freeze([]Record{{7, 1, Sample{11, negzero, 3}}})
	s, err := testRender(t, sources{blocks, parts, head}, []Archive{{1, 100}}, 9, 11, 11, 1<<20)
	if err != nil || math.Float64bits(s.Values[0]) != nan || math.Float64bits(s.Values[1]) != negzero {
		t.Fatal(s, err)
	}
}
func TestConflictingSequenceFails(t *testing.T) {
	f := Freeze([]Record{{7, 1, Sample{10, 0, 1}}, {7, 1, Sample{10, 1, 1}}})
	if _, err := testRender(t, f, []Archive{{1, 100}}, 9, 10, 10, 1<<20); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}

// A render holds at most work/32 slots.
func TestQueryAdmissionAndCancellation(t *testing.T) {
	archives := []Archive{{1, 100}}
	if _, err := testRender(t, sources{}, archives, 0, 50, 50, 100); !errors.Is(err, ErrAdmission) {
		t.Fatal(err)
	}
	if s, err := testRender(t, sources{}, archives, 0, 50, 50, 50*32); err != nil || len(s.Values) != 50 {
		t.Fatal("exact 32B/slot cap rejected", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, w, err := render(ctx, testMetric(archives), sources{}, 0, 1, 1, 1<<20); !errors.Is(err, context.Canceled) || w != nil {
		t.Fatal(err)
	}
}

func BenchmarkSparseRender(b *testing.B) {
	for _, width := range []int64{1, 60, 720, 1440, 3600, 10080, 43200, 86400} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			source := Freeze([]Record{{7, 1, Sample{1800000000, math.Float64bits(42), 1}}})
			m := testMetric([]Archive{{1, 86400}})
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s, w, e := render(ctx, m, source, 1800000000-width, 1800000000, 1800000000, 4<<20)
				if e != nil || len(s.Values) != int(width) {
					b.Fatal(e)
				}
				releaseRenderWorkspace(w)
			}
		})
	}
}

func TestRenderWorkspaceLifetimeAndReset(t *testing.T) {
	for _, width := range []int64{1, 60, 720, 1440, 3600, 10080, 43200, 86400} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			const now int64 = 1800000000
			const bits uint64 = 0x7ff8000000000042
			ctx := context.Background()
			m := testMetric([]Archive{{1, 86400}})
			source := Freeze([]Record{{7, 1, Sample{now, bits, 0}}})
			first, w1, e := render(ctx, m, source, now-width, now, now, 8<<20)
			if e != nil {
				t.Fatal(e)
			}
			second, w2, e := render(ctx, m, source, now-width, now, now, 8<<20)
			if e != nil {
				t.Fatal(e)
			}
			last := len(first.Values) - 1
			if !first.Occupied[last] || math.Float64bits(first.Values[last]) != bits {
				t.Fatal("occupied NaN bits changed")
			}
			second.Values[last] = 42
			second.Occupied[last] = false
			if !first.Occupied[last] || math.Float64bits(first.Values[last]) != bits {
				t.Fatal("later render changed an earlier result")
			}
			releaseRenderWorkspace(w1)
			releaseRenderWorkspace(w2)
			empty, w, e := render(ctx, m, sources{}, now-width, now, now, 8<<20)
			if e != nil {
				t.Fatal(e)
			}
			for i, v := range empty.Values {
				if !math.IsNaN(v) || empty.Occupied[i] {
					t.Fatal("stale pooled slot", i)
				}
			}
			releaseRenderWorkspace(w)
		})
	}
}

// A source failure after filling the whole result must leave no dirty slots
// in the idle cache, including occupied NaNs and sequence-zero winners.
func TestRenderWorkspaceResetAfterPartialFailure(t *testing.T) {
	const now int64 = 64
	var records []Record
	for ts := int64(1); ts <= now; ts++ {
		records = append(records, Record{7, 1, Sample{ts, math.Float64bits(float64(ts)), 0}})
	}
	records = append(records, Record{7, 1, Sample{now, 0x7ff8000000000042, 0}})
	_, w, err := render(context.Background(), testMetric([]Archive{{1, 128}}), Freeze(records), 0, now, now, 4096)
	if !errors.Is(err, ErrConflict) {
		t.Fatal("expected a partially filled conflicting result", err)
	}
	if w == nil || len(w.touched) != int(now) {
		t.Fatal("fixture did not fill the workspace")
	}
	releaseRenderWorkspace(w)
	for i, value := range w.values {
		if !math.IsNaN(value) || w.seen[i] {
			t.Fatal("failed render retained a dirty slot", i)
		}
	}
	if len(w.touched) != 0 {
		t.Fatal("workspace retained")
	}
}

// Ring ranges may select several epochs, including replacements newer than
// the requested timestamps. The index must retain those invalidations.
func TestSlotRangesAndFrozenVersions(t *testing.T) {
	ctx := context.Background()
	a := Archive{Step: 7, Points: 13}
	var input []Record
	for slot := int64(-80); slot <= 80; slot++ {
		input = append(input, Record{7, 7, Sample{slot * 7, uint64(slot + 100), uint64(slot + 100)}})
	}
	f := Freeze(input)
	for from := int64(-26); from < 26; from++ {
		for width := int64(0); width <= 13; width++ {
			ranges, n := SlotRanges(a, from*7, (from+width)*7)
			var got []Sample
			if e := f.Scan(ctx, 7, a, from*7, (from+width)*7, func(p Sample) error { got = append(got, p); return nil }); e != nil {
				t.Fatal(e)
			}
			at := 0
			for _, r := range input {
				slot := (r.Sample.Time / 7) % 13
				if slot < 0 {
					slot += 13
				}
				distance := (r.Sample.Time/7 - from) % 13
				if distance < 0 {
					distance += 13
				}
				wanted := distance < width
				indexed := false
				for _, span := range ranges[:n] {
					indexed = indexed || slot >= span.From && slot < span.Until
				}
				if wanted != indexed {
					t.Fatalf("range from%d width%d slot%d", from, width, slot)
				}
				if wanted {
					if at >= len(got) || got[at] != r.Sample {
						t.Fatalf("lost epoch from%d width%d at%d", from, width, at)
					}
					at++
				}
			}
			if at != len(got) {
				t.Fatal("extra records")
			}
		}
	}
}
