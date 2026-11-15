package partsread

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testCatalog(archives []Archive) Catalog {
	return CatalogFuncs{
		LookupFunc: func(n string) (Metric, bool) { return Metric{ID: 7, Archives: archives}, n == "sample" },
		FindFunc: func(ctx context.Context, p string, y func(string) error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if p == "sample" || p == "*" {
				return y("sample")
			}
			return nil
		},
	}
}
func TestPinnedGenerationAndByteAdmission(t *testing.T) {
	var p Publication
	var closed atomic.Int32
	cat := testCatalog([]Archive{{1, 100}})
	makeView := func(bits uint64) View {
		return View{Catalog: cat, Head: []Source{Freeze([]Record{{7, 1, Sample{10, bits, 1}}})}, RetainedBytes: 40, Close: func() { closed.Add(1) }}
	}
	p.Publish(makeView(math.Float64bits(1)))
	b := NewBudget(680)
	old, err := p.Pin(b, 640)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Pin(b, 1); !errors.Is(err, ErrAdmission) {
		t.Fatal(err)
	}
	p.Publish(makeView(math.Float64bits(2)))
	if closed.Load() != 0 {
		t.Fatal("closed pinned old generation")
	}
	s, err := old.Render(context.Background(), "sample", 9, 10, 10)
	if err != nil || s.Values[0] != 1 {
		t.Fatal(s, err)
	}
	old.Close()
	old.Close()
	if closed.Load() != 1 || b.Used() != 0 {
		t.Fatal("release", closed.Load(), b.Used())
	}
	p.Close()
	if closed.Load() != 2 {
		t.Fatal("publication leaked")
	}
	if _, err := p.Pin(b, 1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestPhysicalSlotWinnerBeforeTimestampFilter(t *testing.T) {
	var p Publication
	p.Publish(View{Catalog: testCatalog([]Archive{{1, 10}}), Parts: []Source{Freeze([]Record{{7, 1, Sample{10, math.Float64bits(1), 1}}})}, Head: []Source{Freeze([]Record{{7, 1, Sample{20, math.Float64bits(2), 2}}})}})
	defer p.Close()
	h, err := p.Pin(NewBudget(1<<20), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	s, err := h.Render(context.Background(), "sample", 9, 10, 10)
	if err != nil || !math.IsNaN(s.Values[0]) || !s.Occupied[0] {
		t.Fatal("resurrected overwritten timestamp", s, err)
	}
}
func TestHeadPartBlockMergeExactBits(t *testing.T) {
	nan := uint64(0x7ff8000000000042)
	negzero := uint64(1) << 63
	var p Publication
	p.Publish(View{Catalog: testCatalog([]Archive{{1, 100}}), Blocks: []Source{Freeze([]Record{{7, 1, Sample{10, 0, 1}}, {7, 1, Sample{11, 0, 1}}})}, Parts: []Source{Freeze([]Record{{7, 1, Sample{10, nan, 2}}})}, Head: []Source{Freeze([]Record{{7, 1, Sample{11, negzero, 3}}})}})
	defer p.Close()
	h, _ := p.Pin(NewBudget(1<<20), 1<<20)
	defer h.Close()
	s, err := h.Render(context.Background(), "sample", 9, 11, 11)
	if err != nil || math.Float64bits(s.Values[0]) != nan || math.Float64bits(s.Values[1]) != negzero {
		t.Fatal(s, err)
	}
	var found []string
	if err = h.Find(context.Background(), "*", func(n string) error { found = append(found, n); return nil }); err != nil || len(found) != 1 {
		t.Fatal(found, err)
	}
}
func TestConflictingSequenceFails(t *testing.T) {
	f := Freeze([]Record{{7, 1, Sample{10, 0, 1}}, {7, 1, Sample{10, 1, 1}}})
	var p Publication
	p.Publish(View{Catalog: testCatalog([]Archive{{1, 100}}), Parts: []Source{f}})
	defer p.Close()
	h, _ := p.Pin(NewBudget(1<<20), 1<<20)
	defer h.Close()
	if _, err := h.Render(context.Background(), "sample", 9, 10, 10); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}
func TestConcurrentPublishPinRelease(t *testing.T) {
	var p Publication
	var closed atomic.Int64
	const count = 2000
	cat := testCatalog([]Archive{{1, 100}})
	p.Publish(View{Catalog: cat, Close: func() { closed.Add(1) }})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := NewBudget(1 << 20)
			for range count {
				h, err := p.Pin(b, 1024)
				if err != nil {
					t.Error(err)
					return
				}
				h.Close()
			}
			if b.Used() != 0 {
				t.Error("budget leak")
			}
		}()
	}
	for range count {
		p.Publish(View{Catalog: cat, Close: func() { closed.Add(1) }})
	}
	wg.Wait()
	p.Close()
	if closed.Load() != count+1 {
		t.Fatal(closed.Load())
	}
}
func TestQueryAdmissionAndCancellation(t *testing.T) {
	var p Publication
	p.Publish(View{Catalog: testCatalog([]Archive{{1, 100}})})
	defer p.Close()
	h, _ := p.Pin(NewBudget(100), 100)
	defer h.Close()
	if _, err := h.Render(context.Background(), "sample", 0, 50, 50); !errors.Is(err, ErrAdmission) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.Render(ctx, "sample", 0, 1, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestAdapterHoldsAdmissionUntilResponseConsumed(t *testing.T) {
	var p Publication
	p.Publish(View{Catalog: testCatalog([]Archive{{1, 100}})})
	defer p.Close()
	b := NewBudget(1024)
	a := Adapter{Select: func(string) *Publication { return &p }, Catalog: &p, Budget: b, WorkBytes: 1024}
	a.Now = func() time.Time { return time.Unix(20, 0) }
	err := a.Fetch(context.Background(), "sample", 10, 20, func(s *Series) error {
		if b.Used() != 1024 || len(s.Values) != 10 {
			t.Fatal("response uncharged", b.Used())
		}
		if _, e := p.Pin(b, 1); !errors.Is(e, ErrAdmission) {
			t.Fatal(e)
		}
		return nil
	})
	if err != nil || b.Used() != 0 {
		t.Fatal(err, b.Used())
	}
}

func BenchmarkSparseRender(b *testing.B) {
	for _, width := range []int64{3600, 86400} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			var p Publication
			p.Publish(View{Catalog: testCatalog([]Archive{{1, 86400}}), Parts: []Source{Freeze([]Record{{7, 1, Sample{1800000000, math.Float64bits(42), 1}}})}})
			defer p.Close()
			budget := NewBudget(8 << 20)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				h, e := p.Pin(budget, 4<<20)
				if e != nil {
					b.Fatal(e)
				}
				s, e := h.Render(ctx, "sample", 1800000000-width, 1800000000, 1800000000)
				if e != nil || len(s.Values) != int(width) {
					b.Fatal(e)
				}
				h.Close()
			}
		})
	}
}

func TestRenderWorkspaceLifetimeAndReset(t *testing.T) {
	for _, width := range []int64{3600, 86400} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			const now int64 = 1800000000
			const bits uint64 = 0x7ff8000000000042
			var p Publication
			cat := testCatalog([]Archive{{1, 86400}})
			p.Publish(View{Catalog: cat, Parts: []Source{Freeze([]Record{{7, 1, Sample{now, bits, 0}}})}})
			budget := NewBudget(16 << 20)
			h, e := p.Pin(budget, 8<<20)
			if e != nil {
				t.Fatal(e)
			}
			first, e := h.Render(context.Background(), "sample", now-width, now, now)
			if e != nil {
				t.Fatal(e)
			}
			second, e := h.Render(context.Background(), "sample", now-width, now, now)
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
			h.Close()
			p.Publish(View{Catalog: cat})
			h, e = p.Pin(budget, 8<<20)
			if e != nil {
				t.Fatal(e)
			}
			empty, e := h.Render(context.Background(), "sample", now-width, now, now)
			if e != nil {
				t.Fatal(e)
			}
			for i, v := range empty.Values {
				if !math.IsNaN(v) || empty.Occupied[i] {
					t.Fatal("stale pooled slot", i)
				}
			}
			h.Close()
			p.Close()
			if budget.Used() != 0 {
				t.Fatal("budget leak")
			}
		})
	}
}
