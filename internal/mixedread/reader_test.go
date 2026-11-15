package mixedread

import (
	"context"
	"errors"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsread"
)

func catalog(ms map[string]partsread.Metric) partsread.CatalogFuncs {
	return partsread.CatalogFuncs{LookupFunc: func(n string) (partsread.Metric, bool) { m, ok := ms[n]; return m, ok }, FindFunc: func(ctx context.Context, p string, y func(string) error) error {
		var names []string
		for n := range ms {
			if p == "*" || n == p {
				names = append(names, n)
			}
		}
		slices.Sort(names)
		for _, n := range names {
			if e := ctx.Err(); e != nil {
				return e
			}
			if e := y(n); e != nil {
				return e
			}
		}
		return nil
	}}
}
func fixture(t *testing.T, records []partsread.Record) (*Reader, *partsread.Publication, partsread.Metric) {
	t.Helper()
	const now = 1794700800
	opts := chunkstore.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(now, 0) }}
	mem := vfs.NewMem()
	s, e := chunkstore.Open("legacy", opts.WithFS(mem))
	if e != nil {
		t.Fatal(e)
	}
	cfg := chunkstore.MetricConfig{Name: "metric", Retentions: []chunkstore.Retention{{Step: 1, Count: 1000}}, AggregationMethod: chunkstore.Last, XFilesFactor: .5}
	if _, e = s.Create(context.Background(), cfg); e != nil {
		t.Fatal(e)
	}
	if e = s.UpdateMany(context.Background(), cfg.Name, []chunkstore.Point{{Timestamp: now - 100, Value: 5}, {Timestamp: now - 99, Value: 7}}); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	opts.ReadOnly = true
	s, e = chunkstore.Open("legacy", opts.WithFS(mem))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.Close() })
	m := partsread.Metric{ID: 42, Archives: []partsread.Archive{{Step: 1, Points: 1000}}, AggregationMethod: "last", XFilesFactor: .5}
	c := catalog(map[string]partsread.Metric{"metric": m})
	p := new(partsread.Publication)
	p.Publish(partsread.View{Catalog: c, Parts: []partsread.Source{partsread.Freeze(records)}})
	t.Cleanup(p.Close)
	r, e := New(Options{Legacy: s, LegacyCatalog: c, Parts: partsread.Adapter{Catalog: p, Select: func(string) *partsread.Publication { return p }, Budget: partsread.NewBudget(1 << 20), WorkBytes: 1 << 18, Now: opts.Now}})
	if e != nil {
		t.Fatal(e)
	}
	return r, p, m
}
func TestHistoricalCorrectionNaNAndRingInvalidation(t *testing.T) {
	const now = 1794700800
	r, _, _ := fixture(t, []partsread.Record{{ID: 42, Step: 1, Sample: partsread.Sample{Time: now - 100, Bits: 0x7ff8000000000042, Sequence: 1}}, {ID: 42, Step: 1, Sample: partsread.Sample{Time: now - 99 + 1000, Bits: math.Float64bits(99), Sequence: 2}}})
	e := r.Fetch(context.Background(), "metric", now-101, now-98, func(s *partsread.Series) error {
		if math.Float64bits(s.Values[0]) != 0x7ff8000000000042 {
			t.Fatal("occupied NaN lost to legacy")
		}
		if !math.IsNaN(s.Values[1]) {
			t.Fatal("wrapped slot leaked legacy value")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
}
func TestUntouchedLegacyAndEpochSuppression(t *testing.T) {
	const now = 1794700800
	r, _, _ := fixture(t, nil)
	fetch := func(want float64) {
		t.Helper()
		if e := r.Fetch(context.Background(), "metric", now-101, now-98, func(s *partsread.Series) error {
			if math.IsNaN(want) {
				if !math.IsNaN(s.Values[0]) {
					t.Fatal("new epoch leaked legacy")
				}
			} else if s.Values[0] != want {
				t.Fatal(s.Values)
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	fetch(5)
	r.opts.SuppressLegacy = func(string) bool { return true }
	fetch(math.NaN())
}
func TestFindUnionDeletionAndBound(t *testing.T) {
	r, p, m := fixture(t, nil)
	r.opts.LegacyCatalog = catalog(map[string]partsread.Metric{"a": m, "shared": m, "deleted": m})
	p.Publish(partsread.View{Catalog: catalog(map[string]partsread.Metric{"b": m, "shared": m})})
	r.opts.SuppressLegacy = func(n string) bool { return n == "deleted" }
	var got []string
	if e := r.Find(context.Background(), "*", func(n string) error { got = append(got, n); return nil }); e != nil {
		t.Fatal(e)
	}
	slices.Sort(got)
	if !reflect.DeepEqual(got, []string{"a", "b", "shared"}) {
		t.Fatal(got)
	}
	r.opts.MaxFindNames = 2
	if e := r.Find(context.Background(), "*", func(string) error { return nil }); !errors.Is(e, partsread.ErrAdmission) {
		t.Fatal(e)
	}
	sentinel := errors.New("consumer stopped")
	if e := r.Find(context.Background(), "*", func(string) error { return sentinel }); !errors.Is(e, sentinel) {
		t.Fatal(e)
	}
}
func TestPolicyMismatchFailsClosedAndReleasesBudget(t *testing.T) {
	r, p, m := fixture(t, nil)
	m.Archives = []partsread.Archive{{Step: 2, Points: 1000}}
	p.Publish(partsread.View{Catalog: catalog(map[string]partsread.Metric{"metric": m})})
	e := r.Fetch(context.Background(), "metric", 1794700600, 1794700800, func(*partsread.Series) error { t.Fatal("consumed conflicting policy"); return nil })
	if !errors.Is(e, ErrPolicy) {
		t.Fatal(e)
	}
	if r.opts.Parts.Budget.Used() != 0 {
		t.Fatal("snapshot budget leaked")
	}
}
func TestLegacyOnlyFindAndRender(t *testing.T) {
	r, p, _ := fixture(t, nil)
	p.Publish(partsread.View{Catalog: catalog(map[string]partsread.Metric{})})
	if e := r.Fetch(context.Background(), "metric", 1794700699, 1794700702, func(s *partsread.Series) error {
		if s.Values[0] != 5 || s.Values[1] != 7 {
			t.Fatal(s.Values)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}
func TestNewRequiresReadOnly(t *testing.T) {
	s, e := chunkstore.Open("store", chunkstore.Options{}.WithFS(vfs.NewMem()))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = New(Options{Legacy: s}); e == nil {
		t.Fatal("accepted writable legacy")
	}
}

func TestLegacyFallbackBudgetHeldAndReleased(t *testing.T) {
	r, p, _ := fixture(t, nil)
	p.Publish(partsread.View{Catalog: catalog(map[string]partsread.Metric{})})
	err := r.Fetch(context.Background(), "metric", 1794700699, 1794700702, func(*partsread.Series) error {
		if r.opts.Parts.Budget.Used() == 0 {
			t.Fatal("legacy-only output escaped reservation")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.opts.Parts.Budget.Used() != 0 {
		t.Fatal("legacy-only reservation leaked")
	}
	r.opts.Parts.Budget = partsread.NewBudget(1)
	if err = r.Fetch(context.Background(), "metric", 1794700699, 1794700702, func(*partsread.Series) error { t.Fatal("consumed without admission"); return nil }); !errors.Is(err, partsread.ErrAdmission) {
		t.Fatal(err)
	}
}
func TestConvertedCursorFrozenInReader(t *testing.T) {
	r, _, _ := fixture(t, nil)
	c := Cursor{Version: 1, Before: []byte("z")}
	opts := r.opts
	opts.Converted = &c
	opts.Parts.WorkBytes /= 2
	r, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	c.Before[0] = 'a'
	if r.legacyAllowed("metric") {
		t.Fatal("cursor changed after publication")
	}
}
func TestZeroWidthLegacyArchive(t *testing.T) {
	r, _, _ := fixture(t, nil)
	if err := r.Fetch(context.Background(), "metric", 1794700700, 1794700700, func(s *partsread.Series) error {
		if len(s.Values) != 1 || s.Values[0] != 7 {
			t.Fatalf("%+v", s)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFindReservationCoversLegacyCallbacks(t *testing.T) {
	r, p, m := fixture(t, nil)
	p.Publish(partsread.View{Catalog: catalog(map[string]partsread.Metric{})})
	r.opts.LegacyCatalog = catalog(map[string]partsread.Metric{"legacy.only": m})
	sentinel := errors.New("stop legacy consumer")
	if err := r.Find(context.Background(), "*", func(string) error {
		if r.opts.Parts.Budget.Used() == 0 {
			t.Fatal("legacy find callback escaped reservation")
		}
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if r.opts.Parts.Budget.Used() != 0 {
		t.Fatal("find reservation leaked")
	}
	r.opts.Parts.WorkBytes = 1
	if err := r.Find(context.Background(), "*", func(string) error { t.Fatal("find exceeded name byte reservation"); return nil }); !errors.Is(err, partsread.ErrAdmission) {
		t.Fatal(err)
	}
}
