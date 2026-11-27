package sealrollup

import (
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func TestSealPlanFrontierCorrections(t *testing.T) {
	c := config("1s:120,1m:1d", Sum, 0)
	state := map[key]Sample{}
	original := []Batch{{Order: 1, Now: 1800000060, Points: []Sample{sample(1800000001, 2, 1), sample(1800000002, 3, 2)}}}
	saved := append([]Sample(nil), original[0].Points...)
	p, err := Plan(c, Frontier{}, nil, original)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(saved, original[0].Points) {
		t.Fatal("mutated input")
	}
	if p.Next.Order != 1 || p.Next.MaxTime != 1800000002 || p.Next.Policy == ([32]byte{}) {
		t.Fatalf("bad frontier %+v", p.Next)
	}
	for _, r := range p.Records {
		install(state, []Delta{r.Delta})
	}
	before := cloneState(state)
	correction := []Batch{{Order: 2, Now: 1800000061, Points: []Sample{sample(1800000001, 7, 3)}}}
	q, err := Plan(c, p.Next, view(state), correction)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, state) {
		t.Fatal("published before commit")
	}
	for _, r := range q.Records {
		if !r.Correction {
			t.Fatal("late replacement not marked")
		}
		if r.Archive == 1 && r.Sample.Bits != math.Float64bits(10) {
			t.Fatal("correction did not recompute full bucket")
		}
		got, e := DecodeRecord(r.Archive, r.Slot, EncodeRecord(r))
		if got.Kind = r.Kind; e != nil || got != r {
			t.Fatalf("record round trip %+v %+v %v", r, got, e)
		}
	}
	if _, err = Plan(c, q.Next, view(state), correction); err == nil {
		t.Fatal("replayed correction accepted")
	}
	altered := c
	altered.Archives = append([]Archive(nil), c.Archives...)
	altered.Archives[1].Count++
	if _, err = Plan(altered, p.Next, view(state), nil); err == nil {
		t.Fatal("changed retention accepted")
	}
	invalid := c
	invalid.Method = 99
	if _, err = Plan(invalid, p.Next, view(state), nil); err == nil {
		t.Fatal("unsupported policy accepted")
	}
}

func TestSealPlanMatrix(t *testing.T) {
	// Full production shape catalogue, all six methods and XFF thresholds. Test
	// incremental seal boundaries against whole-history ordered reduction through
	// wrap, direct old-archive writes, late duplicates and occupied NaNs.
	for _, def := range schemas {
		for m := Average; m <= First; m++ {
			for _, xff := range []float32{0, .5, 1} {
				c := config(def, m, xff)
				step := c.Archives[0].Step
				now := int64(1794700800)
				state := map[key]Sample{}
				prior := Frontier{}
				var history []Batch
				for i := uint64(1); i <= 6; i++ {
					clock := now
					if i >= 5 {
						clock += step * c.Archives[0].Count
					}
					start := align(clock-60*step, step)
					points := []Sample{sample(start, 1, i), sample(start+step, 2, i+1), {Time: start + 2*step, Bits: 0x7ff80000000000ab, Sequence: i + 2}, sample(start+step, 7, i+3)}
					if i == 3 || i == 6 {
						points = []Sample{sample(now-60*step, math.Copysign(0, -1), i), sample(now-59*step, math.SmallestNonzeroFloat64, i+1)}
					}
					batch := Batch{Order: i, Now: clock, Points: points}
					history = append(history, batch)
					p, err := planBoth(t, c, prior, state, []Batch{batch})
					if err != nil {
						t.Fatalf("%s %d %g: %v", def, m, xff, err)
					}
					for _, r := range p.Records {
						got, e := DecodeRecord(r.Archive, r.Slot, EncodeRecord(r))
						if got.Kind = r.Kind; e != nil || got != r {
							t.Fatal("codec", e)
						}
						install(state, []Delta{got.Delta})
					}
					prior = p.Next
				}
				all, err := Reduce(c, nil, history)
				if err != nil {
					t.Fatal(err)
				}
				want := map[key]Sample{}
				install(want, all)
				compare(t, want, state, def)
			}
		}
	}
}

func TestSealRecordRejectsCorruption(t *testing.T) {
	for _, b := range [][]byte{nil, make([]byte, 26), {1, 0}} {
		if _, err := DecodeRecord(0, 0, b); err == nil {
			t.Fatal("accepted corrupt record")
		}
	}
	for _, bits := range []uint64{0, 1 << 63, 0x7ff80000000000ab, 0xfff0000000000000, 1} {
		r := Record{Delta: Delta{Archive: 1, Slot: 3, Order: 7, Sample: Sample{Time: 1800000000, Bits: bits, Sequence: 7}}, Correction: true}
		q, err := DecodeRecord(1, 3, EncodeRecord(r))
		if err != nil || q != r {
			t.Fatal(q, err)
		}
	}
}

// Plan + durable record codec against actual classic Whisper physical archives.
// Includes wrap and direct retained-coarse corrections; this must not inherit
// the continuation oracle's currently documented wrap exemptions.
func TestSealPlanClassic(t *testing.T) {
	for _, def := range schemas {
		for m := Average; m <= First; m++ {
			for _, xff := range []float32{0, .5, 1} {
				c := config(def, m, xff)
				clock := int64(1794700800)
				step := c.Archives[0].Step
				path := filepath.Join(t.TempDir(), "oracle.wsp")
				w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs(def), whisper.AggregationMethod(m), xff, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
				if err != nil {
					t.Fatal(err)
				}
				state := map[key]Sample{}
				candidates := map[key]bool{}
				prior := Frontier{}
				var dense []Sample
				start := align(clock-120*step, step)
				for i := int64(0); i < 90; i++ {
					dense = append(dense, sample(start+i*step, float64(i%17)-8, uint64(i+1)))
				}
				batches := []Batch{{Order: 1, Now: clock, Points: dense}, {Order: 2, Now: clock, Points: []Sample{sample(start+step, 7, 3), sample(start+step, 9, 4), {Time: start + 2*step, Bits: 0x7ff80000000000ab, Sequence: 5}, sample(start+3*step, math.Copysign(0, -1), 6)}}}
				advanced := clock + step*c.Archives[0].Count
				batches = append(batches, Batch{Order: 3, Now: advanced, Points: []Sample{sample(start+step*c.Archives[0].Count, 17, 7)}}, Batch{Order: 4, Now: advanced, Points: []Sample{sample(start+step, 127, 8)}})
				for bi, b := range batches {
					clock = b.Now
					p, e := planBoth(t, c, prior, state, []Batch{b})
					if e != nil {
						t.Fatal(e)
					}
					for _, r := range p.Records {
						got, e := DecodeRecord(r.Archive, r.Slot, EncodeRecord(r))
						if e != nil {
							t.Fatal(e)
						}
						install(state, []Delta{got.Delta})
					}
					prior = p.Next
					ptrs := make([]*whisper.TimeSeriesPoint, len(b.Points))
					for i, p := range b.Points {
						ptrs[i] = &whisper.TimeSeriesPoint{Time: int(p.Time), Value: math.Float64frombits(p.Bits)}
						for ai, a := range c.Archives {
							candidates[key{ai, slot(align(p.Time, a.Step), a)}] = true
						}
					}
					if e = w.UpdateMany(ptrs); e != nil {
						t.Fatal(e)
					}
					checkClassic(t, path, c, state, candidates, bi)
				}
				if err = w.Close(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

// Compare both planning paths before the existing classic physical-slot oracle.
func planBoth(t *testing.T, c Config, prior Frontier, state map[key]Sample, batches []Batch) (SealPlan, error) {
	t.Helper()
	want, err := Plan(c, prior, view(state), batches)
	if err != nil {
		return want, err
	}
	prepared, err := Prepare(c)
	if err != nil {
		return SealPlan{}, err
	}
	ranges := func(a int, start, end int64, dst []Sample) []Sample {
		for k, p := range state {
			if k.archive == a && p.Time >= start && p.Time < end {
				dst = append(dst, p)
			}
		}
		slices.SortFunc(dst, func(a, b Sample) int {
			if a.Time < b.Time {
				return -1
			}
			if a.Time > b.Time {
				return 1
			}
			return 0
		})
		return dst
	}
	got, err := prepared.Plan(prior, view(state), ranges, batches)
	if err == nil && !reflect.DeepEqual(want, got) {
		t.Fatalf("sparse plan differs: got%+v want%+v", got, want)
	}
	return got, err
}
