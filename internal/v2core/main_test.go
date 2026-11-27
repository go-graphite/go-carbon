package v2core

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

func readRaw(db *pebble.DB) ([]row, error) {
	it, e := db.NewIter(nil)
	if e != nil {
		return nil, e
	}
	defer it.Close()
	var rs []row
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if len(k) != 32 {
			return nil, os.ErrInvalid
		}
		if k[8] != 0 || k[9] != 0 || k[10] != 0 || k[11] != 0 {
			continue
		}
		id := uint64(0)
		for _, v := range k[:8] {
			id = id<<8 | uint64(v)
		}
		v := it.Value()
		for len(v) > 0 {
			if len(v) < 24 {
				return nil, os.ErrInvalid
			}
			b := make([]byte, 32)
			for i := 0; i < 8; i++ {
				b[i] = byte(id >> (8 * i))
			}
			copy(b[8:], v[:24])
			rs = append(rs, getRow(b))
			v = v[24:]
		}
	}
	slices.SortFunc(rs, less)
	return rs, it.Error()
}
func TestSealedPartsRawBitsAndWhisperDifferential(t *testing.T) {
	t.Parallel()
	now := int64(1800000000)
	now -= now % 3600
	for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.Min, whisper.Max, whisper.Last} {
		t.Run(method.String(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			cat := catalogue{Metrics: []metric{{"sample", []int{1, 60, 3600}, method, .5}}}
			var rs []row
			// Dense data, an incomplete window, unaligned timestamps, duplicates and a
			// later correction delivered in a separate immutable part.
			for i := int64(0); i < 7200; i++ {
				if i >= 3600 && i < 3640 {
					continue
				}
				rs = append(rs, row{0, now - 7200 + i, math.Float64bits(float64(i%19) - 9), uint64(len(rs))})
			}
			rs = append(rs, row{0, now - 7199, math.Float64bits(math.Copysign(0, -1)), uint64(len(rs))})
			rs = append(rs, row{0, now - 7198, 0x7ff8000000000042, uint64(len(rs))})
			raw := slices.Clone(rs)
			db, e := pebble.Open(filepath.Join(dir, "db"), &pebble.Options{})
			if e != nil {
				t.Fatal(e)
			}
			cut := len(rs) - 2
			for part, r := range [][]row{rs[:cut], rs[cut:]} {
				if _, e = seal(db, dir, 0, uint64(part), slices.Clone(r), cat); e != nil {
					t.Fatal(e)
				}
			}
			if e = db.Close(); e != nil {
				t.Fatal(e)
			}
			db, e = pebble.Open(filepath.Join(dir, "db"), &pebble.Options{})
			if e != nil {
				t.Fatal(e)
			}
			defer db.Close()
			got, e := readRaw(db)
			if e != nil {
				t.Fatal(e)
			}
			slices.SortFunc(raw, less)
			if !slices.Equal(got, raw) {
				t.Fatal("raw IEEE bits/order changed")
			}
			rets := whisper.MustParseRetentionDefs("1s:3h,1m:1d,1h:7d")
			clock := now
			w, e := whisper.CreateWithOptions(filepath.Join(dir, "oracle.wsp"), rets, method, .5, &whisper.Options{Now: func() time.Time { return time.Unix(clock, 0) }})
			if e != nil {
				t.Fatal(e)
			}
			defer w.Close()
			// Apply in original sequence one point at a time, so the oracle cannot
			// reorder same-slot corrections during its own bulk timestamp sort.
			input := slices.Clone(raw)
			slices.SortFunc(input, func(a, b row) int {
				if a.Seq < b.Seq {
					return -1
				}
				if a.Seq > b.Seq {
					return 1
				}
				return 0
			})
			for _, r := range input {
				if e = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(r.Time), Value: math.Float64frombits(r.Bits)}}); e != nil {
					t.Fatal(e)
				}
			}
			merged := canonical(got, 1)
			for k, step := range cat.Metrics[0].Steps {
				if k > 0 {
					merged = rollup(merged, cat.Metrics[0].Steps[k-1], step, cat.Metrics[0])
				}
				switch k {
				case 0:
					clock = now
				case 1:
					clock = now + 4*3600
				case 2:
					clock = now + 2*86400
				}
				series, e := w.Fetch(int(now-7200), int(now-1))
				if e != nil {
					t.Fatal(e)
				}
				if series.Step() != step {
					t.Fatalf("step got%d want%d", series.Step(), step)
				}
				values := map[int]uint64{}
				for _, r := range merged {
					values[int(r.Time)] = r.Bits
				}
				for _, p := range series.Points() {
					want, ok := values[p.Time]
					if !ok {
						if !math.IsNaN(p.Value) {
							t.Fatalf("unexpected rollup at%d step%d: %v", p.Time, step, p.Value)
						}
						continue
					}
					if math.Float64bits(p.Value) != want && !(math.IsNaN(p.Value) && math.IsNaN(math.Float64frombits(want))) {
						t.Fatalf("step%d time%d got%x want%x", step, p.Time, math.Float64bits(p.Value), want)
					}
				}

			}
		})
	}
}
func TestPrepareAndRun(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "journal")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w, e := inputlog.NewSegmentWriter(f, 1)
	if e != nil {
		t.Fatal(e)
	}
	var data []*points.Points
	for i := 0; i < 5; i++ {
		data = append(data, &points.Points{Metric: "metric." + string(rune('a'+i)), Data: []points.Point{{Value: float64(i), Timestamp: 1700000000}, {Value: -0.5, Timestamp: 1700000001}}})
	}
	if _, e = w.Append(data, nil); e != nil {
		t.Fatal(e)
	}
	w.Close()
	f.Close()
	fixture := filepath.Join(dir, "fixture")
	if e = prepare(path, fixture, "", "", 100); e != nil {
		t.Fatal(e)
	}
	c, e := loadCatalogue(fixture)
	if e != nil || c.Points != 10 {
		t.Fatalf("%+v %v", c, e)
	}
	if e = runLayout(fixture, filepath.Join(dir, "out"), 16, 1, 64, 4, "", false); e != nil {
		t.Fatal(e)
	}
	if e = runLayout(fixture, filepath.Join(dir, "out-private"), 16, 1, 64, 4, "1", true); e != nil {
		t.Fatal(e)
	}
	var recovered []row
	for owner := range 16 {
		db, err := pebble.Open(filepath.Join(dir, "out-private", fmt.Sprintf("db-%03d", owner)), &pebble.Options{})
		if err != nil {
			t.Fatal(err)
		}
		rs, err := readRaw(db)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rs {
			if r.ID%16 != uint64(owner) {
				t.Fatal("wrong owner")
			}
		}
		recovered = append(recovered, rs...)
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(recovered) != 10 {
		t.Fatalf("recovered %d points", len(recovered))
	}

}
func TestQueueOwnership(t *testing.T) {
	t.Parallel()
	// Order across many wraps of the two-slot ring; the property does not
	// depend on the count, and each handoff costs a scheduler sleep.
	const n = 1000
	q := new(queue)
	go func() {
		for i := 0; i < n; i++ {
			q.push(&packet{Rows: []row{{Seq: uint64(i)}}})
		}
		q.push(&packet{End: true})
	}()
	for i := 0; i < n; i++ {
		p := q.pop()
		if len(p.Rows) != 1 || p.Rows[0].Seq != uint64(i) {
			t.Fatal("order")
		}
	}
	if !q.pop().End {
		t.Fatal("end")
	}
}

func TestPublishedSnapshotLifetime(t *testing.T) {
	t.Parallel()
	db, e := pebble.Open(filepath.Join(t.TempDir(), "db"), &pebble.Options{})
	if e != nil {
		t.Fatal(e)
	}
	p := new(publication)
	p.publish(db.NewSnapshot())
	done := make(chan error, 8)
	for range 8 {
		go func() {
			for range 1000 {
				pin := p.acquire()
				it, e := pin.s.NewIter(nil)
				if e != nil {
					pin.release()
					done <- e
					return
				}
				it.First()
				e = it.Close()
				pin.release()
				if e != nil {
					done <- e
					return
				}
			}
			done <- nil
		}()
	}
	for range 1000 {
		p.publish(db.NewSnapshot())
	}
	for range 8 {
		if e := <-done; e != nil {
			t.Fatal(e)
		}
	}
	p.clear()
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
}

func TestRollupSignedZeroAndFloat32Occupancy(t *testing.T) {
	negativeZero := math.Float64bits(math.Copysign(0, -1))
	rs := []row{{0, 0, 0, 0}, {0, 1, negativeZero, 1}}
	for _, method := range []whisper.AggregationMethod{whisper.Min, whisper.Max} {
		got := rollup(rs, 1, 2, metric{Method: method, XFF: .5})
		if len(got) != 1 || got[0].Bits != 0 {
			t.Fatalf("%v signed zero tie changed", method)
		}
	}
	got := rollup([]row{{0, 0, negativeZero, 0}}, 1, 1, metric{Method: whisper.Sum, XFF: 1})
	if len(got) != 1 || got[0].Bits != 0 {
		t.Fatal("sum must start at +0, as Whisper does")
	}
	got = rollup([]row{{0, 0, 0, 0}, {0, 1, 0, 1}, {0, 2, 0, 2}}, 1, 5, metric{Method: whisper.Average, XFF: .60000001})
	if len(got) != 1 {
		t.Fatal("Whisper float32 xFilesFactor threshold differs")
	}
}

func TestOwnerDrainsAfterSealFailure(t *testing.T) {
	t.Parallel()
	q := new(queue)
	go func() {
		q.push(&packet{Rows: []row{{ID: 0}, {ID: 0}, {ID: 0}}})
		q.push(&packet{End: true})
	}()
	done := make(chan outcome, 1)
	go func() {
		// A nonexistent directory fails SST creation before touching the DB.
		done <- consume(q, nil, filepath.Join(t.TempDir(), "missing"), 0,
			catalogue{Metrics: []metric{{Steps: []int{1}}}}, 1, nil, nil)
	}()
	select {
	case o := <-done:
		if o.Err == nil {
			t.Fatal("expected seal error")
		}
	case <-time.After(time.Second):
		t.Fatal("owner stopped draining after a failed seal")
	}
}
