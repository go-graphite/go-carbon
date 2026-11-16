package v2core

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
	whisper "github.com/go-graphite/go-whisper"
)

// checkFixture samples actual metric records, writes/reopens immutable SSTs, and
// compares rendered raw+merge-time rollups to the unmodified Whisper engine.
func checkFixture(root, out string) error {
	cat, e := loadCatalogue(root)
	if e != nil {
		return e
	}
	if e = os.Mkdir(out, 0750); e != nil {
		return e
	}
	db, e := pebble.Open(filepath.Join(out, "db"), &pebble.Options{})
	if e != nil {
		return e
	}
	sample := map[uint64][]row{}
	for k := 0; k < 8 && k < len(cat.Metrics); k++ {
		id := uint64(k * len(cat.Metrics) / min(8, len(cat.Metrics)))
		f, e := os.Open(rawPath(root, int(id%buckets)))
		if e != nil {
			return e
		}
		r := bufio.NewReaderSize(f, 1<<20)
		var b [32]byte
		for len(sample[id]) < 8192 {
			_, e := io.ReadFull(r, b[:])
			if e == io.EOF {
				break
			}
			if e != nil {
				f.Close()
				return e
			}
			p := getRow(b[:])
			if p.ID == id {
				sample[id] = append(sample[id], p)
			}
		}
		f.Close()
	}
	var all []row
	for _, rs := range sample {
		all = append(all, rs...)
	}
	if _, e = seal(db, out, 0, 0, all, cat); e != nil {
		return e
	}
	if e = db.Close(); e != nil {
		return e
	}
	db, e = pebble.Open(filepath.Join(out, "db"), &pebble.Options{})
	if e != nil {
		return e
	}
	defer db.Close()
	checked, points := 0, 0
	for id, input := range sample {
		if len(input) == 0 {
			continue
		}
		hi := input[0].Time
		for _, r := range input {
			hi = max(hi, r.Time)
		}
		hi++
		lo := hi - 7200
		filtered := input[:0]
		for _, r := range input {
			if r.Time >= lo {
				filtered = append(filtered, r)
			}
		}
		input = filtered
		slices.SortFunc(input, func(a, b row) int {
			if a.Seq < b.Seq {
				return -1
			}
			if a.Seq > b.Seq {
				return 1
			}
			return 0
		})
		clock := hi
		w, e := whisper.CreateWithOptions(filepath.Join(out, fmt.Sprintf("oracle-%d.wsp", id)), whisper.MustParseRetentionDefs("1s:3h,1m:1d,1h:7d"), cat.Metrics[id].Method, float32(cat.Metrics[id].XFF), &whisper.Options{Now: func() time.Time { return time.Unix(clock, 0) }})
		if e != nil {
			return e
		}
		for _, r := range input {
			if e = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(r.Time), Value: math.Float64frombits(r.Bits)}}); e != nil {
				w.Close()
				return e
			}
		}
		snap := db.NewSnapshot()
		raw, e := readMetric(snap, id, lo, hi)
		snap.Close()
		if e != nil {
			w.Close()
			return e
		}
		slices.SortFunc(raw, less)
		want := slices.Clone(input)
		slices.SortFunc(want, less)
		if !slices.Equal(raw, want) {
			w.Close()
			return fmt.Errorf("metric%d raw bit/order mismatch", id)
		}
		merged := canonical(raw, 1)
		m := cat.Metrics[id]
		for k, step := range []int{1, 60, 3600} {
			if k > 0 {
				from := 1
				if k == 2 {
					from = 60
				}
				merged = rollup(merged, from, step, m)
			}
			switch k {
			case 0:
				clock = hi
			case 1:
				clock = hi + 4*3600
			case 2:
				clock = hi + 2*86400
			}
			got, e := w.Fetch(int(lo), int(hi-1))
			if e != nil {
				w.Close()
				return e
			}
			values := map[int]uint64{}
			for _, r := range merged {
				values[int(r.Time)] = r.Bits
			}
			for _, p := range got.Points() {
				bits, ok := values[p.Time]
				if !ok {
					if !math.IsNaN(p.Value) {
						w.Close()
						return fmt.Errorf("metric%d unexpected step%d time%d", id, step, p.Time)
					}
					continue
				}
				if bits != math.Float64bits(p.Value) && !(math.IsNaN(p.Value) && math.IsNaN(math.Float64frombits(bits))) {
					w.Close()
					return fmt.Errorf("metric%d mismatch step%d time%d got%x want%x", id, step, p.Time, math.Float64bits(p.Value), bits)
				}
				points++
			}
		}
		w.Close()
		checked++
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "differential", "metrics": checked, "checked_slots": points, "result": "pass", "oracle": "unmodified go-whisper; per-point source order; closed/reopened SSTs; 1s/60s/3600s; xFilesFactor"})
}
