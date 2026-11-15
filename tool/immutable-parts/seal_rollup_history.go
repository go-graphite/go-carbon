package main

// Only compressed occupied physical slots survive a seal. One owner's scratch
// decodes one metric at a time; physical replacements precede epoch filtering.
import (
	"errors"
	"slices"

	"github.com/go-graphite/go-carbon/internal/partsrun"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

type rollupHistorySample struct {
	slot   int64
	sample sealrollup.Sample
}

type rollupHistoryWorkspace struct {
	archives []sealrollup.Archive
	samples  [][]rollupHistorySample
	merged   []sealrollup.Sample
}

func historySlot(p sealrollup.Sample, a sealrollup.Archive) int64 { return (p.Time / a.Step) % a.Count }
func historySearch(ps []rollupHistorySample, slot int64) (int, bool) {
	lo, hi := 0, len(ps)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if ps[m].slot < slot {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo, lo < len(ps) && ps[lo].slot == slot
}
func (w *rollupHistoryWorkspace) load(s *atomicRollupState, cfg sealrollup.Config) error {
	w.archives = cfg.Archives
	if cap(w.samples) < len(cfg.Archives) {
		w.samples = make([][]rollupHistorySample, len(cfg.Archives))
	}
	w.samples = w.samples[:len(cfg.Archives)]
	for a, ar := range cfg.Archives {
		ps := w.samples[a][:0]
		if s != nil && s.history != nil && a < len(s.history) && len(s.history[a]) != 0 {
			value := s.history[a]
			h, err := partsrun.Inspect(value, int(min(ar.Count, 1<<26)))
			if err != nil {
				return err
			}
			if cap(ps) < h.Count {
				ps = make([]rollupHistorySample, 0, h.Count)
			}
			err = partsrun.Visit(value, int(min(ar.Count, 1<<26)), func(p partsrun.Sample) error {
				sample := sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Sequence}
				ps = append(ps, rollupHistorySample{historySlot(sample, ar), sample})
				return nil
			})
			if err != nil {
				return err
			}
		} else if s != nil {
			for k, p := range s.Slots {
				if int(k[0]) == a {
					ps = append(ps, rollupHistorySample{k[1], p})
				}
			}
			slices.SortFunc(ps, func(x, y rollupHistorySample) int {
				sx, sy := x.slot, y.slot
				if sx < sy {
					return -1
				}
				if sx > sy {
					return 1
				}
				return 0
			})
		}
		w.samples[a] = ps
	}
	return nil
}
func (w *rollupHistoryWorkspace) view(a int, physical int64) (sealrollup.Sample, bool) {
	ps := w.samples[a]
	i, ok := historySearch(ps, physical)
	if !ok {
		return sealrollup.Sample{}, false
	}
	return ps[i].sample, true
}
func (w *rollupHistoryWorkspace) ranges(a int, start, end int64, dst []sealrollup.Sample) []sealrollup.Sample {
	ar := w.archives[a]
	ps := w.samples[a]
	lo := (start / ar.Step) % ar.Count
	hi := lo + (end-start)/ar.Step
	add := func(first, last int64) {
		i, _ := historySearch(ps, first)
		for ; i < len(ps) && ps[i].slot < last; i++ {
			p := ps[i].sample
			if p.Time >= start && p.Time < end {
				dst = append(dst, p)
			}
		}
	}
	add(lo, min(hi, ar.Count))
	if hi > ar.Count {
		add(0, hi-ar.Count)
	}
	return dst
}
func (w *rollupHistoryWorkspace) seal(records []sealrollup.Record) ([][]byte, error) {
	out := make([][]byte, len(w.archives))
	at := 0
	for a, ar := range w.archives {
		lo := at
		for at < len(records) && records[at].Archive == a {
			at++
		}
		rs := records[lo:at]
		old := w.samples[a]
		if len(rs) == 0 && len(old) == 0 {
			continue
		}
		merged := w.merged[:0]
		i, j := 0, 0
		for i < len(old) || j < len(rs) {
			if j == len(rs) || (i < len(old) && old[i].slot < rs[j].Slot) {
				merged = append(merged, old[i].sample)
				i++
				continue
			}
			if i < len(old) && old[i].slot == rs[j].Slot {
				i++
			}
			merged = append(merged, rs[j].Sample)
			j++
		}
		if int64(len(merged)) > ar.Count {
			return nil, errors.New("history exceeds physical archive")
		}
		w.merged = merged
		v, err := partsrun.Append(nil, 0, len(merged), func(i int) partsrun.Sample {
			p := merged[i]
			return partsrun.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Sequence}
		})
		if err != nil {
			return nil, err
		}
		out[a] = v
	}
	if at != len(records) {
		return nil, errors.New("unordered history archive")
	}
	return out, nil
}

// snapshotSlots is used by differential checks and the uncommon standalone
// adapter. The queued compact engine uses only owner-local reusable scratch.
func (s *atomicRollupState) snapshotSlots() map[[2]int64]sealrollup.Sample {
	if s.history == nil {
		return s.Slots
	}
	var w rollupHistoryWorkspace
	if err := w.load(s, s.Config); err != nil {
		panic(err)
	}
	out := make(map[[2]int64]sealrollup.Sample)
	for a, ps := range w.samples {
		for _, p := range ps {
			out[[2]int64{int64(a), p.slot}] = p.sample
		}
	}
	return out
}
