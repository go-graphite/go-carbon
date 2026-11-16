package main

// Only compressed occupied physical slots survive a seal. One owner's scratch
// decodes one metric at a time; physical replacements precede epoch filtering.
import (
	"encoding/binary"
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
	archives     []sealrollup.Archive
	samples      [][]rollupHistorySample
	merged       []sealrollup.Sample
	codec        partsrun.Workspace
	values       [][]byte
	loaded       []int64
	err          error
	decodedTiles int
}

// This owner-local format is never persisted. Each physical tile is an
// independent partsrun; untouched compressed payloads are copied verbatim into
// the inactive lease. A directory avoids expanding the other occupied slots.
const historyTileSlots int64 = 256
const historyTileHeader = 5
const historyTileEntry = 16

func historyTiled(v []byte) bool    { return len(v) > 0 && v[0] == 0xff }
func historyTileCount(v []byte) int { return int(binary.BigEndian.Uint32(v[1:])) }
func historyTileID(v []byte, i int) int64 {
	return int64(binary.BigEndian.Uint64(v[historyTileHeader+i*historyTileEntry:]))
}
func historyTileValue(v []byte, i int) []byte {
	p := v[historyTileHeader+i*historyTileEntry:]
	o, n := int(binary.BigEndian.Uint32(p[8:])), int(binary.BigEndian.Uint32(p[12:]))
	return v[o : o+n : o+n]
}
func historyTileFind(v []byte, id int64) (int, bool) {
	lo, hi := 0, historyTileCount(v)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if historyTileID(v, m) < id {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo, lo < historyTileCount(v) && historyTileID(v, lo) == id
}
func (w *rollupHistoryWorkspace) decode(a int, value []byte) error {
	ar := w.archives[a]
	ps := w.samples[a][:0]
	err := partsrun.VisitWithWorkspace(value, int(min(ar.Count, 1<<26)), func(p partsrun.Sample) error {
		s := sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Sequence}
		ps = append(ps, rollupHistorySample{historySlot(s, ar), s})
		return nil
	}, &w.codec)
	w.samples[a] = ps
	return err
}
func (w *rollupHistoryWorkspace) tile(a int, id int64) []rollupHistorySample {
	if w.loaded[a] == id {
		return w.samples[a]
	}
	w.loaded[a] = id
	w.samples[a] = w.samples[a][:0]
	i, ok := historyTileFind(w.values[a], id)
	if ok {
		w.decodedTiles++
		w.err = errors.Join(w.err, w.decode(a, historyTileValue(w.values[a], i)))
	}
	return w.samples[a]
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
	w.err = nil
	w.decodedTiles = 0
	if cap(w.samples) < len(cfg.Archives) {
		w.samples = make([][]rollupHistorySample, len(cfg.Archives))
		w.values = make([][]byte, len(cfg.Archives))
		w.loaded = make([]int64, len(cfg.Archives))
	}
	w.samples = w.samples[:len(cfg.Archives)]
	w.values = w.values[:len(cfg.Archives)]
	w.loaded = w.loaded[:len(cfg.Archives)]
	for a, ar := range cfg.Archives {
		w.values[a] = nil
		w.loaded[a] = -1
		ps := w.samples[a][:0]
		if s != nil && s.history != nil && a < len(s.history) && len(s.history[a]) != 0 {
			value := s.history[a]
			if historyTiled(value) {
				if len(value) < historyTileHeader {
					return errors.New("short history tile header")
				}
				n := historyTileCount(value)
				if n > (len(value)-historyTileHeader)/historyTileEntry {
					return errors.New("short history tile directory")
				}
				end := historyTileHeader + n*historyTileEntry
				var prior int64 = -1
				for i := 0; i < n; i++ {
					p := value[historyTileHeader+i*historyTileEntry:]
					id := historyTileID(value, i)
					o, l := int(binary.BigEndian.Uint32(p[8:])), int(binary.BigEndian.Uint32(p[12:]))
					if id <= prior || id > (ar.Count-1)/historyTileSlots || o != end || l > len(value)-o {
						return errors.New("invalid history tile directory")
					}
					if _, err := partsrun.Inspect(value[o:o+l], int(historyTileSlots)); err != nil {
						return err
					}
					end = o + l
					prior = id
				}
				if end != len(value) {
					return errors.New("trailing history tile bytes")
				}
				w.values[a] = value
				w.samples[a] = ps
				continue
			}
			h, err := partsrun.Inspect(value, int(min(ar.Count, 1<<26)))
			if err != nil {
				return err
			}
			if cap(ps) < h.Count {
				ps = make([]rollupHistorySample, 0, h.Count)
			}
			err = partsrun.VisitWithWorkspace(value, int(min(ar.Count, 1<<26)), func(p partsrun.Sample) error {
				sample := sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Sequence}
				ps = append(ps, rollupHistorySample{historySlot(sample, ar), sample})
				return nil
			}, &w.codec)
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
	if w.values[a] != nil {
		ps = w.tile(a, physical/historyTileSlots)
	}
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
		if w.values[a] != nil {
			for id := first / historyTileSlots; id*historyTileSlots < last; id++ {
				ps := w.tile(a, id)
				i, _ := historySearch(ps, first)
				for ; i < len(ps) && ps[i].slot < last; i++ {
					p := ps[i].sample
					if p.Time >= start && p.Time < end {
						dst = append(dst, p)
					}
				}
			}
			return
		}
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
	return w.sealInto(records, nil)
}

// sealInto writes into inactive buffers; callers swap them only after ingest.
func (w *rollupHistoryWorkspace) sealInto(records []sealrollup.Record, out [][]byte) ([][]byte, error) {
	if w.err != nil {
		return nil, w.err
	}
	if cap(out) < len(w.archives) {
		out = make([][]byte, len(w.archives))
	}
	out = out[:len(w.archives)]
	at := 0
	for a, ar := range w.archives {
		lo := at
		for at < len(records) && records[at].Archive == a {
			at++
		}
		rs := records[lo:at]
		if w.values[a] != nil {
			var err error
			out[a], err = w.sealTiles(a, rs, out[a][:0])
			if err != nil {
				return nil, err
			}
			continue
		}
		old := w.samples[a]
		if len(rs) == 0 && len(old) == 0 {
			out[a] = out[a][:0]
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
		if len(merged) > int(historyTileSlots) {
			var err error
			out[a], err = w.encodeTiles(a, merged, out[a][:0])
			if err != nil {
				return nil, err
			}
			continue
		}
		v, err := partsrun.AppendWithWorkspace(out[a][:0], 0, len(merged), func(i int) partsrun.Sample {
			p := merged[i]
			return partsrun.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Sequence}
		}, &w.codec)
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

func historyDirectory(dst []byte, n int) []byte {
	dst = append(dst, make([]byte, historyTileHeader+n*historyTileEntry)...)
	dst[0] = 0xff
	binary.BigEndian.PutUint32(dst[1:], uint32(n))
	return dst
}
func historySetEntry(dst []byte, i int, id int64, start int) {
	p := dst[historyTileHeader+i*historyTileEntry:]
	binary.BigEndian.PutUint64(p, uint64(id))
	binary.BigEndian.PutUint32(p[8:], uint32(start))
	binary.BigEndian.PutUint32(p[12:], uint32(len(dst)-start))
}
func (w *rollupHistoryWorkspace) appendSamples(dst []byte, ps []sealrollup.Sample) ([]byte, error) {
	return partsrun.AppendWithWorkspace(dst, 0, len(ps), func(i int) partsrun.Sample {
		p := ps[i]
		return partsrun.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Sequence}
	}, &w.codec)
}
func (w *rollupHistoryWorkspace) encodeTiles(a int, ps []sealrollup.Sample, dst []byte) ([]byte, error) {
	ar := w.archives[a]
	n := 0
	prior := int64(-1)
	for _, p := range ps {
		id := historySlot(p, ar) / historyTileSlots
		if id != prior {
			n++
			prior = id
		}
	}
	dst = historyDirectory(dst, n)
	entry := 0
	for lo := 0; lo < len(ps); {
		id := historySlot(ps[lo], ar) / historyTileSlots
		hi := lo + 1
		for hi < len(ps) && historySlot(ps[hi], ar)/historyTileSlots == id {
			hi++
		}
		start := len(dst)
		var err error
		dst, err = w.appendSamples(dst, ps[lo:hi])
		if err != nil {
			return nil, err
		}
		historySetEntry(dst, entry, id, start)
		entry++
		lo = hi
	}
	return dst, nil
}
func (w *rollupHistoryWorkspace) sealTiles(a int, rs []sealrollup.Record, dst []byte) ([]byte, error) {
	old := w.values[a]
	n := historyTileCount(old)
	for lo := 0; lo < len(rs); {
		id := rs[lo].Slot / historyTileSlots
		if _, ok := historyTileFind(old, id); !ok {
			n++
		}
		lo++
		for lo < len(rs) && rs[lo].Slot/historyTileSlots == id {
			lo++
		}
	}
	dst = historyDirectory(dst, n)
	i, j, entry := 0, 0, 0
	for i < historyTileCount(old) || j < len(rs) {
		id := int64(1 << 62)
		if i < historyTileCount(old) {
			id = historyTileID(old, i)
		}
		if j < len(rs) {
			id = min(id, rs[j].Slot/historyTileSlots)
		}
		start := len(dst)
		if j == len(rs) || rs[j].Slot/historyTileSlots != id {
			dst = append(dst, historyTileValue(old, i)...)
			i++
		} else {
			lo := j
			for j < len(rs) && rs[j].Slot/historyTileSlots == id {
				j++
			}
			ps := w.tile(a, id)
			if w.err != nil {
				return nil, w.err
			}
			merged := w.merged[:0]
			x, y := 0, lo
			for x < len(ps) || y < j {
				if y == j || (x < len(ps) && ps[x].slot < rs[y].Slot) {
					merged = append(merged, ps[x].sample)
					x++
					continue
				}
				if x < len(ps) && ps[x].slot == rs[y].Slot {
					x++
				}
				merged = append(merged, rs[y].Sample)
				y++
			}
			w.merged = merged
			var err error
			dst, err = w.appendSamples(dst, merged)
			if err != nil {
				return nil, err
			}
			if i < historyTileCount(old) && historyTileID(old, i) == id {
				i++
			}
		}
		historySetEntry(dst, entry, id, start)
		entry++
	}
	return dst, nil
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
		if w.values[a] != nil {
			for i := 0; i < historyTileCount(w.values[a]); i++ {
				for _, p := range w.tile(a, historyTileID(w.values[a], i)) {
					out[[2]int64{int64(a), p.slot}] = p.sample
				}
			}
			continue
		}
		for _, p := range ps {
			out[[2]int64{int64(a), p.slot}] = p.sample
		}
	}
	return out
}
