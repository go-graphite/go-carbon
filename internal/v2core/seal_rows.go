package v2core

import (
	"cmp"
	"slices"
	"sort"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// sealKeyPosMask selects the position from a sorted seal key.
const sealKeyPosMask = 1<<32 - 1

// sealKeys returns one key per application of v, ID<<32 | the application's
// first position, sorted by ID and stable. Positions are arrival order and
// an application's rows are consecutive, so each name's applications form
// one run in arrival order and last-write-wins per slot holds by
// construction. LSD radix passes run over the ID bits only, as many as the
// largest ID needs. IDs of 32 bits or more take a cold comparison sort on
// position-only keys (wide). The caller bounds v.Len() to 32 bits. It also
// returns the spare buffer for reuse, the largest ID and the point count.
func (v PendingView) sealKeys(keys, spare []uint64) (sorted, tmp []uint64, maxID uint64, wide bool, points int) {
	keys = sealScratch(keys, v.n)
	for si := range v.segs {
		s := &v.segs[si]
		start := uint64(s.start)
		if s.cold != nil {
			for i := range s.cold {
				b := &s.cold[i]
				id := b.rowID()
				keys = append(keys, id<<32|(start+uint64(i)))
				maxID = max(maxID, id)
				points += len(b.Points)
			}
			continue
		}
		base := s.firstRow()
		esc := sort.Search(len(s.escapes), func(i int) bool { return s.escapes[i].Row >= base })
		for pi := range s.packets {
			pk := &s.packets[pi]
			var deltas []uint32
			if pk.Flags&pendingMulti != 0 {
				deltas = s.deltas[pk.Deltas : pk.Deltas+pk.Rows]
			}
			for k := range pk.Rows {
				if deltas != nil && k > 0 && deltas[k] == deltas[k-1] {
					continue
				}
				row := pk.FirstRow + k
				id := uint64(s.rows[row].ID)
				for esc < len(s.escapes) && s.escapes[esc].Row < row {
					esc++
				}
				if esc < len(s.escapes) && s.escapes[esc].Row == row {
					id = s.escapes[esc].ID
				}
				keys = append(keys, id<<32|(start+uint64(row-base)))
				maxID = max(maxID, id)
			}
			points += int(pk.Rows)
		}
	}
	if maxID > sealKeyPosMask {
		for i := range keys {
			keys[i] &= sealKeyPosMask
		}
		slices.SortStableFunc(keys, func(a, b uint64) int { return cmp.Compare(v.idAt(int(a)), v.idAt(int(b))) })
		return keys, spare, maxID, true, points
	}
	n := len(keys)
	if n < 256 {
		slices.Sort(keys)
		return keys, spare, maxID, false, points
	}
	const bits = 11
	const mask = 1<<bits - 1
	var count [1 << bits]int
	tmp = sealScratch(spare, n)[:n]
	src := keys
	for shift := uint(32); shift < 64 && maxID>>(shift-32) != 0; shift += bits {
		clear(count[:])
		for _, k := range src {
			count[(k>>shift)&mask]++
		}
		sum := 0
		for d, c := range count {
			count[d] = sum
			sum += c
		}
		for _, k := range src {
			d := (k >> shift) & mask
			tmp[count[d]] = k
			count[d]++
		}
		src, tmp = tmp, src
	}
	return src, tmp[:0], maxID, false, points
}

// sealScratch returns s emptied with room for n. Scratch retained between
// seals is sized by the current seal: a buffer over twice that (a first-wave
// or catch-up seal) is dropped, so one large seal does not pin its peak.
func sealScratch[S ~[]E, E any](s S, n int) S {
	if cap(s) > 2*max(n, 4096) {
		s = nil
	}
	return slices.Grow(s[:0], n)
}

// idAt returns the ID of the application holding pos.
func (v PendingView) idAt(pos int) uint64 {
	s := v.seg(pos)
	if s.cold != nil {
		return s.cold[pos-s.start].rowID()
	}
	row := s.firstRow() + uint32(pos-s.start)
	if e, ok := s.escape(row); ok {
		return e.ID
	}
	return uint64(s.rows[row].ID)
}

// pendingApp is one application of a frozen view, read in place for seal
// planning: a cold descriptor, or rows lo..hi of packet pk in segment s.
type pendingApp struct {
	ID, Order uint64
	Clock     int64
	cold      *coreBatch
	s         *pendingSegment
	pk        *pendingPacket
	lo, hi    uint32
}

// app reads the application starting at pos.
func (v PendingView) app(pos int) pendingApp {
	s := v.seg(pos)
	if s.cold != nil {
		b := &s.cold[pos-s.start]
		return pendingApp{ID: b.ID, Order: b.Order, Clock: b.Clock, cold: b}
	}
	lo := s.firstRow() + uint32(pos-s.start)
	pk := &s.packets[s.packetAt(lo)]
	d, hi := s.delta(pk, lo), lo+1
	if pk.Flags&pendingMulti != 0 {
		for end := pk.FirstRow + pk.Rows; hi < end && s.deltas[pk.Deltas+hi-pk.FirstRow] == d; hi++ {
		}
	}
	id := uint64(s.rows[lo].ID)
	if len(s.escapes) != 0 {
		if e, ok := s.escape(lo); ok {
			id = e.ID
		}
	}
	return pendingApp{ID: id, Order: pk.FirstOrder + uint64(d), Clock: pk.Clock, s: s, pk: pk, lo: lo, hi: hi}
}

func (a *pendingApp) points() int {
	if a.cold != nil {
		return len(a.cold.Points)
	}
	return int(a.hi - a.lo)
}

// samples writes the application's points as reducer samples;
// len(dst) == a.points().
func (a *pendingApp) samples(dst []sealrollup.Sample) {
	if a.cold != nil {
		for k, p := range a.cold.Points {
			dst[k] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}
		}
		return
	}
	s, pk := a.s, a.pk
	for k := a.lo; k < a.hi; k++ {
		r := &s.rows[k]
		t := pk.Base + int64(r.DT)
		if len(s.escapes) != 0 {
			if e, ok := s.escape(k); ok {
				t = e.Time
			}
		}
		seq := a.Order
		if pk.Flags&pendingSeq != 0 {
			seq = s.seqs[pk.Seqs+k-pk.FirstRow]
		}
		dst[k-a.lo] = sealrollup.Sample{Time: t, Bits: r.Bits, Sequence: seq}
	}
}

// sealKeySeen reports whether this seal staged id: a bitset probe when the
// IDs were dense enough for one, otherwise a binary search of the sorted keys.
func sealKeySeen(keys, seen []uint64, maxID, id uint64, keyID func(uint64) uint64) bool {
	if id > maxID {
		return false
	}
	if len(seen) != 0 {
		return seen[id>>6]&(1<<(id&63)) != 0
	}
	_, ok := slices.BinarySearchFunc(keys, id, func(k, id uint64) int { return cmp.Compare(keyID(k), id) })
	return ok
}
