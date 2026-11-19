package v2core

import (
	"fmt"
	"slices"
	"sort"
	"testing"
)

// BenchmarkPendingRunVsHashIndex sizes the in-memory sorted run (VM-style
// in-memory part) against the reader's per-publish hash chunk, on one publish
// window of 16-byte owner rows resolved through a real PolicyCatalog.
//
//   - hash-chunk: what v2read's pendingChunk.load does today: one catalog
//     lookup + NameHash per row, then bucket heads and chains.
//   - run-pdq / run-radix: keys ID<<32|position sorted by ID (positions stay
//     ascending within an ID), built from row IDs alone, no catalog access.
//
// lookup-* probe one name (or ID) across 10 chunks/runs, the steady state of
// 10 s seals with 1 s publications.
func BenchmarkPendingRunVsHashIndex(b *testing.B) {
	const window, names, perPacket, runs = 1 << 18, 1 << 20, 512, 10
	cat := &PolicyCatalog{}
	for id := uint64(1); id <= names; id++ {
		if _, err := cat.publish(&PolicyIdentity{id: id, nameHash: NameHash(fmt.Sprintf("bench.metric.%d", id))}); err != nil {
			b.Fatal(err)
		}
	}
	lookup := cat.lookup
	head := &pendingHead{}
	order := uint64(1)
	for done := 0; done < window; done += perPacket {
		packet := make([]coreBatch, perPacket)
		pts := make([]row, perPacket)
		for i := range packet {
			id := uint64((done+i)*2654435761%names) + 1
			pts[i] = row{ID: id, Time: 1_700_000_000 - int64(i%60), Bits: uint64(done + i), Seq: order}
			packet[i] = coreBatch{ID: id, Order: order, Clock: 1_700_000_000, Registered: lookup(id), Points: pts[i : i+1 : i+1]}
			order++
		}
		head.add(packet, lookup)
	}
	v := head.view(lookup)
	if v.Len() != window {
		b.Fatalf("view has %d positions, want %d", v.Len(), window)
	}

	type chunk struct {
		hashes      []uint64
		heads, next []int32
	}
	buildHash := func() chunk {
		n := v.Len()
		hashes := make([]uint64, n)
		v.Refs(0, n, func(pos int, _ uint64, registered *ApplicationPolicy, _ []byte) bool {
			hashes[pos] = registered.NameHash()
			return true
		})
		size := 1
		for size < 2*n {
			size <<= 1
		}
		heads := make([]int32, size)
		for i := range heads {
			heads[i] = -1
		}
		next := make([]int32, n)
		mask := uint64(size - 1)
		for i := n - 1; i >= 0; i-- {
			bk := hashes[i] & mask
			next[i] = heads[bk]
			heads[bk] = int32(i)
		}
		return chunk{hashes, heads, next}
	}
	keys := func() []uint64 {
		out := make([]uint64, 0, v.Len())
		for si := range v.segs {
			s := &v.segs[si]
			base := s.firstRow()
			for k := range uint32(s.count) {
				id, _ := s.idTime(&s.packets[0], base+k) // escapes carry wide IDs; time unused
				out = append(out, id<<32|uint64(s.start)+uint64(k))
			}
		}
		return out
	}
	radix := func(a []uint64) []uint64 {
		tmp := make([]uint64, len(a))
		for shift := 32; shift < 64; shift += 8 { // stable LSD over the ID half
			var count [257]int
			for _, k := range a {
				count[(k>>shift)&0xff+1]++
			}
			for i := 1; i < 257; i++ {
				count[i] += count[i-1]
			}
			for _, k := range a {
				d := (k >> shift) & 0xff
				tmp[count[d]] = k
				count[d]++
			}
			a, tmp = tmp, a
		}
		return a
	}
	b.Run("build/hash-chunk", func(b *testing.B) {
		var c chunk
		for b.Loop() {
			c = buildHash()
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/window, "ns/row")
		b.ReportMetric(float64(8*len(c.hashes)+4*len(c.heads)+4*len(c.next))/window, "B/row")
	})
	b.Run("build/run-pdq", func(b *testing.B) {
		var k []uint64
		for b.Loop() {
			k = keys()
			slices.Sort(k)
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/window, "ns/row")
		b.ReportMetric(float64(8*len(k))/window, "B/row")
	})
	b.Run("build/run-radix", func(b *testing.B) {
		var k []uint64
		for b.Loop() {
			k = radix(keys())
		}
		if !slices.IsSorted(k) {
			b.Fatal("radix run not sorted")
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/window, "ns/row")
		b.ReportMetric(float64(8*len(k))/window, "B/row")
	})

	c := buildHash()
	run := radix(keys())
	wantID := uint64(window/2*2654435761%names) + 1
	wantHash := lookup(wantID).NameHash()
	b.Run("lookup/hash-10-chunks", func(b *testing.B) {
		sink := 0
		mask := uint64(len(c.heads) - 1)
		for b.Loop() {
			for range runs {
				for j := c.heads[wantHash&mask]; j >= 0; j = c.next[j] {
					if c.hashes[j] == wantHash {
						sink++
					}
				}
			}
		}
		if sink == 0 {
			b.Fatal("hash lookup found nothing")
		}
	})
	b.Run("lookup/run-10-runs", func(b *testing.B) {
		sink := 0
		for b.Loop() {
			for range runs {
				i := sort.Search(len(run), func(i int) bool { return run[i]>>32 >= wantID })
				for ; i < len(run) && run[i]>>32 == wantID; i++ {
					sink++
				}
			}
		}
		if sink == 0 {
			b.Fatal("run lookup found nothing")
		}
	})

	// The owner's per-second maintenance scan visits every pending
	// application and skips IDs its metric map already holds. A run built at
	// publication could flag new IDs once, from its sorted keys, instead.
	metrics := make(map[uint64]*rollupMetric, names)
	known := &rollupMetric{}
	for id := uint64(1); id <= names; id++ {
		metrics[id] = known
	}
	var fresh int
	b.Run("maint/scan-each-map", func(b *testing.B) {
		for b.Loop() {
			v.Each(func(a *PendingApplication) bool {
				if metrics[a.ID] == nil {
					fresh++
				}
				return true
			})
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/window, "ns/row")
	})
	b.Run("maint/run-newid-map", func(b *testing.B) {
		for b.Loop() {
			last := ^uint64(0)
			for _, k := range run {
				if id := k >> 32; id != last {
					last = id
					if metrics[id] == nil {
						fresh++
					}
				}
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/window, "ns/row")
	})
	if fresh != 0 {
		b.Fatalf("%d IDs reported new, want 0", fresh)
	}
}
