package v2core

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// pendingRowsScript is one random sequence of submitted packets.
type pendingRowsScript struct {
	packets [][]coreBatch
	lookup  map[uint64]*ApplicationPolicy
}

func newPendingRowsScript(seed uint64) pendingRowsScript {
	rng := rand.New(rand.NewPCG(seed, 0x5a))
	s := pendingRowsScript{lookup: map[uint64]*ApplicationPolicy{}}
	ids := make([]uint64, 64)
	for i := range ids {
		ids[i] = uint64(i*7 + 1)
		if i%29 == 28 {
			ids[i] = math.MaxUint32 + uint64(i) // escapes: wide ID
		}
		s.lookup[ids[i]] = &ApplicationPolicy{}
	}
	order, clock := uint64(rng.IntN(1000)+1), int64(1_700_000_000)
	for p := 0; p < 40; p++ {
		apps := 1 + rng.IntN(200)
		if p == 7 {
			apps = 40000 // more than one packet header of single-point rows
		}
		multi := rng.IntN(3) == 0
		explicitSeq := rng.IntN(4) == 0
		packet := make([]coreBatch, 0, apps)
		for a := 0; a < apps; a++ {
			switch rng.IntN(50) {
			case 0:
				clock += int64(1 + rng.IntN(3)) // clock change inside a packet
			case 1:
				order -= uint64(rng.IntN(5)) // non-increasing order: new header
			case 2:
				order += math.MaxUint32 // order delta beyond u32
			}
			order++
			id := ids[rng.IntN(len(ids))]
			n := 1
			if multi && rng.IntN(3) == 0 {
				n = 2 + rng.IntN(6)
			}
			pts := make([]row, n)
			for j := range pts {
				t := clock - int64(rng.IntN(120))
				if rng.IntN(100) == 0 {
					t += int64(rng.IntN(2)*2-1) << 40 // escapes: wide time
				}
				seq := order
				if explicitSeq {
					seq = rng.Uint64()
				}
				pts[j] = row{ID: id, Time: t, Bits: rng.Uint64(), Seq: seq}
			}
			packet = append(packet, coreBatch{ID: id, Order: order, Clock: clock, Registered: s.lookup[id], Points: pts})
		}
		s.packets = append(s.packets, packet)
	}
	return s
}

func (s pendingRowsScript) find(id uint64) *ApplicationPolicy { return s.lookup[id] }

func TestPendingRowsLayout(t *testing.T) {
	if n := unsafe.Sizeof(pendingRow{}); n != 16 {
		t.Fatalf("pendingRow is %d bytes, want 16", n)
	}
	var walk func(reflect.Type)
	walk = func(ty reflect.Type) {
		switch ty.Kind() {
		case reflect.Struct:
			for i := 0; i < ty.NumField(); i++ {
				walk(ty.Field(i).Type)
			}
		case reflect.Slice:
			walk(ty.Elem())
		case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func, reflect.Interface, reflect.String:
			t.Fatalf("pending element type %v holds a pointer", ty)
		}
	}
	for _, v := range []any{pendingRow{}, pendingPacket{}, pendingEscape{}, uint32(0), uint64(0)} {
		walk(reflect.TypeOf(v))
	}
	// Every pendingRows field is a slice of those or a scalar.
	ty := reflect.TypeOf(pendingRows{})
	for i := 0; i < ty.NumField(); i++ {
		walk(ty.Field(i).Type)
	}
}

func TestPendingRowsRoundTrip(t *testing.T) {
	var multi, seq, escapes, headers int
	for seed := uint64(1); seed <= 8; seed++ {
		s := newPendingRowsScript(seed)
		var p pendingRows
		var want []coreBatch
		for _, packet := range s.packets {
			if !pendingRowsFit(packet, s.find) {
				t.Fatalf("seed %d: representable packet rejected", seed)
			}
			p.append(packet)
			want = append(want, packet...)
		}
		got := p.batches(s.find)
		if len(got) != len(want) {
			t.Fatalf("seed %d: %d batches, want %d", seed, len(got), len(want))
		}
		for i := range want {
			if !reflect.DeepEqual(got[i], want[i]) || got[i].Registered != want[i].Registered || cap(got[i].Points) != len(got[i].Points) {
				t.Fatalf("seed %d: batch %d\n got %+v\nwant %+v", seed, i, got[i], want[i])
			}
		}
		for _, pk := range p.packets {
			if pk.Rows > pendingPacketRows {
				t.Fatalf("seed %d: packet of %d rows", seed, pk.Rows)
			}
			multi += int(pk.Flags & pendingMulti)
			seq += int(pk.Flags&pendingSeq) >> 1
		}
		escapes += len(p.escapes)
		headers += len(p.packets)
		p.reset()
		if p.len() != 0 || len(p.batches(s.find)) != 0 {
			t.Fatalf("seed %d: reset left rows", seed)
		}
	}
	if multi == 0 || seq == 0 || escapes == 0 || headers == 0 {
		t.Fatalf("coverage: multi %d seq %d escapes %d headers %d", multi, seq, escapes, headers)
	}
	t.Logf("coverage: headers %d multi %d explicit-seq %d escapes %d", headers, multi, seq, escapes)
}

// TestPendingRowsRoutedCopy: the router encodes each owner's share of a
// packet and the owner copies it (appendRows); the owner's rows, headers and
// side arrays must equal encoding the owner's batches directly (append).
func TestPendingRowsRoutedCopy(t *testing.T) {
	const owners = 4
	for seed := uint64(1); seed <= 8; seed++ {
		s := newPendingRowsScript(seed)
		var direct, copied [owners]pendingRows
		var routed [owners]pendingRows // reused per packet, as the lease is
		for _, packet := range s.packets {
			if !pendingBatchesFit(packet) {
				t.Fatalf("seed %d: representable packet rejected", seed)
			}
			var byOwner [owners][]coreBatch
			for i := range routed {
				routed[i].reset()
			}
			for _, b := range packet {
				o := int(b.ID % owners)
				byOwner[o] = append(byOwner[o], b)
				routed[o].startApplication(b.Order, b.Clock, b.Points[0].Time, len(b.Points))
				for _, r := range b.Points {
					routed[o].addRow(b.ID, r.Time, r.Bits, r.Seq)
				}
			}
			for o := range routed {
				routed[o].closePacket()
				if len(byOwner[o]) > 0 {
					direct[o].append(byOwner[o])
					copied[o].appendRows(&routed[o])
				}
			}
		}
		for o := range direct {
			d, c := &direct[o], &copied[o]
			if !reflect.DeepEqual(d.rows, c.rows) || !reflect.DeepEqual(d.packets, c.packets) || !reflect.DeepEqual(d.deltas, c.deltas) ||
				!reflect.DeepEqual(d.seqs, c.seqs) || !reflect.DeepEqual(d.escapes, c.escapes) || d.lastOrder != c.lastOrder || d.open != c.open {
				t.Fatalf("seed %d owner %d: copied rows differ: %d/%d rows, %d/%d headers, %d/%d deltas, %d/%d seqs, %d/%d escapes",
					seed, o, len(c.rows), len(d.rows), len(c.packets), len(d.packets), len(c.deltas), len(d.deltas), len(c.seqs), len(d.seqs), len(c.escapes), len(d.escapes))
			}
		}
	}
}

func TestPendingRowsFitRejects(t *testing.T) {
	pol, other := &ApplicationPolicy{}, &ApplicationPolicy{}
	lookup := func(uint64) *ApplicationPolicy { return pol }
	ok := coreBatch{ID: 3, Order: 1, Registered: pol, Points: []row{{ID: 3}}}
	if !pendingRowsFit([]coreBatch{ok}, lookup) {
		t.Fatal("plain batch rejected")
	}
	bad := []func(*coreBatch){
		func(b *coreBatch) { b.Points = nil },
		func(b *coreBatch) { b.Policy = []byte{1} },
		func(b *coreBatch) { b.Registered = other },
		func(b *coreBatch) { b.Points = []row{{ID: 4}} },
		func(b *coreBatch) { b.Points = make([]row, pendingPacketRows+1) },
	}
	for i, mutate := range bad {
		b := ok
		mutate(&b)
		if pendingRowsFit([]coreBatch{b}, lookup) {
			t.Errorf("case %d: unrepresentable batch accepted", i)
		}
	}
}

// pendingRowsSealPlanner seals what the owner hands the planner: runs of
// representable batches (up to 37 per packet) become pending rows, and
// policy-byte batches cold coreBatch segments in arrival order, all in one
// frozen pendingHead view that the planner reads in place. Plans and part
// bytes must match the legacy oracle on the decoded batches.
func pendingRowsSealPlanner(r *coreRollups, in coreSealInput) (coreSealPlan, error) {
	lookup := map[uint64]*ApplicationPolicy{}
	for i := range in.Batches {
		if in.Batches[i].Registered != nil {
			lookup[in.Batches[i].ID] = in.Batches[i].Registered
		}
	}
	find := func(id uint64) *ApplicationPolicy { return lookup[id] }
	var head pendingHead
	var packet []coreBatch
	flush := func() {
		if len(packet) > 0 {
			head.add(packet, find)
			packet = nil
		}
	}
	for _, b := range in.Batches {
		if !pendingRowsFit([]coreBatch{b}, find) {
			flush()
			head.add([]coreBatch{b}, find)
			continue
		}
		if packet = append(packet, b); len(packet) == 37 {
			flush()
		}
	}
	flush()
	view := head.view(find)
	rows := 0
	for _, s := range view.segs {
		if s.cold == nil {
			rows += s.count
		}
	}
	if len(view.Batches()) != len(in.Batches) {
		return coreSealPlan{}, errors.New("pending rows lost or added applications")
	}
	pendingRowsEncoded.Add(int64(rows))
	in.Batches, in.Pending = nil, view
	return r.planParts(in)
}

var pendingRowsEncoded atomic.Int64

func TestPendingRowsSealDifferential(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3} {
		s := newSealSweepScript(seed)
		for _, lanes := range []int{1, 3} {
			diff, cov, digest := runSealSweepDifferential(t, s, lanes, legacySealPlanner, pendingRowsSealPlanner)
			if diff != nil {
				t.Fatalf("seed %d lanes %d: pending rows change the seal at %s", seed, lanes, diff)
			}
			t.Logf("seed %d lanes %d: digest %s coverage %+v", seed, lanes, digest, cov)
		}
	}
	if pendingRowsEncoded.Load() == 0 {
		t.Fatal("coverage: no seal point went through pending rows")
	}
	t.Logf("points through pending rows: %d", pendingRowsEncoded.Load())
}

// receiverPackets models receiver traffic: single-point applications with
// contiguous orders, Seq == Order and one clock per packet.
func receiverPackets(points int) ([][]coreBatch, []*ApplicationPolicy) {
	const perPacket = 512
	pols := make([]*ApplicationPolicy, 1<<16)
	for i := range pols {
		pols[i] = &ApplicationPolicy{}
	}
	var out [][]coreBatch
	order := uint64(1)
	for done := 0; done < points; {
		n := min(perPacket, points-done)
		packet := make([]coreBatch, n)
		pts := make([]row, n)
		clock := int64(1_700_000_000 + done/perPacket/100)
		for i := range packet {
			id := uint64((done+i)*2654435761%len(pols)) + 1
			pts[i] = row{ID: id, Time: clock - int64(i%60), Bits: uint64(done + i), Seq: order}
			packet[i] = coreBatch{ID: id, Order: order, Clock: clock, Registered: pols[id-1], Points: pts[i : i+1 : i+1]}
			order++
		}
		out = append(out, packet)
		done += n
	}
	return out, pols
}

// BenchmarkPendingRows compares today's resident pending form (one 80-byte
// coreBatch descriptor plus a 32-byte point per application, as
// submitPacketMode builds and the owner appends) with 16-byte rows at a fixed
// point count: append ns and allocations per point, live heap per point, and
// the GC cycle cost the live structure adds, per point.
func BenchmarkPendingRows(b *testing.B) {
	const points, perPacket = 1 << 20, 512
	packets, pols := receiverPackets(points)
	lookup := func(id uint64) *ApplicationPolicy { return pols[id-1] }
	todayAppend := func(dst []coreBatch, packet []coreBatch) []coreBatch {
		// submitPacketMode: one descriptor and one point arena per packet.
		descriptors := make([]coreBatch, len(packet))
		arena := make([]row, len(packet))
		off := 0
		for i, bt := range packet {
			n := copy(arena[off:], bt.Points)
			bt.Points = arena[off : off+n : off+n]
			bt.Registered = lookup(bt.ID)
			descriptors[i] = bt
			off += n
		}
		// Owner: batches = append(batches, p.batches...).
		return append(dst, descriptors...)
	}
	gcCost := func() time.Duration {
		const cycles = 8
		start := time.Now()
		for range cycles {
			runtime.GC()
		}
		return time.Since(start) / cycles
	}
	liveHeap := func(build func()) (heap uint64, gc time.Duration) {
		var before, after runtime.MemStats
		runtime.GC()
		base := gcCost()
		runtime.ReadMemStats(&before)
		build()
		runtime.GC()
		runtime.ReadMemStats(&after)
		return after.HeapAlloc - before.HeapAlloc, gcCost() - base
	}
	b.Run("coreBatch", func(b *testing.B) {
		var batches []coreBatch
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			batches = batches[:0]
			for _, packet := range packets {
				batches = todayAppend(batches, packet)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/points, "ns/point")
		batches = nil
		var keep []coreBatch
		heap, gc := liveHeap(func() {
			for _, packet := range packets {
				keep = todayAppend(keep, packet)
			}
		})
		b.ReportMetric(float64(heap)/points, "heapB/point")
		b.ReportMetric(float64(gc.Nanoseconds())/points, "gcns/point")
		runtime.KeepAlive(keep)
	})
	b.Run("rows16", func(b *testing.B) {
		var p pendingRows
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			p.reset()
			for _, packet := range packets {
				if !pendingRowsFit(packet, lookup) {
					b.Fatal("receiver packet rejected")
				}
				p.append(packet)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/points, "ns/point")
		p = pendingRows{}
		var keep pendingRows
		heap, gc := liveHeap(func() {
			for _, packet := range packets {
				keep.append(packet)
			}
		})
		b.ReportMetric(float64(heap)/points, "heapB/point")
		b.ReportMetric(float64(gc.Nanoseconds())/points, "gcns/point")
		if got := keep.batches(lookup); len(got) != points {
			b.Fatal(fmt.Sprintf("decoded %d batches", len(got)))
		}
		runtime.KeepAlive(&keep)
	})
}

// BenchmarkPendingRowsRouted isolates the submit → owner step for receiver
// packets over four owners: per-packet descriptors and point arena plus the
// owner's encode (descriptors) against the router's encode into reused
// per-owner rows plus the owner's copy (routed).
func BenchmarkPendingRowsRouted(b *testing.B) {
	const points, perPacket, owners = 1 << 20, 512, 4
	packets, pols := receiverPackets(points)
	lookup := func(id uint64) *ApplicationPolicy { return pols[id-1] }
	b.Run("descriptors", func(b *testing.B) {
		var heads [owners]pendingHead
		var counts [owners]int
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			heads = [owners]pendingHead{}
			for _, packet := range packets {
				clear(counts[:])
				for _, bt := range packet {
					counts[bt.ID%owners]++
				}
				descriptors := make([]coreBatch, len(packet))
				arena := make([]row, len(packet))
				var byOwner [owners][]coreBatch
				off := 0
				for o, n := range counts {
					byOwner[o] = descriptors[off : off : off+n]
					off += n
				}
				off = 0
				for _, bt := range packet {
					n := copy(arena[off:], bt.Points)
					bt.Points = arena[off : off+n : off+n]
					bt.Registered = lookup(bt.ID)
					off += n
					byOwner[bt.ID%owners] = append(byOwner[bt.ID%owners], bt)
				}
				for o := range heads {
					heads[o].add(byOwner[o], lookup)
				}
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/points, "ns/point")
	})
	b.Run("routed", func(b *testing.B) {
		var heads [owners]pendingHead
		var routed [owners]pendingRows
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			heads = [owners]pendingHead{}
			for _, packet := range packets {
				if !pendingBatchesFit(packet) {
					b.Fatal("receiver packet rejected")
				}
				for o := range routed {
					routed[o].reset()
				}
				for _, bt := range packet {
					pk := &routed[bt.ID%owners]
					pk.startApplication(bt.Order, bt.Clock, bt.Points[0].Time, len(bt.Points))
					for _, r := range bt.Points {
						pk.addRow(bt.ID, r.Time, r.Bits, r.Seq)
					}
				}
				for o := range heads {
					routed[o].closePacket()
					heads[o].addRows(&routed[o])
				}
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/points, "ns/point")
	})
}

// TestOutboxRoutesApplications checks that an Outbox puts each application,
// with its order, clock, point times, value bits and sequences, into the
// owner its vshard routes to, in arrival order, and tracks each owner's
// oldest point. Wide IDs and times take the escape form.
func TestOutboxRoutesApplications(t *testing.T) {
	const shards = 7
	c := &PolicyCatalog{}
	out := &Outbox{catalog: c, owners: make([]pendingRows, shards)}
	handles := map[uint64]*ApplicationPolicy{}
	var ids []uint64
	for i := range 64 {
		id := uint64(i + 1)
		if i == 63 {
			id = 1 << 33
		}
		name := fmt.Sprintf("outbox.m%d", i)
		raw, err := EncodeRollupPolicy(RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 60}}}})
		if err != nil {
			t.Fatal(err)
		}
		if handles[id], err = c.intern(id, name, raw); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := out.Start(handles[1], 1, 0, 0, 0); err == nil {
		t.Fatal("empty application accepted")
	}
	if err := out.Start(&ApplicationPolicy{}, 1, 0, 0, 1); err == nil {
		t.Fatal("foreign handle accepted")
	}
	rng := rand.New(rand.NewPCG(7, 11))
	var want [shards][]coreBatch
	var oldest [shards]int64
	var seen [shards]bool
	order := uint64(0)
	for range 4000 {
		h := handles[ids[rng.IntN(len(ids))]]
		order += 1 + uint64(rng.IntN(3))
		clock := int64(1_700_000_000) + int64(order/700)
		pts := make([]row, 1+rng.IntN(3)*rng.IntN(2))
		for j := range pts {
			pts[j] = row{ID: h.id, Time: clock - int64(rng.IntN(4000)), Bits: rng.Uint64(), Seq: order}
			if rng.IntN(50) == 0 {
				pts[j].Time = clock - 1<<40
			}
			if rng.IntN(20) == 0 {
				pts[j].Seq = rng.Uint64()
			}
		}
		if err := out.Start(h, order, clock, pts[0].Time, len(pts)); err != nil {
			t.Fatal(err)
		}
		s := int(h.vshard) % shards
		for _, p := range pts {
			out.Point(p.Time, p.Bits, p.Seq)
			if !seen[s] || p.Time < oldest[s] {
				oldest[s], seen[s] = p.Time, true
			}
		}
		want[s] = append(want[s], coreBatch{ID: h.id, Order: order, Clock: clock, Registered: h, Points: pts})
	}
	lookup := func(id uint64) *ApplicationPolicy { return handles[id] }
	for s := range out.owners {
		out.owners[s].closePacket()
		if len(want[s]) == 0 {
			t.Fatalf("owner %d got no applications; widen the script", s)
		}
		if got := out.owners[s].decodeFrom(lookup); !reflect.DeepEqual(got, want[s]) {
			t.Fatalf("owner %d decodes %d applications, want %d", s, len(got), len(want[s]))
		}
		if out.owners[s].oldest != oldest[s] {
			t.Fatalf("owner %d oldest %d, want %d", s, out.owners[s].oldest, oldest[s])
		}
	}
	out.reset()
	for s := range out.owners {
		if len(out.owners[s].rows) != 0 {
			t.Fatal("reset kept rows")
		}
	}
}

// A short packet after an admission burst must release the burst's arrays,
// and subsequent applications must still preserve their encoded order/points.
func TestPendingRowsReuseAfterBurst(t *testing.T) {
	var p pendingRows
	p.startApplication(1, 100, 100, 16384)
	for i := range 16384 {
		p.addRow(7, 100, uint64(i), uint64(i+1))
	}
	p.closePacket()
	p.reset()
	p.startApplication(20000, 100, 100, 1)
	p.addRow(7, 100, 42, 20000)
	p.closePacket()
	p.reset()
	if cap(p.rows) > 2048 || cap(p.seqs) > 2048 || cap(p.deltas) > 2048 {
		t.Fatal("admission burst arrays retained after a short released packet")
	}
	p.startApplication(30000, 101, 101, 2)
	p.addRow(7, 101, 43, 30000)
	p.addRow(7, 102, 44, 30001)
	p.closePacket()
	got := p.decodeFrom(func(uint64) *ApplicationPolicy { return nil })
	if len(got) != 1 || len(got[0].Points) != 2 || got[0].Points[0].Bits != 43 || got[0].Points[1].Bits != 44 || got[0].Points[1].Seq != 30001 {
		t.Fatalf("reused packet changed: %+v", got)
	}
}
