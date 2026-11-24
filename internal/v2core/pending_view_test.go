package v2core

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"reflect"
	"runtime"
	"runtime/metrics"
	"slices"
	"testing"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// TestPendingViewMatchesBatches drives an owner pendingHead with row and cold
// (policy-byte) packets, publishes views at random boundaries, and checks
// every view against the submitted applications: full decode, Each, every
// position's Ref and Application, Refs, Suffix at earlier publication
// boundaries and concat across heads, and Find against a linear scan.
func TestPendingViewMatchesBatches(t *testing.T) {
	var coldApps, rowApps, views int
	for seed := uint64(1); seed <= 6; seed++ {
		s := newPendingRowsScript(seed)
		rng := rand.New(rand.NewPCG(seed, 0x5b))
		var h pendingHead
		var c pendingCompaction
		var want []coreBatch
		type published struct {
			v    PendingView
			apps int
		}
		var pubs []published
		for _, packet := range s.packets {
			if rng.IntN(5) == 0 {
				cold := make([]coreBatch, len(packet))
				for i, b := range packet {
					b.Policy = []byte{'p', byte(i)}
					cold[i] = b
				}
				packet = cold
				coldApps += len(packet)
			} else {
				rowApps += len(packet)
			}
			h.add(packet, s.find)
			want = append(want, packet...)
			if rng.IntN(3) == 0 {
				c.poll(&h)
				pubs = append(pubs, published{h.view(s.find), len(want)})
				c.start(&h)
			}
		}
		c.wait(&h)
		pubs = append(pubs, published{h.view(s.find), len(want)})
		for k, p := range pubs {
			views++
			checkPendingView(t, seed, p.v, want[:p.apps])
			if k > 0 {
				prev := pubs[k-1]
				suffix := p.v.Suffix(prev.v.Len())
				checkPendingView(t, seed, suffix, want[prev.apps:p.apps])
				checkPendingView(t, seed, prev.v.concat(suffix), want[:p.apps])
			}
		}
		// concat across heads: the second head's positions and runs shift.
		var other pendingHead
		for _, packet := range s.packets[:3] {
			other.add(packet, s.find)
		}
		ov := other.view(s.find)
		joined := ov.concat(pubs[len(pubs)-1].v)
		var all []coreBatch
		for _, packet := range s.packets[:3] {
			all = append(all, packet...)
		}
		checkPendingView(t, seed, joined, append(all, want...))
	}
	if coldApps == 0 || rowApps == 0 {
		t.Fatalf("coverage: cold %d rows %d", coldApps, rowApps)
	}
	t.Logf("coverage: %d views, %d row applications, %d cold applications", views, rowApps, coldApps)
}

// A merge may finish after more publications or a seal. It must preserve
// pinned views and new tails, and never install an old head into its successor.
func TestPendingCompactionLifetime(t *testing.T) {
	s := newPendingRowsScript(19)
	var h pendingHead
	var c pendingCompaction
	var want []coreBatch
	var pinned []PendingView
	var counts []int
	for _, packet := range s.packets[:12] {
		h.add(packet, s.find)
		want = append(want, packet...)
		pinned = append(pinned, h.view(s.find))
		counts = append(counts, len(want))
	}
	c.start(&h)
	if c.done == nil {
		t.Fatal("no compaction started")
	}
	flight := c.done
	for _, packet := range s.packets[12:15] {
		h.add(packet, s.find)
		want = append(want, packet...)
		h.view(s.find)
		c.start(&h)
		if c.done != flight {
			t.Fatal("started another merge before consuming the first")
		}
	}
	c.wait(&h)
	checkPendingView(t, 19, h.view(s.find), want)
	for i, v := range pinned {
		checkPendingView(t, 19, v, want[:counts[i]])
	}
	// Force another eligible pair, then reset while its result is outstanding.
	for range 2 {
		h.add(s.packets[0], s.find)
		h.view(s.find)
	}
	c.start(&h)
	if c.done == nil {
		t.Fatal("no compaction across reset")
	}
	h = pendingHead{}
	h.add(s.packets[1], s.find)
	v := h.view(s.find)
	c.wait(&h)
	checkPendingView(t, 19, h.view(s.find), s.packets[1])
	checkPendingView(t, 19, v, s.packets[1])
}

func samePendingApp(got, want *coreBatch) bool {
	return reflect.DeepEqual(*got, *want) && got.Registered == want.Registered
}

func checkPendingView(t *testing.T, seed uint64, v PendingView, want []coreBatch) {
	t.Helper()
	got := v.Batches()
	if len(got) != len(want) {
		t.Fatalf("seed %d: %d batches, want %d", seed, len(got), len(want))
	}
	for i := range want {
		if !samePendingApp(&got[i], &want[i]) {
			t.Fatalf("seed %d: batch %d\n got %+v\nwant %+v", seed, i, got[i], want[i])
		}
	}
	// The L0 image encodes the packed rows directly: the same bytes as the
	// detached batches, and it decodes back to them.
	image := encodeL0(nil, v)
	if ref := encodeL0(nil, PendingBatches(want)); !bytes.Equal(image, ref) {
		t.Fatalf("seed %d: L0 image of the view (%d bytes) differs from the batches' (%d bytes)", seed, len(image), len(ref))
	}
	decoded, err := decodeL0(image)
	if err != nil || len(decoded) != len(want) {
		t.Fatalf("seed %d: decodeL0: %d applications, %v; want %d", seed, len(decoded), err, len(want))
	}
	for i := range want {
		w := want[i]
		w.Registered = nil
		if !samePendingApp(&decoded[i], &w) {
			t.Fatalf("seed %d: L0 application %d\n got %+v\nwant %+v", seed, i, decoded[i], w)
		}
	}
	i := 0
	v.Each(func(a *PendingApplication) bool {
		w := &want[i]
		if a.ID != w.ID || a.Order != w.Order || a.Clock != w.Clock || a.Points != len(w.Points) || a.Registered() != w.Registered || !reflect.DeepEqual(a.Policy, w.Policy) {
			t.Fatalf("seed %d: Each %d = %+v, want %+v", seed, i, *a, *w)
		}
		i++
		return true
	})
	if i != len(want) {
		t.Fatalf("seed %d: Each visited %d, want %d", seed, i, len(want))
	}
	// Positions: every position maps to its application; ranges tile [0, n).
	app, pos := 0, 0
	refs := make([]uint64, 0, v.Len())
	for p := range v.Len() {
		id, _, _ := v.Ref(p)
		refs = append(refs, id)
	}
	// ids: the same positions and IDs without the catalog; cold positions
	// carry their own descriptor. maxClock: the latest application clock.
	n, colds, wantColds, clock := 0, 0, 0, int64(-1)
	for i := range want {
		clock = max(clock, want[i].Clock)
		if want[i].Policy != nil {
			wantColds++
		}
	}
	v.IDs(0, v.Len(), func(p int, id uint64, cold *coreBatch) bool {
		if p != n || id != refs[p] || (cold != nil && (cold.ID != id || cold.Policy == nil)) {
			t.Fatalf("seed %d: ids position %d id %d cold %v", seed, p, id, cold != nil)
		}
		if cold != nil {
			colds++
		}
		n++
		return true
	})
	if n != v.Len() || colds != wantColds {
		t.Fatalf("seed %d: ids visited %d (%d cold), want %d (%d cold)", seed, n, colds, v.Len(), wantColds)
	}
	if got := v.maxClock(-1); got != clock {
		t.Fatalf("seed %d: maxClock %d, want %d", seed, got, clock)
	}
	// Find: per ID, the binary-searched run positions equal the linear scan's
	// row positions in arrival order; cold positions come only from the cold
	// predicate.
	byID := map[uint64][]int{}
	var coldPos []int
	v.IDs(0, v.Len(), func(p int, id uint64, cold *coreBatch) bool {
		if cold != nil {
			coldPos = append(coldPos, p)
		} else {
			byID[id] = append(byID[id], p)
		}
		return true
	})
	for id, wantPos := range byID {
		got, err := v.Find([]uint64{id}, func(*coreBatch) (bool, error) { return false, nil })
		if err != nil || !slices.Equal(got, wantPos) {
			t.Fatalf("seed %d: Find(%d) = %v, %v, want %v", seed, id, got, err, wantPos)
		}
	}
	if got, err := v.Find(nil, func(*coreBatch) (bool, error) { return true, nil }); err != nil || !slices.Equal(got, coldPos) {
		t.Fatalf("seed %d: Find(cold) = %v, %v, want %v", seed, got, err, coldPos)
	}
	for pos < v.Len() {
		b, lo, hi := v.Application(pos)
		if lo != pos || hi <= lo || !samePendingApp(&b, &want[app]) {
			t.Fatalf("seed %d: Application(%d) = [%d,%d) %+v, want app %d %+v", seed, pos, lo, hi, b, app, want[app])
		}
		for q := lo; q < hi; q++ {
			id, registered, policy := v.Ref(q)
			if id != want[app].ID || registered != want[app].Registered || !reflect.DeepEqual(policy, want[app].Policy) || refs[q] != id {
				t.Fatalf("seed %d: Ref(%d) = %d", seed, q, id)
			}
			if _, l2, h2 := v.Application(q); l2 != lo || h2 != hi {
				t.Fatalf("seed %d: Application(%d) range [%d,%d), want [%d,%d)", seed, q, l2, h2, lo, hi)
			}
		}
		pos, app = hi, app+1
	}
	if app != len(want) {
		t.Fatalf("seed %d: positions cover %d applications, want %d", seed, app, len(want))
	}
}

// BenchmarkPendingHead measures the owner's resident pending set at a fixed
// size (8M receiver-shape points), before (submitted descriptors appended to
// []coreBatch) and after (the same descriptors encoded into pendingHead, with
// one published view per 64 packets): in-use heap and objects with the set
// resident, then GC cycles and GC CPU for a fixed 4 GiB of short-lived
// pointerful garbage while it stays resident.
func BenchmarkPendingHead(b *testing.B) {
	const points, perPacket = 8 << 20, 512
	packets, pols := receiverPackets(points)
	lookup := func(id uint64) *ApplicationPolicy { return pols[id-1] }
	submit := func(packet []coreBatch) []coreBatch {
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
		return descriptors
	}
	gcCPU := func() float64 {
		s := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
		metrics.Read(s)
		return s[0].Value.Float64()
	}
	type node struct {
		next *node
		pad  [6]uint64
	}
	churn := func() {
		// 4 GiB in 64-byte pointerful objects, in short chains.
		var keep *node
		for i := 0; i < (4<<30)/64; i++ {
			n := &node{next: keep}
			if i%64 == 0 {
				n.next = nil
			}
			keep = n
		}
		runtime.KeepAlive(keep)
	}
	measure := func(b *testing.B, build func() any) {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		start := time.Now()
		live := build()
		buildNS := time.Since(start)
		runtime.GC()
		runtime.ReadMemStats(&after)
		cpu0, gc0, t0 := gcCPU(), after.NumGC, time.Now()
		churn()
		var end runtime.MemStats
		runtime.ReadMemStats(&end)
		churnWall := time.Since(t0)
		cpu1 := gcCPU()
		runtime.KeepAlive(live)
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/points, "heapB/point")
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/(1<<20), "heapMiB")
		b.ReportMetric(float64(int64(after.HeapObjects)-int64(before.HeapObjects)), "objects")
		b.ReportMetric(float64(buildNS.Nanoseconds())/points, "buildns/point")
		b.ReportMetric(float64(end.NumGC-gc0), "gc-cycles/4GiB")
		b.ReportMetric(cpu1-cpu0, "gc-cpu-s/4GiB")
		b.ReportMetric(churnWall.Seconds(), "churn-wall-s")
	}
	b.Run("coreBatch", func(b *testing.B) {
		for range b.N {
			measure(b, func() any {
				var batches []coreBatch
				for _, packet := range packets {
					batches = append(batches, submit(packet)...)
				}
				return batches
			})
		}
	})
	b.Run("rows16", func(b *testing.B) {
		for range b.N {
			measure(b, func() any {
				h := &pendingHead{}
				var v PendingView
				for i, packet := range packets {
					h.add(submit(packet), lookup)
					if i%64 == 63 {
						v = h.view(lookup)
					}
				}
				_ = v
				return h
			})
		}
	})
}

// TestPendingMetadataDueIncremental checks the owner's incremental metadata
// scan against a full scan of the head after every packet: installed names
// never make a seal due, a not-installed name does (expiry on, or no
// handle), and positions cleared by an earlier pass are never revisited.
func TestPendingMetadataDueIncremental(t *testing.T) {
	for seed := uint64(1); seed <= 4; seed++ {
		s := newPendingRowsScript(seed)
		rng := rand.New(rand.NewPCG(seed, 0x6d))
		r := newCoreRollups()
		if seed%2 == 0 {
			r.expire = func(RollupPolicy, RollupMetadata, time.Time) bool { return false }
		}
		// Real catalog handles: half the names have a coarse archive.
		handles := map[uint64]*ApplicationPolicy{}
		intern := func(id uint64) {
			archives := []sealrollup.Archive{{Step: 1, Count: 60}}
			if id%2 == 0 {
				archives = append(archives, sealrollup.Archive{Step: 10, Count: 60})
			}
			name := fmt.Sprintf("scan.m%d", id)
			raw, err := EncodeRollupPolicy(RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: archives}})
			if err != nil {
				t.Fatal(err)
			}
			if handles[id], err = r.identities.intern(id, name, raw); err != nil {
				t.Fatal(err)
			}
		}
		for id := range s.lookup {
			intern(id)
		}
		freshID := uint64(1 << 31)
		find := func(id uint64) *ApplicationPolicy { return handles[id] }
		full := func(h *pendingHead) bool {
			due := false
			h.view(find).Each(func(a *PendingApplication) bool {
				b := coreBatch{ID: a.ID, Registered: a.Registered(), Policy: a.Policy}
				due = r.installed(a.ID) == nil && r.needsMetadataSeal(&b)
				return !due
			})
			return due
		}
		var h pendingHead
		var dues, clears int
		for _, src := range s.packets {
			packet := make([]coreBatch, len(src))
			cold := rng.IntN(5) == 0
			for i, b := range src {
				b.Registered = handles[b.ID]
				if cold {
					b.Policy, b.Registered = []byte{'p', byte(i)}, nil
				}
				packet[i] = b
			}
			// Every script name is installed. At most one application per
			// packet carries a never-seen name (some wide), often the packet's
			// first, so a pass that skips one position diverges.
			for _, b := range packet {
				r.metrics[b.ID] = uint32(len(r.unranked))
				r.unranked = append(r.unranked, &PolicyIdentity{})
			}
			if rng.IntN(2) == 0 {
				fresh := 0
				if rng.IntN(2) == 0 {
					fresh = rng.IntN(len(packet))
				}
				freshID++
				if rng.IntN(4) == 0 {
					freshID += 1 << 32
				}
				intern(freshID)
				b := packet[fresh]
				pts := append([]row(nil), b.Points...)
				for k := range pts {
					pts[k].ID = freshID
				}
				b.ID, b.Points = freshID, pts
				if b.Policy == nil {
					b.Registered = handles[freshID]
				}
				packet[fresh] = b
			}
			h.add(packet, find)
			want := full(&h)
			if got := r.pendingMetadataDue(&h, find); got != want {
				t.Fatalf("seed %d: incremental due %v, full %v at %d positions", seed, got, want, h.n)
			}
			if want {
				dues++
				// A due pass seals: install every pending name, as the seal
				// would, and start a fresh head.
				h.view(find).IDs(0, h.n, func(_ int, id uint64, _ *coreBatch) bool {
					r.metrics[id] = uint32(len(r.unranked))
					r.unranked = append(r.unranked, &PolicyIdentity{})
					return true
				})
				h = pendingHead{}
			} else {
				clears++
				if h.scanned != h.n {
					t.Fatalf("seed %d: clear pass left scanned %d of %d", seed, h.scanned, h.n)
				}
			}
		}
		if dues == 0 || clears == 0 {
			t.Fatalf("seed %d: coverage: %d due passes, %d clear passes", seed, dues, clears)
		}
	}
}

// BenchmarkPendingScan measures the owner loop's pending work for one
// 10-second head of receiver-shape packets (4M points, 512 per packet, every
// name installed) with a maintenance pass each second, then the seal's
// ArchiveClock pass. "before" appends descriptors (pendingRowsFit), scans the
// whole view each second with Each and takes the clock with Each; "after"
// appends router-encoded rows (addRows), scans with pendingMetadataDue and
// takes maxClock. "routed" is before with addRows; "v2" is the resident
// []coreBatch list. Router-side encoding is outside the timer.
func BenchmarkPendingScan(b *testing.B) {
	const points, perPacket, ticks = 4 << 20, 512, 10
	packets, pols := receiverPackets(points)
	var lookups int
	lookup := func(id uint64) *ApplicationPolicy { lookups++; return pols[id-1] }
	r := newCoreRollups()
	for id := range pols {
		r.metrics[uint64(id)+1] = uint32(len(r.unranked))
		r.unranked = append(r.unranked, &PolicyIdentity{})
	}
	encoded := make([]pendingRows, len(packets))
	for i, packet := range packets {
		for _, bt := range packet {
			encoded[i].startApplication(bt.Order, bt.Clock, bt.Points[0].Time, len(bt.Points))
			for _, p := range bt.Points {
				encoded[i].addRow(bt.ID, p.Time, p.Bits, p.Seq)
			}
		}
		encoded[i].closePacket()
	}
	perTick := len(packets) / ticks
	run := func(b *testing.B, routed, after bool) {
		l0 := lookups
		var scanNS, appendNS, clockNS time.Duration
		for range b.N {
			h := &pendingHead{}
			for i := range packets {
				t0 := time.Now()
				if routed {
					h.addRows(&encoded[i])
				} else {
					h.add(packets[i], lookup)
				}
				appendNS += time.Since(t0)
				if i%perTick != perTick-1 {
					continue
				}
				t0 = time.Now()
				due := false
				if after {
					due = r.pendingMetadataDue(h, lookup)
				} else {
					h.view(lookup).Each(func(a *PendingApplication) bool {
						if r.installed(a.ID) != nil {
							return true
						}
						c := coreBatch{ID: a.ID, Registered: a.Registered(), Policy: a.Policy}
						due = r.needsMetadataSeal(&c)
						return !due
					})
				}
				scanNS += time.Since(t0)
				if due {
					b.Fatal("due")
				}
			}
			t0 := time.Now()
			var c int64
			if after {
				c = h.view(lookup).maxClock(0)
			} else {
				h.view(lookup).Each(func(a *PendingApplication) bool {
					c = max(c, a.Clock)
					return true
				})
			}
			clockNS += time.Since(t0)
			if c == 0 {
				b.Fatal("clock")
			}
		}
		per := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / float64(b.N) / points }
		b.ReportMetric(per(appendNS+scanNS+clockNS), "owner-ns/point")
		b.ReportMetric(per(appendNS), "append-ns/point")
		b.ReportMetric(per(scanNS), "scan-ns/point")
		b.ReportMetric(per(clockNS), "clock-ns/point")
		b.ReportMetric(float64(lookups-l0)/float64(b.N)/points, "lookups/point")
	}
	b.Run("v2", func(b *testing.B) {
		// v2's resident []coreBatch: descriptors appended as submitted, the
		// whole list scanned each second, the clock taken per descriptor.
		var scanNS, appendNS, clockNS time.Duration
		for range b.N {
			var batches []coreBatch
			for i := range packets {
				t0 := time.Now()
				batches = append(batches, packets[i]...)
				appendNS += time.Since(t0)
				if i%perTick != perTick-1 {
					continue
				}
				t0 = time.Now()
				for j := range batches {
					if c := &batches[j]; r.installed(c.ID) == nil && r.needsMetadataSeal(c) {
						b.Fatal("due")
					}
				}
				scanNS += time.Since(t0)
			}
			t0 := time.Now()
			var c int64
			for j := range batches {
				c = max(c, batches[j].Clock)
			}
			clockNS += time.Since(t0)
			if c == 0 {
				b.Fatal("clock")
			}
		}
		per := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / float64(b.N) / points }
		b.ReportMetric(per(appendNS+scanNS+clockNS), "owner-ns/point")
		b.ReportMetric(per(appendNS), "append-ns/point")
		b.ReportMetric(per(scanNS), "scan-ns/point")
		b.ReportMetric(per(clockNS), "clock-ns/point")
		b.ReportMetric(0, "lookups/point")
	})
	b.Run("before", func(b *testing.B) { run(b, false, false) })
	b.Run("routed", func(b *testing.B) { run(b, true, false) })
	b.Run("after", func(b *testing.B) { run(b, true, true) })
}

// Find must remain independent of the catalog across a frozen head and its
// successor, including suffixes and IDs wider than the run key's ID half.
func TestPendingRunsAcrossSealAvoidCatalog(t *testing.T) {
	s := newPendingRowsScript(31)
	var frozen, next pendingHead
	for _, p := range s.packets[:8] {
		frozen.add(p, s.find)
	}
	for _, p := range s.packets[8:] {
		next.add(p, s.find)
	}
	a, b := frozen.view(s.find), next.view(s.find)
	joined := a.concat(b)
	for _, v := range []PendingView{a, b, joined, joined.Suffix(a.Len())} {
		want := map[uint64][]int{}
		v.IDs(0, v.Len(), func(pos int, id uint64, cold *coreBatch) bool {
			if cold != nil {
				t.Fatal("expected row applications")
			}
			want[id] = append(want[id], pos)
			return true
		})
		v.lookup = func(uint64) *ApplicationPolicy { t.Fatal("Find fetched catalog identity"); return nil }
		for id, positions := range want {
			got, err := v.Find([]uint64{id}, func(*Batch) (bool, error) { t.Fatal("unexpected cold application"); return false, nil })
			if err != nil || !slices.Equal(got, positions) {
				t.Fatalf("Find(%d): positions differ (got %d, want %d), error %v", id, len(got), len(positions), err)
			}
		}
	}
}

// TestPendingHeadBlocks checks that a head copies producer-encoded rows into
// blocks of at most pendingBlockRows that are never reallocated once
// published, wastes at most one block, and decodes the same applications as
// one contiguous copy.
func TestPendingHeadBlocks(t *testing.T) {
	packets, pols := receiverPackets(5 * pendingBlockRows)
	lookup := func(id uint64) *ApplicationPolicy { return pols[id-1] }
	var flat pendingRows
	encoded := make([]pendingRows, len(packets))
	for i, packet := range packets {
		for _, bt := range packet {
			encoded[i].startApplication(bt.Order, bt.Clock, bt.Points[0].Time, len(bt.Points))
			for _, p := range bt.Points {
				encoded[i].addRow(bt.ID, p.Time, p.Bits, p.Seq)
			}
		}
		encoded[i].closePacket()
		flat.appendRows(&encoded[i])
	}
	var h pendingHead
	var views []PendingView
	for i := range encoded {
		h.addRows(&encoded[i])
		if i%7 == 0 {
			views = append(views, h.view(lookup))
		}
	}
	final := h.view(lookup)
	if !reflect.DeepEqual(final.Batches(), flat.decodeFrom(lookup)) {
		t.Fatal("blocks decode different applications")
	}
	used, reserved := 0, 0
	for _, s := range final.segs {
		if cap(s.rows) > pendingBlockRows {
			t.Fatalf("block holds %d rows, cap %d", cap(s.rows), pendingBlockRows)
		}
		used, reserved = used+len(s.rows), reserved+cap(s.rows)
	}
	if used != len(flat.rows) || reserved-used > pendingBlockRows || len(final.segs) < 5 {
		t.Fatalf("%d rows in %d blocks reserve %d", used, len(final.segs), reserved)
	}
	// Every earlier view still shares its blocks with the final view.
	for _, v := range views {
		for i, s := range v.segs {
			if unsafe.SliceData(s.rows) != unsafe.SliceData(final.segs[i].rows) {
				t.Fatalf("block %d was reallocated after publication", i)
			}
		}
	}
}

// TestL0EncodeAllocs: an owner's L0 write of a row head encodes straight from
// the packed arrays, so with a sized buffer it allocates nothing; decoding the
// view into batches first (the earlier path) allocates per application.
func TestL0EncodeAllocs(t *testing.T) {
	s := newPendingRowsScript(3)
	var h pendingHead
	for _, packet := range s.packets {
		h.add(packet, s.find)
	}
	v := h.rawView(nil)
	buf := encodeL0(nil, v)
	if n := testing.AllocsPerRun(20, func() { buf = encodeL0(buf, v) }); n != 0 {
		t.Fatalf("encodeL0 of %d row positions: %.0f allocations, want 0", v.Len(), n)
	}
	viaBatches := testing.AllocsPerRun(20, func() { buf = encodeL0(buf, PendingBatches(h.rawView(s.find).Batches())) })
	if viaBatches == 0 {
		t.Fatalf("decoding to batches allocated nothing; the comparison is void")
	}
	t.Logf("%d positions: 0 allocations direct, %.0f via Batches", v.Len(), viaBatches)
}
