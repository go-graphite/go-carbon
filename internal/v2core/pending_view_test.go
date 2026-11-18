package v2core

import (
	"math/rand/v2"
	"reflect"
	"runtime"
	"runtime/metrics"
	"testing"
	"time"
)

// TestPendingViewMatchesBatches drives an owner pendingHead with row and cold
// (policy-byte) packets, publishes views at random boundaries, and checks
// every view against the submitted applications: full decode, Each, every
// position's Ref and Application, Refs, Suffix at earlier publication
// boundaries, Extends and concat across heads.
func TestPendingViewMatchesBatches(t *testing.T) {
	var coldApps, rowApps, views int
	for seed := uint64(1); seed <= 6; seed++ {
		s := newPendingRowsScript(seed)
		rng := rand.New(rand.NewPCG(seed, 0x5b))
		var h pendingHead
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
				pubs = append(pubs, published{h.view(s.find), len(want)})
			}
		}
		pubs = append(pubs, published{h.view(s.find), len(want)})
		for k, p := range pubs {
			views++
			checkPendingView(t, seed, p.v, want[:p.apps])
			if k > 0 {
				prev := pubs[k-1]
				if !p.v.Extends(prev.v) {
					t.Fatalf("seed %d: view %d does not extend view %d", seed, k, k-1)
				}
				if prev.v.Len() < p.v.Len() && prev.v.Extends(p.v) {
					t.Fatalf("seed %d: shorter view extends longer", seed)
				}
				suffix := p.v.Suffix(prev.v.Len())
				checkPendingView(t, seed, suffix, want[prev.apps:p.apps])
			}
		}
		// A second head over the same packets shares no arrays: no reuse.
		var other pendingHead
		for _, packet := range s.packets[:3] {
			other.add(packet, s.find)
		}
		ov := other.view(s.find)
		if pubs[len(pubs)-1].v.Extends(ov) {
			t.Fatalf("seed %d: unrelated head reported as prefix", seed)
		}
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
	v.Refs(0, v.Len(), func(p int, id uint64, registered *ApplicationPolicy, policy []byte) bool {
		if p != len(refs) {
			t.Fatalf("seed %d: Refs position %d, want %d", seed, p, len(refs))
		}
		refs = append(refs, id)
		return true
	})
	if len(refs) != v.Len() {
		t.Fatalf("seed %d: Refs visited %d, want %d", seed, len(refs), v.Len())
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
	packets, pols := receiverPackets(points, perPacket)
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
