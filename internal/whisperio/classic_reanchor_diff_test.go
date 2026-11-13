package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// placedClassicImage stores each archive's points at explicit physical slots,
// reproducing images that Whisper and the pre-1cece5866 restore produced.
func placedClassicImage(t testing.TB, retentions []store.Retention, archives []map[int]store.Point) (mapContinuationIO, store.Metadata) {
	t.Helper()
	kv, m := absoluteClassicImage(t, retentions, make([][]store.Point, len(retentions)))
	fs := newContinuationFiles(kv, false)
	file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	f := file.(*continuationFile)
	for ai, slots := range archives {
		groups := map[int]*store.PointBlock{}
		for slot, p := range slots {
			block := groups[slot/128]
			if block == nil {
				block = &store.PointBlock{}
				groups[slot/128] = block
			}
			block.Present[slot%128/64] |= uint64(1) << uint(slot%64)
			block.Points[slot%128] = p
			f.allocateClassicPages(f.data.layout.Archives[ai].Offset+slot*12, f.data.layout.Archives[ai].Offset+slot*12+12)
		}
		for group, block := range groups {
			if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, group), block); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := fs.flush(); err != nil {
		t.Fatal(err)
	}
	return kv, m
}

// prodShapedClassicSlots models damaged production archives: a restore wrote
// almost a full retention at absolute slots but missed absolute slot zero,
// then live writes anchored at the first live point filled slots 0..live-1.
// Every live point aliases a restored point one ring older.
func prodShapedClassicSlots(r store.Retention, liveClass, live int) map[int]store.Point {
	step, count := int64(r.Step), int64(r.Count)
	restoreAt := (1793000000/(step*count)+1)*step*count + int64(liveClass)*step
	slots := map[int]store.Point{}
	for ts := restoreAt - (count-1)*step; ts < restoreAt; ts += step {
		if class := int(ts / step % count); class != 0 {
			slots[class] = store.Point{Timestamp: ts, Value: float64(ts % 977)}
		}
	}
	for k := 0; k < live; k++ {
		slots[k] = store.Point{Timestamp: restoreAt + int64(k)*step, Value: float64(k) + 0.5}
	}
	return slots
}

func cloneContinuationIO(kv mapContinuationIO) mapContinuationIO {
	out := mapContinuationIO{}
	for k, v := range kv {
		out[k] = bytes.Clone(v)
	}
	return out
}

func requireSameClassicSnapshots(t *testing.T, codec *ContinuationCodec, want, got mapContinuationIO, m store.Metadata) {
	t.Helper()
	a, err := codec.Snapshot(context.Background(), want, m, classicTestClock)
	if err != nil {
		t.Fatal(err)
	}
	b, err := codec.Snapshot(context.Background(), got, m, classicTestClock)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Archives) != len(b.Archives) {
		t.Fatalf("archives %d != %d", len(a.Archives), len(b.Archives))
	}
	for ai := range a.Archives {
		pa, pb := a.Archives[ai].Points, b.Archives[ai].Points
		if len(pa) != len(pb) {
			t.Fatalf("archive %d: %d points != %d", ai, len(pa), len(pb))
		}
		for i := range pa {
			if pa[i].Timestamp != pb[i].Timestamp || math.Float64bits(pa[i].Value) != math.Float64bits(pb[i].Value) {
				t.Fatalf("archive %d point %d: %+v != %+v", ai, i, pa[i], pb[i])
			}
		}
	}
}

type classicDiffFixture struct {
	name string
	kv   mapContinuationIO
	m    store.Metadata
	next []store.Point
}

func classicDiffFixtures(t *testing.T) []classicDiffFixture {
	var out []classicDiffFixture
	kv, snap := mixedClassicFixture(t)
	out = append(out, classicDiffFixture{"mixed", kv, snap.Metadata, []store.Point{{Timestamp: 999980, Value: 9}}})
	r := []store.Retention{{Step: 60, Count: 5040}, {Step: 600, Count: 600}}
	kv, m := placedClassicImage(t, r, []map[int]store.Point{prodShapedClassicSlots(r[0], 2000, 800), prodShapedClassicSlots(r[1], 100, 40)})
	out = append(out, classicDiffFixture{"prod-shaped", kv, m, nil})
	rng := rand.New(rand.NewSource(311))
	for i := 0; i < 200; i++ {
		fine := 60 + rng.Intn(400)
		r := []store.Retention{{Step: 10, Count: fine}, {Step: 60, Count: fine/6 + 1 + rng.Intn(200)}}
		archives := make([]map[int]store.Point, len(r))
		for ai, rr := range r {
			archives[ai] = map[int]store.Point{}
			phases := 1 + rng.Intn(3)
			anchors := make([]int64, phases)
			for p := range anchors {
				anchors[p] = (999000/int64(rr.Step) - int64(rng.Intn(rr.Count))) * int64(rr.Step)
			}
			for n := rng.Intn(rr.Count * 2); n > 0; n-- {
				anchor := anchors[rng.Intn(phases)]
				ts := anchor + int64(rng.Intn(2*rr.Count))*int64(rr.Step)
				slot := int((ts - anchor) / int64(rr.Step) % int64(rr.Count))
				if rng.Intn(4) == 0 {
					slot = rng.Intn(rr.Count) // Duplicate timestamps in other slots.
				}
				var value float64
				switch rng.Intn(5) {
				case 0:
					value = math.Float64frombits(0x7ff8000000000000 | uint64(rng.Intn(1<<20)))
				case 1:
					value = math.Copysign(0, -1)
				default:
					value = rng.NormFloat64()
				}
				archives[ai][slot] = store.Point{Timestamp: ts, Value: value}
			}
		}
		kv, m := placedClassicImage(t, r, archives)
		out = append(out, classicDiffFixture{fmt.Sprintf("random-%d", i), kv, m, []store.Point{{Timestamp: 999990, Value: 1}}})
	}
	return out
}

// The optimised repair must keep exactly the points the original inline repair
// kept, in every archive, before and after a following ordinary write.
func TestClassicReanchorMatchesReference(t *testing.T) {
	ctx := context.Background()
	codec := &ContinuationCodec{}
	written := [2]int{}
	for _, fx := range classicDiffFixtures(t) {
		for ai := range fx.m.Retentions {
			want, got := cloneContinuationIO(fx.kv), cloneContinuationIO(fx.kv)
			wantDrops, errWant := reanchorClassicArchiveReference(ctx, want, ai)
			overlay := newContinuationOverlay(got)
			gotDrops, errGot := reanchorClassicArchive(ctx, overlay, ai)
			if (errWant == nil) != (errGot == nil) {
				t.Fatalf("%s/%d: errors differ: %v vs %v", fx.name, ai, errWant, errGot)
			}
			if errWant != nil {
				continue
			}
			if wantDrops != gotDrops {
				t.Fatalf("%s/%d: drops %d != %d", fx.name, ai, wantDrops, gotDrops)
			}
			for k := range overlay.values {
				if len(k) == 20 && k[9] == 'p' {
					written[1]++
				}
			}
			for k, v := range want {
				if len(k) == 20 && k[9] == 'p' && !bytes.Equal(v, fx.kv[k]) {
					written[0]++
				}
			}
			if err := overlay.commit(); err != nil {
				t.Fatal(err)
			}
			requireSameClassicSnapshots(t, codec, want, got, fx.m)
			plan, err := codec.PlanArchiveAnchors(ctx, got)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Archives[ai].Misplaced != 0 || plan.Archives[ai].Blocked != "" {
				t.Fatalf("%s/%d: repaired archive still misplaced: %+v", fx.name, ai, plan.Archives[ai])
			}
			if fx.next == nil {
				continue
			}
			errWant = codec.Update(ctx, want, fx.m, fx.next, -1, classicTestClock, nil)
			errGot = codec.Update(ctx, got, fx.m, fx.next, -1, classicTestClock, nil)
			if (errWant == nil) != (errGot == nil) {
				t.Fatalf("%s/%d: follow-up errors differ: %v vs %v", fx.name, ai, errWant, errGot)
			}
			requireSameClassicSnapshots(t, codec, want, got, fx.m)
		}
	}
	if written[1] > written[0] {
		t.Fatalf("optimised repair wrote %d groups, reference changed %d", written[1], written[0])
	}
	t.Logf("groups written: reference changed %d, optimised wrote %d", written[0], written[1])
}

func BenchmarkClassicReanchorProdShaped(b *testing.B) {
	r := store.Retention{Step: 60, Count: 50400}
	for _, live := range []int{8000, 30000} {
		kv, _ := placedClassicImage(b, []store.Retention{r}, []map[int]store.Point{prodShapedClassicSlots(r, 20000, live)})
		for _, impl := range []struct {
			name string
			fn   func(context.Context, store.ContinuationIO, int) (uint64, error)
		}{{"reference", reanchorClassicArchiveReference}, {"optimised", reanchorClassicArchive}} {
			b.Run(fmt.Sprintf("live=%d/%s", live, impl.name), func(b *testing.B) {
				var blocks, bytesWritten int
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					overlay := newContinuationOverlay(kv)
					if _, err := impl.fn(context.Background(), overlay, 0); err != nil {
						b.Fatal(err)
					}
					blocks, bytesWritten = 0, 0
					for k, v := range overlay.values {
						if len(k) == 20 && k[9] == 'p' && binary.BigEndian.Uint16([]byte(k[10:12])) == 0 {
							blocks++
							bytesWritten += len(v)
						}
					}
				}
				b.ReportMetric(float64(blocks), "blocks-written/op")
				b.ReportMetric(float64(bytesWritten), "bytes-written/op")
			})
		}
	}
}

// reanchorClassicArchiveReference is the original inline repair, kept as the
// oracle for the differential test of reanchorClassicArchive.
func reanchorClassicArchiveReference(ctx context.Context, kv store.ContinuationIO, ai int) (uint64, error) {
	fs := newContinuationFiles(kv, false)
	file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	f := file.(*continuationFile)
	if f.data.layout.Compressed || ai < 0 || ai >= len(f.data.layout.Archives) {
		return 0, store.ErrFormat
	}
	a := f.data.layout.Archives[ai]
	prefix := continuationBlockPrefix(f.data.id, 'p', ai, 0)
	type locatedPoint struct {
		point store.Point
		slot  int
	}
	var points []locatedPoint
	var oldGroups []int
	err = kv.Visit(prefix, func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(key) != len(prefix)+4 {
			return store.ErrFormat
		}
		index := int(binary.BigEndian.Uint32(key[len(prefix):]))
		var block store.PointBlock
		if err := store.DecodePointBlock(value, &block); err != nil {
			return err
		}
		oldGroups = append(oldGroups, index)
		for i, p := range block.Points {
			if block.Present[i/64]&(uint64(1)<<uint(i%64)) == 0 {
				continue
			}
			if index*128+i >= a.Count || p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || p.Timestamp%int64(a.Step) != 0 {
				return fmt.Errorf("invalid classic point in archive %d: %w", ai, store.ErrFormat)
			}
			points = append(points, locatedPoint{p, index*128 + i})
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	oldBase := int64(0)
	for _, p := range points {
		if p.slot == 0 {
			oldBase = p.point.Timestamp
			break
		}
	}
	// Congruence is independent of the chosen base. A ring cannot represent
	// two distinct timestamps in one congruence class; retain the newer one.
	// For duplicate timestamps prefer the copy visible at the existing anchor.
	winners := make(map[int64]locatedPoint, len(points))
	var drops uint64
	for _, p := range points {
		absolute := p.point.Timestamp / int64(a.Step) % int64(a.Count)
		previous, exists := winners[absolute]
		if exists && previous.point.Timestamp != p.point.Timestamp {
			drops++
		}
		visible := oldBase != 0 && int(((p.point.Timestamp-oldBase)/int64(a.Step)%int64(a.Count)+int64(a.Count))%int64(a.Count)) == p.slot
		if !exists || p.point.Timestamp > previous.point.Timestamp || p.point.Timestamp == previous.point.Timestamp && visible {
			winners[absolute] = p
		}
	}
	base := int64(math.MaxInt64)
	for _, p := range winners {
		base = min(base, p.point.Timestamp)
	}
	fresh := make(map[int]*store.PointBlock)
	for _, p := range winners {
		slot := int((p.point.Timestamp - base) / int64(a.Step) % int64(a.Count))
		block := fresh[slot/128]
		if block == nil {
			block = &store.PointBlock{}
			fresh[slot/128] = block
		}
		block.Present[slot%128/64] |= uint64(1) << uint(slot%64)
		block.Points[slot%128] = p.point // Preserve IEEE bits, including NaN payloads.
		f.allocateClassicPages(a.Offset+slot*12, a.Offset+slot*12+12)
	}
	// The optional seek certificate covered the old phase. Invalidate it;
	// ordinary reads safely fall back without touching sibling archive data.
	f.data.windowIndex = nil
	f.data.windowLoaded, f.data.windowDirty = true, true
	for _, index := range oldGroups {
		if fresh[index] == nil {
			if err := kv.Delete(continuationBodyKey(f.data.id, 'p', ai, 0, index)); err != nil {
				return 0, err
			}
		}
	}
	for index, block := range fresh {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, index), block); err != nil {
			return 0, err
		}
	}
	return drops, fs.flush()
}
