package chunkstore

import (
	"math"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"testing"
)

// Frozen pre-bitmap implementation is the oracle, including in-place writes.
func retainTransferSlotsMapOracle(points []Point, retention Retention) []Point {
	if len(points) < 2 || (points[len(points)-1].Timestamp-points[0].Timestamp)/int64(retention.Step) < int64(retention.Count) {
		return points
	}
	seen := make(map[int64]struct{})
	start := len(points)
	for i := len(points) - 1; i >= 0; i-- {
		slot := points[i].Timestamp / int64(retention.Step) % int64(retention.Count)
		if _, ok := seen[slot]; ok {
			continue
		}
		seen[slot] = struct{}{}
		start--
		points[start] = points[i]
	}
	return points[start:]
}

func TestRetainTransferSlotsBitmapDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(7944))
	special := []uint64{0, 1, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7ff0000000000001, 0x0010000000000000, 0x7fefffffffffffff}
	for trial := 0; trial < 600; trial++ {
		counts := []int{1, 2, 63, 64, 65, 2160, 8640, 10080, 65535, 65536, 65537, 1 << 30}
		r := Retention{Step: []int{1, 10, 60, 3600}[trial%4], Count: counts[trial%len(counts)]}
		n := []int{0, 1, 2, 127, 128, 129, 512, 8192}[trial%8]
		p := make([]Point, n)
		base := int64(1790000000)
		if trial%7 == 0 {
			base = -int64(r.Count*r.Step) * 2
		}
		for i := range p {
			// Repeated slots, duplicate timestamps, sparse and dense multiple epochs.
			ts := base + int64(rng.Intn(r.Count))*int64(r.Step) + int64(rng.Intn(4))*int64(r.Count)*int64(r.Step)
			v := rng.Uint64()
			if i%3 == 0 {
				v = special[(i+trial)%len(special)]
			}
			p[i] = Point{Timestamp: ts, Value: math.Float64frombits(v)}
		}
		sort.SliceStable(p, func(i, j int) bool { return p[i].Timestamp < p[j].Timestamp })
		old, fast := slices.Clone(p), slices.Clone(p)
		want, got := retainTransferSlotsMapOracle(old, r), retainTransferSlots(fast, r)
		if len(want) != len(got) {
			t.Fatalf("trial %d lengths %d/%d", trial, len(want), len(got))
		}
		for i := range old {
			if old[i].Timestamp != fast[i].Timestamp || math.Float64bits(old[i].Value) != math.Float64bits(fast[i].Value) {
				t.Fatalf("trial %d backing point %d differs", trial, i)
			}
		}
		oldFuture, fastFuture := slices.Clone(p), slices.Clone(p)
		now := base + int64(r.Count)*int64(r.Step)/2
		wantFuture := retainTransferSlotsAtOracle(oldFuture, r, now)
		gotFuture := retainTransferSlotsAt(fastFuture, r, now)
		if len(wantFuture) != len(gotFuture) {
			t.Fatalf("trial %d future lengths differ", trial)
		}
		for i := range oldFuture {
			if oldFuture[i].Timestamp != fastFuture[i].Timestamp || math.Float64bits(oldFuture[i].Value) != math.Float64bits(fastFuture[i].Value) {
				t.Fatalf("trial %d future point %d differs", trial, i)
			}
		}
		for i := range want {
			if want[i].Timestamp != got[i].Timestamp || math.Float64bits(want[i].Value) != math.Float64bits(got[i].Value) {
				t.Fatalf("trial %d result point %d differs", trial, i)
			}
		}
	}
}

func BenchmarkRetainTransferSlotsBitmap(b *testing.B) {
	for _, count := range []int{2160, 10080, 65536} {
		r := Retention{Step: 10, Count: count}
		input := make([]Point, count*2)
		for i := range input {
			input[i] = Point{Timestamp: int64(i * 10), Value: math.Float64frombits(uint64(i))}
		}
		for _, fast := range []bool{false, true} {
			name := "map"
			if fast {
				name = "bitmap"
			}
			b.Run(strconv.Itoa(count)+"/"+name, func(b *testing.B) {
				data := make([]Point, len(input))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					copy(data, input)
					if fast {
						retainTransferSlots(data, r)
					} else {
						retainTransferSlotsMapPooledBenchmark(data, r)
					}
				}
			})
		}
	}
}

// The baseline borrows the production map pool, so allocation reuse is matched.
func retainTransferSlotsMapPooledBenchmark(points []Point, r Retention) []Point {
	seen := borrowTransferSlots()
	defer releaseTransferSlots(seen)
	start := len(points)
	for i := len(points) - 1; i >= 0; i-- {
		slot := points[i].Timestamp / int64(r.Step) % int64(r.Count)
		if _, ok := seen[slot]; ok {
			continue
		}
		seen[slot] = struct{}{}
		start--
		points[start] = points[i]
	}
	return points[start:]
}

func retainTransferSlotsAtOracle(points []Point, r Retention, now int64) []Point {
	if len(points) > 1 && points[len(points)-1].Timestamp > now {
		retained := make(map[int64]struct{})
		cutoff := now - int64(r.Step)*int64(r.Count)
		for _, p := range points {
			if p.Timestamp > cutoff && p.Timestamp <= now {
				retained[p.Timestamp/int64(r.Step)%int64(r.Count)] = struct{}{}
			}
		}
		points = slices.DeleteFunc(points, func(p Point) bool {
			_, occupied := retained[p.Timestamp/int64(r.Step)%int64(r.Count)]
			return p.Timestamp > now && occupied
		})
	}
	return retainTransferSlotsMapOracle(points, r)
}
