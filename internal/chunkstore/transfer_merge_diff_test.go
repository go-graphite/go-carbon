package chunkstore_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// mergeDiffCase is one random name: its source history, its live target
// points (none when absent) and the live points written after the import.
type mergeDiffCase struct {
	kind   string
	config store.MetricConfig
	source [][]store.Point // UpdateMany calls, in order
	target [][]store.Point
	after  [][]store.Point
	// The first sourceClassic (targetClassic) calls are stored classic by a
	// RestoreArchives import, as converted and once-merged copies are; the
	// rest is written live on top. 0 keeps the copy compressed.
	sourceClassic, targetClassic int
	// The source is written with the clock at its newest point, as the donor
	// wrote it, so its rings hold slots the target's newer points displace.
	// A non-zero targetEarlyAt writes target[0] at that earlier clock: stale
	// target slots that newer source points displace.
	sourceAt, targetEarlyAt int64
}

// at returns the clock for writing call i of the source or the target.
func (c mergeDiffCase) at(target bool, i int, now int64) int64 {
	switch {
	case !target:
		return c.sourceAt
	case i == 0 && c.targetEarlyAt != 0:
		return c.targetEarlyAt
	}
	return now
}

func (c mergeDiffCase) format() string {
	f := func(n int) string {
		if n > 0 {
			return "classic"
		}
		return "compressed"
	}
	return f(c.sourceClassic) + "/" + f(c.targetClassic)
}

const mergeDiffMinute = 60

// mergeDiffPoints returns points on the 1-minute grid in [from, until), thinned
// as a downsampled real archive is (minutely within a day of now, 10-minute
// within a week, hourly beyond), with random gaps and NaNs.
func mergeDiffPoints(r *rand.Rand, from, until, now int64, nan float64) []store.Point {
	var points []store.Point
	level := 1e6 * r.Float64()
	from -= from % mergeDiffMinute
	for ts := from; ts < until; ts += mergeDiffMinute {
		switch age := now - ts; {
		case age > 7*86400 && ts%3600 != 0, age > 86400 && ts%600 != 0:
			continue
		}
		if r.Float64() < 0.05 {
			continue
		}
		level = math.Max(0, level+1000*r.NormFloat64())
		v := math.Round(level) / 8
		switch x := r.Float64(); {
		case x < nan/2:
			v = math.NaN()
		case x < nan:
			v = math.Float64frombits(0x7ff8000000000001 + uint64(r.Intn(1<<20)))
		}
		points = append(points, store.Point{Timestamp: ts, Value: v})
	}
	return points
}

// mergeDiffCalls splits points into UpdateMany calls. ordered=false shuffles
// the calls (late points reach the sidecar) and the points inside each call.
func mergeDiffCalls(r *rand.Rand, points []store.Point, ordered bool) [][]store.Point {
	var calls [][]store.Point
	for len(points) > 0 {
		n := min(len(points), 1+r.Intn(400))
		calls = append(calls, slices.Clone(points[:n]))
		points = points[n:]
	}
	if !ordered {
		r.Shuffle(len(calls), func(i, j int) { calls[i], calls[j] = calls[j], calls[i] })
		for _, c := range calls {
			r.Shuffle(len(c), func(i, j int) { c[i], c[j] = c[j], c[i] })
		}
	}
	return calls
}

var mergeDiffKinds = []string{"absent", "present", "present", "present", "overlap", "late", "aliased", "stale"}

func newMergeDiffCase(r *rand.Rand, name string, now int64) mergeDiffCase {
	kind := mergeDiffKinds[r.Intn(len(mergeDiffKinds))]
	methods := []store.AggregationMethod{store.Average, store.Sum, store.Last, store.Max, store.Min, store.First}
	c := mergeDiffCase{kind: kind, config: store.MetricConfig{
		Name:              name,
		Retentions:        []store.Retention{{Step: 60, Count: 1440}, {Step: 600, Count: 1008}, {Step: 3600, Count: 720}},
		AggregationMethod: methods[r.Intn(len(methods))],
		XFilesFactor:      []float32{0, 0, 0.5}[r.Intn(3)],
	}}
	nan := []float64{0, 0, 0.02, 0.3}[r.Intn(4)]
	handoff := now - int64(10+r.Intn(2*24*60))*mergeDiffMinute
	span := int64(1+r.Intn(20*24)) * 3600 // up to 20 days: inside the hourly ring
	if kind == "aliased" {
		span = int64(31+r.Intn(30)) * 86400 // every ring wraps
	}
	sourceEnd := handoff
	if kind == "overlap" {
		sourceEnd = handoff + (now-handoff)*int64(1+r.Intn(9))/10
	}
	c.source = mergeDiffCalls(r, mergeDiffPoints(r, handoff-span, sourceEnd, now, nan), r.Intn(3) > 0)
	c.sourceAt = sourceEnd
	classic := func(calls [][]store.Point) int {
		if len(calls) == 0 || r.Intn(3) == 0 {
			return 0
		}
		// Usually all calls, sometimes leaving live writes on top.
		return len(calls) - r.Intn(min(len(calls), 3))
	}
	c.sourceClassic = classic(c.source)
	if kind == "absent" {
		return c
	}
	live := mergeDiffPoints(r, handoff, now, now, nan)
	c.target = mergeDiffCalls(r, live, r.Intn(3) > 0)
	if kind == "stale" {
		// The name lived on the target before: a burst written two days
		// before the handoff, never overwritten since.
		at := handoff - 2*86400
		at -= at % mergeDiffMinute
		c.target = append([][]store.Point{mergeDiffPoints(r, at-6*3600, at, at, nan)}, c.target...)
		c.targetEarlyAt = at
	}
	if kind == "late" {
		// Late input reaches the target after its newest point, inside and
		// before the source's range.
		var late []store.Point
		for range 1 + r.Intn(20) {
			ts := handoff - int64(r.Intn(3*24*60))*mergeDiffMinute
			late = append(late, store.Point{Timestamp: ts, Value: float64(r.Intn(1000))})
		}
		c.target = append(c.target, late)
	}
	c.targetClassic = classic(c.target)
	return c
}

// mergeDiffAfter is the live input written after the import: the next hour.
func mergeDiffAfter(r *rand.Rand, now int64) [][]store.Point {
	return mergeDiffCalls(r, mergeDiffPoints(r, now, now+3600, now+3600, 0.02), r.Intn(2) == 0)
}

func openMergeDiffStore(t *testing.T, clock func() time.Time) *store.Store {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "originals"), 0750); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(filepath.Join(root, "pool"), store.Options{ExactBlocks: true, CacheSize: 16 << 20, MemTableSize: 16 << 20, SyncInterval: time.Hour, Now: clock, Continuation: &whisperio.ContinuationCodec{DirectLateWrites: true}, IncrementalSource: filepath.Join(root, "originals"), TimeBuckets: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func writeMergeDiff(t *testing.T, s *store.Store, config store.MetricConfig, calls [][]store.Point) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Metadata(ctx, config.Name); err != nil {
		if _, err := s.Create(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	for _, points := range calls {
		if err := s.UpdateMany(ctx, config.Name, slices.Clone(points)); err != nil {
			t.Fatal(err)
		}
	}
}

// storeMergeDiffClassic writes each case's first n calls (n from pick) to a
// scratch store and imports them into every dst, where the name exists but is
// empty, with the append merge off: RestoreArchives stores them classic. The
// remaining calls are written live on top.
func storeMergeDiffClassic(t *testing.T, cases []mergeDiffCase, list []string, now func() time.Time, pick func(mergeDiffCase) ([][]store.Point, int), set func(c mergeDiffCase, call int), dsts ...*store.Store) {
	t.Helper()
	ctx := context.Background()
	scratch := openMergeDiffStore(t, now)
	var names []string
	for i, c := range cases {
		calls, n := pick(c)
		if n == 0 {
			continue
		}
		writeMergeDiffAt(t, scratch, c.config, calls[:n], func(i int) { set(c, i) })
		names = append(names, list[i])
		for _, dst := range dsts {
			if _, err := dst.Create(ctx, c.config); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(names) > 0 {
		records, err := scratch.ExportTransfer(ctx, names)
		if err != nil {
			t.Fatal(err)
		}
		for _, dst := range dsts {
			batch := slices.Clone(records)
			for i := range batch {
				batch[i].Keys = slices.Clone(batch[i].Keys)
			}
			dst.SetTransferMergeAppendForTest(false)
			if _, err := dst.ImportTransfer(ctx, batch); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, c := range cases {
		calls, n := pick(c)
		if n == 0 {
			continue
		}
		for _, dst := range dsts {
			writeMergeDiffAt(t, dst, c.config, calls[n:], func(i int) { set(c, n+i) })
		}
	}
}

// writeMergeDiffAt writes calls like writeMergeDiff, calling set(i) before
// call i to move the clock.
func writeMergeDiffAt(t *testing.T, s *store.Store, config store.MetricConfig, calls [][]store.Point, set func(i int)) {
	t.Helper()
	for i := range calls {
		set(i)
		writeMergeDiff(t, s, config, calls[i:i+1])
	}
}

// diffSnapshots reports the first bit difference between two snapshots.
func diffSnapshots(a, b store.Snapshot) string {
	if len(a.Archives) != len(b.Archives) {
		return fmt.Sprintf("archives %d != %d", len(a.Archives), len(b.Archives))
	}
	for i := range a.Archives {
		pa, pb := a.Archives[i].Points, b.Archives[i].Points
		if a.Archives[i].Retention != b.Archives[i].Retention {
			return fmt.Sprintf("archive %d retention", i)
		}
		for j := 0; j < max(len(pa), len(pb)); j++ {
			switch {
			case j >= len(pa):
				return fmt.Sprintf("archive %d: new path has extra point %d %v", i, pb[j].Timestamp, pb[j].Value)
			case j >= len(pb):
				return fmt.Sprintf("archive %d: new path lacks point %d %v", i, pa[j].Timestamp, pa[j].Value)
			case pa[j].Timestamp != pb[j].Timestamp || math.Float64bits(pa[j].Value) != math.Float64bits(pb[j].Value):
				return fmt.Sprintf("archive %d point %d: old %d %x, new %d %x", i, j, pa[j].Timestamp, math.Float64bits(pa[j].Value), pb[j].Timestamp, math.Float64bits(pb[j].Value))
			}
		}
	}
	return ""
}

// TestTransferMergeAppendMatchesRestore is the gate for the append merge of
// present continuation names, and for the in-place merges of classic copies
// (a classic source, or a compressed source meeting a classic target). Random transfers (absent, present, overlapping,
// NaN, late, ring-aliased, stale names, written out of order; sources and
// targets each compressed or classic) are imported into two
// identically seeded targets: one with the append merge, one forced through
// RestoreArchives. Receipts, snapshots and snapshots after one more hour of
// live input must match bit for bit, name by name.
//
// CHUNKSTORE_MERGE_DIFF_ROUNDS raises the number of 64-name rounds (default 2).
func TestTransferMergeAppendMatchesRestore(t *testing.T) {
	rounds := 2
	if v, err := strconv.Atoi(os.Getenv("CHUNKSTORE_MERGE_DIFF_ROUNDS")); err == nil && v > 0 {
		rounds = v
	}
	if testing.Short() {
		rounds = 1
	}
	const names, group = 64, 16
	for round := range rounds {
		t.Run(fmt.Sprintf("round%d", round), func(t *testing.T) {
			ctx := context.Background()
			base := int64(1763000000)
			base -= base % 3600
			var clock atomic.Int64
			clock.Store(base)
			now := func() time.Time { return time.Unix(clock.Load(), 0) }
			r := rand.New(rand.NewSource(int64(round) + 1))
			cases := make([]mergeDiffCase, names)
			list := make([]string, names)
			source := openMergeDiffStore(t, now)
			oldPath, newPath := openMergeDiffStore(t, now), openMergeDiffStore(t, now)
			oldPath.SetTransferMergeAppendForTest(false)
			for i := range cases {
				list[i] = fmt.Sprintf("merge.diff.r%d.n%03d", round, i)
				cases[i] = newMergeDiffCase(r, list[i], base)
				cases[i].after = mergeDiffAfter(r, base)
				c := cases[i]
				if c.sourceClassic == 0 {
					writeMergeDiffAt(t, source, c.config, c.source, func(k int) { clock.Store(c.at(false, k, base)) })
				}
				if c.target != nil && c.targetClassic == 0 {
					writeMergeDiffAt(t, oldPath, c.config, c.target, func(k int) { clock.Store(c.at(true, k, base)) })
					writeMergeDiffAt(t, newPath, c.config, c.target, func(k int) { clock.Store(c.at(true, k, base)) })
				}
			}
			storeMergeDiffClassic(t, cases, list, now, func(c mergeDiffCase) ([][]store.Point, int) { return c.source, c.sourceClassic },
				func(c mergeDiffCase, k int) { clock.Store(c.at(false, k, base)) }, source)
			storeMergeDiffClassic(t, cases, list, now, func(c mergeDiffCase) ([][]store.Point, int) { return c.target, c.targetClassic },
				func(c mergeDiffCase, k int) { clock.Store(c.at(true, k, base)) }, oldPath, newPath)
			clock.Store(base)
			newPath.SetTransferMergeAppendForTest(true)
			ns0, os0 := newPath.Stats(), oldPath.Stats()
			records, err := source.ExportTransfer(ctx, list)
			if err != nil {
				t.Fatal(err)
			}
			proofs := map[*store.Store][]store.TransferProof{}
			for _, target := range []*store.Store{oldPath, newPath} {
				for start := 0; start < names; start += group {
					batch := slices.Clone(records[start : start+group])
					for i := range batch {
						batch[i].Keys = slices.Clone(batch[i].Keys)
					}
					got, err := target.ImportTransfer(ctx, batch)
					if err != nil {
						t.Fatal(err)
					}
					if err := target.VerifyTransfer(ctx, got); err != nil {
						t.Fatal(err)
					}
					proofs[target] = append(proofs[target], got...)
				}
			}
			diffs := 0
			compare := func(phase string) {
				for i, name := range list {
					a, err := oldPath.Snapshot(ctx, name)
					if err != nil {
						t.Fatal(err)
					}
					b, err := newPath.Snapshot(ctx, name)
					if err != nil {
						t.Fatal(err)
					}
					if d := diffSnapshots(a, b); d != "" {
						diffs++
						t.Errorf("%s %s (%s, %s, %s, xff %v): %s", phase, name, cases[i].kind, cases[i].format(), cases[i].config.AggregationMethod, cases[i].config.XFilesFactor, d)
					}
				}
			}
			for i := range list {
				if !reflect.DeepEqual(proofs[oldPath][i], proofs[newPath][i]) {
					diffs++
					t.Errorf("receipt %s (%s): old %+v new %+v", list[i], cases[i].kind, proofs[oldPath][i], proofs[newPath][i])
				}
			}
			compare("import")
			// One more hour of live input must land the same way on both.
			clock.Store(base + 3600)
			for i := range cases {
				writeMergeDiff(t, oldPath, cases[i].config, cases[i].after)
				writeMergeDiff(t, newPath, cases[i].config, cases[i].after)
			}
			compare("after")
			kinds, formats := map[string]int{}, map[string]int{}
			classicPresent, classicTargetPresent := 0, 0
			for _, c := range cases {
				kinds[c.kind]++
				if c.kind != "absent" {
					formats[c.format()]++
					if c.sourceClassic > 0 {
						classicPresent++
					} else if c.targetClassic > 0 {
						classicTargetPresent++
					}
				}
			}
			// Count only the gate import, not the classic seeding imports.
			ns, os := newPath.Stats(), oldPath.Stats()
			appended, classic := ns.TransferMergeAppended-ns0.TransferMergeAppended, ns.TransferMergeAppendedClassic-ns0.TransferMergeAppendedClassic
			fallbacks := map[string]uint64{}
			for reason, n := range ns.TransferMergeFallbacks {
				if d := n - ns0.TransferMergeFallbacks[reason]; d != 0 {
					fallbacks[reason] = d
				}
			}
			causes := map[string]uint64{}
			for cause, c := range ns.TransferMergeFallbackCauses {
				if d := c.Count - ns0.TransferMergeFallbackCauses[cause].Count; d != 0 {
					causes[cause] = d
				}
			}
			t.Logf("kinds %v; present formats (source/target) %v; new path appended %d (classic %d) fallbacks %v causes %v; differences %d", kinds, formats, appended, classic, fallbacks, causes, diffs)
			if os.TransferMergeAppended != os0.TransferMergeAppended || fallbacks[store.TransferMergeFallbackDisabled] != 0 {
				t.Fatalf("path switch leaked: old appended %d, new disabled %d", os.TransferMergeAppended-os0.TransferMergeAppended, fallbacks[store.TransferMergeFallbackDisabled])
			}
			// The disjoint present names must take the append path, and classic
			// sources the in-place path, or the comparison above proves nothing.
			if int(appended) < kinds["present"]/4 {
				t.Fatalf("only %d of %d present names appended", appended, kinds["present"])
			}
			// A compressed source meeting a classic target is merged into a copy
			// of the target in place, never refused for the layout alone.
			if int(classic) < (classicPresent+classicTargetPresent)/3 {
				t.Fatalf("only %d of %d present names with a classic source or target merged in place", classic, classicPresent+classicTargetPresent)
			}
			if n := causes[store.TransferMergeFallbackLayout+"/classic-target"]; n != 0 {
				t.Fatalf("%d compressed sources refused for a classic target", n)
			}
		})
	}
}
