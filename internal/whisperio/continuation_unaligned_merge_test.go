package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// unalignedCWhisper writes a CWhisper file of the sparse schema whose base
// archive's first block holds points off the 60-second grid, as production
// files with corrupt blocks do: the block's raw first point is moved by
// residue seconds, which moves every delta-encoded point after it by the same
// amount, and the header's block range and encoder state follow. CWhisper
// reads and appends to such a file. It returns the base block's points.
func unalignedCWhisper(t testing.TB, path string, now func() time.Time, start, minutes, residue int) []whisper.TimeSeriesPoint {
	t.Helper()
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1m:14d,30m:2y"), whisper.Sum, 0,
		&whisper.Options{Compressed: true, OutOfOrder: true, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	var seed []*whisper.TimeSeriesPoint
	for i := 0; i < minutes; i++ {
		v := 0.0
		if i%37 == 0 {
			v = 58
		}
		seed = append(seed, &whisper.TimeSeriesPoint{Time: start + 60*i, Value: v})
	}
	if err := w.UpdateMany(seed); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := whisper.OpenWithOptions(path, &whisper.Options{})
	if err != nil {
		t.Fatal(err)
	}
	layout, err := source.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	var block whisper.ContinuationBlock
	var points []whisper.TimeSeriesPoint
	for _, b := range layout.Blocks {
		if b.Archive == 0 && b.Index == 0 {
			block = b
			if points, err = source.ContinuationBlockPoints(b); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if len(points) < 3 {
		t.Fatalf("base block 0 holds %d points", len(points))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	be := func(v int) []byte { return binary.BigEndian.AppendUint32(nil, uint32(v)) }
	header := data[:len(layout.Header)]
	// p0 is the encoder state's first point and the block range start; pn1
	// and pn2 are the encoder state's last two points, pn1 also the range end.
	for _, v := range []int{points[0].Time, points[len(points)-1].Time, points[len(points)-2].Time} {
		if n := bytes.Count(header, be(v)); n == 0 || n > 2 {
			t.Fatalf("interval %d appears %d times in the header", v, n)
		}
		header = bytes.ReplaceAll(header, be(v), be(v+residue))
	}
	copy(data, header)
	if !bytes.Equal(data[block.Offset:block.Offset+4], be(points[0].Time)) {
		t.Fatal("block does not start with its raw first point")
	}
	copy(data[block.Offset:], be(points[0].Time+residue))
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	source, err = whisper.OpenWithOptions(path, &whisper.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	layout, err = source.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range layout.Blocks {
		if b.Archive != 0 || b.Index != 0 {
			continue
		}
		got, err := source.ContinuationBlockPoints(b)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(points) {
			t.Fatalf("patched block holds %d points, want %d", len(got), len(points))
		}
		for i := range got {
			if got[i].Time != points[i].Time+residue || math.Float64bits(got[i].Value) != math.Float64bits(points[i].Value) {
				t.Fatalf("patched point %d: %+v, want %d %v", i, got[i], points[i].Time+residue, points[i].Value)
			}
		}
		return got
	}
	t.Fatal("patched block 0 missing")
	return nil
}

// TestUnalignedBlockSurvivesMerge converts a CWhisper file whose base block
// holds points off the archive grid (accepted since conversion stopped
// requiring alignment), then writes late points and lets maintenance fold the
// sidecar, as the persister does. Every rewrite reads each block back through
// the virtual file, which re-encodes the block's exact points: that encoder
// rejected off-grid points, so the merge failed with
//
//	merge out-of-order points: archives[0].blocks[0].file.read: continuation points must increase on their archive grid
//
// and the persister requeued the batch forever. The block must stay readable,
// writable and mergeable, and a merge must keep what reads served: a late
// point at an interval wins over a stored point off that interval's start.
//
// With direct late writes, late points and an import fold reach the off-grid
// block through planBlock, which leaves it to the rewrite (sidecar), and the
// source may arrive with a sidecar of its own holding a point in that block.
func TestUnalignedBlockSurvivesMerge(t *testing.T) {
	type variant struct{ buckets, direct, sidecar bool }
	var variants []variant
	for _, buckets := range []bool{false, true} {
		for _, direct := range []bool{false, true} {
			for _, sidecar := range []bool{false, true} {
				variants = append(variants, variant{buckets, direct, sidecar})
			}
		}
	}
	for _, v := range variants {
		buckets, direct, sidecar := v.buckets, v.direct, v.sidecar
		for _, cache := range []int64{0, 64 << 20} {
			t.Run(fmt.Sprintf("v7=%v/direct=%v/sidecar=%v/cache=%v", buckets, direct, sidecar, cache > 0), func(t *testing.T) {
				ctx := context.Background()
				start := 1_792_400_400 + 7*60 // on the minute grid, off the 30-minute grid
				const minutes, residue = 3 * 1440, 17
				now := time.Unix(int64(start+60*minutes), 0)
				clock := func() time.Time { return now }
				root := t.TempDir()
				sourceDir := filepath.Join(root, "source")
				if err := os.Mkdir(sourceDir, 0700); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(sourceDir, "m.wsp")
				block := unalignedCWhisper(t, path, clock, start, minutes, residue)
				// An aligned slot inside the off-grid block that the source's
				// sidecar holds, as a late point the original file kept there.
				sourceLate := block[len(block)/4].Time - residue
				if sidecar {
					w, err := whisper.OpenWithOptions(path, &whisper.Options{OutOfOrder: true, Now: clock})
					if err != nil {
						t.Fatal(err)
					}
					if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: sourceLate, Value: 777}}); err != nil {
						t.Fatal(err)
					}
					hasSidecar := w.OutOfOrderPath() != ""
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					if !hasSidecar {
						t.Fatal("the source's late point did not go to a sidecar")
					}
				}
				folds := importFoldCounts()
				aligned := whisper.RewriteBlockFallbackCount(whisper.RewriteBlockAligned)
				db, err := openMemoryTestStore(t, filepath.Join(root, "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: direct},
					IncrementalSource: sourceDir, Now: clock, TimeBuckets: buckets, MetadataCacheSize: cache,
					CacheSize: 8 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if cache == 0 {
					defer db.DisableMetadataCacheForTest()()
				}
				capture, err := ReadSnapshotIn(ctx, "m", path, root)
				if err != nil {
					t.Fatal(err)
				}
				staged, err := db.StageFileSnapshot(ctx, capture)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.PublishFileOwnership(ctx, capture, staged); err != nil {
					t.Fatal(err)
				}
				// What reads serve: each stored point at the start of its
				// interval, and every later write of an interval in its place.
				want := map[int]float64{}
				for _, p := range block {
					want[p.Time-residue] = p.Value
				}
				if sidecar {
					want[sourceLate] = 777
					// A fold keeping the stored off-grid point next to the
					// sidecar's would hide the sidecar's value from reads.
					if d := importFoldDelta(folds); d[whisper.InPlaced.String()] != 0 {
						t.Fatalf("import folded the sidecar into the off-grid block: %v", d)
					} else if direct && len(d) == 0 {
						t.Fatal("import with direct late writes did not try to fold the sidecar")
					}
				}
				late := block[len(block)/2].Time - residue // an aligned slot inside the off-grid block
				merges := 0
				for round := 0; round < 6; round++ {
					now = now.Add(5 * time.Minute)
					var input []store.Point
					for i := 4; i >= 0; i-- {
						input = append(input, store.Point{Timestamp: now.Unix() - int64(60*i) - 3, Value: 58})
					}
					if round%2 == 1 {
						input = append(input, store.Point{Timestamp: int64(late + 60*round), Value: 1000 + float64(round)})
					}
					maintenance := store.ContinuationMaintenance{Allow: func() bool { return round%2 == 1 }, Result: func(merged bool, err error) {
						if merged {
							merges++
						}
					}}
					if err := db.UpdateManyMaintained(ctx, "m", input, maintenance); err != nil {
						t.Fatalf("round %d: %v", round, err)
					}
					if direct && db.Sidecars().Count != 0 {
						t.Fatalf("round %d: direct update retained a sidecar", round)
					}
					for _, p := range input {
						want[int(p.Timestamp)-int(p.Timestamp)%60] = p.Value
					}
					got, err := db.Fetch(ctx, "m", block[0].Time-residue, int(now.Unix()))
					if err != nil {
						t.Fatalf("round %d fetch: %v", round, err)
					}
					checked := 0
					for ts, v := range want {
						if ts < got.FromTime || ts >= got.UntilTime {
							continue // a fetch starts one step after from
						}
						if x := got.Values[(ts-got.FromTime)/got.Step]; x != v {
							t.Fatalf("round %d: slot %d reads %v, want %v", round, ts, x, v)
						}
						checked++
					}
					if checked < len(block)-2 {
						t.Fatalf("round %d: checked only %d slots", round, checked)
					}
				}
				if !direct && merges == 0 {
					t.Fatal("no sidecar merge ran")
				}
				// Import or an update rewrote the off-grid block onto the grid; this is
				// the path that repairs a block converted from a corrupt source.
				if whisper.RewriteBlockFallbackCount(whisper.RewriteBlockAligned) == aligned {
					t.Fatal("no rewrite aligned the off-grid block")
				}
			})
		}
	}
}

// offGridPoints counts the stored points of the main file's populated blocks
// that lie off their archive's grid.
func offGridPoints(t testing.TB, kv store.ContinuationIO) int {
	t.Helper()
	fs := newContinuationFiles(kv, true)
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	physical, err := w.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	in, err := fs.OpenFile(fs.main(), os.O_RDONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	n := 0
	for _, b := range physical.Blocks {
		if b.Count == 0 {
			continue
		}
		points, err := in.(*continuationFile).ReadStorageBlock(b)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range points {
			if p.Time%b.Step != 0 {
				n++
			}
		}
	}
	return n
}

// TestMergeRepairsConvertedOffGridBlock repairs a metric stuck as production's
// was, in place: its converted base block holds points off the grid, which
// failed every merge. The first merge rewrites the block onto the grid,
// keeping what reads served plus the merged points, so no stored point is off
// the grid afterwards and the file reads whole.
func TestMergeRepairsConvertedOffGridBlock(t *testing.T) {
	ctx := context.Background()
	start := 1_792_400_400 + 7*60
	const minutes, residue = 3 * 1440, 17
	now := start + 60*minutes
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "m", Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Sum}}
	path := filepath.Join(t.TempDir(), "m.wsp")
	block := unalignedCWhisper(t, path, clock, start, minutes, residue)
	codec := &ContinuationCodec{ReuseBlockKeys: true}
	kv := mapContinuationIO{}
	if err := codec.Import(ctx, kv, path, m, clock); err != nil {
		t.Fatal(err)
	}
	if n := offGridPoints(t, kv); n < len(block) {
		t.Fatalf("converted file holds %d off-grid points, want at least %d", n, len(block))
	}
	want := map[int]float64{}
	for _, p := range block {
		want[p.Time-residue] = p.Value
	}
	now += 300
	late := block[len(block)/3].Time - residue
	input := []store.Point{{Timestamp: int64(now - 63), Value: 58}, {Timestamp: int64(late), Value: 4242}}
	if err := codec.Update(ctx, kv, m, input, -1, clock, &store.ContinuationMaintenance{Allow: func() bool { return true }}); err != nil {
		t.Fatal(err)
	}
	want[now-63-(now-63)%60], want[late] = 58, 4242
	if n := offGridPoints(t, kv); n != 0 {
		t.Fatalf("%d stored points still off the grid after the merge", n)
	}
	if err := readWholeContinuationFile(kv, now); err != nil {
		t.Fatal(err)
	}
	got, err := codec.Fetch(ctx, kv, m, start, now, clock)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for ts, v := range want {
		if ts < got.FromTime || ts >= got.UntilTime {
			continue
		}
		if x := got.Values[(ts-got.FromTime)/got.Step]; !sameValue(x, v) {
			t.Fatalf("slot %d reads %v, want %v", ts, x, v)
		}
		checked++
	}
	if checked < len(block) {
		t.Fatalf("checked only %d slots", checked)
	}
}

// TestDisorderedBlockErrorNamesThePoint corrupts a stored base block so one
// point repeats an earlier time, which no read accepts, then lets maintenance
// merge a late point. The failure must name the file, block, position and
// both times, so a production log identifies the block and the cause.
func TestDisorderedBlockErrorNamesThePoint(t *testing.T) {
	ctx := context.Background()
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "m", Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Sum}}
	codec := &ContinuationCodec{}
	kv := mapContinuationIO{}
	start := 1_792_400_400
	now := start
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	if err := codec.Create(ctx, kv, m, clock); err != nil {
		t.Fatal(err)
	}
	var history []store.Point
	for i := 0; i < 600; i++ {
		history = append(history, store.Point{Timestamp: int64(start + 60*i), Value: float64(i)})
	}
	now = start + 60*600
	if err := codec.Update(ctx, kv, m, history, -1, clock, nil); err != nil {
		t.Fatal(err)
	}
	// Point 5 of base block 0 repeats point 3's time.
	id := binary.BigEndian.Uint64(kv["n/main.wsp"])
	key := string(binary.BigEndian.AppendUint32(continuationBlockPrefix(id, 'c', 0, 0), 0))
	var group store.PointBlock
	if err := store.DecodePointBlock(kv[key], &group); err != nil {
		t.Fatal(err)
	}
	group.Points[5].Timestamp = group.Points[3].Timestamp
	kv[key] = store.EncodePointBlock(&group)
	now += 60
	late := []store.Point{{Timestamp: int64(now - 60), Value: 1}, {Timestamp: int64(start + 60*100), Value: 7777}}
	err := codec.Update(ctx, kv, m, late, -1, clock, &store.ContinuationMaintenance{Allow: func() bool { return true }})
	if err == nil {
		t.Fatal("merge of a disordered block succeeded")
	}
	t.Log(err)
	for _, want := range []string{fmt.Sprintf("file %d archive 0 block 0", id), "step 60",
		fmt.Sprintf("point 5 present true at %d follows %d", group.Points[3].Timestamp, group.Points[4].Timestamp)} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
	// The exact-point fallback reads the same refused points; say it once.
	if n := strings.Count(err.Error(), "invalid continuation physical point"); n != 1 {
		t.Fatalf("error %q names the cause %d times", err, n)
	}
}

// TestRewriteReadsExactPointsOfAnOverfullBlock stores a base block whose exact
// points no longer fit its byte capacity when encoded (their values are
// replaced by incompressible ones, as an encoder change could leave). The
// virtual file cannot produce the block's bytes, so a rewrite used to fail;
// it now streams the block's exact points instead.
func TestRewriteReadsExactPointsOfAnOverfullBlock(t *testing.T) {
	ctx := context.Background()
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "m", Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Sum}}
	codec := &ContinuationCodec{}
	kv := mapContinuationIO{}
	start := 1_792_400_400
	now := start
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	if err := codec.Create(ctx, kv, m, clock); err != nil {
		t.Fatal(err)
	}
	var history []store.Point
	for i := 0; i < 4000; i++ {
		history = append(history, store.Point{Timestamp: int64(start + 60*i), Value: 0})
	}
	now = start + 60*4000
	if err := codec.Update(ctx, kv, m, history, -1, clock, nil); err != nil {
		t.Fatal(err)
	}
	id := binary.BigEndian.Uint64(kv["n/main.wsp"])
	prefix := string(continuationBlockPrefix(id, 'c', 0, 0))
	rng := rand.New(rand.NewSource(1))
	want := map[int64]float64{}
	for key, value := range kv {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		var group store.PointBlock
		if err := store.DecodePointBlock(value, &group); err != nil {
			t.Fatal(err)
		}
		for slot := range group.Points {
			if group.Present[slot/64]&(1<<uint(slot%64)) != 0 {
				group.Points[slot].Value = float64(rng.Int63()) / 7
				want[group.Points[slot].Timestamp] = group.Points[slot].Value
			}
		}
		kv[key] = store.EncodePointBlock(&group)
	}
	if err := readWholeContinuationFile(kv, now); err == nil || !strings.Contains(err.Error(), "exceed their original block") {
		t.Fatalf("overfull block reads as bytes: %v", err)
	}
	before := whisper.RewriteBlockFallbackCount(whisper.RewriteBlockExact)
	now += 60
	late := []store.Point{{Timestamp: int64(now - 60), Value: 1}, {Timestamp: int64(start + 60*100), Value: 7777}}
	if err := codec.Update(ctx, kv, m, late, -1, clock, &store.ContinuationMaintenance{Allow: func() bool { return true }}); err != nil {
		t.Fatal(err)
	}
	if whisper.RewriteBlockFallbackCount(whisper.RewriteBlockExact) == before {
		t.Fatal("no rewrite read exact points")
	}
	if err := readWholeContinuationFile(kv, now); err != nil {
		t.Fatalf("rewritten file: %v", err)
	}
	want[int64(start+60*100)] = 7777
	got, err := codec.Fetch(ctx, kv, m, start, now, clock)
	if err != nil {
		t.Fatal(err)
	}
	for ts, v := range want {
		if int(ts) < got.FromTime || int(ts) >= got.UntilTime {
			continue
		}
		if x := got.Values[(int(ts)-got.FromTime)/got.Step]; !sameValue(x, v) {
			t.Fatalf("slot %d reads %v, want %v", ts, x, v)
		}
	}
}
