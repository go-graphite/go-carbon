package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func absoluteClassicImage(t testing.TB, retentions []store.Retention, archives [][]store.Point) (mapContinuationIO, store.Metadata) {
	t.Helper()
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "mixed.classic", Retentions: retentions, AggregationMethod: store.Sum, XFilesFactor: 1}}
	snap := store.Snapshot{Metadata: m}
	for i, r := range retentions {
		snap.Archives = append(snap.Archives, store.Archive{Retention: r, Points: archives[i]})
	}
	kv := mapContinuationIO{}
	if err := (&ContinuationCodec{}).RestoreArchives(context.Background(), kv, snap, classicTestClock); err != nil {
		t.Fatal(err)
	}
	fs := newContinuationFiles(kv, false)
	file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	f := file.(*continuationFile)
	for ai, r := range retentions {
		if err := kv.DeletePrefix(continuationBlockPrefix(f.data.id, 'p', ai, 0)); err != nil {
			t.Fatal(err)
		}
		groups := map[int]*store.PointBlock{}
		for _, p := range archives[ai] {
			slot := int(p.Timestamp / int64(r.Step) % int64(r.Count))
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

func TestClassicLoss30Backfill319(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1794480000, 0) }
	var old []store.Point
	for i, ts := range []int64{1793012460, 1793012580, 1793021880, 1793028540, 1793047260} {
		old = append(old, store.Point{Timestamp: ts, Value: float64(i + 1)})
	}
	for i := 0; i < 25; i++ {
		old = append(old, store.Point{Timestamp: 1793100000 + int64(i*60), Value: float64(i + 10)})
	}
	kv, m := absoluteClassicImage(t, []store.Retention{{Step: 60, Count: 50400}}, [][]store.Point{old})
	codec := &ContinuationCodec{}
	if err := codec.Update(ctx, kv, m, []store.Point{{Timestamp: 1793980560, Value: 1}}, -1, clock, nil); err != nil {
		t.Fatal(err)
	}
	var input []store.Point
	for _, p := range old {
		input = append(input, store.Point{Timestamp: p.Timestamp + 748560, Value: 42})
	}
	for i := 0; i < 289; i++ {
		input = append(input, store.Point{Timestamp: 1794250020 + int64(i*60), Value: float64(i)})
	}
	if err := codec.Update(ctx, kv, m, input, -1, clock, nil); err != nil {
		t.Fatal(err)
	}
	got, err := codec.Snapshot(ctx, kv, m, clock)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append(old, input...), store.Point{Timestamp: 1793980560, Value: 1})
	slices.SortFunc(want, func(a, b store.Point) int {
		if a.Timestamp < b.Timestamp {
			return -1
		}
		if a.Timestamp > b.Timestamp {
			return 1
		}
		return 0
	})
	if err := store.EqualSnapshots(store.Snapshot{Metadata: m, Archives: []store.Archive{{Retention: m.Retentions[0], Points: want}}}, got); err != nil {
		t.Fatal(err)
	}
}

func TestClassicReanchorOnlyConflictingArchive(t *testing.T) {
	r := []store.Retention{{Step: 10, Count: 60}, {Step: 60, Count: 60}}
	kv, m := absoluteClassicImage(t, r, [][]store.Point{{{Timestamp: 999780, Value: 6}, {Timestamp: 999990, Value: 3}}, {{Timestamp: 999600, Value: 123}, {Timestamp: 999960, Value: 456}}})
	codec := &ContinuationCodec{}
	if err := codec.Update(context.Background(), kv, m, []store.Point{{Timestamp: 999800, Value: 1}}, 600, classicTestClock, nil); err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for k, v := range kv {
		before[k] = bytes.Clone(v)
	}
	stats := ClassicReanchors()
	if err := codec.Update(context.Background(), kv, m, []store.Point{{Timestamp: 999980, Value: 9}}, 600, classicTestClock, nil); err != nil {
		t.Fatal(err)
	}
	if ClassicReanchors().ReanchoredArchives-stats.ReanchoredArchives != 1 {
		t.Fatal("expected a fine-archive re-anchor")
	}
	requireClassicPoints(t, codec, kv, m, []store.Point{{Timestamp: 999780, Value: 6}, {Timestamp: 999990, Value: 3}, {Timestamp: 999980, Value: 9}})
	for k, v := range before {
		if len(k) == 20 && k[9] == 'p' && k[11] == 1 && !bytes.Equal(v, kv[k]) {
			t.Fatal("sibling archive changed")
		}
	}
	for k := range kv {
		if len(k) == 20 && k[9] == 'p' && k[11] == 1 && before[k] == nil {
			t.Fatal("sibling group added")
		}
	}
}

func TestClassicReanchorUnbufferedWrites(t *testing.T) {
	kv, snap := mixedClassicFixture(t)
	codec := &ContinuationCodec{keepTailReads: true}
	input := []store.Point{{Timestamp: 999980, Value: 9}}
	if err := codec.Update(context.Background(), kv, snap.Metadata, input, -1, classicTestClock, nil); err != nil {
		t.Fatal(err)
	}
	requireClassicPoints(t, codec, kv, snap.Metadata, append(snap.Archives[0].Points, input...))
}

func TestClassicReanchorPropagationConflict(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1000400, 0) }
	kv, m := absoluteClassicImage(t, []store.Retention{{Step: 10, Count: 60}, {Step: 60, Count: 60}}, [][]store.Point{{{Timestamp: 1000020, Value: 123}}, {{Timestamp: 997800, Value: 17}}})
	codec := &ContinuationCodec{}
	if err := codec.Update(ctx, kv, m, []store.Point{{Timestamp: 999600, Value: 1}}, 3600, clock, nil); err != nil {
		t.Fatal(err)
	}
	var input []store.Point
	for i := 0; i < 6; i++ {
		input = append(input, store.Point{Timestamp: 1000200 + int64(i*10), Value: 2})
	}
	before := ClassicReanchors()
	if err := codec.Update(ctx, kv, m, input, -1, clock, nil); err != nil {
		t.Fatal(err)
	}
	if ClassicReanchors().ReanchoredArchives-before.ReanchoredArchives != 1 {
		t.Fatal("coarse propagation did not repair")
	}
	got, err := codec.Snapshot(ctx, kv, m, clock)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []store.Point{{Timestamp: 997800, Value: 17}, {Timestamp: 999600, Value: 1}, {Timestamp: 1000200, Value: 12}} {
		if !slices.Contains(got.Archives[1].Points, want) {
			t.Fatalf("coarse point lost: %+v in %+v", want, got.Archives[1].Points)
		}
	}
	if !slices.Contains(got.Archives[0].Points, store.Point{Timestamp: 1000020, Value: 123}) {
		t.Fatal("retry lost unrelated fine history")
	}
}

func TestClassicReanchorMixedPhasesAndDuplicatePrecedence(t *testing.T) {
	kv, snap := mixedClassicFixture(t)
	for k, v := range kv {
		if len(k) != 20 || k[9] != 'p' {
			continue
		}
		var block store.PointBlock
		if err := store.DecodePointBlock(v, &block); err != nil {
			t.Fatal(err)
		}
		// A third phase and two values for the same timestamp. The value
		// already readable at the current anchor (slot 7) must survive.
		for slot, p := range map[int]store.Point{4: {Timestamp: 999850, Value: 4}, 7: {Timestamp: 999870, Value: 77}, 8: {Timestamp: 999870, Value: 88}} {
			block.Present[slot/64] |= 1 << uint(slot%64)
			block.Points[slot] = p
		}
		kv[k] = store.EncodePointBlock(&block)
	}
	if _, err := reanchorClassicArchive(context.Background(), kv, 0); err != nil {
		t.Fatal(err)
	}
	requireClassicPoints(t, &ContinuationCodec{}, kv, snap.Metadata, []store.Point{{Timestamp: 999780, Value: 6}, {Timestamp: 999800, Value: 1}, {Timestamp: 999850, Value: 4}, {Timestamp: 999870, Value: 77}, snap.Archives[0].Points[1]})
	plan, err := (&ContinuationCodec{}).PlanArchiveAnchors(context.Background(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Archives[0].Misplaced != 0 || plan.Archives[0].Blocked != "" {
		t.Fatalf("mixed phases remain: %+v", plan)
	}
}

func TestClassicReanchorRejectsMalformedImageWithoutPublication(t *testing.T) {
	kv, snap := mixedClassicFixture(t)
	for k, v := range kv {
		if len(k) != 20 || k[9] != 'p' {
			continue
		}
		var block store.PointBlock
		if err := store.DecodePointBlock(v, &block); err != nil {
			t.Fatal(err)
		}
		block.Present[0] |= 1 << 5
		block.Points[5] = store.Point{Timestamp: 999781, Value: 9}
		kv[k] = store.EncodePointBlock(&block)
	}
	before := mapContinuationIO{}
	for k, v := range kv {
		before[k] = bytes.Clone(v)
	}
	err := (&ContinuationCodec{}).Update(context.Background(), kv, snap.Metadata, []store.Point{{Timestamp: 999980, Value: 9}}, -1, classicTestClock, nil)
	if !errors.Is(err, store.ErrFormat) {
		t.Fatalf("malformed layout: %v", err)
	}
	if !reflect.DeepEqual(before, kv) {
		t.Fatal("malformed archive partially repaired")
	}
}

type classicFaultCodec struct {
	*ContinuationCodec
	fault      func()
	repairOnly bool
	healthy    bool
}
type classicFaultIO struct {
	store.ContinuationIO
	fault func()
}

func (io classicFaultIO) Set(k, v []byte) error {
	if err := io.ContinuationIO.Set(k, v); err != nil {
		return err
	}
	io.fault()
	return errors.New("injected partial batch failure")
}
func (c *classicFaultCodec) Update(ctx context.Context, kv store.ContinuationIO, m store.Metadata, p []store.Point, r int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	if c.healthy {
		return c.ContinuationCodec.Update(ctx, kv, m, p, r, clock, maintenance)
	}
	if c.repairOnly {
		if _, err := reanchorClassicArchive(ctx, kv, 0); err != nil {
			return err
		}
		c.fault() // Crash between the rotation and the incoming ordinary write.
	}
	return c.ContinuationCodec.Update(ctx, classicFaultIO{kv, c.fault}, m, p, r, clock, maintenance)
}

func TestClassicReanchorFailedPublicationRollsBack(t *testing.T) {
	for _, cacheSize := range []int64{0, 1 << 20} {
		t.Run(fmt.Sprint(cacheSize), func(t *testing.T) {
			codec := &classicFaultCodec{ContinuationCodec: &ContinuationCodec{}, fault: func() {}}
			opts := coldOptions(codec)
			opts.Now, opts.MetadataCacheSize = classicTestClock, cacheSize
			db, err := openTestStore(t.TempDir(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.StopBackground()
			kv, snap := mixedClassicFixture(t)
			if _, err := db.ImportTransfer(context.Background(), []store.TransferMetric{classicTransfer(kv, snap)}); err != nil {
				t.Fatal(err)
			}
			before, err := db.ExportTransfer(context.Background(), []string{snap.Metadata.Name})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.UpdateMany(context.Background(), snap.Metadata.Name, []store.Point{{Timestamp: 999980, Value: 9}}); err == nil {
				t.Fatal("fault ignored")
			}
			after, err := db.ExportTransfer(context.Background(), []string{snap.Metadata.Name})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("partial layout or revision published")
			}
			// A retry must still see the original image, including cached headers.
			codec.healthy = true
			if err := db.UpdateMany(context.Background(), snap.Metadata.Name, []store.Point{{Timestamp: 999980, Value: 9}}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestClassicReanchorCrashAtomicity(t *testing.T) {
	if root := os.Getenv("CLASSIC_REANCHOR_CRASH_ROOT"); root != "" {
		phase := os.Getenv("CLASSIC_REANCHOR_CRASH_PHASE")
		var codec store.ContinuationCodec = &ContinuationCodec{}
		if phase != "acknowledged" {
			codec = &classicFaultCodec{ContinuationCodec: &ContinuationCodec{}, repairOnly: phase == "reanchored", fault: func() { os.Exit(0) }}
		}
		opts := coldOptions(codec)
		opts.Now = classicTestClock
		db, err := store.Open(root, opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateMany(context.Background(), "anchor", []store.Point{{Timestamp: 999980, Value: 9}}); err != nil {
			t.Fatal(err)
		}
		if phase != "acknowledged" {
			t.Fatal("fault was not reached")
		}
		os.Exit(0) // Deliberately bypass Close and its sync.
	}
	for _, phase := range []string{"reanchored", "staging", "acknowledged"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			opts := coldOptions(&ContinuationCodec{})
			opts.Now = classicTestClock
			db, err := store.Open(root, opts)
			if err != nil {
				t.Fatal(err)
			}
			kv, snap := mixedClassicFixture(t)
			if _, err := db.ImportTransfer(context.Background(), []store.TransferMetric{classicTransfer(kv, snap)}); err != nil {
				t.Fatal(err)
			}
			before, err := db.ExportTransfer(context.Background(), []string{snap.Metadata.Name})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestClassicReanchorCrashAtomicity$")
			cmd.Env = append(os.Environ(), "CLASSIC_REANCHOR_CRASH_ROOT="+root, "CLASSIC_REANCHOR_CRASH_PHASE="+phase)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child: %v: %s", err, output)
			}
			db, err = store.Open(root, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			after, err := db.ExportTransfer(context.Background(), []string{snap.Metadata.Name})
			if err != nil {
				t.Fatal(err)
			}
			if phase != "acknowledged" {
				if !reflect.DeepEqual(before, after) {
					t.Fatal("uncommitted repair survived crash")
				}
			} else {
				out := mapContinuationIO{}
				for _, p := range after[0].Keys {
					out[string(p.Key)] = p.Value
				}
				requireClassicPoints(t, &ContinuationCodec{}, out, snap.Metadata, append(snap.Archives[0].Points, store.Point{Timestamp: 999980, Value: 9}))
				plan, err := (&ContinuationCodec{}).PlanArchiveAnchors(context.Background(), out)
				if err != nil {
					t.Fatal(err)
				}
				if plan.Archives[0].Misplaced != 0 {
					t.Fatal("acknowledged repair incomplete")
				}
			}
		})
	}
}

func BenchmarkClassicReanchorArchive(b *testing.B) {
	for _, count := range []int{60, 50400} {
		b.Run(fmt.Sprintf("points=%d", count), func(b *testing.B) {
			points := make([]store.Point, count-1)
			for i := range points {
				points[i] = store.Point{Timestamp: 1793012460 + int64(i*60), Value: float64(i)}
			}
			kv, _ := absoluteClassicImage(b, []store.Retention{{Step: 60, Count: count}}, [][]store.Point{points})
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				overlay := newContinuationOverlay(kv)
				if _, err := reanchorClassicArchive(context.Background(), overlay, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
