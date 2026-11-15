package whisperio

import (
	"bytes"
	"context"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// These are captured encoded images, with only the metric name anonymized.
// The target's hourly slot holds a zero in 2097, exactly 2871 rings ahead
// of a retained hour present on both independent sources. The daily future
// point is unrelated and must survive. Receipt success alone missed this bug.
func TestFutureSlotCapturedNativeImport(t *testing.T) {
	ctx := context.Background()
	const timestamp int64 = 1794042000
	clock := func() time.Time { return time.Unix(1794759182, 0) }
	load := func(file string) []store.TransferMetric {
		t.Helper()
		data, e := os.ReadFile("testdata/future-slot/" + file + ".bin")
		if e != nil {
			t.Fatal(e)
		}
		r, e := store.ReadTransfer(bytes.NewReader(data), 1<<24)
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	for _, donor := range []string{"source-a", "source-b"} {
		for _, mode := range []string{"transfer", "native", "window"} {
			t.Run(donor+"/"+mode, func(t *testing.T) {
				source, target := combineMemoryStore(t, clock, false), combineMemoryStore(t, clock, false)
				a, b := load(donor), load("target")
				name := a[0].Config.Name
				if _, e := source.ImportTransfer(ctx, a); e != nil {
					t.Fatal(e)
				}
				if _, e := target.ImportTransfer(ctx, b); e != nil {
					t.Fatal(e)
				}
				before, e := target.Fetch(ctx, name, int(timestamp-3600), int(timestamp+7200))
				if e != nil {
					t.Fatal(e)
				}
				if !math.IsNaN(before.Values[0]) {
					t.Fatal("fixture no longer reproduces missing hour", before)
				}
				if mode == "transfer" {
					if _, e := target.ImportTransfer(ctx, a); e != nil {
						t.Fatal(e)
					}
				} else {
					pin, e := source.PinNativeSource()
					if e != nil {
						t.Fatal(e)
					}
					defer pin.Close()
					var wire bytes.Buffer
					req := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "source", End: 1, Names: []string{name}, Combine: true}
					if mode == "window" {
						req.Window = &store.NativeWindow{From: timestamp - 3600, Until: timestamp + 7200}
						e = pin.WriteWindow(ctx, req.Names, &wire, req.Window)
					} else {
						e = pin.Write(ctx, req.Names, &wire)
					}
					if e != nil {
						t.Fatal(e)
					}
					if result, err := target.NativeImport(ctx, req, &wire); err != nil {
						t.Fatal(err)
					} else if result.CopiedCount != 1 || result.DeferredCount != 0 {
						t.Fatalf("import not completed: %+v", result)
					}
				}
				got, err := target.Fetch(ctx, name, int(timestamp-3600), int(timestamp+7200))
				if err != nil {
					t.Fatal(err)
				}
				if math.Float64bits(got.Values[0]) != math.Float64bits(54.833333333333336) {
					t.Fatalf("successful import left hour %v", got.Values)
				}
				snap, err := target.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				future := false
				for _, p := range snap.Archives[2].Points {
					if p.Timestamp == 4026499200 && math.Float64bits(p.Value) == 0 {
						future = true
					}
				}
				if !future {
					t.Fatal("unrelated future daily history lost")
				}
				expected := map[[2]int64]uint64{}
				for _, records := range [][]store.TransferMetric{a, b} {
					snapshot, err := store.TransferSnapshot(ctx, records[0], &ContinuationCodec{}, clock)
					if err != nil {
						t.Fatal(err)
					}
					for ai, ar := range snapshot.Archives {
						for _, p := range ar.Points {
							if mode == "window" && !(&store.NativeWindow{From: timestamp - 3600, Until: timestamp + 7200}).Contains(p.Timestamp) {
								continue
							}
							if p.Timestamp <= clock().Unix() && p.Timestamp > clock().Unix()-int64(ar.Retention.Step)*int64(ar.Retention.Count) {
								expected[[2]int64{int64(ai), p.Timestamp}] = math.Float64bits(p.Value)
							}
						}
					}
				}
				for ai, ar := range snap.Archives {
					for _, p := range ar.Points {
						key := [2]int64{int64(ai), p.Timestamp}
						if bits, ok := expected[key]; ok {
							if bits != math.Float64bits(p.Value) {
								t.Fatalf("changed valid point %v", key)
							}
							delete(expected, key)
						}
					}
				}
				// A subsequent direct archive write of the impossible future
				// epoch must not undo the repair (the ordinary classic write path).
				if err := target.UpdateManyForArchive(ctx, name, []store.Point{{Timestamp: 4026531600, Value: 0}}, 3600*216); err != nil {
					t.Fatal(err)
				}
				after, err := target.Fetch(ctx, name, int(timestamp-3600), int(timestamp+7200))
				if err != nil || math.Float64bits(after.Values[0]) != math.Float64bits(54.833333333333336) {
					t.Fatalf("future write undid repair: %+v %v", after, err)
				}
				if len(expected) > 0 {
					t.Fatalf("lost %d retained points", len(expected))
				}
			})
		}
	}
}

func TestFutureSlotReanchor(t *testing.T) {
	kv, snap := mixedClassicFixture(t)
	r := snap.Archives[0].Retention
	valid := snap.Archives[0].Points[0]
	future := valid.Timestamp + int64(r.Step)*int64(r.Count)*1000
	for k, v := range kv {
		if len(k) != 20 || k[9] != 'p' || k[11] != 0 {
			continue
		}
		var block store.PointBlock
		if err := store.DecodePointBlock(v, &block); err != nil {
			t.Fatal(err)
		}
		block.Present[0] |= 1 << 55
		block.Points[55] = store.Point{Timestamp: future, Value: 0}
		kv[k] = store.EncodePointBlock(&block)
		break
	}
	if _, err := reanchorClassicArchiveAt(context.Background(), kv, 0, classicTestClock().Unix()); err != nil {
		t.Fatal(err)
	}
	got, err := (&ContinuationCodec{}).Snapshot(context.Background(), kv, snap.Metadata, classicTestClock)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range got.Archives[0].Points {
		if p.Timestamp == future {
			t.Fatal("future epoch displaced retained point")
		}
		if p.Timestamp == valid.Timestamp && math.Float64bits(p.Value) == math.Float64bits(valid.Value) {
			found = true
		}
	}
	if !found {
		t.Fatal("retained point lost")
	}
}
