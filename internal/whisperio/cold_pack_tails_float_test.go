package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func TestColdPackTailsFloatLifecycle(t *testing.T) {
	for _, policyCase := range []struct {
		mode  string
		tails bool
	}{{"exact", true}, {"float-repack", true}, {"float-repack", false}} {
		mode := policyCase.mode
		for _, exhaustive := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/tails=%t/exhaustive=%t", mode, policyCase.tails, exhaustive), func(t *testing.T) {
				ctx := context.Background()
				now := time.Unix(1793700000, 0)
				cfg := store.MetricConfig{Name: "tails.float", Retentions: []store.Retention{{Step: 60, Count: 43200}}, AggregationMethod: store.Average}
				codec := &tailSeedCodec{seeds: map[string]mapContinuationIO{}}
				codec.seeds[cfg.Name] = tailSeed(t, codec, cfg, now, 48*time.Hour, 5000, false, 3)
				opts := coldOptions(codec)
				opts.TimeBuckets, opts.RelayoutMinFreeBytes = false, 1
				opts.Now = func() time.Time { return now }
				path := t.TempDir()
				db := tailOpen(t, path, opts)
				defer func() { _ = db.Close() }()
				if _, err := db.Create(ctx, cfg); err != nil {
					t.Fatal(err)
				}
				db = tailRelayout(t, db, path, &opts)
				want, err := db.Snapshot(ctx, cfg.Name)
				if err != nil {
					t.Fatal(err)
				}
				before := tailDescriptors(t, db, cfg.Name)
				if mode == "float-repack" {
					tailSweep(t, db, true, "exact")
					if db.TransferFormat() != 9 {
						t.Fatal("legacy tail packing changed fence")
					}
				}
				policy := store.ColdPackOptions{Mode: mode, PackTails: policyCase.tails, FloatCodec: true, FloatExhaustive: exhaustive,
					WriteWindow: 24 * time.Hour, CPUFraction: 1, BytesPerSecond: 1 << 40, MinFreeBytes: 1}
				if err := db.ConfigureColdPack(policy); err != nil {
					t.Fatal(err)
				}
				finishFloatSweep(t, db)
				keys := tailKeys(t, db, cfg.Name)
				tails := 0
				for key, desc := range tailDescriptors(t, db, cfg.Name) {
					if before[key].bucketed || before[key].count == 0 {
						continue
					}
					tails++
					if !desc.bucketed || desc.packed != store.ColdPackPoints || desc.count != before[key].count {
						t.Fatalf("tail not sealed: %+v", desc)
					}
					groupKey := desc.groupKey(binary.BigEndian.Uint64([]byte(key)[1:9]), 0, int(binary.BigEndian.Uint32([]byte(key)[12:])), 0)
					value := keys[string(groupKey)]
					if len(value) == 0 || value[0] != 4 {
						t.Fatalf("tail did not use float codec: %x", groupKey)
					}
					if exhaustive {
						points, err := store.DecodeSealedPoints(value)
						if err != nil {
							t.Fatal(err)
						}
						encoded, err := store.EncodeSealedPointsFloatMin(points, 60)
						if err != nil || !bytes.Equal(value, encoded) {
							t.Fatal("tail lost exhaustive selector", err)
						}
					}
				}
				if tails != 1 || db.TransferFormat() != 11 {
					t.Fatalf("tails=%d format=%d", tails, db.TransferFormat())
				}
				got, err := db.Snapshot(ctx, cfg.Name)
				if err != nil {
					t.Fatal(err)
				}
				coldEqual(t, want, got, false)
				// Rollback must cover previously packed tails even with the opt-in
				// now off, and must leave the v11 fence until finalization.
				floatSweep(t, db, "float-downgrade", false)
				if _, new := floatGroups(t, db, cfg.Name); new != 0 || db.TransferFormat() != 11 {
					t.Fatalf("downgrade groups=%d format=%d", new, db.TransferFormat())
				}
				if err := db.DowngradeFloatCodec(ctx); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db = tailOpen(t, path, opts)
				if db.TransferFormat() != 9 {
					t.Fatal("float downgrade fence not durable")
				}
				got, err = db.Snapshot(ctx, cfg.Name)
				if err != nil {
					t.Fatal(err)
				}
				coldEqual(t, want, got, false)
			})
		}
	}
}

func TestFloatRepackProtectsBufferedHead(t *testing.T) {
	for _, tails := range []bool{false, true} {
		t.Run(fmt.Sprintf("tails=%t", tails), func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1793700000, 0)
			cfg := store.MetricConfig{Name: "float.buffer", Retentions: []store.Retention{{Step: 60, Count: 43200}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Average, XFilesFactor: .5}
			codec := &tailSeedCodec{seeds: map[string]mapContinuationIO{}}
			seed := tailSeed(t, codec, cfg, now, 48*time.Hour, 5000, true, 3)
			fs := newContinuationFiles(seed, true)
			src, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, func() time.Time { return now }))
			if err != nil {
				t.Fatal(err)
			}
			physical, err := src.ContinuationLayout()
			if err != nil {
				t.Fatal(err)
			}
			bounds := append(src.StoredArchiveBounds(), src.BufferedRollupWindows()...)
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			var head whisper.ContinuationBlock
			for _, b := range physical.Blocks {
				if b.Archive == 1 && b.Last > head.Last {
					head = b
				}
			}
			until := 0
			for _, b := range bounds {
				if b.Archive == 1 {
					until = max(until, b.Until)
				}
			}
			if head.Count == 0 || int64(head.Last) >= now.Add(-24*time.Hour).Unix() || int64(until) < now.Add(-24*time.Hour).Unix() {
				t.Fatalf("fixture must have a cold native head behind a hot rollup buffer: %+v until=%d", head, until)
			}
			// Model a legacy packed tail that became active only in a finer
			// rollup buffer. Native timestamps alone still look cold.
			identity, err := seed.Get(continuationNameKey("main.wsp"))
			if err != nil {
				t.Fatal(err)
			}
			id := binary.BigEndian.Uint64(identity)
			manifest, err := seed.Get(continuationManifestKey(id))
			if err != nil {
				t.Fatal(err)
			}
			d, err := decodeContinuationManifest(manifest)
			if err != nil {
				t.Fatal(err)
			}
			d.id = id
			f := &continuationFile{fs: newContinuationFiles(seed, false), data: d}
			points, err := f.ReadStorageBlock(head)
			if err != nil {
				t.Fatal(err)
			}
			input := make([]store.Point, len(points))
			for i, p := range points {
				input[i] = store.Point{Timestamp: int64(p.Time), Value: p.Value}
			}
			legacy, err := store.EncodeSealedPoints(input, head.Step)
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := store.EncodeSealedPointsFloat(input, head.Step)
			if err != nil || candidate[0] != 4 || len(candidate) >= len(legacy) {
				t.Fatalf("fixture must be a profitable float rewrite: points=%d legacy=%d candidate=%d format=%d err=%v", len(input), len(legacy), len(candidate), candidate[0], err)
			}
			desc := blockDescriptor{count: len(input)}
			if err := seed.DeletePrefix(desc.groupPrefix(id, 1, head.Index)); err != nil {
				t.Fatal(err)
			}
			desc.bucketed, desc.bucket, desc.packed = true, continuationHistoryBucket, store.ColdPackPoints
			groupKey := desc.groupKey(id, 1, head.Index, 0)
			seed[string(groupKey)] = legacy
			seed[string(continuationBlockPrefix(id, 'b', 1, head.Index))] = desc.encode()
			codec.seeds[cfg.Name] = seed
			opts := coldOptions(codec)
			opts.TimeBuckets, opts.RelayoutMinFreeBytes = false, 1
			opts.Now = func() time.Time { return now }
			path := t.TempDir()
			db := tailOpen(t, path, opts)
			defer func() { _ = db.Close() }()
			if _, err := db.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			db = tailRelayout(t, db, path, &opts)
			want, err := db.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "float-repack", PackTails: tails, FloatCodec: true,
				WriteWindow: 24 * time.Hour, CPUFraction: 1, BytesPerSecond: 1 << 40, MinFreeBytes: 1}); err != nil {
				t.Fatal(err)
			}
			finishFloatSweep(t, db)
			if got := tailKeys(t, db, cfg.Name)[string(groupKey)]; !bytes.Equal(legacy, got) {
				t.Fatal("float-repack rewrote the active buffered head")
			}
			got, err := db.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			coldEqual(t, want, got, false)
		})
	}
}
