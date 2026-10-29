package whisperio

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Embedding the interface keeps the reference publication on Verify + Snapshot,
// without promoting the optional combined verifier implemented by the codec.
type referenceConversionCodec struct{ store.ContinuationCodec }

func (c referenceConversionCodec) Drop(ctx context.Context, kv store.ContinuationIO) error {
	return c.ContinuationCodec.(store.ContinuationDropper).Drop(ctx, kv)
}

type conversionReadback struct {
	*ContinuationCodec
	values map[string][]byte
}

func (c *conversionReadback) read(kv store.ContinuationIO) error {
	c.values = make(map[string][]byte)
	return visitConversionData(kv, func(k, v []byte) error { c.values[string(k)] = bytes.Clone(v); return nil })
}

// Visit(nil) covers only metric-local keys in v7. Follow the block
// descriptors into every bucket to include the actual stored point values.
func visitConversionData(kv store.ContinuationIO, visit func([]byte, []byte) error) error {
	buckets := make(map[uint32]bool)
	if err := kv.Visit(nil, func(k, v []byte) error {
		if len(k) == 16 && k[9] == 'b' {
			desc, err := decodeBlockDescriptor(v)
			if err != nil {
				return err
			}
			if desc.bucketed {
				buckets[desc.bucket] = true
			}
		}
		return visit(k, v)
	}); err != nil {
		return err
	}
	for bucket := range buckets {
		if err := kv.Visit(store.ContinuationBucketKey(bucket, nil), visit); err != nil {
			return err
		}
	}
	return nil
}

func (c *conversionReadback) Verify(ctx context.Context, kv store.ContinuationIO, path string, m store.Metadata) error {
	if err := c.read(kv); err != nil {
		return err
	}
	return c.ContinuationCodec.Verify(ctx, kv, path, m)
}
func (c *conversionReadback) VerifySnapshot(ctx context.Context, kv store.ContinuationIO, path string, m store.Metadata, clock func() time.Time) (store.Snapshot, error) {
	if err := c.read(kv); err != nil {
		return store.Snapshot{}, err
	}
	return c.ContinuationCodec.VerifySnapshot(ctx, kv, path, m, clock)
}

func conversionSourceFixture(t testing.TB, path string, compressed, sparse, sidecar bool) {
	t.Helper()
	const now = 1793200000
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:2h,60s:7d,3600s:1y"), whisper.Average, .5,
		&whisper.Options{Compressed: compressed, OutOfOrder: compressed, PointsPerBlock: 128, Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	count, stride := 3000, 1
	if sparse {
		count, stride = 17, 71
	}
	points := make([]whisper.TimeSeriesPoint, count)
	input := make([]*whisper.TimeSeriesPoint, count)
	for i := range points {
		points[i] = whisper.TimeSeriesPoint{Time: now - count*stride + i*stride, Value: float64(i%23) + .1}
		input[i] = &points[i]
	}
	if err := w.UpdateMany(input); err != nil {
		t.Fatal(err)
	}
	if sidecar {
		if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: points[2].Time, Value: 991.25}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// Compares the actual continuation keys/values read back from Pebble before
// publication, in addition to the visible values and physical verifier. Both
// fresh ingestion and a restage after an unowned candidate exercise real DBs.
func TestConversionSourceByteIdentical(t *testing.T) {
	for _, shape := range []struct {
		name                        string
		compressed, sparse, sidecar bool
	}{
		{"dense-rollups", true, false, false}, {"sparse", true, true, false},
		{"classic", false, false, false}, {"ooo", true, false, true},
	} {
		for _, buckets := range []bool{false, true} {
			for _, ingest := range []bool{false, true} {
				for _, fold := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/v7=%v/ingest=%v/fold=%v", shape.name, buckets, ingest, fold), func(t *testing.T) {
						root := t.TempDir()
						source := filepath.Join(root, "source")
						if err := os.Mkdir(source, 0700); err != nil {
							t.Fatal(err)
						}
						path := filepath.Join(source, "metric.wsp")
						conversionSourceFixture(t, path, shape.compressed, shape.sparse, shape.sidecar)
						var want map[string][]byte
						var wantSnapshot store.Snapshot
						for _, cached := range []bool{false, true} {
							readback := &conversionReadback{ContinuationCodec: &ContinuationCodec{DirectLateWrites: fold}}
							var codec store.ContinuationCodec = referenceConversionCodec{readback}
							ctx := context.Background()
							if cached {
								codec = readback
								ctx = WithConversionSource(ctx)
							}
							db, err := store.Open(filepath.Join(root, fmt.Sprint(cached)), store.Options{ExactBlocks: true, Continuation: codec, IncrementalSource: source, TimeBuckets: buckets,
								Now: func() time.Time { return time.Unix(1793200000, 0) }, IngestFileStages: ingest, IngestLinger: time.Nanosecond, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20})
							if err != nil {
								t.Fatal(err)
							}
							snapshot, err := ReadSnapshotIn(ctx, "metric", path, root)
							if err != nil {
								t.Fatal(err)
							}
							snapshot.Metadata.LastUpdate = time.Unix(1793200000, 0)
							// The second stage simulates resume: decoded state is not durable.
							for stage := 0; stage < 2; stage++ {
								if stage == 1 && cached {
									ctx = WithConversionSource(context.Background())
									snapshot, err = ReadSnapshotIn(ctx, "metric", path, root)
									if err != nil {
										t.Fatal(err)
									}
									snapshot.Metadata.LastUpdate = time.Unix(1793200000, 0)
								}
								m, err := db.StageFileSnapshot(ctx, snapshot)
								if err != nil {
									t.Fatal(err)
								}
								if stage == 1 {
									if _, err := db.PublishFileOwnership(ctx, snapshot, m); err != nil {
										t.Fatal(err)
									}
								}
							}
							got, err := db.Snapshot(context.Background(), "metric")
							if err != nil {
								t.Fatal(err)
							}
							if !cached {
								want, wantSnapshot = readback.values, got
							} else {
								if !reflect.DeepEqual(want, readback.values) {
									for key, value := range want {
										if !bytes.Equal(value, readback.values[key]) {
											t.Fatalf("stored continuation key %x differs from uncached reference", []byte(key))
										}
									}
									t.Fatal("stored continuation key sets differ from uncached reference")
								}
								if !reflect.DeepEqual(wantSnapshot.Metadata, got.Metadata) {
									t.Fatal("catalog metadata differs from uncached reference")
								}
								if err := store.EqualSnapshots(wantSnapshot, got); err != nil {
									t.Fatal(err)
								}
							}
							if err := db.Close(); err != nil {
								t.Fatal(err)
							}
						}
					})
				}
			}
		}
	}
}

func TestConversionSourceChecksCurrentBlockBytes(t *testing.T) {
	ctx := WithConversionSource(context.Background())
	root := t.TempDir()
	path := filepath.Join(root, "metric.wsp")
	conversionSourceFixture(t, path, true, false, false)
	snapshot, err := ReadSnapshot(ctx, "metric", path)
	if err != nil {
		t.Fatal(err)
	}
	codec := &ContinuationCodec{}
	kv := mapContinuationIO{}
	if err := codec.Import(ctx, kv, path, snapshot.Metadata, nil); err != nil {
		t.Fatal(err)
	}
	source, err := sourceContinuationFile(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	layout, err := source.ContinuationLayout()
	if err != nil {
		t.Fatal(err)
	}
	block := layout.Blocks[0]
	points, err := source.ContinuationBlockPoints(block)
	if err != nil {
		t.Fatal(err)
	}
	source.Close()
	points[len(points)/2].Value = math.Float64frombits(math.Float64bits(points[len(points)/2].Value) ^ 1)
	data, err := whisper.EncodeContinuationBlock(block, points)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(data, int64(block.Offset)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	// The complete header/descriptor, file length, and even mtime are unchanged.
	// A stale source cache would compare the import against itself and pass.
	if _, err := codec.VerifySnapshot(ctx, kv, path, snapshot.Metadata, nil); err == nil {
		t.Fatal("verification accepted changed source bytes")
	}
	fresh := mapContinuationIO{}
	if err := codec.Import(ctx, fresh, path, snapshot.Metadata, nil); err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(kv, fresh) {
		t.Fatal("changed block was not reread")
	}
	if _, err := codec.VerifySnapshot(ctx, fresh, path, snapshot.Metadata, nil); err != nil {
		t.Fatal(err)
	}
}

func TestConversionSourceStillDecodesStoredValues(t *testing.T) {
	ctx := WithConversionSource(context.Background())
	path := filepath.Join(t.TempDir(), "metric.wsp")
	conversionSourceFixture(t, path, true, false, false)
	snapshot, err := ReadSnapshot(ctx, "metric", path)
	if err != nil {
		t.Fatal(err)
	}
	codec := &ContinuationCodec{}
	kv := mapContinuationIO{}
	if err := codec.Import(ctx, kv, path, snapshot.Metadata, nil); err != nil {
		t.Fatal(err)
	}
	// Caller expectations never alias the private, physically certified view.
	snapshot.Archives[0].Points[0].Value++
	got, err := codec.VerifySnapshot(ctx, kv, path, snapshot.Metadata, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(snapshot, got); err == nil {
		t.Fatal("caller changed the certified view")
	}
	changed := false
	for key, value := range kv {
		if len(key) != 20 || key[9] != 'c' {
			continue
		}
		var block store.PointBlock
		if err := store.DecodePointBlock(value, &block); err != nil {
			t.Fatal(err)
		}
		for slot := range block.Points {
			if block.Present[slot/64]&(1<<uint(slot%64)) == 0 {
				continue
			}
			block.Points[slot].Value++
			kv[key] = store.EncodePointBlock(&block)
			changed = true
			break
		}
		if changed {
			break
		}
	}
	if !changed {
		t.Fatal("no stored point changed")
	}
	if _, err := codec.VerifySnapshot(ctx, kv, path, snapshot.Metadata, nil); err == nil {
		t.Fatal("verification reused import output instead of decoding stored values")
	}
}

type corruptConversionImport struct{ *ContinuationCodec }

func (c *corruptConversionImport) Import(ctx context.Context, kv store.ContinuationIO, path string, m store.Metadata, clock func() time.Time) error {
	if err := c.ContinuationCodec.Import(ctx, kv, path, m, clock); err != nil {
		return err
	}
	var key, value []byte
	if err := visitConversionData(kv, func(k, v []byte) error {
		body := k
		if len(k) > 5 && k[0] == 'T' {
			body = k[5:]
		}
		if key == nil && len(body) == 20 && body[9] == 'c' {
			key, value = bytes.Clone(k), bytes.Clone(v)
		}
		return nil
	}); err != nil {
		return err
	}
	if key == nil {
		return fmt.Errorf("fixture has no compressed group")
	}
	var block store.PointBlock
	if err := store.DecodePointBlock(value, &block); err != nil {
		return err
	}
	block.Points[0].Value++
	return kv.Set(key, store.EncodePointBlock(&block))
}

func TestConversionSourceRejectsCorruptPebbleReadback(t *testing.T) {
	for _, ingest := range []bool{false, true} {
		t.Run(fmt.Sprint(ingest), func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			if err := os.Mkdir(source, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(source, "metric.wsp")
			conversionSourceFixture(t, path, true, false, false)
			ctx := WithConversionSource(context.Background())
			snapshot, err := ReadSnapshot(ctx, "metric", path)
			if err != nil {
				t.Fatal(err)
			}
			db, err := store.Open(filepath.Join(root, "shared"), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &corruptConversionImport{&ContinuationCodec{}}, IncrementalSource: source,
				Now: func() time.Time { return time.Unix(1793200000, 0) }, IngestFileStages: ingest, IngestLinger: time.Nanosecond, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			m, err := db.StageFileSnapshot(ctx, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.PublishFileOwnership(ctx, snapshot, m); err == nil {
				t.Fatal("published corrupted Pebble values")
			}
			if _, err := db.Metadata(ctx, "metric"); err == nil {
				t.Fatal("failed verification published ownership")
			}
		})
	}
}
