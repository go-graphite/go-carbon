package whisperio

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A private captured transfer can be used locally without publishing source
// metric names or payloads. Both readers use the exact same immutable KV image.
func TestSparseSumCapturedTransfer(t *testing.T) {
	path := os.Getenv("DIGEST_TRANSFER_FIXTURE")
	if path == "" {
		t.Skip("set DIGEST_TRANSFER_FIXTURE to an immutable transfer capture")
	}
	wire, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.ReadTransfer(bytes.NewReader(wire), int64(len(wire)))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct{ Now int }
	data, err := os.ReadFile(path + ".json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return time.Unix(int64(manifest.Now), 0) }
	ctx := context.Background()
	tested := 0
	for ri, record := range records {
		if !record.Continuation {
			continue
		}
		kv := mapContinuationIO{}
		for _, e := range record.Keys {
			kv[string(e.Key)] = e.Value
		}
		m := store.Metadata{MetricConfig: record.Config, ID: record.Version.ID, Generation: record.Version.Generation, Revision: record.Version.Revision, Continuation: true}
		for _, bounds := range [][2]int{{0, 0}, {manifest.Now - 86400, manifest.Now - 60}, {manifest.Now - 400*86400, manifest.Now - 3600}} {
			want, _ := sumOracle(t, kv, m, manifest.Now, bounds[0], bounds[1])
			got, err := (&ContinuationCodec{}).SumDigest(ctx, kv, m, bounds[0], bounds[1], clock)
			if err != nil {
				t.Fatalf("record %d: %v", ri, err)
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("record %d bounds %v: got %+v want %+v", ri, bounds, got, want)
			}
			tested++
		}
	}
	if tested == 0 {
		t.Fatal("no continuation records")
	}
	t.Logf("%d canonical digest comparisons across %d captured records", tested, len(records))
}

func BenchmarkSparseColdSum(b *testing.B) {
	db, _, now := rangeFetchFixtureSchema(b, sumFixtureValues("integer-long-retentions"), true)
	kv, m := sumBenchExport(b, db)
	clock := func() time.Time { return time.Unix(int64(now), 0) }
	// Use the read-only map so each iteration remains a cold-cache digest.
	for _, dense := range []bool{true, false} {
		name := "sparse"
		if dense {
			name = "dense"
		}
		b.Run(name, func(b *testing.B) {
			codec := &ContinuationCodec{keepDenseSumReads: dense}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := codec.SumDigest(context.Background(), kv.mapContinuationIO, m, 0, 0, clock); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCapturedColdSum(b *testing.B) {
	path := os.Getenv("DIGEST_TRANSFER_FIXTURE")
	if path == "" {
		b.Skip("private transfer fixture required")
	}
	wire, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	records, err := store.ReadTransfer(bytes.NewReader(wire), int64(len(wire)))
	if err != nil {
		b.Fatal(err)
	}
	var manifest struct{ Now int }
	data, _ := os.ReadFile(path + ".json")
	if err = json.Unmarshal(data, &manifest); err != nil {
		b.Fatal(err)
	}
	clock := func() time.Time { return time.Unix(int64(manifest.Now), 0) }
	type input struct {
		kv mapContinuationIO
		m  store.Metadata
	}
	inputs := []input{}
	for _, record := range records {
		if !record.Continuation {
			continue
		}
		kv := mapContinuationIO{}
		for _, e := range record.Keys {
			kv[string(e.Key)] = e.Value
		}
		inputs = append(inputs, input{kv, store.Metadata{MetricConfig: record.Config, Revision: record.Version.Revision, Continuation: true}})
	}
	for _, dense := range []bool{true, false} {
		name := "sparse"
		if dense {
			name = "dense"
		}
		b.Run(name, func(b *testing.B) {
			codec := &ContinuationCodec{keepDenseSumReads: dense}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				in := inputs[i%len(inputs)]
				if _, err := codec.SumDigest(context.Background(), in.kv, in.m, 0, 0, clock); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}

func capturedDigestStore(tb testing.TB) (*store.Store, []string, int) {
	tb.Helper()
	path := os.Getenv("DIGEST_TRANSFER_FIXTURE")
	if path == "" {
		tb.Skip("private transfer fixture required")
	}
	wire, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	records, err := store.ReadTransfer(bytes.NewReader(wire), int64(len(wire)))
	if err != nil {
		tb.Fatal(err)
	}
	var manifest struct{ Now int }
	data, _ := os.ReadFile(path + ".json")
	if err = json.Unmarshal(data, &manifest); err != nil {
		tb.Fatal(err)
	}
	db, err := openTestStore(tb.TempDir(), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &ContinuationCodec{}, Now: func() time.Time { return time.Unix(int64(manifest.Now), 0) }, CacheSize: 16 << 20, MemTableSize: 4 << 20, FetchSharedIterator: true})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", FloatCodec: true}); err != nil {
		tb.Fatal(err)
	}
	db.StopBackground()
	if _, err = db.ImportTransfer(context.Background(), records); err != nil {
		tb.Fatal(err)
	}
	names := make([]string, len(records))
	for i, record := range records {
		names[i] = record.Config.Name
	}
	return db, names, manifest.Now
}

func TestCapturedDigestBatchDifferential(t *testing.T) {
	db, names, now := capturedDigestStore(t)
	ctx := context.Background()
	type result struct {
		hash [32]byte
		sums []store.SumArchive
	}
	want := make([]result, len(names))
	from, until := now-180*86400, now-600
	for i, name := range names {
		series, err := db.Fetch(ctx, name, from, until)
		if err != nil {
			t.Fatal(err)
		}
		want[i].hash = shaSeries(series, from, until)
		want[i].sums, err = db.SumDigest(ctx, name, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := db.DigestBatch(ctx, names, func(ctx context.Context, i int) error {
		series, err := db.Fetch(ctx, names[i], from, until)
		if err != nil {
			return err
		}
		sums, err := db.SumDigest(ctx, names[i], 0, 0)
		if err != nil {
			return err
		}
		if shaSeries(series, from, until) != want[i].hash || !reflect.DeepEqual(sums, want[i].sums) {
			t.Fatalf("captured record %d changed digest/verify verdict", i)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d captured SHA and sum batch verdicts identical", len(names))
}

func BenchmarkCapturedDigestBatch(b *testing.B) {
	db, names, now := capturedDigestStore(b)
	from, until := now-180*86400, now-600
	ctx := context.Background()
	for _, batch := range []bool{false, true} {
		name := "individual"
		if batch {
			name = "batch"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				visit := func(ctx context.Context, i int) error {
					series, err := db.Fetch(ctx, names[i], from, until)
					if err == nil {
						shaSeries(series, from, until)
					}
					return err
				}
				if batch {
					if err := db.DigestBatch(ctx, names, visit); err != nil {
						b.Fatal(err)
					}
				} else {
					for i := range names {
						if err := visit(ctx, i); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(names)), "ns/name")
		})
	}
}
