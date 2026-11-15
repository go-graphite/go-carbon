package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type referenceReadIO struct {
	store.ContinuationIO
	fail  map[string]error
	reads []string
}

func (kv *referenceReadIO) Get(k []byte) ([]byte, error) {
	kv.reads = append(kv.reads, string(k))
	if err := kv.fail[string(k)]; err != nil {
		return nil, err
	}
	return kv.ContinuationIO.Get(k)
}

func (kv *referenceReadIO) Probe(k []byte) ([]byte, error) { return kv.Get(k) }

func TestReferenceReadProbePrecedenceAndErrors(t *testing.T) {
	p := continuationBodyKey(1, 'p', 0, 0, 0)
	a := continuationBodyKey(1, 'a', 0, 0, 0)
	r := continuationBodyKey(1, 'r', 0, 0, 1)
	var block store.PointBlock
	block.Present[0] = 1
	block.Points[0] = store.Point{Timestamp: 60, Value: math.Float64frombits(0x7ff8000000000042)}
	bad := errors.New("read failed")
	for _, tc := range []struct {
		name   string
		values mapContinuationIO
		fail   map[string]error
	}{
		{"missing", mapContinuationIO{}, nil},
		{"reference", mapContinuationIO{string(a): encodePointReferences([]pointReference{{ID: 1, Count: 1}}), string(r): store.EncodePointBlock(&block)}, nil},
		{"override", mapContinuationIO{string(p): store.EncodePointBlock(&block), string(a): {0}}, nil},
		{"empty-shadow", mapContinuationIO{string(p): store.EncodePointBlock(&store.PointBlock{}), string(a): {0}}, map[string]error{string(a): bad}},
		{"bad-override", mapContinuationIO{string(p): {0}, string(a): encodePointReferences(nil)}, nil},
		{"bad-alias", mapContinuationIO{string(a): {0}}, nil},
		{"override-error", mapContinuationIO{}, map[string]error{string(p): bad}},
		{"alias-error", mapContinuationIO{}, map[string]error{string(a): bad}},
		{"payload-error", mapContinuationIO{string(a): encodePointReferences([]pointReference{{ID: 1, Count: 1}})}, map[string]error{string(r): bad}},
		{"bad-payload", mapContinuationIO{string(a): encodePointReferences([]pointReference{{ID: 1, Count: 1}}), string(r): {0}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var want []byte
			var wantErr error
			var wantReads []string
			for _, readOnly := range []bool{false, true} {
				kv := &referenceReadIO{ContinuationIO: tc.values, fail: tc.fail}
				fs := newContinuationFiles(kv, readOnly)
				f := &continuationFile{fs: fs, data: &continuationFileData{id: 1, references: true}}
				got, err := f.readReferencedClassicGroup(0, 0, p)
				var encoded []byte
				if got != nil {
					encoded = store.EncodePointBlock(got)
				}
				if !readOnly {
					want, wantErr, wantReads = encoded, err, kv.reads
					continue
				}
				if !bytes.Equal(want, encoded) || !errors.Is(err, wantErr) || !reflect.DeepEqual(wantReads, kv.reads) {
					t.Fatalf("changed result/error/read order: %v/%v %q/%q", wantErr, err, wantReads, kv.reads)
				}
			}
		})
	}
}

func referenceFetchStore(tb testing.TB) (*store.Store, []string, int) {
	tb.Helper()
	ctx := context.Background()
	const base int64 = 1793808000
	clock := func() time.Time { return time.Unix(base+2048*60, 0) }
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "reference.fetch", Retentions: []store.Retention{{Step: 60, Count: 4096}}, AggregationMethod: store.Last}}
	pts := make([]store.Point, 2048)
	bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7ff0000000000001}
	for i := range pts {
		pts[i] = store.Point{Timestamp: base + int64(i)*60, Value: math.Float64frombits(bits[i%len(bits)])}
	}
	data, err := store.EncodeSealedPoints(pts, 60)
	if err != nil {
		tb.Fatal(err)
	}
	payload := &splicePayload{data: data, sealed: true}
	part := splicePart{fragment: spliceFragment{first: base, count: len(pts), payload: payload}, slot: int(base / 60 % 4096), count: len(pts)}
	keys, err := (&ContinuationCodec{}).writeReferencedArchives(ctx, m, [][]splicePart{{part}}, []int64{base}, &store.NativeCombineClass{}, clock)
	if err != nil {
		tb.Fatal(err)
	}
	kv := mapContinuationIO{}
	for _, item := range keys {
		kv[string(item.Key)] = item.Value
	}
	db, err := openTestStore(tb.TempDir(), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, Now: clock, CacheSize: 16 << 20, MemTableSize: 1 << 20, L0CompactionThreshold: 100, L0StopWritesThreshold: 200})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	db.StopBackground()
	names := make([]string, 16)
	for i := range names {
		m.Name = fmt.Sprintf("reference.fetch.%02d", i)
		names[i] = m.Name
		if _, err := db.ImportTransfer(ctx, []store.TransferMetric{classicTransfer(kv, store.Snapshot{Metadata: m})}); err != nil {
			tb.Fatal(err)
		}
		if i%2 == 1 {
			if err := db.Flush(); err != nil {
				tb.Fatal(err)
			}
		}
	}
	return db, names, int(clock().Unix())
}

func TestReferenceFetchProbesBitExact(t *testing.T) {
	db, names, now := referenceFetchStore(t)
	rng := rand.New(rand.NewSource(871))
	for i := 0; i < 100; i++ {
		name := names[i%len(names)]
		from := now - 4096*60 + rng.Intn(2048)*60
		until := from + 1 + rng.Intn(now-from)
		var want *store.Series
		for _, shared := range []bool{false, true} {
			restore := db.SetFetchSharedIteratorForTest(shared)
			got, err := db.Fetch(context.Background(), name, from, until)
			restore()
			if err != nil {
				t.Fatal(err)
			}
			if !shared {
				want = got
				continue
			}
			if got.FromTime != want.FromTime || got.UntilTime != want.UntilTime || got.Step != want.Step || len(got.Values) != len(want.Values) {
				t.Fatal("series shape changed")
			}
			for j, v := range want.Values {
				if math.Float64bits(v) != math.Float64bits(got.Values[j]) {
					t.Fatalf("query %d point %d changed", i, j)
				}
			}
		}
	}
}

func BenchmarkReferenceFetchProbes(b *testing.B) {
	db, names, now := referenceFetchStore(b)
	for _, shared := range []bool{false, true} {
		b.Run(fmt.Sprint(shared), func(b *testing.B) {
			restore := db.SetFetchSharedIteratorForTest(shared)
			defer restore()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := db.Fetch(context.Background(), names[0], now-4096*60, now); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
