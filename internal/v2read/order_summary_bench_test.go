package v2read

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Isolate the dense decoder and the real renderer from SST IO/cache effects.
// Scan decodes each time, like a cold run in sealedSource; all codecs feed the
// same one-archive renderer and must produce the same IEEE value bits.
type summaryBenchSource struct {
	data    []byte
	records []sealrollup.Record
	slot    int64
}

func (s *summaryBenchSource) HasArchive(context.Context, uint64, int64) (bool, error) {
	return true, nil
}
func (s *summaryBenchSource) Scan(_ context.Context, _ uint64, _ Archive, _, _ int64, yield func(Sample) error) error {
	var err error
	s.records, err = v2part.DecodeSlotsInto(s.records, 0, s.slot, s.data)
	if err != nil {
		return err
	}
	for _, r := range s.records {
		if err = yield(Sample{Time: r.Sample.Time, Bits: r.Sample.Bits, Sequence: r.Order}); err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkOrderSummaryDecodeRender(b *testing.B) {
	// Singleton summary requests have the identical exact encoding: the only
	// order is already the maximum, so there is no extension tag to decode.
	const base int64 = 1800000000
	for _, n := range []int{1, 4, 64, 1024} {
		for _, codec := range []struct {
			name string
			mode v2part.OrderEncoding
		}{{"exact", v2part.ExactOrders}, {"predicted", v2part.PredictedOrders}, {"summary", v2part.SummaryOrders}} {
			records := make([]sealrollup.Record, n)
			for i := range records {
				stamp := base + int64(i) + 1
				order := uint64(i*1000003 + 1)
				records[i] = sealrollup.Record{Delta: sealrollup.Delta{Slot: stamp % 86400, Order: order, Sample: sealrollup.Sample{Time: stamp, Bits: math.Float64bits(float64(i) / 7), Sequence: order}}}
			}
			// Construct both representations with the production writer.
			path := filepath.Join(b.TempDir(), "part.sst")
			name := v2part.ID("order.bench")
			spec := v2part.Spec{Scope: v2part.Scope{Group: "g", VShard: v2part.GroupVShard, Part: v2part.PartID{1}}, Source: v2part.Provenance{Stream: "bench", Incarnation: v2part.PartID{2}, MinOrder: 1, MaxOrder: 1 << 40, Seq: 1}}
			definition, _ := json.Marshal(sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}})
			spec.Policies = []v2part.ArchivePolicy{{Archive: 0, Digest: sha256.Sum256(definition), Definition: definition}}
			w, err := v2part.NewDenseWriter(path, spec)
			if err != nil {
				b.Fatal(err)
			}
			if err = w.SetOrderEncoding(codec.mode); err != nil {
				b.Fatal(err)
			}
			key, _ := v2part.Encode(v2part.Key{Group: "g", VShard: v2part.Route(name), Part: spec.Scope.Part, Metric: name, BlockStart: records[0].Slot})
			if err = w.AppendVersions(key, records); err != nil {
				b.Fatal(err)
			}
			d, err := w.Close()
			if err != nil {
				b.Fatal(err)
			}
			var data []byte
			if err = v2part.Read(context.Background(), path, &d, func(_ v2part.Key, p []byte) error { data = append([]byte(nil), p...); return nil }); err != nil {
				b.Fatal(err)
			}
			for _, rendering := range []bool{false, true} {
				kind := "decode"
				if rendering {
					kind = "render"
				}
				b.Run(fmt.Sprintf("%s/n%d/%s", kind, n, codec.name), func(b *testing.B) {
					source := &summaryBenchSource{data: data, slot: records[0].Slot, records: make([]sealrollup.Record, 0, n)}
					m := testMetric([]Archive{{1, 86400}})
					ctx := context.Background()
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if !rendering {
							source.records, err = v2part.DecodeSlotsInto(source.records, 0, source.slot, data)
							if err != nil {
								b.Fatal(err)
							}
						} else {
							s, workspace, e := render(ctx, m, source, base, base+int64(n), base+int64(n), 4<<20)
							if e != nil || len(s.Values) != n {
								b.Fatal(e)
							}
							if math.Float64bits(s.Values[n-1]) != records[n-1].Sample.Bits {
								b.Fatal("render bits changed")
							}
							releaseRenderWorkspace(workspace)
						}
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/record")
				})
			}
		}
	}
}
