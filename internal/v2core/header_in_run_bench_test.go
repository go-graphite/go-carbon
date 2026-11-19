package v2core

// BenchmarkHeaderInRun compares seal parts with a separate envelope KV per
// name (v2part.LegacyEnvelopeKV) against envelopes folded into each name's
// last run, on the production planner's tuples and seal writer. HIR_NAMES
// (default 262144) and HIR_K (default 1,10 points per name per seal) pick the
// sweep. Run: go test -run '^$' -bench HeaderInRun ./internal/v2core/

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// readHeaderPart visits a part's logical stream and decodes runs and envelope
// headers, as a merge or full-part read does.
func readHeaderPart(path string, d *v2part.Descriptor, buf []sealrollup.Record) (points, envelopes int, err error) {
	err = v2part.Read(context.Background(), path, d, func(k v2part.Key, value []byte) error {
		if k.Archive == v2part.MetadataArchive {
			_, _, e := decodeRollupMetadataHeader(value)
			envelopes++
			return e
		}
		var e error
		buf, e = v2part.DecodeSlotsInto(buf[:0], k.Archive, k.BlockStart, value)
		points += len(buf)
		return e
	})
	return points, envelopes, err
}

func BenchmarkHeaderInRun(b *testing.B) {
	names := 262144
	if v := os.Getenv("HIR_NAMES"); v != "" {
		names, _ = strconv.Atoi(v)
	}
	ks := []int{1, 10}
	if v := os.Getenv("HIR_K"); v != "" {
		ks = ks[:0]
		for _, f := range strings.Split(v, ",") {
			k, _ := strconv.Atoi(f)
			ks = append(ks, k)
		}
	}
	shape := sealCPUShape{"single10s", []sealrollup.Archive{{Step: 10, Count: 8640}}, sealrollup.Last, 0, 10}
	defer v2part.LegacyEnvelopeKV.Store(false)
	seq := 0
	for _, k := range ks {
		shape.points = k
		r, batches, points := sealCPUFixture(b, shape, names)
		plan, err := r.planParts(SealInput{Batches: batches})
		if err != nil {
			b.Fatal(err)
		}
		for _, layout := range []string{"envelopeKV", "headerInRun"} {
			legacy := layout == "envelopeKV"
			dir := b.TempDir()
			var descs []v2part.Descriptor
			var paths []string
			b.Run(fmt.Sprintf("write/%s/k%d", layout, k), func(b *testing.B) {
				v2part.LegacyEnvelopeKV.Store(legacy)
				b.ReportAllocs()
				var stored, kvs uint64
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					for _, old := range paths {
						_ = os.Remove(old)
					}
					b.StartTimer()
					descs, paths = descs[:0], paths[:0]
					stored, kvs = 0, 0
					for f, part := range plan.Parts {
						part.Spec.Source.Incarnation, part.Spec.Source.Stream = v2part.PartID{1}, "bench"
						seq++
						path := filepath.Join(dir, fmt.Sprintf("p%d-%d.sst", seq, f))
						d, err := writePlannedRollupTuples(path, part)
						if err != nil {
							b.Fatal(err)
						}
						stored += d.Bytes
						kvs += d.Coverage.Records
						descs, paths = append(descs, d), append(paths, path)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*names), "ns/name")
				b.ReportMetric(float64(stored)/float64(points), "storedB/point")
				b.ReportMetric(float64(stored)/float64(names), "storedB/name")
				b.ReportMetric(float64(kvs)/float64(names), "KV/name")
			})
			b.Run(fmt.Sprintf("read/%s/k%d", layout, k), func(b *testing.B) {
				b.ReportAllocs()
				buf := make([]sealrollup.Record, 0, 64)
				var got, envs int
				for i := 0; i < b.N; i++ {
					got, envs = 0, 0
					for f := range paths {
						p, e, err := readHeaderPart(paths[f], &descs[f], buf)
						if err != nil {
							b.Fatal(err)
						}
						got, envs = got+p, envs+e
					}
				}
				if envs != names || got != points {
					b.Fatalf("read %d envelopes %d points, want %d %d", envs, got, names, points)
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*names), "ns/name")
			})
			_ = os.RemoveAll(dir)
		}
		plan.Abort()
	}
}
