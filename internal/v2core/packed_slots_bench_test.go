//go:build packedslots

package v2core

// BenchmarkPackedSlots compares today's seal part layout (one dense run KV per
// (name, archive) plus one envelope KV per name) with the bench-only packed
// layout (envelope folded into the name's last run) on the planner's tuples.
// PS_NAMES (default 262144) and PS_K (default 1,2,6,10 points per name per
// seal) pick the sweep. Run: go test -tags packedslots -run '^$' -bench PackedSlots.

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

func writePlannedRollupTuplesPacked(path string, part plannedRollupPart) (v2part.Descriptor, error) {
	w, err := v2part.NewDenseWriter(path, part.Spec)
	if err != nil {
		return v2part.Descriptor{}, err
	}
	defer w.Abort()
	w.SealCompression()
	var shards []uint16
	for _, segment := range part.Segments {
		for _, kv := range segment {
			if len(shards) == 0 || shards[len(shards)-1] != kv.vshard {
				shards = append(shards, kv.vshard)
			}
			if kv.record != nil {
				err = w.SetPlannedRecord(kv.vshard, kv.name, kv.record)
			} else {
				err = w.SetPlannedMetadataPacked(kv.vshard, kv.name, kv.value)
			}
			if err != nil {
				return v2part.Descriptor{}, err
			}
		}
	}
	d, err := w.Close()
	if err != nil {
		return v2part.Descriptor{}, err
	}
	d.VShards = shards
	return d, d.Validate()
}

// readPartDecoded visits every KV of a part and decodes runs and envelope
// headers, as a merge or full-part read does. It returns points and envelopes.
func readPartDecoded(path string, d *v2part.Descriptor, buf []sealrollup.Record) (points, envelopes int, err error) {
	err = v2part.Read(context.Background(), path, d, func(k v2part.Key, value []byte) error {
		if k.Archive == v2part.MetadataArchive {
			if _, _, e := decodeRollupMetadataHeader(value); e != nil {
				return e
			}
			envelopes++
			return nil
		}
		env, run, packed, e := v2part.SplitPacked(value)
		if e != nil {
			return e
		}
		if packed {
			if _, _, e = decodeRollupMetadataHeader(env); e != nil {
				return e
			}
			envelopes++
		}
		buf, e = v2part.DecodeSlotsInto(buf[:0], k.Archive, k.BlockStart, run)
		points += len(buf)
		return e
	})
	return points, envelopes, err
}

func BenchmarkPackedSlots(b *testing.B) {
	names := 262144
	if v := os.Getenv("PS_NAMES"); v != "" {
		names, _ = strconv.Atoi(v)
	}
	ks := []int{1, 2, 6, 10}
	if v := os.Getenv("PS_K"); v != "" {
		ks = ks[:0]
		for _, f := range strings.Split(v, ",") {
			k, _ := strconv.Atoi(f)
			ks = append(ks, k)
		}
	}
	shapes := []sealCPUShape{
		{"single10s", []sealrollup.Archive{{Step: 10, Count: 8640}}, sealrollup.Last, 0, 10},
		{"multi", []sealrollup.Archive{{Step: 10, Count: 8640}, {Step: 60, Count: 10080}, {Step: 600, Count: 52560}}, sealrollup.Average, 0, 10},
	}
	if v := os.Getenv("PS_SHAPE"); v != "" {
		for _, s := range shapes {
			if s.name == v {
				shapes = []sealCPUShape{s}
			}
		}
	}
	type layout struct {
		name  string
		write func(string, plannedRollupPart) (v2part.Descriptor, error)
	}
	seq := 0
	layouts := []layout{{"today", writePlannedRollupTuples}, {"packed", writePlannedRollupTuplesPacked}}
	for _, shape := range shapes {
		for _, k := range ks {
			shape.points = k
			r, batches, points := sealCPUFixture(b, shape, names)
			b.Run(fmt.Sprintf("plan/%s/k%d", shape.name, k), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					plan, err := r.planParts(SealInput{Batches: batches})
					if err != nil {
						b.Fatal(err)
					}
					plan.Abort()
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*points), "ns/point")
			})
			plan, err := r.planParts(SealInput{Batches: batches})
			if err != nil {
				b.Fatal(err)
			}
			for _, l := range layouts {
				dir := b.TempDir()
				var descs []v2part.Descriptor
				var paths []string
				b.Run(fmt.Sprintf("write/%s/%s/k%d", l.name, shape.name, k), func(b *testing.B) {
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
							d, err := l.write(path, part)
							if err != nil {
								b.Fatal(err)
							}
							stored += d.Bytes
							kvs += d.Coverage.Records
							descs, paths = append(descs, d), append(paths, path)
						}
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*points), "ns/point")
					b.ReportMetric(float64(stored)/float64(points), "storedB/point")
					b.ReportMetric(float64(kvs)/float64(names), "KV/name")
				})
				b.Run(fmt.Sprintf("read/%s/%s/k%d", l.name, shape.name, k), func(b *testing.B) {
					b.ReportAllocs()
					buf := make([]sealrollup.Record, 0, 64)
					var got, envs int
					for i := 0; i < b.N; i++ {
						got, envs = 0, 0
						for f := range paths {
							p, e, err := readPartDecoded(paths[f], &descs[f], buf)
							if err != nil {
								b.Fatal(err)
							}
							got, envs = got+p, envs+e
						}
					}
					if envs != names {
						b.Fatalf("envelopes %d, want %d", envs, names)
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*points), "ns/point")
					b.ReportMetric(float64(got)/float64(points), "records/point")
				})
				_ = os.RemoveAll(dir)
			}
			plan.Abort()
		}
	}
}
