package v2core

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// This opt-in experiment writes actual dense SSTs and invokes the production
// selector, streaming writer and atomic replacement. Only time is accelerated.
// Accounting is archive SST bytes, excluding Pebble manifests/WAL/input logs.
func TestMergeLifetimeFixture(t *testing.T) {
	if os.Getenv("V2_MERGE_LIFETIME") != "1" {
		t.Skip("opt-in disk experiment")
	}
	t.Setenv("CORE_MERGE_WORKERS", "0")
	type result struct {
		Policy                                                                                               string
		Points, InitialBytes, RewritePoints, MergeBytes, LargestMerge, PeakTemporary, MaxRewrites, LiveParts uint64
		RewritesPerPoint, BytesPerPoint                                                                      float64
	}
	var results []result
	var reference []sealrollup.Record
	const seals = 1024
	const width = 128
	now := time.Now().Unix()
	start := (now/86400)*86400 - 4*86400
	for _, bounded := range []bool{false, true} {
		label := "old"
		cap := uint64(1 << 50)
		if bounded {
			// Monthly partitions with the default (ClickHouse-sized) cap.
			label = "month"
			cap = 150 << 30
		}
		t.Setenv("CORE_MERGE_MAX_BYTES", fmt.Sprint(cap))
		e := mergeFixture(t, t.TempDir(), nil)
		o := e.owners[0]
		p, enc := rollupTestPolicy(t, "lifetime.metric", "1s:7d", sealrollup.Last, .5)
		digest, _ := sealrollup.PolicyDigest(p.Config)
		rng := rand.New(rand.NewPCG(1, 2))
		r := result{Policy: label, Points: seals * width}
		type lineage struct{ points, depth uint64 }
		ancestry := map[v2part.Digest]lineage{}
		for n := uint64(1); n <= seals; n++ {
			stamp := start + int64(n)*300
			rs := make([]sealrollup.Record, width)
			for j := range rs {
				at := stamp + int64(j)
				rs[j] = sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: at % p.Config.Archives[0].Count, Order: n, Sample: sealrollup.Sample{Time: at, Bits: rng.Uint64()}}}
			}
			partition := int64(0)
			if bounded {
				partition = rollupMonthPartition(rs[0].Sample.Time)
			}
			for _, partition := range []int64{partition} {
				var part v2part.PartID
				binary.BigEndian.PutUint64(part[:8], n^uint64(partition))
				spec := v2part.Spec{Scope: v2part.Scope{Group: p.Group, VShard: v2part.GroupVShard, Partition: partition, Part: part}, Source: o.partSource, MinTime: stamp, MaxTime: stamp + width - 1}
				spec.Source.MinOrder = n
				spec.Source.MaxOrder = n
				def, _ := json.Marshal(p.Config)
				spec.Policies = []v2part.ArchivePolicy{{Archive: 0, Definition: def, Digest: sha256.Sum256(def)}}
				path := filepath.Join(o.dir, "fixture.sst")
				w, err := v2part.NewDenseWriter(path, spec)
				if err != nil {
					t.Fatal(err)
				}
				k := v2part.Key{Group: p.Group, VShard: v2part.Route(p.Name), Partition: partition, Part: part, Metric: p.Name}
				slices.SortFunc(rs, func(a, b sealrollup.Record) int { return int(a.Slot - b.Slot) })
				for _, record := range rs {
					if bounded && partition == 0 {
						continue
					}
					k.BlockStart = record.Slot
					key, _ := v2part.Encode(k)
					if err = w.SetRecordEncoded(key, record); err != nil {
						t.Fatal(err)
					}
				}
				k.Archive = v2part.MetadataArchive
				k.BlockStart = 0
				key, _ := v2part.Encode(k)
				m := RollupMetadata{Version: 1, ID: 7, Policy: enc, Frontier: sealrollup.Frontier{Order: n, MaxTime: stamp + width - 1, Policy: digest}, LastUpdate: (stamp + width) * 1e9, Revision: n}
				if err = w.SetEncoded(key, AppendRollupMetadata(nil, m)); err != nil {
					t.Fatal(err)
				}
				d, err := w.Close()
				if err != nil {
					t.Fatal(err)
				}
				d.VShards = []uint16{v2part.Route(p.Name)}
				kv, _, err := retainSealedPart(o.dir, path, d)
				if err != nil {
					t.Fatal(err)
				}
				_ = os.Remove(path)
				o.gc.mu.Lock()
				err = o.db.Set(kv.Key, kv.Value, pebble.NoSync)
				o.gc.inventory = o.gc.inventory.with([]coreKV{kv}, nil)
				o.gc.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				r.InitialBytes += d.Bytes
				points := uint64(width)
				if bounded && partition == 0 {
					points = 0
				}
				ancestry[d.SHA256] = lineage{points, 0}
			}
		}
		for {
			s := o.db.NewSnapshot()
			inputs, err := o.selectMerge(s)
			_ = s.Close()
			if err != nil {
				t.Fatal(err)
			}
			if len(inputs) == 0 {
				break
			}
			var next lineage
			for _, d := range inputs {
				a := ancestry[d.SHA256]
				next.points += a.points
				next.depth = max(next.depth, a.depth+1)
			}
			before := e.PartGCStats()[0]
			changed, err := o.mergeSealedParts(nil, false)
			if err != nil || !changed {
				t.Fatalf("merge %v %v", changed, err)
			}
			after := e.PartGCStats()[0]
			written := after.MergeWrittenBytes - before.MergeWrittenBytes
			r.RewritePoints += next.points
			r.LargestMerge = max(r.LargestMerge, written)
			r.PeakTemporary = max(r.PeakTemporary, written)
			if next.points > 0 {
				r.MaxRewrites = max(r.MaxRewrites, next.depth)
			}
			o.gc.mu.Lock()
			for _, v := range o.gc.inventory.bindings {
				d, err := v2part.ParseDescriptor(v)
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := ancestry[d.SHA256]; !ok {
					ancestry[d.SHA256] = next
				}
			}
			o.gc.mu.Unlock()
		}
		s := o.db.NewSnapshot()
		got := mergeRead(t, s)
		_ = s.Close()
		if !bounded {
			reference = got
		} else if !reflect.DeepEqual(got, reference) {
			t.Fatal("month-partitioned records differ bitwise")
		}
		stats := e.PartGCStats()[0]
		r.MergeBytes = stats.MergeWrittenBytes
		r.LiveParts = stats.LiveParts
		r.RewritesPerPoint = float64(r.RewritePoints) / float64(r.Points)
		r.BytesPerPoint = float64(r.InitialBytes+r.MergeBytes) / float64(r.Points)
		results = append(results, r)
		if err := e.close(); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := json.MarshalIndent(results, "", "  ")
	t.Log("LIFETIME " + string(b))
	if out := os.Getenv("V2_MERGE_LIFETIME_OUTPUT"); out != "" {
		if err := os.WriteFile(out, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Monthly partitions must not fan out parts or add rewrites (#1543's
	// expiry-day partitions left 83 quiescent parts here versus 4).
	if results[1].MaxRewrites > results[0].MaxRewrites || 4*results[1].LiveParts > 5*results[0].LiveParts+4 {
		t.Fatal("monthly partitions added rewrites or parts")
	}

}
