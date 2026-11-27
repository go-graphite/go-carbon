package v2core

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Many names with sparse, partly overlapping routes per input, like short live
// seals. Fixture creation is excluded from the measured merge.
func BenchmarkSealedMergeSparseNames(b *testing.B) {
	dir := b.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "parts"), 0700); err != nil {
		b.Fatal(err)
	}
	// Model one owner's 64 routed shards, with only 16 present per seal.
	var names []v2part.ID
	for candidate := 0; len(names) < 16384; candidate++ {
		name := v2part.ID(fmt.Sprintf("live.service.%04d.counter.%07d", candidate%127, candidate))
		if v2part.Route(name) < 64 {
			names = append(names, name)
		}
	}
	var inputs []v2part.Descriptor
	for part := 0; part < 16; part++ {
		var rows []KV
		for n, name := range names {
			route := v2part.Route(v2part.ID(name))
			if int(route)%4 != part%4 {
				continue
			}
			// Fine slots advance; coarse replacements overlap across seals.
			p := RollupPolicy{Name: v2part.ID(name), Group: "live", Config: sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5}}
			// Only archive count is consulted by merging; canonical policy is shared
			// across replacements of this name.
			p.Config.Archives = []sealrollup.Archive{{Step: 1, Count: 3600}, {Step: 60, Count: 1440}}
			digest, err := sealrollup.PolicyDigest(p.Config)
			if err != nil {
				b.Fatal(err)
			}
			policy, err := json.Marshal(p)
			if err != nil {
				b.Fatal(err)
			}
			k := v2part.Key{Group: "live", VShard: route, Part: v2part.PartID{byte(part + 1)}, Metric: v2part.ID(name)}
			for archive := 0; archive < 2; archive++ {
				k.Archive = uint32(archive)
				k.BlockStart = int64(part)
				if archive == 1 {
					k.BlockStart = 0
				}
				enc, err := v2part.Encode(k)
				if err != nil {
					b.Fatal(err)
				}
				r := sealrollup.Record{Delta: sealrollup.Delta{Archive: archive, Slot: k.BlockStart, Order: uint64(part + 1), Sample: sealrollup.Sample{Time: int64(1000 + part), Bits: math.Float64bits(float64(n))}}}
				rows = append(rows, KV{Key: enc, Value: sealrollup.EncodeRecord(r)})
			}
			k.Archive = v2part.MetadataArchive
			k.BlockStart = 0
			enc, err := v2part.Encode(k)
			if err != nil {
				b.Fatal(err)
			}
			rows = append(rows, KV{Key: enc, Value: AppendRollupMetadata(nil, RollupMetadata{Version: 2, ID: uint64(n + 1), Policy: policy, Frontier: sealrollup.Frontier{Order: uint64(part + 1), MaxTime: 1015, Policy: digest}, Revision: uint64(part + 1)})})
		}
		slices.SortFunc(rows, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
		path := filepath.Join(dir, fmt.Sprintf("part-%d.sst", part))
		d, err := WriteDenseRollupPart(path, rows, v2part.Provenance{Stream: "bench", Incarnation: v2part.PartID{99}, MinOrder: uint64(part + 1), MaxOrder: uint64(part + 1)})
		if err != nil {
			b.Fatal(err)
		}
		if err = os.Rename(path, filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst")); err != nil {
			b.Fatal(err)
		}
		inputs = append(inputs, d)
	}
	var size uint64
	for _, d := range inputs {
		size += d.Bytes
	}
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(dir, "output.sst")
		if _, err := mergePartFiles(context.Background(), dir, path, inputs, 0, nil); err != nil {
			b.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			b.Fatal(err)
		}
	}
}
