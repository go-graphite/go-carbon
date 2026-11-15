package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// archiveProbe compares logical source KVs and the exact sealed payload codec.
// It cannot attribute physical SST compression or obsolete bytes to these names.
func archiveProbe(dir string) {
	files, e := filepath.Glob(filepath.Join(dir, "*.transfer"))
	must(e)
	var names, occupied, capacity, kvBytes, sealedBytes, wireBytes int64
	policies := map[string]int64{}
	strata := map[string][5]int64{}
	var fallbackBlocks, packedDescriptors, packedNames int64
	var codec whisperio.ContinuationCodec
	for _, path := range files {
		f, e := os.Open(path)
		must(e)
		st, e := f.Stat()
		must(e)
		wireBytes += st.Size()
		stream, e := chunkstore.NewTransferStream(f)
		must(e)
		for {
			r, e := stream.Next()
			if e == io.EOF {
				break
			}
			must(e)
			if r.NativeSkip != nil || len(r.WindowSkipped) != 0 {
				panic("incomplete native export")
			}
			snap, e := chunkstore.TransferSnapshot(context.Background(), r, &codec, time.Now)
			must(e)
			p, _ := json.Marshal(r.Config.Retentions)
			policies[string(p)]++
			beforeOccupied, beforeCapacity, beforeKV, beforeSealed := occupied, capacity, kvBytes, sealedBytes
			var namePacked bool
			for _, kv := range r.Keys {
				if len(kv.Key) >= 10 && kv.Key[0] == 'f' && kv.Key[9] == 'b' && len(kv.Value) == 12 {
					packedDescriptors++
					namePacked = true
				}
				kvBytes += int64(len(kv.Key) + len(kv.Value))
			}
			for _, a := range snap.Archives {
				capacity += int64(a.Retention.NumberOfPoints())
				occupied += int64(len(a.Points))
				sort.SliceStable(a.Points, func(i, j int) bool { return a.Points[i].Timestamp < a.Points[j].Timestamp })
				for start := 0; start < len(a.Points); start += chunkstore.MaxSealedPoints {
					end := min(start+chunkstore.MaxSealedPoints, len(a.Points))
					ps := a.Points[start:end]
					// This is the same ordinary-domain primitive used by partspack.EncodeBlock.
					b, e := EncodeBlock(ps, uint32(a.Retention.SecondsPerPoint()))
					must(e)
					if b[0] == 0 {
						fallbackBlocks++
					}
					back, e := DecodeBlock(b)
					must(e)
					if len(back) != len(ps) {
						panic("sealed count mismatch")
					}
					for i, p := range ps {
						if p.Timestamp != back[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(back[i].Value) {
							panic("sealed point mismatch")
						}
					}
					sealedBytes += int64(len(b))
				}
			}
			if namePacked {
				packedNames++
			}
			z := strata[string(p)]
			z[0]++
			z[1] += occupied - beforeOccupied
			z[2] += capacity - beforeCapacity
			z[3] += kvBytes - beforeKV
			z[4] += sealedBytes - beforeSealed
			strata[string(p)] = z
			names++
		}
		_, _, e = stream.Finish()
		must(e)
		must(f.Close())
		emit(map[string]any{"stage": "archives_progress", "files": path, "names": names, "occupied_slots": occupied, "capacity_slots": capacity, "source_kv_bytes": kvBytes, "sealed_payload_bytes": sealedBytes, "when_cet": time.Now().Format(time.RFC3339)})
	}
	if names == 0 {
		panic("no archive records")
	}
	fmt.Fprintln(os.Stderr, "source KV bytes are logical; sealed bytes exclude keys/policy/SST/manifest")
	emit(map[string]any{"stage": "archives_done", "names": names, "occupied_slots": occupied, "capacity_slots": capacity, "source_kv_bytes": kvBytes, "sealed_payload_bytes": sealedBytes, "wire_bytes": wireBytes, "policies": policies, "source_kv_per_occupied": float64(kvBytes) / float64(occupied), "sealed_payload_per_occupied": float64(sealedBytes) / float64(occupied), "source_kv_per_capacity": float64(kvBytes) / float64(capacity), "exact": true, "fallback_blocks": fallbackBlocks, "source_packed_descriptors": packedDescriptors, "names_with_packed_descriptors": packedNames, "strata_name_occupied_capacity_source_sealed": strata, "when_cet": time.Now().Format(time.RFC3339)})
}
