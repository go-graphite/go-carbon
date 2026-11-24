package v2core

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Opt-in populated fixture. Each epoch advances an hour, writes four occupied
// slots per name and leaves earlier immutable parts live. The same test runs
// against a source overlay of the unchanged runtime for heap/read comparison.
func TestRollupDirectoryPopulatedScale(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("V2_MEM2_NAMES"))
	if n == 0 {
		t.Skip("opt-in populated read-directory fixture")
	}
	epochs, _ := strconv.Atoi(os.Getenv("V2_MEM2_EPOCHS"))
	if epochs == 0 {
		epochs = 6
	}
	root := os.Getenv("V2_MEM2_ROOT")
	if root == "" {
		root = t.TempDir()
	}
	out := os.Getenv("V2_MEM2_OUTPUT")
	if out == "" {
		out = t.TempDir()
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		t.Fatal(err)
	}
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()
	db, err := pebble.Open(root, &pebble.Options{Cache: cache, MemTableSize: 16 << 20, MaxConcurrentCompactions: func() int { return 1 }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	ctx := context.Background()
	var prior *RollupSnapshot
	var pin *pebble.Snapshot
	name := func(i int) string {
		s := fmt.Sprintf("fixture.region.component.host%08d.requests.duration", i)
		return s + strings.Repeat("x", 96-len(s))
	}
	config := sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 10080}}}
	digest, err := sealrollup.PolicyDigest(config)
	if err != nil {
		t.Fatal(err)
	}
	identities := &PolicyCatalog{}
	writers := newCoreRollups()
	writers.identities = identities
	var output []map[string]any
	for epoch := 1; epoch <= epochs; epoch++ {
		batch := db.NewBatch()
		for i := 0; i < n; i++ {
			metric := v2part.ID(name(i))
			raw, err := EncodeRollupPolicy(RollupPolicy{Name: metric, Group: "fixture", Config: config})
			if err != nil {
				t.Fatal(err)
			}
			if epoch == 1 {
				m, err := writers.metric(uint64(i+1), raw)
				if err != nil {
					t.Fatal(err)
				}
				rollupTestInstall(writers, uint64(i+1), m, freshMetadata)
			}
			key := v2part.Key{Group: "fixture", VShard: v2part.Route(metric), Metric: metric}
			binary.BigEndian.PutUint64(key.Part[:8], uint64(epoch))
			stamp := int64(1700000000 + epoch*3600)
			for point := int64(0); point < 4; point++ {
				slot := (stamp + point) % config.Archives[0].Count
				key.Archive = 0
				key.BlockStart = slot
				encoded, err := v2part.Encode(key)
				if err != nil {
					t.Fatal(err)
				}
				r := sealrollup.Record{Delta: sealrollup.Delta{Order: uint64(epoch), Archive: 0, Slot: slot, Sample: sealrollup.Sample{Time: stamp + point, Bits: math.Float64bits(float64(i) + float64(point)/4), Sequence: uint64(epoch*4) + uint64(point)}}}
				if err := batch.Set(encoded, sealrollup.EncodeRecord(r), nil); err != nil {
					t.Fatal(err)
				}
			}
			key.Archive = v2part.MetadataArchive
			key.BlockStart = 0
			encoded, err := v2part.Encode(key)
			if err != nil {
				t.Fatal(err)
			}
			meta := RollupMetadata{Version: 1, ID: uint64(i + 1), Policy: raw, Revision: uint64(epoch), LastUpdate: stamp * 1e9, Frontier: sealrollup.Frontier{Order: uint64(epoch), MaxTime: stamp + 3, Policy: digest}}
			if err := batch.Set(encoded, AppendRollupMetadata(nil, meta), nil); err != nil {
				t.Fatal(err)
			}
			if i%4096 == 4095 {
				if err := batch.Commit(pebble.NoSync); err != nil {
					t.Fatal(err)
				}
				batch.Reset()
			}
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		batch.Close()
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		nextPin := db.NewSnapshot()
		started := time.Now()
		next, err := NewRollupSnapshot(ctx, nextPin, prior, identities, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		build := time.Since(started)
		prior = next
		if pin != nil {
			pin.Close()
		}
		pin = nextPin
		runtime.GC()
		runtime.GC()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		f, err := os.Create(filepath.Join(out, fmt.Sprintf("heap-%02d.pb.gz", epoch)))
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.WriteHeapProfile(f); err != nil {
			t.Fatal(err)
		}
		f.Close()
		var latencies []int64
		h := sha256.New()
		for j := 0; j < 128; j++ {
			metric := name((j * 7919) % n)
			started := time.Now()
			rs, err := prior.Records(ctx, metric)
			latencies = append(latencies, time.Since(started).Nanoseconds())
			if err != nil {
				t.Fatal(err)
			}
			slices.SortFunc(rs, func(a, b sealrollup.Record) int {
				if a.Archive != b.Archive {
					return a.Archive - b.Archive
				}
				if a.Slot < b.Slot {
					return -1
				}
				if a.Slot > b.Slot {
					return 1
				}
				return 0
			})
			for _, r := range rs {
				h.Write(sealrollup.EncodeRecord(r))
			}
			if len(rs) != 4*epoch {
				t.Fatalf("populated rows=%d want=%d", len(rs), 4*epoch)
			}
		}
		slices.Sort(latencies)
		row := map[string]any{"names": n, "epoch": epoch, "simulated_hours": epoch, "parts": len(prior.parts), "heap_live": memory.HeapAlloc, "heap_delta_per_name": float64(int64(memory.HeapAlloc)-int64(before.HeapAlloc)) / float64(n), "snapshot_build_seconds": build.Seconds(), "read_p50_ns": latencies[len(latencies)/2], "read_p99_ns": latencies[len(latencies)*99/100], "read_digest": hex.EncodeToString(h.Sum(nil))}
		output = append(output, row)
		b, _ := json.MarshalIndent(output, "", "  ")
		if err := os.WriteFile(filepath.Join(out, "measurements.json"), b, 0600); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("MEM2 %s\n", mustJSON(row))
	}
	if pin != nil {
		defer pin.Close()
	}
	runtime.KeepAlive(prior)
	runtime.KeepAlive(writers)
}
func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
