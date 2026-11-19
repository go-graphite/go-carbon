package v2daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2index"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/points"
)

// Opt-in real journal/core/reader/discovery scaling, without TCP or classic
// data. Run create and recover as separate processes with a fresh absolute root.
func TestStoreNameScale(t *testing.T) {
	if os.Getenv("V2_STORE_SCALE_NAMES") == "" {
		t.Skip("opt-in store scaling measurement")
	}
	n, err := strconv.Atoi(os.Getenv("V2_STORE_SCALE_NAMES"))
	dir, phase := os.Getenv("V2_STORE_SCALE_DIR"), os.Getenv("V2_STORE_SCALE_PHASE")
	if err != nil || n < 1 || n > 20_000_000 || !filepath.IsAbs(dir) || (phase != "create" && phase != "recover") {
		t.Fatal("requires V2_STORE_SCALE_NAMES=1..20000000, absolute V2_STORE_SCALE_DIR and create/recover V2_STORE_SCALE_PHASE")
	}
	pointCount := 1
	if raw := os.Getenv("V2_STORE_SCALE_POINTS"); raw != "" {
		pointCount, err = strconv.Atoi(raw)
		if err != nil || pointCount < 1 || pointCount > 4096 {
			t.Fatal("V2_STORE_SCALE_POINTS must be 1..4096")
		}
	}
	if phase == "create" {
		if _, err := os.Stat(filepath.Join(dir, "restart.json")); !os.IsNotExist(err) {
			t.Fatal("create requires a fresh root")
		}
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	r, err := v2read.New(4, 256<<20, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	r.Identities = &v2core.PolicyCatalog{}
	s, err := Open(dir, Config{Layer: 3, Producers: 4, Shards: 4, MaxPendingRows: 32768, MaxCoreRows: 32768, Hooks: v2core.Hooks{Identities: r.Identities, DenseRollups: true, PublishBatches: r.Publish}})
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	defer func() {
		r.Close()
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	if phase == "create" {
		for producer, p := range s.Producers {
			var apps []Application
			for i := producer; i < n; i += 4 {
				length := [...]int{64, 96, 128, 192}[i%4]
				base := fmt.Sprintf("app.region.cluster.component.host%08d.requests.duration", i)
				name := base + strings.Repeat("x", max(0, length-len(base)))
				archives := []sealrollup.Archive{{Step: 10, Count: 8640}}
				if i%3 > 0 {
					archives = append(archives, sealrollup.Archive{Step: 60, Count: 10080})
				}
				if i%3 > 1 {
					archives = append(archives, sealrollup.Archive{Step: 3600, Count: 8760})
				}
				method := [...]sealrollup.Method{sealrollup.Average, sealrollup.Sum, sealrollup.Last}[i%3]
				policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "local", Config: sealrollup.Config{Method: method, XFilesFactor: .5, Archives: archives}})
				if err != nil {
					t.Fatal(err)
				}
				ps := make([]points.Point, pointCount)
				seq := make([]uint64, pointCount)
				for j := range ps {
					ps[j] = points.Point{Timestamp: 1_800_000_000 + int64(j)*10, Value: float64(i + j)}
					seq[j] = uint64(i*pointCount + j + 1)
				}
				apps = append(apps, Application{Name: name, Policy: policy, Order: uint64(i + 1), Clock: 1_800_000_000 + int64(pointCount-1)*10, Points: ps, Sequence: seq})
				if len(apps)*pointCount >= 4096 || i+4 >= n {
					if err := p.AppendApplications(apps); err != nil {
						t.Fatal(err)
					}
					apps = nil
				}
			}
			if err := p.Drain(); err != nil {
				t.Fatal(err)
			}
			// A synchronous empty seal fences the preceding pipelined submissions.
			if p.cursor > 0 {
				if err := s.Core.AppendBatches(producer, nil, p.cursor, true); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	opened := time.Since(start)
	names, _, err := r.NamesWithMasks(context.Background())
	if err != nil || len(names) != n {
		t.Fatalf("discovery names=%d want=%d err=%v", len(names), n, err)
	}
	var catalog v2index.Catalog
	// The daemon maps its index base from <v2.path>/index; measure that shape.
	if err := catalog.SetDir(filepath.Join(dir, "index")); err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	if err := catalog.Publish(1, names); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	// Persist readiness in the test log even if the profiling GC times out.
	fmt.Printf("SCALE_READY phase=%s names=%d core_open_seconds=%.9f discovery_ready_seconds=%.9f\n", phase, n, opened.Seconds(), elapsed.Seconds())
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	peak := usage.Maxrss
	if runtime.GOOS != "darwin" {
		peak *= 1024
	}
	f, err := os.Create(filepath.Join(dir, phase+"-heap.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	result := map[string]any{"when_cet": time.Now().In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "phase": phase, "names": n, "points_per_name": pointCount, "core_open_seconds": opened.Seconds(), "discovery_ready_seconds": elapsed.Seconds(), "heap_alloc": memory.HeapAlloc, "heap_bytes_per_name": float64(int64(memory.HeapAlloc)-int64(before.HeapAlloc)) / float64(n), "heap_objects": memory.HeapObjects, "total_alloc_bytes": memory.TotalAlloc - before.TotalAlloc, "mallocs": memory.Mallocs - before.Mallocs, "peak_rss_bytes": peak, "scope": "journal_core_reader_discovery", "shards": 4, "producers": 4, "core_max_rows": 32768, "gomemlimit": os.Getenv("GOMEMLIMIT")}
	b, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, phase+".json"), append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(b))
	runtime.KeepAlive(&catalog)
}
