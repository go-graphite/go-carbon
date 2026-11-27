package partlog_test

import (
	"bytes"
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

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Run create and recover in separate processes to measure real startup RSS.
// This measures the durable producer name catalog, not the entire daemon.
// Profiles, data and JSON remain in the explicitly supplied directory.
func TestNameCatalogScale(t *testing.T) {
	if os.Getenv("PARTLOG_SCALE_NAMES") == "" {
		t.Skip("opt-in large-catalog measurement")
	}
	n, err := strconv.Atoi(os.Getenv("PARTLOG_SCALE_NAMES"))
	if err != nil || n < 1 || n > 20_000_000 {
		t.Fatal("PARTLOG_SCALE_NAMES must be 1..20000000")
	}
	dir := os.Getenv("PARTLOG_SCALE_DIR")
	phase := os.Getenv("PARTLOG_SCALE_PHASE")
	if !filepath.IsAbs(dir) || (phase != "create" && phase != "recover") {
		t.Fatal("absolute PARTLOG_SCALE_DIR and create/recover PARTLOG_SCALE_PHASE required")
	}
	if phase == "create" {
		if _, err := os.Stat(filepath.Join(dir, "catalog.wal")); !os.IsNotExist(err) {
			t.Fatal("create requires a fresh catalog directory")
		}
	}
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	start := time.Now()
	c, err := partlog.OpenNameCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var nameBytes, definitionBytes uint64
	if phase == "create" {
		for i := 0; i < n; i++ {
			name, policy := scaleBinding(t, i)
			id, err := c.Intern(name, policy)
			if err != nil || id != uint64(i+1) {
				t.Fatalf("intern %d: id=%d err=%v", i, id, err)
			}
			nameBytes += uint64(len(name))
			definitionBytes += uint64(len(policy))
		}
		if err := c.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	if c.Count() != uint64(n) {
		t.Fatalf("count=%d, want=%d", c.Count(), n)
	}
	// Verify IDs, full names, immutable policy bytes and duplicate interning
	// after recovery without retaining another O(names) reference catalog.
	for i := 0; i < n; i += 997 {
		name, policy := scaleBinding(t, i)
		gotName, gotPolicy, ok := c.Lookup(uint64(i + 1))
		if !ok || gotName != name || !bytes.Equal(gotPolicy, policy) {
			t.Fatalf("binding mismatch at %d", i)
		}
		if id, err := c.Intern(name, policy); err != nil || id != uint64(i+1) {
			t.Fatalf("recovered duplicate %d: id=%d err=%v", i, id, err)
		}
	}
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	status, _ := os.ReadFile("/proc/self/status")
	var resident []string
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "VmRSS:") || strings.HasPrefix(line, "VmHWM:") {
			resident = append(resident, line)
		}
	}
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	peakRSS := usage.Maxrss
	if runtime.GOOS != "darwin" {
		peakRSS *= 1024
	}
	profile, err := os.Create(filepath.Join(dir, phase+"-heap.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.WriteHeapProfile(profile); err != nil {
		t.Fatal(err)
	}
	if err := profile.Close(); err != nil {
		t.Fatal(err)
	}
	result := map[string]any{"when_cet": time.Now().In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "phase": phase, "peak_rss_bytes": peakRSS, "goos": runtime.GOOS, "names": n, "seconds": elapsed.Seconds(), "heap_alloc": memory.HeapAlloc, "heap_delta": int64(memory.HeapAlloc) - int64(baseline.HeapAlloc), "heap_bytes_per_name": float64(int64(memory.HeapAlloc)-int64(baseline.HeapAlloc)) / float64(n), "heap_objects": memory.HeapObjects, "total_alloc_delta": memory.TotalAlloc - baseline.TotalAlloc, "proc": resident, "name_bytes": nameBytes, "definition_bytes": definitionBytes, "scope": "producer_name_catalog_only", "gomemlimit": os.Getenv("GOMEMLIMIT")}
	b, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, phase+".json"), append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(b))
	runtime.KeepAlive(c)
}

// Synthetic assumptions: 64/96/128/192-byte dotted names, evenly mixed;
// average/sum/last policies with one/two/three archives. Policies include the
// name, as the production v2 application catalog does. This is not a traffic capture.
func scaleBinding(t *testing.T, i int) (string, []byte) {
	t.Helper()
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
	p, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "local", Config: sealrollup.Config{Method: method, XFilesFactor: .5, Archives: archives}})
	if err != nil {
		t.Fatal(err)
	}
	return name, p
}
