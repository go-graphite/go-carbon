package carbonserver

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/blevesearch/vellum"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// This opt-in probe times one full scan of an existing Whisper tree, which it
// only reads. With GO_CARBON_SCAN_CACHE it hardlinks that saved generation into
// GO_CARBON_SCAN_DIR (same filesystem) and runs the parallel scan from it;
// otherwise it first builds a generation with the sequential scan and reports
// that time too. GO_CARBON_SCAN_SYNTH creates a synthetic tree of that many
// metrics under GO_CARBON_SCAN_ROOT first.
func TestScanProbe(t *testing.T) {
	root, out := os.Getenv("GO_CARBON_SCAN_ROOT"), os.Getenv("GO_CARBON_SCAN_DIR")
	if root == "" || out == "" {
		t.Skip("set the Whisper root and a scratch directory")
	}
	workers, _ := strconv.Atoi(os.Getenv("GO_CARBON_SCAN_WORKERS"))
	if n, _ := strconv.Atoi(os.Getenv("GO_CARBON_SCAN_SYNTH")); n > 0 {
		started := time.Now()
		writeSyntheticScanTree(t, root, n)
		t.Logf("synthetic tree: %d metrics in %s", n, time.Since(started))
	}
	if err := os.MkdirAll(out, 0o700); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(out, "files.gzip")
	if source := os.Getenv("GO_CARBON_SCAN_CACHE"); source != "" {
		linkScanGeneration(t, source, cache)
	} else {
		l, log := newScanTestListener(root, cache, 1)
		l.SetEstimateSize(nil)
		l.updateFileList(root, nil, nil)
		started, usage := time.Now(), scanUsage()
		l.updateFileList(root, nil, nil)
		t.Logf("sequential scan: %s, %s, files=%v", time.Since(started), usage.since(), log.last(t)["Files"])
	}
	if parts, _ := strconv.Atoi(os.Getenv("GO_CARBON_SCAN_PLAN_PARTS")); parts > 0 {
		scanPlanOverride = func(s *indexSnapshot, _ int) []snapshotMetricRange { return s.scanRangesInto(parts, parts) }
		defer func() { scanPlanOverride = nil }()
	}
	l, log := newScanTestListener(root, cache, workers)
	l.SetEstimateSize(nil)
	l.logger = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), log, zap.DebugLevel))
	if !l.updateFileList(root, nil, nil) {
		t.Fatal("saved generation not loaded")
	}
	repeat, _ := strconv.Atoi(os.Getenv("GO_CARBON_SCAN_REPEAT"))
	for i := range max(repeat, 1) {
		old := l.CurrentFileIndex().trieIdx.snapshot.manifest.Records
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		heap := sampleHeapPeak()
		profile := os.Getenv("GO_CARBON_SCAN_CPUPROFILE")
		if profile != "" && i == max(repeat, 1)-1 {
			f, err := os.Create(profile)
			if err != nil {
				t.Fatal(err)
			}
			if err := pprof.StartCPUProfile(f); err != nil {
				t.Fatal(err)
			}
			defer f.Close()
		}
		if marker := os.Getenv("GO_CARBON_SCAN_MARKER"); marker != "" {
			_ = os.WriteFile(marker, []byte(fmt.Sprint(i+1)), 0o644)
		}
		started, usage := time.Now(), scanUsage()
		l.updateFileList(root, nil, nil)
		if profile != "" && i == max(repeat, 1)-1 {
			pprof.StopCPUProfile()
		}
		t.Logf("scan %d: go heap in use: %dMB before, %dMB peak during scan", i+1, before.HeapInuse>>20, heap()>>20)
		fields := log.last(t)
		t.Logf("scan %d: parallel scan: %s, %s, workers=%v ranges=%v splits=%v idle=%v listed=%v replayed=%v files=%v new=%v plan=%v walk=%v slowest=%v publish=%v", i+1,
			time.Since(started), usage.since(), fields["scan_workers"], fields["scan_ranges"], fields["scan_splits"], fields["scan_idle_time"],
			fields["scan_listed_dirs"], fields["scan_replayed_dirs"], fields["Files"], fields["scan_new_metrics"],
			fields["scan_plan_time"], fields["scan_walk_time"], fields["scan_slowest_range"], fields["scan_publish_time"])
		logScanRanges(t, log)
		snapshot := l.CurrentFileIndex().trieIdx.snapshot
		t.Logf("scan %d: records: %d before, %d after", i+1, old, snapshot.manifest.Records)
		if manifest := snapshot.manifest; manifest.Dirs != nil {
			t.Logf("scan %d: catalogue %dMB, index %dMB", i+1, manifest.Dirs.Size>>20, manifest.Index.Size>>20)
		}
		if os.Getenv("GO_CARBON_SCAN_VERIFY") != "0" {
			verifyScanGeneration(t, cache, snapshot)
		}
		log.mu.Lock()
		log.ranges = nil
		log.mu.Unlock()
	}
}

// logScanRanges reports the slowest ranges of the last scan, which bound
// its duration when work is unevenly spread.
func logScanRanges(t *testing.T, log *scanLog) {
	t.Helper()
	log.mu.Lock()
	ranges := slices.Clone(log.ranges)
	log.mu.Unlock()
	slices.SortFunc(ranges, func(a, b map[string]any) int { return cmp.Compare(b["elapsed"].(float64), a["elapsed"].(float64)) })
	var total float64
	for _, r := range ranges {
		total += r["elapsed"].(float64)
	}
	t.Logf("ranges: %d, summed walk time %.1fs", len(ranges), total)
	for _, r := range ranges[:min(len(ranges), 12)] {
		t.Logf("  range %v: %.1fs files=%v metrics=%v expected=%v start=%q", r["range"], r["elapsed"], r["files"], r["metrics"], r["expected"], r["start"])
	}
}

func linkScanGeneration(t *testing.T, source, cache string) {
	t.Helper()
	manifest, err := readIndexSnapshotManifest(snapshotManifestPath(source))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(source)
	for from, to := range map[string]string{
		source:                                     cache,
		snapshotManifestPath(source):               snapshotManifestPath(cache),
		filepath.Join(dir, manifest.Index.Name):    filepath.Join(filepath.Dir(cache), manifest.Index.Name),
		filepath.Join(dir, manifest.Metadata.Name): filepath.Join(filepath.Dir(cache), manifest.Metadata.Name),
	} {
		if err := os.Link(from, to); err != nil {
			t.Fatal(err)
		}
	}
}

// verifyScanGeneration checks the joined FST and metadata against the file
// list cache records the workers wrote independently, in order.
func verifyScanGeneration(t *testing.T, cache string, snapshot *indexSnapshot) {
	t.Helper()
	started := time.Now()
	flc, err := NewFileListCache(cache, FLCVersion2, 'r')
	if err != nil {
		t.Fatal(err)
	}
	defer flc.Close()
	it, err := snapshot.index.Iterator(nil, nil)
	var key []byte
	var rows uint64
	for ; ; rows++ {
		entry, readErr := flc.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
		if err != nil {
			t.Fatalf("snapshot ends at row %d: %v", rows, err)
		}
		k, row := it.Current()
		if key, err = encodeSnapshotPath(key, entry.Path); err != nil || !bytes.Equal(k, key) || row != rows {
			t.Fatalf("row %d: snapshot %q=%d, cache %q", rows, k, row, entry.Path)
		}
		values, getErr := snapshot.metadata.get(row)
		if getErr != nil || values != [4]int64{entry.LogicalSize, entry.PhysicalSize, entry.DataPoints, entry.FirstSeenAt} {
			t.Fatalf("row %d metadata %v, cache %+v (%v)", rows, values, entry, getErr)
		}
		if rows%997 == 0 {
			if got, ok, getErr := snapshot.index.Get(key); getErr != nil || !ok || got != row {
				t.Fatalf("lookup %q: %d %t %v", entry.Path, got, ok, getErr)
			}
		}
		err = it.Next()
	}
	if !errors.Is(err, vellum.ErrIteratorDone) || rows != snapshot.manifest.Records || uint64(snapshot.index.Len()) != rows {
		t.Fatalf("snapshot has %d records, cache %d (%v)", snapshot.manifest.Records, rows, err)
	}
	t.Logf("verified %d records against the file list cache in %s", rows, time.Since(started))
}

// sampleHeapPeak samples the heap in use until the returned function reports
// the largest value seen.
func sampleHeapPeak() func() uint64 {
	var peak uint64
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		written := uint64(0)
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			peak = max(peak, m.HeapInuse)
			// Keep a heap profile of the largest heap seen.
			if dir := os.Getenv("GO_CARBON_SCAN_HEAP_DIR"); dir != "" && peak > written+written/4+64<<20 {
				written = peak
				if f, err := os.Create(filepath.Join(dir, "scan-heap.pb.gz")); err == nil {
					_ = pprof.Lookup("heap").WriteTo(f, 0)
					_ = f.Close()
				}
			}
			select {
			case <-done:
				return
			case <-ticker.C:
			}
		}
	}()
	return func() uint64 {
		close(done)
		<-stopped
		return peak
	}
}

type scanResourceUsage struct {
	started time.Time
	cpu     time.Duration
}

func scanUsage() scanResourceUsage {
	return scanResourceUsage{time.Now(), scanCPU()}
}

func scanCPU() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func (u scanResourceUsage) since() string {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	maxRSS := ru.Maxrss << 10 // kilobytes on Linux
	if runtime.GOOS == "darwin" {
		maxRSS = ru.Maxrss
	}
	return fmt.Sprintf("cpu=%s maxrss=%dMB", (scanCPU() - u.cpu).Round(time.Millisecond), maxRSS>>20)
}

// writeSyntheticScanTree mimics a production tree: deep, sparse directories,
// about two directories per metric, and a few large namespaces.
func writeSyntheticScanTree(t *testing.T, root string, metrics int) {
	t.Helper()
	paths := make(chan string, 1024)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for p := range paths {
				err := os.WriteFile(p, nil, 0o644)
				if errors.Is(err, os.ErrNotExist) {
					if err = os.MkdirAll(filepath.Dir(p), 0o755); err == nil {
						err = os.WriteFile(p, nil, 0o644)
					}
				}
				if err != nil {
					t.Error(err)
				}
			}
		})
	}
	for i := range metrics {
		ns := i % 97
		if i%3 == 0 {
			ns = i % 5
		}
		parts := []string{root, "aggregations", "secondly", fmt.Sprintf("ns%02d", ns), fmt.Sprintf("svc%d", i%13), fmt.Sprintf("host%05d", i/40)}
		if i%2 == 0 {
			parts = append(parts, fmt.Sprintf("op%d", i%40), "latency")
		}
		parts = append(parts, fmt.Sprintf("m%d.wsp", i%40))
		paths <- filepath.Join(parts...)
	}
	close(paths)
	wg.Wait()
}
