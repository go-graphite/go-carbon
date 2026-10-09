package carbonserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-whisper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// scanLog keeps the fields of each "file list updated" record.
type scanLog struct {
	mu      sync.Mutex
	updates []map[string]any
	ranges  []map[string]any
}

func (l *scanLog) Write(p []byte) (int, error) {
	var record map[string]any
	if json.Unmarshal(p, &record) == nil {
		l.mu.Lock()
		switch record["msg"] {
		case "file list updated":
			l.updates = append(l.updates, record)
		case "scan range":
			l.ranges = append(l.ranges, record)
		}
		l.mu.Unlock()
	}
	return len(p), nil
}

func (*scanLog) Sync() error { return nil }

func (l *scanLog) last(t *testing.T) map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.updates) == 0 {
		t.Fatal("no file list update logged")
	}
	return l.updates[len(l.updates)-1]
}

func newScanTestListener(root, cache string, workers int) (*CarbonserverListener, *scanLog) {
	l := NewCarbonserverListener(nil)
	log := &scanLog{}
	l.logger = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), log, zap.InfoLevel))
	l.SetWhisperData(root)
	l.SetTrieIndex(true)
	l.SetConcurrentIndex(true)
	l.SetFileListCache(cache)
	l.SetFileListCacheVersion(int(FLCVersion2))
	l.SetScanWorkers(workers)
	l.SetEstimateSize(func(metric string) (int64, int64, int64) { return 1, 2, int64(len(metric)) })
	l.SetRealtimeIndex(64)
	return l, log
}

func writeScanFile(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
}

// writeScanTree creates a skewed tree: a few large namespaces, deep chains,
// sidecars with plain and hashed names, lock files, symlinks and empty dirs.
func writeScanTree(t *testing.T, root string, rng *rand.Rand) []string {
	t.Helper()
	var metrics []string
	for ns := 0; ns < 12; ns++ {
		n := 10 + rng.Intn(40)
		if ns%5 == 0 {
			n = 400
		}
		for m := 0; m < n; m++ {
			parts := []string{fmt.Sprintf("ns%02d", ns)}
			for d := rng.Intn(5); d > 0; d-- {
				parts = append(parts, fmt.Sprintf("l%d", rng.Intn(4)))
			}
			parts = append(parts, fmt.Sprintf("m%03d.wsp", m))
			rel := filepath.Join(parts...)
			writeScanFile(t, filepath.Join(root, rel), 1+rng.Intn(5000))
			metrics = append(metrics, rel)
		}
	}
	long := filepath.Join(root, "ns01", strings.Repeat("x", 250)+".wsp")
	writeScanFile(t, long, 10)
	writeScanFile(t, whisper.OutOfOrderSidecarPath(long), 3000)
	for _, rel := range metrics[:40] {
		writeScanFile(t, whisper.OutOfOrderSidecarPath(filepath.Join(root, rel)), 100+rng.Intn(9000))
	}
	for _, rel := range metrics[40:60] {
		writeScanFile(t, filepath.Join(root, rel)+".lock", 0)
	}
	writeScanFile(t, filepath.Join(root, "ns02", "orphan.wsp.ooo"), 4096)
	writeScanFile(t, filepath.Join(root, "ns02", "notes.txt"), 7)
	writeScanFile(t, filepath.Join(root, "ns03", ".hidden.wsp"), 9)
	writeScanFile(t, filepath.Join(root, "dir.wsp", "inner.wsp"), 11) // a directory named like a metric
	for _, dir := range []string{"empty", "ns04/empty/deeper"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(root, metrics[0]), filepath.Join(root, "ns05", "link.wsp")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "ns06"), filepath.Join(root, "ns05", "linkdir")); err != nil {
		t.Fatal(err)
	}
	return metrics
}

// mutateScanTree adds, removes and resizes metrics between generations.
func mutateScanTree(t *testing.T, root string, metrics []string, rng *rand.Rand) {
	t.Helper()
	for i := 0; i < 60; i++ {
		writeScanFile(t, filepath.Join(root, fmt.Sprintf("ns%02d", rng.Intn(14)), fmt.Sprintf("new%d", rng.Intn(3)), fmt.Sprintf("n%03d.wsp", i)), 1+rng.Intn(3000))
	}
	writeScanFile(t, filepath.Join(root, "ns00", "new0", "n000.wsp"), 321)
	for _, rel := range metrics[100:160] {
		if err := os.Remove(filepath.Join(root, rel)); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	if err := os.RemoveAll(filepath.Join(root, "ns07")); err != nil {
		t.Fatal(err)
	}
	for _, rel := range metrics[200:210] {
		writeScanFile(t, filepath.Join(root, rel), 7777)
		writeScanFile(t, whisper.OutOfOrderSidecarPath(filepath.Join(root, rel)), 1234)
	}
}

// linkGeneration hardlinks a saved generation into dir, keeping the file
// identities the snapshot manifest checks.
func linkGeneration(t *testing.T, cache, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(cache))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := os.Link(filepath.Join(filepath.Dir(cache), e.Name()), filepath.Join(dir, e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(dir, filepath.Base(cache))
}

func readScanCache(t *testing.T, path string) []FLCEntry {
	t.Helper()
	flc, err := NewFileListCache(path, FLCVersion2, 'r')
	if err != nil {
		t.Fatal(err)
	}
	defer flc.Close()
	var entries []FLCEntry
	for {
		entry, err := flc.Read()
		if errors.Is(err, io.EOF) {
			return entries
		}
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, *entry)
	}
}

func readScanSnapshot(t *testing.T, cache, root string) []FLCEntry {
	t.Helper()
	snapshot, err := openIndexSnapshot(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	var entries []FLCEntry
	it, err := snapshot.index.Iterator(nil, nil)
	for err == nil {
		key, row := it.Current()
		values, getErr := snapshot.metadata.get(row)
		if getErr != nil || row != uint64(len(entries)) {
			t.Fatalf("row %d of %q: %v", row, key, getErr)
		}
		entries = append(entries, FLCEntry{strings.ReplaceAll(string(key), "\x00", "/"), values[0], values[1], values[2], values[3]})
		err = it.Next()
	}
	return entries
}

// compareScanEntries requires identical entries except for the first-seen time
// of metrics created after the base generation, which each scan stamps itself.
func compareScanEntries(t *testing.T, what string, got, want []FLCEntry, base map[string]int64, from, to int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d entries, want %d", what, len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if _, existed := base[w.Path]; !existed && g.FirstSeenAt >= from && g.FirstSeenAt <= to && w.FirstSeenAt >= from && w.FirstSeenAt <= to {
			g.FirstSeenAt = w.FirstSeenAt
		}
		if g != w {
			t.Fatalf("%s entry %d: got %+v, want %+v", what, i, g, w)
		}
	}
}

// ageScanGeneration rewrites a saved generation with past first-seen times
// and returns them by path.
func ageScanGeneration(t *testing.T, cache, root string) map[string]int64 {
	t.Helper()
	entries := readScanCache(t, cache)
	writer, err := NewFileListCache(cache, FLCVersion2, 'w')
	if err != nil {
		t.Fatal(err)
	}
	aged := make(map[string]int64, len(entries))
	for i := range entries {
		entries[i].FirstSeenAt = int64(1000 + i)
		aged[entries[i].Path] = entries[i].FirstSeenAt
		if err := writer.Write(&entries[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := buildSnapshotFromCache(cache, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = snapshot.close()
	return aged
}

func TestParallelScanMatchesSequential(t *testing.T) {
	for _, workers := range []int{2, 3, 8} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(workers)))
			root := t.TempDir()
			metrics := writeScanTree(t, root, rng)
			baseCache := filepath.Join(t.TempDir(), "flc.bin")
			base, _ := newScanTestListener(root, baseCache, 1)
			base.updateFileList(root, nil, nil) // first index and cache
			base.updateFileList(root, nil, nil) // first snapshot
			if _, err := openIndexSnapshot(baseCache, root); err != nil {
				t.Fatal("base generation has no snapshot:", err)
			}
			// Distinct past first-seen times show which value each scan keeps.
			baseEntries := ageScanGeneration(t, baseCache, root)
			mutateScanTree(t, root, metrics, rng)

			type result struct {
				cache  string
				l      *CarbonserverListener
				log    *scanLog
				cached map[string]struct{}
			}
			results := map[int]*result{}
			from := time.Now().Unix()
			for _, n := range []int{1, workers} {
				cache := linkGeneration(t, baseCache, t.TempDir())
				l, log := newScanTestListener(root, cache, n)
				if !l.updateFileList(root, nil, nil) {
					t.Fatal("saved generation not loaded")
				}
				// Names reported by the cache scan, found on disk or not yet written.
				cached := map[string]struct{}{"/" + filepath.ToSlash(metrics[0]): {}, "/ns00/pending/metric.wsp": {}}
				l.newMetricsChan <- "ns00.realtime.only"
				l.newMetricsChan <- strings.TrimSuffix(strings.ReplaceAll(metrics[1], "/", "."), ".wsp")
				// A metric the realtime index saw before its file appeared.
				if _, err := l.CurrentFileIndex().trieIdx.insert("/ns00/new0/n000.wsp", 0, 0, 0, 12345); err != nil {
					t.Fatal(err)
				}
				l.updateFileList(root, cached, nil)
				results[n] = &result{cache, l, log, cached}
			}
			to := time.Now().Unix()
			seq, par := results[1], results[workers]
			if fields := par.log.last(t); fields["scan_workers"] != float64(workers) || fields["scan_ranges"].(float64) < 2 {
				t.Fatalf("parallel scan not used: %v", fields)
			}
			if _, ok := seq.log.last(t)["scan_workers"]; ok {
				t.Fatal("sequential scan used workers")
			}
			for _, field := range []string{"Files", "metrics_known", "index_type"} {
				if g, w := par.log.last(t)[field], seq.log.last(t)[field]; g != w {
					t.Fatalf("%s: parallel %v, sequential %v", field, g, w)
				}
			}
			compareScanEntries(t, "file list cache", readScanCache(t, par.cache), readScanCache(t, seq.cache), baseEntries, from, to)
			compareScanEntries(t, "snapshot", readScanSnapshot(t, par.cache, root), readScanSnapshot(t, seq.cache, root), baseEntries, from, to)
			if g, w := par.l.metrics, seq.l.metrics; g.OOOFiles != w.OOOFiles || g.OOOPhysicalBytes != w.OOOPhysicalBytes || g.LockFiles != w.LockFiles || g.MetricsKnown != w.MetricsKnown {
				t.Fatalf("gauges: parallel %d/%d/%d/%d, sequential %d/%d/%d/%d", g.OOOFiles, g.OOOPhysicalBytes, g.LockFiles, g.MetricsKnown, w.OOOFiles, w.OOOPhysicalBytes, w.LockFiles, w.MetricsKnown)
			}
			if !maps(par.cached, seq.cached) {
				t.Fatalf("cache-scan names: parallel %v, sequential %v", par.cached, seq.cached)
			}
			gotNames := par.l.CurrentFileIndex().trieIdx.allMetrics('.')
			wantNames := seq.l.CurrentFileIndex().trieIdx.allMetrics('.')
			slices.Sort(gotNames)
			slices.Sort(wantNames)
			if !slices.Equal(gotNames, wantNames) {
				t.Fatalf("indexed metrics differ: parallel %d, sequential %d", len(gotNames), len(wantNames))
			}
			for _, name := range []string{"ns00.realtime.only", "ns00.pending.metric"} {
				if par.l.MetricExists(name) != seq.l.MetricExists(name) {
					t.Fatalf("%s: overlay differs", name)
				}
			}
		})
	}
}

func maps(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func parallelScanFixture(t *testing.T) (root, cache string, l *CarbonserverListener, log *scanLog) {
	t.Helper()
	root = t.TempDir()
	writeScanTree(t, root, rand.New(rand.NewSource(7)))
	cache = filepath.Join(t.TempDir(), "flc.bin")
	l, log = newScanTestListener(root, cache, 4)
	l.updateFileList(root, nil, nil)
	l.updateFileList(root, nil, nil)
	if _, err := openIndexSnapshot(cache, root); err != nil {
		t.Fatal(err)
	}
	return root, cache, l, log
}

func generationFiles(t *testing.T, cache string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(cache))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(filepath.Dir(cache), e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[e.Name()] = data
	}
	return files
}

func assertGenerationUnchanged(t *testing.T, cache string, before map[string][]byte) {
	t.Helper()
	after := generationFiles(t, cache)
	if len(after) != len(before) {
		t.Fatalf("generation files changed: %d -> %d", len(before), len(after))
	}
	for name, data := range before {
		if !bytes.Equal(after[name], data) {
			t.Fatalf("%s changed", name)
		}
	}
}

func TestParallelScanCancelKeepsGeneration(t *testing.T) {
	root, cache, l, _ := parallelScanFixture(t)
	before := generationFiles(t, cache)
	close(l.exitChan)
	if l.updateFileList(root, nil, nil) {
		t.Fatal("cancelled scan reported a cache load")
	}
	assertGenerationUnchanged(t, cache, before)
}

func TestParallelScanFailureKeepsGeneration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission checks do not apply to root")
	}
	root, cache, l, log := parallelScanFixture(t)
	before := generationFiles(t, cache)
	snapshot := l.CurrentFileIndex().trieIdx.snapshot
	locked := filepath.Join(root, "ns05")
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(locked, 0o755)
	l.updateFileList(root, nil, nil)
	if log.last(t)["scan_workers"] != float64(4) {
		t.Fatal("parallel scan not used")
	}
	assertGenerationUnchanged(t, cache, before)
	if l.CurrentFileIndex().trieIdx.snapshot != snapshot {
		t.Fatal("incomplete scan replaced the snapshot")
	}
	if !l.MetricExists("ns05.link") {
		t.Fatal("incomplete scan dropped unreadable metrics")
	}
}

// Splitting a running range must cover every entry exactly once. A single
// planned range forces all parallelism to come from splits; the default plan
// with many workers splits ranges while they still descend to a deep start.
func TestParallelScanSplitsRunningRanges(t *testing.T) {
	root, cache, l, log := parallelScanFixture(t)
	want := readScanCache(t, cache)
	single := func(s *indexSnapshot, _ int) []snapshotMetricRange {
		return []snapshotMetricRange{{[]byte{0}, scanEnd, 0, s.index.Len()}}
	}
	defer func(rows int64) { scanMinSplitRows = rows }(scanMinSplitRows)
	scanMinSplitRows = 4
	coarse := func(s *indexSnapshot, _ int) []snapshotMetricRange { return s.scanRangesInto(3, 3) }
	for round := range 6 {
		workers, plan, name := 2+round%3*7, single, "single"
		if round >= 3 {
			plan, name = coarse, "coarse"
		}
		t.Run(fmt.Sprintf("workers=%d/%s", workers, name), func(t *testing.T) {
			l.SetScanWorkers(workers)
			scanPlanOverride = plan
			defer func() { scanPlanOverride = nil }()
			previous := l.CurrentFileIndex().trieIdx.snapshot
			l.updateFileList(root, nil, nil)
			fields := log.last(t)
			if fields["scan_workers"] != float64(workers) || fields["scan_splits"].(float64) == 0 {
				t.Fatalf("running ranges were not split: %v", fields)
			}
			if g, w := fields["Files"], log.updates[0]["Files"]; g != w {
				t.Fatalf("%v files, want %v", g, w)
			}
			if l.CurrentFileIndex().trieIdx.snapshot == previous {
				t.Fatal("no new generation published")
			}
			got := readScanCache(t, cache)
			if len(got) != len(want) {
				t.Fatalf("%d entries, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i].Path != want[i].Path || got[i].LogicalSize != want[i].LogicalSize {
					t.Fatalf("entry %d is %+v, want %+v", i, got[i], want[i])
				}
			}
		})
	}
}
