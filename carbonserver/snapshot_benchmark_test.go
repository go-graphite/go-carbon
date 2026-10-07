package carbonserver

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// This opt-in probe reads a captured FLC and writes only into a separate scratch
// directory. It never scans or writes the live Whisper directory.
func TestCapturedIndexSnapshot(t *testing.T) {
	source, out := os.Getenv("GO_CARBON_SNAPSHOT_FLC"), os.Getenv("GO_CARBON_SNAPSHOT_DIR")
	if source == "" || out == "" {
		t.Skip("set captured FLC and scratch directory")
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(out, "files.gzip")
	if err := os.Symlink(source, cache); err != nil && !errors.Is(err, os.ErrExist) {
		t.Fatal(err)
	}
	root := filepath.Join(out, "data-root")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	var quotas []*Quota
	if path := os.Getenv("GO_CARBON_SNAPSHOT_QUOTAS"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(data, &quotas); err != nil {
			t.Fatal(err)
		}
	} else {
		quotas = []*Quota{{Pattern: "/", Metrics: 1 << 60}, {Pattern: "*", Metrics: 1 << 60}, {Pattern: "*.*", Metrics: 1 << 60}}
	}
	depth := 0
	for _, q := range quotas {
		if n := strings.Count(q.Pattern, ".") + 1; n > depth {
			depth = n
		}
	}
	expected := make(map[string]QuotaUsage)
	var samples []FLCEntry
	buildStart := time.Now()
	writer, err := newIndexSnapshotWriter(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewFileListCache(source, FLCVersionUnspecified, 'r')
	if err != nil {
		t.Fatal(err)
	}
	var entry FLCEntry
	var previous []string
	var records uint64
	for {
		err = reader.(interface{ readInto(*FLCEntry) error }).readInto(&entry)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = writer.append(&entry); err != nil {
			t.Fatal(err)
		}
		if records%65536 == 0 {
			samples = append(samples, entry)
		}
		parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(entry.Path, "/"), ".wsp"), "/")
		same := 0
		for same < len(previous)-1 && same < len(parts)-1 && previous[same] == parts[same] {
			same++
		}
		prefix := "/"
		for d := 0; d < len(parts) && d <= depth; d++ {
			u := expected[prefix]
			u.Metrics++
			u.LogicalSize += entry.LogicalSize
			u.PhysicalSize += entry.PhysicalSize
			u.DataPoints += entry.DataPoints
			if d < len(parts)-1 && d >= same {
				u.Namespaces++
			}
			expected[prefix] = u
			if d == 0 {
				prefix = parts[0]
			} else {
				prefix += "." + parts[d]
			}
		}
		previous = parts
		records++
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err = writer.finish(); err != nil {
		t.Fatal(err)
	}
	t.Logf("build records=%d seconds=%.6f", records, time.Since(buildStart).Seconds())
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	s, err := openIndexSnapshot(cache, root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	openTime := time.Since(start)
	index := newTrie(".wsp", 0, nil)
	index.snapshot = s
	start = time.Now()
	tp, err := index.applyQuotas(time.Minute, quotas...)
	if err != nil {
		t.Fatal(err)
	}
	applyTime := time.Since(start)
	start = time.Now()
	files := index.refreshUsage(tp)
	usageTime := time.Since(start)
	runtime.ReadMemStats(&after)
	if files != records {
		t.Fatalf("files %d != %d", files, records)
	}
	for name, node := range index.quotaNodes {
		if want := expected[name]; *node.usage != want {
			t.Fatalf("quota %q got %+v want %+v", name, *node.usage, want)
		}
	}
	for _, want := range samples {
		got, ok, err := s.lookup(want.Path)
		if err != nil || !ok || *got != want {
			t.Fatalf("sample mismatch: %v/%v/%v", got, ok, err)
		}
		name := strings.TrimSuffix(want.Path, ".wsp")
		names, leaves, _, _, err := index.query(name, 10, nil)
		found := false
		for i, n := range names {
			if leaves[i] && n == strings.ReplaceAll(name[1:], "/", ".") {
				found = true
			}
		}
		if err != nil || !found {
			t.Fatalf("query sample missing: %q/%v", name, err)
		}
	}
	t.Logf("ready records=%d quotas=%d sampled=%d open_seconds=%.6f apply_seconds=%.6f usage_seconds=%.6f total_seconds=%.6f allocated_bytes=%d heap_bytes=%d", records, len(index.quotaNodes), len(samples), openTime.Seconds(), applyTime.Seconds(), usageTime.Seconds(), (openTime + applyTime + usageTime).Seconds(), after.TotalAlloc-before.TotalAlloc, after.HeapAlloc)
}

// Separately runnable so the caller can evict only these disposable checkpoint
// files before measuring the application's full initial-index publication path.
func TestCapturedIndexSnapshotOpen(t *testing.T) {
	out := os.Getenv("GO_CARBON_SNAPSHOT_DIR")
	if out == "" {
		t.Skip("set captured snapshot scratch directory")
	}
	var quotas []*Quota
	if path := os.Getenv("GO_CARBON_SNAPSHOT_QUOTAS"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(data, &quotas); err != nil {
			t.Fatal(err)
		}
	}
	l := NewCarbonserverListener(nil)
	l.SetWhisperData(filepath.Join(out, "data-root"))
	l.SetTrieIndex(true)
	l.SetConcurrentIndex(true)
	l.SetFileListCache(filepath.Join(out, "files.gzip"))
	l.SetFileListCacheVersion(int(FLCVersion2))
	l.SetScanFrequency(time.Hour)
	l.SetRealtimeIndex(1024)
	l.SetQuotaUsageReportFrequency(time.Minute)
	l.SetQuotas(quotas)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	l.WarmupIndex()
	<-l.indexWarmupDone
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	index := l.CurrentFileIndex()
	if index == nil || index.trieIdx.snapshot == nil {
		t.Fatal("complete snapshot not published")
	}
	if os.Getenv("GO_CARBON_REQUIRE_OVERLAY") != "" {
		m, err := readOverlayManifest(overlayManifestPath(l.fileListCache))
		if err != nil || uint64(index.trieIdx.fileCount) != m.Records {
			t.Fatal("captured overlay not fully loaded", index.trieIdx.fileCount, m.Records, err)
		}
	}
	t.Logf("index_publication records=%d overlay_records=%d quotas=%d seconds=%.6f allocated_bytes=%d heap_bytes=%d", index.trieIdx.snapshot.manifest.Records, index.trieIdx.fileCount, len(index.trieIdx.quotaNodes), elapsed.Seconds(), after.TotalAlloc-before.TotalAlloc, after.HeapAlloc)
}

// Benchmark captured names rather than short synthetic keys. Both paths must
// agree before comparing membership cost at a real catalogue size.
func BenchmarkCapturedSnapshotMembership(b *testing.B) {
	dir := os.Getenv("GO_CARBON_SNAPSHOT_DIR")
	if dir == "" {
		b.Skip("set prepared snapshot directory")
	}
	cache, root := filepath.Join(dir, "files.gzip"), filepath.Join(dir, "data-root")
	s, err := openIndexSnapshot(cache, root)
	if err != nil {
		b.Fatal(err)
	}
	defer s.close()
	listener := NewCarbonserverListener(nil)
	ti := newTrie(".wsp", 0, nil)
	ti.snapshot = s
	listener.UpdateFileIndex(&fileIndex{trieIdx: ti})
	lookup := listener.SavedMetricLookup()
	reader, err := NewFileListCache(cache, FLCVersionUnspecified, 'r')
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()
	var names []string
	var namespaces []string
	seen := make(map[string]bool)
	for n := 0; n < 1000000; n++ {
		e, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			b.Fatal(err)
		}
		if n%127 != 0 {
			continue
		}
		name := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(e.Path, "/"), ".wsp"), "/", ".")
		if !lookup(name) {
			b.Fatalf("missing %q", name)
		}
		names = append(names, name)
		metricNamespaces(name, func(prefix string) {
			if !seen[prefix] {
				seen[prefix] = true
				namespaces = append(namespaces, prefix)
			}
		})
	}
	if len(names) == 0 {
		b.Fatal("empty fixture")
	}
	for _, prefix := range namespaces {
		start, end, err := s.namespaceRange(prefix)
		if err != nil || s.namespaceExists(prefix) != (start != end) {
			b.Fatalf("namespace differs %q: %v", prefix, err)
		}
	}
	b.Run("metric-before", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			name := names[i%len(names)]
			_, found, err := s.lookup("/" + strings.ReplaceAll(name, ".", "/") + ".wsp")
			if err != nil || !found {
				b.Fatal(name, err)
			}
		}
	})
	b.Run("metric-reader", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if !lookup(names[i%len(names)]) {
				b.Fatal("missing")
			}
		}
	})
	b.Run("namespace-range", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			start, end, err := s.namespaceRange(namespaces[i%len(namespaces)])
			if err != nil || start == end {
				b.Fatal("missing", err)
			}
		}
	})
	b.Run("namespace-prefix", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if !s.namespaceExists(namespaces[i%len(namespaces)]) {
				b.Fatal("missing")
			}
		}
	})
	// Repeated new names hit quota admission on every incoming point until
	// persistence creates them. Exercise the whole ancestor walk, including
	// long real prefixes, rather than only known-metric membership.
	newNames := make([]string, len(names))
	for i, name := range names {
		newNames[i] = name + ".__snapshot_benchmark_new__"
		want, wantNew := snapshotMetricPathWithRanges(ti, newNames[i], make([]*trieNode, 0, 32))
		got, gotNew := ti.metricPath(newNames[i], make([]*trieNode, 0, 32))
		if gotNew != wantNew || !gotNew || len(got) != len(want) {
			b.Fatalf("ancestor mismatch: %q", newNames[i])
		}
		for j := range want {
			if got[j] != want[j] {
				b.Fatalf("quota ancestor mismatch: %q at %d", newNames[i], j)
			}
		}
	}
	for _, variant := range []struct {
		name string
		path func(*trieIndex, string, []*trieNode) ([]*trieNode, bool)
	}{{"admission-ranges", snapshotMetricPathWithRanges}, {"admission-prefix", (*trieIndex).metricPath}} {
		b.Run(variant.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, isNew := variant.path(ti, newNames[i%len(newNames)], make([]*trieNode, 0, 32)); !isNew {
					b.Fatal("unexpected existing metric")
				}
			}
		})
	}
}

// Original mapped admission path retained as an independent benchmark oracle.
func snapshotMetricPathWithRanges(ti *trieIndex, metric string, dirs []*trieNode) ([]*trieNode, bool) {
	if _, found, err := ti.snapshot.lookup("/" + strings.ReplaceAll(metric, ".", "/") + ti.fileExt); err == nil && found {
		return dirs, false
	}
	if _, isNew := ti.metricPathMutable(metric, nil); !isNew {
		return dirs, false
	}
	dirs = append(dirs, ti.root)
	for end := 0; end < len(metric); end++ {
		if metric[end] != '.' {
			continue
		}
		name := metric[:end]
		start, stop, err := ti.snapshot.namespaceRange(name)
		exists := err == nil && start < stop
		if !exists {
			exists = ti.mutableDirectory(name) != nil
		}
		if !exists {
			break
		}
		dirs = append(dirs, ti.snapshot.directoryNode(name))
	}
	return dirs, true
}
