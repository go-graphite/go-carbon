package carbonserver

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func benchmarkFileListCachePath(b *testing.B) string {
	b.Helper()
	path := os.Getenv("GO_CARBON_FLC_BENCHMARK")
	if path == "" {
		path = filepath.Join(b.TempDir(), "files.gz")
		writer, err := NewFileListCache(path, FLCVersion2, 'w')
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < 100000; i++ {
			entry := FLCEntry{Path: fmt.Sprintf("/stats/service%03d/host%06d/request/duration/bucket%03d.wsp", i/1000, i/10, i%10), LogicalSize: 4096, PhysicalSize: 4096, DataPoints: 360, FirstSeenAt: 1700000000}
			if err := writer.Write(&entry); err != nil {
				b.Fatal(err)
			}
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}
	return path
}

// BenchmarkFileListCacheWarmup measures saved-index decoding and trie construction.
// GO_CARBON_FLC_BENCHMARK optionally selects a read-only captured cache. Only
// the first million records are loaded, bounding an offline production probe.
func BenchmarkFileListCacheWarmup(b *testing.B) {
	path := benchmarkFileListCachePath(b)
	b.Run("incremental", func(b *testing.B) { benchmarkFileListCacheWarmup(b, path, false) })
	b.Run("bulk", func(b *testing.B) { benchmarkFileListCacheWarmup(b, path, true) })
}

func benchmarkFileListCacheWarmup(b *testing.B, path string, bulk bool) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	var records int
	for n := 0; n < b.N; n++ {
		reader, err := NewFileListCache(path, FLCVersionUnspecified, 'r')
		if err != nil {
			b.Fatal(err)
		}
		trie := newTrie(".wsp", 0, nil)
		if bulk {
			trie.builder = &trieBulkBuilder{}
		}
		var entry FLCEntry
		records = 0
		for ; records < 1000000; records++ {
			if into, ok := reader.(interface{ readInto(*FLCEntry) error }); ok {
				err = into.readInto(&entry)
			} else {
				var next *FLCEntry
				next, err = reader.Read()
				if err == nil {
					entry = *next
				}
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			if _, err := trie.insert(entry.Path, entry.LogicalSize, entry.PhysicalSize, entry.DataPoints, entry.FirstSeenAt); err != nil {
				b.Fatal(err)
			}
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
		if trie.fileCount == 0 {
			b.Fatal("no metrics loaded")
		}
	}
	b.ReportMetric(float64(records), "records/op")
}
