package chunkstore

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
)

// ReadOnly prevents recovery flushes, compaction and manifest/WAL changes.
// This diagnosis does not clear any conversion or reconciliation marker.
func TestNativeReadOnlyProductionShape(t *testing.T) {
	dir, output := os.Getenv("GRAPHITE_READONLY_SHAPE_DB"), os.Getenv("GRAPHITE_READONLY_SHAPE_OUTPUT")
	if dir == "" || output == "" {
		t.Skip("requires a closed retained conversion and private output")
	}
	if !strings.HasSuffix(filepath.Clean(dir), "/full-baseline/exact") || !strings.Contains(filepath.Clean(output), "/production-replay-lab-v1/") {
		t.Fatal("requires closed retained conversion and private output")
	}
	cache := pebble.NewCache(256 << 20)
	defer cache.Unref()
	db, err := pebble.Open(dir, &pebble.Options{ReadOnly: true, Merger: exactChunkMerger, Cache: cache})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m := db.Metrics()
	tables, err := db.SSTables()
	if err != nil {
		t.Fatal(err)
	}
	type boundary struct {
		key   []byte
		delta int
	}
	var bounds []boundary
	for _, table := range tables[0] {
		bounds = append(bounds, boundary{table.Smallest.UserKey, 1}, boundary{table.Largest.UserKey, -1})
	}
	sort.Slice(bounds, func(i, j int) bool {
		if cmp := bytes.Compare(bounds[i].key, bounds[j].key); cmp != 0 {
			return cmp < 0
		}
		return bounds[i].delta > bounds[j].delta
	})
	overlap, maximum := 0, 0
	for _, b := range bounds {
		overlap += b.delta
		maximum = max(maximum, overlap)
	}
	data, err := json.MarshalIndent(map[string]any{"read_only": true, "l0_max_file_range_overlap_upper_bound": maximum, "levels": m.Levels, "memtable": m.MemTable, "wal": m.WAL, "compaction": m.Compact, "summary": m.String(), "scope": "ReadOnly omits Pebble sublevel initialization. File-range overlap is a separate conservative bound, not the stop-write sublevel count."}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(output, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
