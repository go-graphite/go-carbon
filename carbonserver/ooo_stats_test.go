package carbonserver

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/go-graphite/go-whisper"
)

func TestFileScanOutOfOrderGauges(t *testing.T) {
	for _, trie := range []bool{false, true} {
		t.Run(fmt.Sprintf("trie=%t", trie), func(t *testing.T) {
			dir := t.TempDir()
			paths := []string{
				filepath.Join(dir, "metric.wsp"),
				filepath.Join(dir, strings.Repeat("m", 250)+".wsp"),
			}
			for _, path := range paths {
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			sidecars := []string{
				whisper.OutOfOrderSidecarPath(paths[0]),
				whisper.OutOfOrderSidecarPath(paths[1]),
				filepath.Join(dir, "orphan.wsp.ooo"),
			}
			if !strings.HasPrefix(filepath.Base(sidecars[1]), ".") {
				t.Fatal("expected a hashed sidecar filename")
			}
			if err := os.Mkdir(filepath.Join(dir, "directory.ooo"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(sidecars[0], filepath.Join(dir, "symlink.ooo")); err != nil {
				t.Fatal(err)
			}
			listener := NewCarbonserverListener(nil)
			listener.timeBuckets = make([]uint64, listener.buckets+1)
			listener.SetWhisperData(dir)
			listener.SetTrieIndex(trie)
			scan := func(wantSidecars []string) {
				t.Helper()
				listener.updateFileList(dir, nil, nil)
				var wantBytes uint64
				for _, path := range wantSidecars {
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					stat, ok := info.Sys().(*syscall.Stat_t)
					if !ok {
						t.Fatal("filesystem does not report allocated blocks")
					}
					wantBytes += uint64(stat.Blocks) * 512
				}
				assertOutOfOrderGauges(t, listener, uint64(len(wantSidecars)), wantBytes)
				if listener.metrics.MetricsKnown != uint64(len(paths)) {
					t.Fatalf("metrics known = %d, want %d", listener.metrics.MetricsKnown, len(paths))
				}
			}
			scan(nil)
			for _, path := range sidecars {
				if err := os.WriteFile(path, []byte("late points"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(path, 1<<20); err != nil {
					t.Fatal(err)
				}
			}
			scan(sidecars)
			for i, path := range sidecars {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				scan(sidecars[i+1:])
			}
		})
	}
}

func TestOutOfOrderGaugesPreserveLastScan(t *testing.T) {
	for _, name := range []string{"file-list cache", "cancelled scan", "scan error"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			listener := NewCarbonserverListener(nil)
			listener.timeBuckets = make([]uint64, listener.buckets+1)
			listener.SetWhisperData(dir)
			listener.SetTrieIndex(true)
			listener.metrics.OOOFiles = 3
			listener.metrics.OOOPhysicalBytes = 8192
			switch name {
			case "file-list cache":
				flcPath := filepath.Join(dir, "file-list-cache")
				flc, err := NewFileListCache(flcPath, FLCVersion2, 'w')
				if err != nil {
					t.Fatal(err)
				}
				if err := flc.Write(&FLCEntry{Path: "/metric.wsp"}); err != nil {
					t.Fatal(err)
				}
				if err := flc.Close(); err != nil {
					t.Fatal(err)
				}
				listener.SetFileListCache(flcPath)
				if !listener.updateFileList(dir, nil, nil) {
					t.Fatal("expected a file-list cache load")
				}
			case "cancelled scan":
				close(listener.exitChan)
				listener.updateFileList(dir, nil, nil)
			case "scan error":
				u := newFileListUpdate(listener, nil)
				path := filepath.Join(dir, "unreadable")
				walkErr := &os.PathError{Op: "lstat", Path: path, Err: os.ErrPermission}
				if err := u.walkFile(path, nil, walkErr, nil); err != nil {
					t.Fatal(err)
				}
				u.publish(dir, nil)
			}
			assertOutOfOrderGauges(t, listener, 3, 8192)
		})
	}
}

func assertOutOfOrderGauges(t *testing.T, listener *CarbonserverListener, wantFiles, wantBytes uint64) {
	t.Helper()
	for _, counters := range []bool{false, true} {
		listener.SetMetricsAsCounters(counters)
		for range 2 {
			stats := make(map[string]float64)
			listener.Stat(func(metric string, value float64) { stats[metric] = value })
			for metric, want := range map[string]uint64{"oooFiles": wantFiles, "oooPhysicalBytes": wantBytes} {
				if got, ok := stats[metric]; !ok || got != float64(want) {
					t.Errorf("%s (counters=%t) = %v, present=%t; want %d", metric, counters, got, ok, want)
				}
			}
		}
	}
}
