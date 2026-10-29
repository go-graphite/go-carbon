package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func makeDir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
}
func putFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestWalkDryRunAndPrune(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	makeDir(t, filepath.Join(root, "empty", "leaf"))
	makeDir(t, filepath.Join(root, "occupied"))
	makeDir(t, filepath.Join(root, ".ns-quota-trim", "empty"))
	makeDir(t, filepath.Join(root, "store", "empty"))
	for _, suffix := range []string{".wsp", ".wsp.ooo", ".wsp.lock", ".wsp.ooo.lock", ".log"} {
		putFile(t, filepath.Join(root, "occupied", "keep"+suffix))
	}
	makeDir(t, filepath.Join(outside, "empty"))
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	w := walker{dry: true, exclude: map[string]bool{"store": true}}
	if err := w.walk(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.counts.Dirs != 4 || w.counts.EmptyDirs != 1 || w.counts.RemovableDirs != 2 || w.counts.Files != 5 || w.counts.Symlinks != 1 || w.counts.Excluded != 2 || w.counts.Removed != 0 {
		t.Fatalf("counts=%+v", w.counts)
	}
	if _, err := os.Stat(filepath.Join(root, "empty", "leaf")); err != nil {
		t.Fatal("dry run modified tree", err)
	}
	w = walker{exclude: map[string]bool{"store": true}}
	if err := w.walk(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.counts.Removed != 2 {
		t.Fatalf("counts=%+v", w.counts)
	}
	for _, path := range []string{root, filepath.Join(root, ".ns-quota-trim", "empty"), filepath.Join(root, "store", "empty"), filepath.Join(outside, "empty"), filepath.Join(root, "occupied", "keep.wsp")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("changed protected path", path, err)
		}
	}
	for _, suffix := range []string{".wsp", ".wsp.ooo", ".wsp.lock", ".wsp.ooo.lock", ".log"} {
		if w.counts.Suffixes[suffix] != 2 {
			t.Fatalf("suffixes=%v", w.counts.Suffixes)
		}
	}
}

func TestWalkConcurrentCreateAndResume(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "leaf")
	makeDir(t, dir)
	w := walker{}
	calls := 0
	w.before = func() error {
		calls++
		if calls == 2 {
			putFile(t, filepath.Join(dir, "new.wsp"))
		}
		return nil
	}
	if err := w.walk(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.counts.Removed != 0 {
		t.Fatalf("counts=%+v", w.counts)
	}
	if _, err := os.Stat(filepath.Join(dir, "new.wsp")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		makeDir(t, filepath.Join(root, fmt.Sprint(i), "leaf"))
	}
	w = walker{}
	w.before = func() error {
		if w.counts.Removed == 2 {
			return context.Canceled
		}
		return nil
	}
	if err := w.walk(context.Background(), root); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	w = walker{}
	if err := w.walk(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.counts.Removed != 6 {
		t.Fatalf("resume counts=%+v", w.counts)
	}
}

func TestLocalGateStopAndDryRun(t *testing.T) {
	for _, body := range []string{
		`{}`, `{"storage":{"migration":{"phase":"files","in_flight":0}}}`,
		`{"storage":{"migration":{"phase":"shared"}}}`,
		`{"storage":{"migration":{"phase":"shared","in_flight":1}}}`,
		`{"storage":{"migration":{"phase":"shared","in_flight":0,"skipped":1}}}`,
		`{"storage":{"migration":{"phase":"shared","in_flight":0}}}`,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
			defer server.Close()
			root := t.TempDir()
			makeDir(t, filepath.Join(root, "empty"))
			o := options{root: root, admin: server.URL, metrics: server.URL, dirsPerSecond: 1e9, interval: time.Second, dry: true}
			var out bytes.Buffer
			err := run(context.Background(), o, &out)
			valid := strings.Contains(body, `"in_flight":0`) && strings.Contains(body, `"phase":"shared"`) && !strings.Contains(body, `"skipped":1`)
			if (err == nil) != valid {
				t.Fatalf("error=%v valid=%t", err, valid)
			}
			if _, err := os.Stat(filepath.Join(root, "empty")); err != nil {
				t.Fatal("modified dry-run/refused tree", err)
			}
			if valid {
				o.dry = false
				o.stopFile = filepath.Join(t.TempDir(), "stop")
				putFile(t, o.stopFile)
				if err := run(context.Background(), o, &out); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(root, "empty")); err != nil {
					t.Fatal("ignored stop", err)
				}
				if err := os.Remove(o.stopFile); err != nil {
					t.Fatal(err)
				}
				if err := run(context.Background(), o, &out); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(root, "empty")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("did not resume", err)
				}
			}
		})
	}
	for _, url := range []string{"https://127.0.0.1/", "http://example.com/", "http://127.0.0.1@example.com/"} {
		if err := localURL(url); err == nil {
			t.Fatal("accepted nonlocal URL", url)
		}
	}
}

func TestDiskStatsAndAutoBudget(t *testing.T) {
	data := " 8 0 sda 10 0 0 20 30 0 0 60 0 100 0 2 0 0 8 3 9\n"
	stats, err := parseDiskStats(data, []string{"sda"})
	if err != nil {
		t.Fatal(err)
	}
	if stats["sda"] != (diskStat{45, 97, 100}) {
		t.Fatalf("stats=%+v", stats)
	}
	if _, err := parseDiskStats(data, []string{"missing"}); err == nil {
		t.Fatal("missing device accepted")
	}
	counters, err := writeCounters(strings.NewReader("persister_pebble_updates_total{host=\"a\"} 10\npersister_pebble_write_seconds_total 0.1\npersister_pebble_write_failures_total 0\n"))
	if err != nil || counters != (writes{10, .1, 0}) {
		t.Fatalf("counters=%+v %v", counters, err)
	}
	for _, bad := range []string{"", "persister_pebble_updates_total NaN\n"} {
		if _, err := writeCounters(strings.NewReader(bad)); err == nil {
			t.Fatal("accepted missing/invalid counters")
		}
	}
	c := controller{options: options{dirsPerSecond: 200, maxRate: 250}, rate: 200}
	c.adjust(false, true)
	c.adjust(false, true)
	if c.rate != 200 {
		t.Fatal("raised before sustained health")
	}
	c.adjust(false, true)
	if c.rate != 250 {
		t.Fatal("did not raise", c.rate)
	}
	c.adjust(true, true)
	if c.rate != 125 {
		t.Fatal("did not back off", c.rate)
	}
	c.rate = 250
	c.adjust(false, false)
	if c.rate != 200 {
		t.Fatal("idle writes authorized increase", c.rate)
	}
}

func TestGateChangesAndEndpointFailuresStopBeforeMoreRemoval(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		phase := "shared"
		if requests > 1 {
			phase = "files"
		}
		fmt.Fprintf(w, `{"storage":{"migration":{"phase":%q,"in_flight":0}}}`, phase)
	}))
	defer server.Close()
	root := t.TempDir()
	makeDir(t, filepath.Join(root, "a", "b"))
	var out bytes.Buffer
	err := run(context.Background(), options{root: root, admin: server.URL, metrics: server.URL, dirsPerSecond: 10, interval: 100 * time.Millisecond}, &out)
	if err == nil {
		t.Fatal("phase change ignored")
	}
	if _, err := os.Stat(filepath.Join(root, "a", "b")); err != nil {
		t.Fatal("deleted after phase change", err)
	}
	for _, mode := range []string{"redirect", "unavailable", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "redirect":
					http.Redirect(w, r, "http://example.com", http.StatusFound)
				case "unavailable":
					http.Error(w, "unavailable", 503)
				case "malformed":
					fmt.Fprint(w, "not-json")
				}
			}))
			defer server.Close()
			err := run(context.Background(), options{root: root, admin: server.URL, metrics: server.URL, dirsPerSecond: 1e9, interval: time.Second}, &out)
			if err == nil {
				t.Fatal("invalid endpoint allowed removal")
			}
		})
	}
}

func TestWalkPrunesAcrossDirectoryReadBatches(t *testing.T) {
	root := t.TempDir()
	// A small getdents buffer exercises mutation between batches with only a
	// few directories, instead of building a large filesystem benchmark here.
	const dirs = 64
	for i := 0; i < dirs; i++ {
		makeDir(t, filepath.Join(root, fmt.Sprintf("d%03d", i)))
	}
	w := walker{buffers: [][]byte{make([]byte, 256)}}
	if err := w.walk(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.counts.Removed != dirs {
		t.Fatalf("skipped directories during self-mutation: %+v", w.counts)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("root not retained empty: %d %v", len(entries), err)
	}
}
