package carbonserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

func savedIndex(t *testing.T, names ...string) *CarbonserverListener {
	t.Helper()
	dir := t.TempDir()
	l := NewCarbonserverListener(nil)
	l.SetWhisperData(dir)
	l.SetTrieIndex(true)
	l.SetConcurrentIndex(true)
	l.SetScanFrequency(time.Hour)
	l.SetMaxGlobs(100)
	l.SetMaxMetricsGlobbed(100)
	l.SetRealtimeIndex(10)
	l.SetFileListCache(filepath.Join(t.TempDir(), "flc"))
	l.SetFileListCacheVersion(int(FLCVersion2))
	f, err := NewFileListCache(l.fileListCache, FLCVersion2, 'w')
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		p := "/" + strings.ReplaceAll(name, ".", "/") + ".wsp"
		if err := f.Write(&FLCEntry{Path: p, LogicalSize: 1024, PhysicalSize: 1024, DataPoints: 60}); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Stop() })
	return l
}

func waitWarmup(t *testing.T, l *CarbonserverListener) {
	t.Helper()
	select {
	case <-l.indexWarmupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("index warmup did not finish")
	}
}

func TestWarmupBeforeListenAndScanAfterRestore(t *testing.T) {
	defer zapwriter.Test()()
	defer func() {
		if t.Failed() {
			t.Log(zapwriter.TestString())
		}
	}()
	l := savedIndex(t, "namespace.existing")
	l.SetEstimateSize(func(string) (int64, int64, int64) { return 1024, 1024, 60 })
	l.SetQuotaUsageReportFrequency(time.Minute)
	l.SetQuotas([]*Quota{{Pattern: "/", Metrics: 1}})
	l.WarmupIndex()
	l.WarmupIndex()
	waitWarmup(t, l)
	if !l.MetricExists("namespace.existing") || l.tcpListener != nil {
		t.Fatal("warmup must publish the saved index without opening HTTP")
	}
	if !l.ShouldThrottleMetric(points.OnePoint("namespace.new", 1, 1), false) {
		t.Fatal("warmed index must enforce quotas")
	}
	// This file represents a metric created by restore after the saved index loaded.
	if err := os.WriteFile(filepath.Join(l.whisperData, "namespace", "restored.wsp"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if l.MetricExists("namespace.restored") {
		t.Fatal("warmup unexpectedly scanned disk")
	}
	if err := l.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	url := "http://" + l.tcpListener.Addr().String() + "/metrics/find/?query=namespace.*&format=json"
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("warmed reads unavailable: %d %s", res.StatusCode, body)
		}
		if strings.Contains(string(body), "namespace.restored") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("post-restore scan did not find restored file: %s", body)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWarmupInvalidCacheDefersDiskScan(t *testing.T) {
	for _, kind := range []string{"missing", "corrupt", "partial-record"} {
		t.Run(kind, func(t *testing.T) {
			l := savedIndex(t, "on.disk")
			switch kind {
			case "missing":
				if err := os.Remove(l.fileListCache); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				if err := os.WriteFile(l.fileListCache, []byte("corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			case "partial-record":
				f, err := NewFileListCache(l.fileListCache, FLCVersion2, 'w')
				if err != nil {
					t.Fatal(err)
				}
				if err := f.Write(&FLCEntry{Path: "/on/disk.wsp"}); err != nil {
					t.Fatal(err)
				}
				// Valid gzip and a complete first entry, followed by a record
				// header without its payload. This is not a complete index.
				var header [8]byte
				binary.BigEndian.PutUint64(header[:], 5)
				if _, err := f.(*fileListCacheV2).writer.Write(header[:]); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}
			l.WarmupIndex()
			waitWarmup(t, l)
			if l.CurrentFileIndex() != nil {
				t.Fatal("invalid cache must leave disk scanning until restore finishes")
			}
			if err := l.Listen("127.0.0.1:0"); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for !l.MetricExists("on.disk") {
				if time.Now().After(deadline) {
					t.Fatal("fallback scan stalled")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestStopDuringWarmup(t *testing.T) {
	l := savedIndex(t, "existing.metric")
	// Stop must work even before sockets exist, and be safe to repeat.
	l.WarmupIndex()
	done := make(chan struct{})
	go func() { l.Stop(); l.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop leaked the warmup worker")
	}
	waitWarmup(t, l)
}

func TestMetricExistsMatchesExactIndexedNames(t *testing.T) {
	l := NewCarbonserverListener(nil)
	l.SetTrieIndex(true)
	l.SetConcurrentIndex(true)
	if l.MetricExists("anything") {
		t.Fatal("unpublished index claimed a metric")
	}
	trie := newTrie(".wsp", 0, nil)
	names := map[string]bool{"a": true, "a.b": true, "a.b.c": true, "alpha.beta": true, "alpha.betamax": true, "utf8.日本語": true}
	for i := 0; i < 1000; i++ {
		names[fmt.Sprintf("namespace.host%d.value", i)] = true
	}
	for name := range names {
		if _, err := trie.insert("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	l.UpdateFileIndex(&fileIndex{trieIdx: trie})
	for name := range names {
		for _, candidate := range []string{name, name + ".missing", name + "x", strings.TrimSuffix(name, "value"), ""} {
			if got := l.MetricExists(candidate); got != names[candidate] {
				t.Fatalf("%q: got %v, want %v", candidate, got, names[candidate])
			}
		}
	}
	if n := testing.AllocsPerRun(100, func() { l.MetricExists("namespace.host123.value") }); n != 0 {
		t.Fatalf("existence check allocates: %v", n)
	}
	l.SetConcurrentIndex(false)
	if l.MetricExists("a") {
		t.Fatal("non-concurrent trie cannot be consulted during ingestion")
	}
}

func TestMetricExistsConcurrentInsert(t *testing.T) {
	l := NewCarbonserverListener(nil)
	l.SetTrieIndex(true)
	l.SetConcurrentIndex(true)
	trie := newTrie(".wsp", 0, nil)
	l.UpdateFileIndex(&fileIndex{trieIdx: trie})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				l.MetricExists(fmt.Sprintf("prefix.host%d.value", i%1000))
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		l.insertRealtimeMetric(trie, fmt.Sprintf("prefix.host%d.value", i))
	}
	wg.Wait()
	for i := 0; i < 1000; i++ {
		if !l.MetricExists(fmt.Sprintf("prefix.host%d.value", i)) {
			t.Fatalf("insert %d lost", i)
		}
	}
}

func TestAbortedCachePreservesPreviousSnapshot(t *testing.T) {
	l := savedIndex(t, "previous.metric")
	before, err := os.ReadFile(l.fileListCache)
	if err != nil {
		t.Fatal(err)
	}
	l.WarmupIndex()
	waitWarmup(t, l)
	l.Stop()
	l.updateFileListWithCache(l.whisperData, nil, nil, false)
	after, err := os.ReadFile(l.fileListCache)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("cancellation replaced the saved index")
	}
	f, err := NewFileListCache(l.fileListCache, FLCVersion2, 'w')
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Write(&FLCEntry{Path: "/partial.wsp"}); err != nil {
		t.Fatal(err)
	}
	if err := f.Abort(); err != nil {
		t.Fatal(err)
	}
	after, err = os.ReadFile(l.fileListCache)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatal("aborted writer replaced previous snapshot", err)
	}
	if _, err := os.Stat(l.fileListCache + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("aborted cache left a temporary file", err)
	}
}
