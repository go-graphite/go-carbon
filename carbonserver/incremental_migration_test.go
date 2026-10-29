package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/persister"
	whisper "github.com/go-graphite/go-whisper"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

type incrementalWireCase struct {
	metric, path, format string
	from, until          int64
}

func incrementalWireReply(t *testing.T, l *CarbonserverListener, c incrementalWireCase) (int, []byte) {
	t.Helper()
	query := url.Values{"format": {c.format}}
	if c.path == "/metrics/find/" {
		query.Set("query", c.metric)
	} else {
		query.Set("target", c.metric)
	}
	if c.path == "/render/" {
		query.Set("from", strconv.FormatInt(c.from, 10))
		query.Set("until", strconv.FormatInt(c.until, 10))
	}
	var data []byte
	if c.format == "carbonapi_v3_pb" {
		var err error
		if c.path == "/render/" {
			request := protov3.MultiFetchRequest{Metrics: []*protov3.FetchRequest{{Name: c.metric, PathExpression: c.metric, StartTime: c.from, StopTime: c.until}}}
			data, err = request.MarshalVT()
		} else {
			request := protov3.MultiGlobRequest{Metrics: []string{c.metric}}
			data, err = request.MarshalVT()
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(http.MethodPost, c.path+"?"+query.Encode(), bytes.NewReader(data))
	if c.format == "carbonapi_v3_pb" {
		req.Header.Set("Accept", "application/x-carbonapi-v3-pb")
		req.Header.Set("Content-Type", "application/x-carbonapi-v3-pb")
	}
	response := httptest.NewRecorder()
	switch c.path {
	case "/render/":
		l.renderHandler(response, req)
	case "/info/":
		l.infoHandler(response, req)
	case "/metrics/find/":
		l.findHandler(response, req)
	}
	return response.Code, append([]byte(nil), response.Body.Bytes()...)
}

func TestIncrementalMixedAPIAndMappedIndexSurviveReclamation(t *testing.T) {
	t.Parallel()
	cachePath, root, entries := snapshotFixture(t)
	now := time.Now().Unix()
	var names []string
	for _, entry := range entries {
		path := filepath.Join(root, entry.Path)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		w, err := whisper.CreateWithOptions(path, whisper.NewRetentionsNoPointer([]whisper.Retention{whisper.NewRetention(1, 3600)}), whisper.Average, .5, &whisper.Options{Compressed: true, OutOfOrder: true, FLock: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(now - 300), Value: math.Copysign(0, -1)}, {Time: int(now - 290), Value: .1}, {Time: int(now - 280), Value: 1e100}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		names = append(names, metricFromVirtualPath(entry.Path))
	}
	l := overlayListener(t, cachePath, root)
	l.cacheGet = cache.New().Get
	l.SetMaxGlobs(100)
	l.SetMaxMetricsRendered(100)
	l.SetQuotas(nil)
	l.SetFileListCacheVersion(2)
	var cases []incrementalWireCase
	var golden [][]byte
	for _, name := range names {
		for _, path := range []string{"/render/", "/info/", "/metrics/find/"} {
			for _, format := range []string{"json", "carbonapi_v2_pb", "carbonapi_v3_pb"} {
				c := incrementalWireCase{name, path, format, now - 310, now - 270}
				status, body := incrementalWireReply(t, l, c)
				if status != http.StatusOK {
					t.Fatalf("source oracle %s %s %s: %d %s", name, path, format, status, body)
				}
				cases = append(cases, c)
				golden = append(golden, body)
			}
		}
	}
	destination := t.TempDir()
	db, err := store.Open(destination, store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: root, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l.SetMetricStore(db)
	l.SetMetricStoreFileIndex(true)
	l.SetIncrementalIndexScratch(filepath.Join(destination, "index"))
	if err := l.UseMirroredFilesForHandoff(); err == nil {
		t.Fatal("allowed fallback to reclaimed originals")
	}
	p := persister.NewWhisper(root, nil, persister.NewWhisperAggregation(), nil, nil, nil, nil)
	p.SetMetricStore(db)
	if err := p.SetIncrementalMigration(db, destination); err != nil {
		t.Fatal(err)
	}
	check := func(label string) {
		t.Helper()
		for i, c := range cases {
			status, body := incrementalWireReply(t, l, c)
			if status != 200 || !bytes.Equal(body, golden[i]) {
				t.Fatalf("%s changed %s %s %s: status=%d\nwant=%q\ngot=%q", label, c.metric, c.path, c.format, status, golden[i], body)
			}
		}
	}
	check("all legacy")
	// Keep actual API requests in flight across ownership publication, unlink
	// and index replacement, rather than comparing only quiet phase boundaries.
	readCtx, stopReaders := context.WithCancel(context.Background())
	var readers sync.WaitGroup
	var readCount atomic.Uint64
	for worker := range 3 {
		readers.Go(func() {
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for i := worker; ; i += 3 {
				select {
				case <-readCtx.Done():
					return
				case <-ticker.C:
				}
				index := i % len(cases)
				status, body := incrementalWireReply(t, l, cases[index])
				if status != 200 || !bytes.Equal(body, golden[index]) {
					t.Errorf("concurrent conversion changed %s %s %s: %d %q", cases[index].metric, cases[index].path, cases[index].format, status, body)
					return
				}
				readCount.Add(1)
			}
		})
	}
	defer func() { stopReaders(); readers.Wait() }()
	epoch := db.ReadCacheEpoch()
	for _, name := range names[:3] {
		if _, err := p.ConvertFile(context.Background(), name); err != nil {
			t.Fatal(err)
		}
	}
	check("mixed ownership")
	l.updateFileList(root, nil, nil)
	if !l.HasMappedIndex() {
		t.Fatal("mixed rebuild lost compact index")
	}
	indexed, found, indexErr := l.CurrentFileIndex().trieIdx.snapshot.lookup(metricStoreVirtualPath(names[0]))
	if indexErr != nil || !found || indexed.PhysicalSize != 0 {
		t.Fatalf("owned metric was not published in the union snapshot: %+v %v", indexed, indexErr)
	}
	check("mixed index rebuilt")
	for _, name := range names[3:] {
		if _, err := p.ConvertFile(context.Background(), name); err != nil {
			t.Fatal(err)
		}
	}
	check("all originals reclaimed")
	l.updateFileList(root, nil, nil)
	if !l.HasMappedIndex() {
		t.Fatal("full conversion materialized a heap index")
	}
	if db.ReadCacheEpoch() != epoch {
		t.Fatal("conversion invalidated every render cache entry")
	}
	check("shared-only index rebuilt")
	l.SetMetricStoreFileIndex(false)
	if err := l.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	if !l.HasMappedIndex() {
		t.Fatal("completed migration materialized heap trie")
	}
	check("completed catalog mapped")
	stopReaders()
	readers.Wait()
	if readCount.Load() == 0 {
		t.Fatal("concurrent API reader did not run")
	}
	if atomic.LoadUint64(&l.metrics.NotFound) != 0 {
		t.Fatal("normal legacy fallback counted as missing")
	}
	for _, name := range names {
		if _, err := os.Stat(filepath.Join(root, strings.ReplaceAll(name, ".", "/")+".wsp")); !os.IsNotExist(err) {
			t.Fatalf("source retained: %s %v", name, err)
		}
	}
	next := unstartedOverlayListener(t, cachePath, root)
	next.cacheGet = cache.New().Get
	next.SetMaxGlobs(100)
	next.SetMaxMetricsRendered(100)
	next.SetQuotas(nil)
	next.SetFileListCacheVersion(2)
	next.SetMetricStore(db)
	next.SetMetricStoreFileIndex(true)
	next.SetIncrementalIndexScratch(filepath.Join(destination, "index"))
	next.WarmupIndex()
	next.WaitForWarmup()
	for i, c := range cases {
		status, body := incrementalWireReply(t, next, c)
		if status != 200 || !bytes.Equal(body, golden[i]) {
			t.Fatalf("restart changed %s %s %s", c.metric, c.path, c.format)
		}
	}
	if err := db.Delete(context.Background(), names[0]); err != nil {
		t.Fatal(err)
	}
	next.updateFileList(root, nil, nil)
	if next.MetricExists(names[0]) {
		t.Fatal("union index resurrected an expired converted metric")
	}
	for _, name := range names[1:] {
		if !next.MetricExists(name) {
			t.Fatalf("rebuild lost %s", name)
		}
	}
}

func TestIncrementalIndexSortUsesBoundedRunsAndSharedDuplicateWinner(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	destination := t.TempDir()
	ctx := context.Background()
	db, err := store.Open(destination, store.Options{ExactBlocks: true, IncrementalSource: root})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l := NewCarbonserverListener(nil)
	l.SetWhisperData(root)
	l.SetTrieIndex(true)
	u := newFileListUpdate(l, nil)
	for _, name := range []string{"a", "a.child", "a-sibling.child", "a0.child", "空间.值"} {
		if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Sum}); err != nil {
			t.Fatal(err)
		}
	}
	runs, err := u.incrementalCatalogRuns(db, t.TempDir(), 1, nil)
	if err != nil || len(runs) != 5 {
		t.Fatalf("bounded runs: %d %v", len(runs), err)
	}
	source := filepath.Join(t.TempDir(), "source.flc")
	writer, err := NewFileListCache(source, FLCVersion2, 'w')
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(&FLCEntry{Path: "/a.wsp", LogicalSize: 999, PhysicalSize: 999}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	var got []string
	err = mergeIncrementalEntries(append([]string{source}, runs...), func(entry *FLCEntry) error {
		if entry.Path == "/a.wsp" && entry.PhysicalSize != 0 {
			t.Error("legacy duplicate overrode shared ownership")
		}
		got = append(got, entry.Path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/a/child.wsp", "/a-sibling/child.wsp", "/a.wsp", "/a0/child.wsp", "/空间/值.wsp"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("directory-first union order changed: %v", got)
	}
}

func TestIncrementalIndexMergesMoreRunsThanOpenReaderBudget(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	var runs []string
	const count = 75
	for i := count - 1; i >= 0; i-- {
		path := filepath.Join(directory, fmt.Sprintf("run-%d.flc", i))
		writer, err := NewFileListCache(path, FLCVersion2, 'w')
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Write(&FLCEntry{Path: fmt.Sprintf("/run/%03d.wsp", i)}); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		runs = append(runs, path)
	}
	u := newFileListUpdate(NewCarbonserverListener(nil), nil)
	paths, err := u.compactIncrementalRuns(runs, directory)
	if err != nil || len(paths) >= incrementalIndexFanIn {
		t.Fatalf("failed to enforce final reader budget: %d %v", len(paths), err)
	}
	next := 0
	err = mergeIncrementalEntries(paths, func(entry *FLCEntry) error {
		if entry.Path != fmt.Sprintf("/run/%03d.wsp", next) {
			return fmt.Errorf("unexpected entry %s at %d", entry.Path, next)
		}
		next++
		return nil
	})
	if err != nil || next != count {
		t.Fatalf("multi-pass index lost or reordered names: %d %v", next, err)
	}
}
