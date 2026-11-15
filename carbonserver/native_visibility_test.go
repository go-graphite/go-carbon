package carbonserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"go.uber.org/zap"
)

// Native pulls bypass the ingestion cache. Their committed names must reach
// normal reads even while startup recovery blocks every catalog scan.
func TestNativePullVisibleWhileScansGated(t *testing.T) {
	for _, combine := range []bool{false, true} {
		t.Run(fmt.Sprintf("combine=%t", combine), func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().Unix()
			open := func() *store.Store {
				db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(vfs.NewMem()))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				return db
			}
			donor, target := open(), open()
			name := "copied.new"
			if _, err := donor.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 3600}}, AggregationMethod: store.Sum}); err != nil {
				t.Fatal(err)
			}
			if err := donor.UpdateMany(ctx, name, []store.Point{{Timestamp: now - 2, Value: 42}}); err != nil {
				t.Fatal(err)
			}
			service := func(db *store.Store) *buckyd.Service {
				s, err := buckyd.New(buckyd.Config{TmpDir: t.TempDir(), TransferSources: []string{"127.0.0.1"}}, db)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := s.Stop(); err != nil {
						t.Error(err)
					}
				})
				return s
			}
			source, destination := service(donor), service(target)
			remote := httptest.NewServer(source.Handler())
			defer remote.Close()
			l := NewCarbonserverListener(cache.New().Get)
			l.logger, l.accessLogger = zap.NewNop(), zap.NewNop()
			l.SetWhisperData(t.TempDir())
			l.SetMetricStore(target)
			l.SetTrieIndex(true)
			l.SetConcurrentIndex(true)
			l.SetRealtimeIndex(1)
			l.SetMaxGlobs(100)
			l.SetMaxMetricsGlobbed(100)
			l.SetMaxMetricsRendered(100)
			l.SetMaxFetchDataGoroutines(1)
			l.SetFileListCache(filepath.Join(t.TempDir(), "files.gz"))
			l.SetFileListCacheVersion(2)
			l.SetQuotaUsageReportFrequency(time.Hour)
			if err := l.RefreshMetricStoreIndex(); err != nil {
				t.Fatal(err)
			}
			l.SetStartupScanGate(make(chan struct{}))
			force := make(chan struct{}, 1)
			force <- struct{}{}
			l.startFileListUpdater(l.whisperData, nil, force, l.exitChan)
			t.Cleanup(func() { _ = l.Stop() })
			destination.SetOnNativeImport(l.QueueNewMetric)
			destination.SetOnChange(func() {
				select {
				case force <- struct{}{}:
				default:
				}
			})
			call := func(s *buckyd.Service, path string, body any) *httptest.ResponseRecorder {
				data, err := json.Marshal(body)
				if err != nil {
					t.Fatal(err)
				}
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data)))
				if w.Code != 200 {
					t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
				}
				return w
			}
			var pin struct{ ID string }
			if err := json.Unmarshal(call(source, "/v1/native/source", struct{}{}).Body.Bytes(), &pin); err != nil {
				t.Fatal(err)
			}
			request := struct {
				Range   store.NativeRange
				Session string
			}{
				Range: store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: remote.URL, End: 1, Names: []string{name}, Combine: combine}, Session: pin.ID,
			}
			index := l.CurrentFileIndex()
			call(destination, "/v1/native/pull", request)
			deadline := time.Now().Add(5 * time.Second)
			for !l.MetricExists(name) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !l.MetricExists(name) {
				t.Fatal("committed native name missing with scans gated")
			}
			if l.CurrentFileIndex() != index {
				t.Fatal("scan replaced index while recovery gate was closed")
			}
			for _, path := range []string{
				"/metrics/find/?format=json&query=copied.*",
				"/render/?format=json&target=" + name + "&from=" + fmt.Sprint(now-10) + "&until=" + fmt.Sprint(now),
			} {
				w := httptest.NewRecorder()
				r := httptest.NewRequest(http.MethodGet, path, nil)
				if strings.HasPrefix(path, "/render/") {
					l.renderHandler(w, r)
				} else {
					l.findHandler(w, r)
				}
				if w.Code != 200 {
					t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
				}
				if !strings.Contains(w.Body.String(), name) {
					t.Fatalf("normal read omitted copied name: %s", w.Body.String())
				}
				if strings.HasPrefix(path, "/render/") && !strings.Contains(w.Body.String(), "42") {
					t.Fatalf("copied value missing: %s", w.Body.String())
				}
			}
			// A second scan sees the name in the catalog; pending-name pruning must
			// preserve it despite the absence of both a Whisper file and cache points.
			if err := l.RefreshMetricStoreIndex(); err != nil {
				t.Fatal(err)
			}
			if !l.MetricExists(name) {
				t.Fatal("catalog reconciliation pruned copied name")
			}
		})
	}
}
