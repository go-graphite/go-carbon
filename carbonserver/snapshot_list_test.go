package carbonserver

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/blevesearch/vellum"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Preserve the pre-fix enumeration as the benchmark's output and cost oracle.
func snapshotListReference(ti *trieIndex, sep byte) []string {
	files := ti.allMetricsMutable(sep)
	_ = ti.snapshot.walkNamespace("/", func(name string, _ uint64) bool {
		if sep != '.' {
			name = strings.ReplaceAll(name, ".", string(sep))
		}
		files = append(files, name)
		return true
	})
	sort.Strings(files)
	return files
}

// GO_CARBON_SNAPSHOT_FST optionally selects a captured immutable index. Only
// the captured file is read; no live service or snapshot files are changed.
func snapshotListBenchmarkIndex(b *testing.B) (*trieIndex, int) {
	b.Helper()
	var index *vellum.FST
	var err error
	if path := os.Getenv("GO_CARBON_SNAPSHOT_FST"); path != "" {
		index, err = vellum.Open(path)
	} else {
		var data bytes.Buffer
		builder, buildErr := vellum.New(&data, nil)
		if buildErr != nil {
			b.Fatal(buildErr)
		}
		for i := 0; i < 1000000; i++ {
			key := fmt.Sprintf("\x00metrics\x00namespace%04d\x00by_region\x00region%02d\x00by_service\x00service%04d\x00requests\x00duration\x00percentile%03d.wsp", i/10000, i/1000%10, i/10%100, i%10)
			if err := builder.Insert([]byte(key), uint64(i)); err != nil {
				b.Fatal(err)
			}
		}
		if err := builder.Close(); err != nil {
			b.Fatal(err)
		}
		index, err = vellum.Load(data.Bytes())
	}
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = index.Close() })
	ti := newTrie(".wsp", 0, nil)
	ti.snapshot = &indexSnapshot{index: index}
	// Spread new metrics across the captured namespaces instead of appending
	// one artificial namespace that sorts entirely before or after the base.
	stride, overlays := max(index.Len()/10000, 1), 0
	err = ti.snapshot.walkNamespace("/", func(name string, row uint64) bool {
		if row%uint64(stride) == 0 {
			path := "/" + strings.ReplaceAll(name, ".", "/") + "/__snapshot_benchmark_new__.wsp"
			if _, err := ti.insertMutable(path, 0, 0, 0, 0); err != nil {
				b.Fatal(err)
			}
			overlays++
		}
		return overlays < 10000
	})
	if err != nil {
		b.Fatal(err)
	}
	return ti, overlays
}

func BenchmarkSnapshotMetricList(b *testing.B) {
	ti, overlays := snapshotListBenchmarkIndex(b)
	var digest [sha256.Size]byte
	haveReference := false
	for _, variant := range []struct {
		name string
		list func(*trieIndex, byte) []string
	}{{"reference", snapshotListReference}, {"candidate", (*trieIndex).allMetrics}} {
		b.Run(variant.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				files := variant.list(ti, '.')
				b.StopTimer()
				if !sort.StringsAreSorted(files) || len(files) != ti.snapshot.index.Len()+overlays {
					b.Fatal("incorrect list")
				}
				h := sha256.New()
				for _, name := range files {
					_, _ = h.Write([]byte(name))
					_, _ = h.Write([]byte{0})
				}
				got := [sha256.Size]byte(h.Sum(nil))
				if variant.name == "reference" {
					digest = got
					haveReference = true
				} else if haveReference && got != digest {
					b.Fatal("list differs from reference")
				}
				b.StartTimer()
			}
		})
		runtime.GC()
	}
}

// Exercise the actual handler and loopback HTTP transfer. The original list and
// encoder establish the complete wire digest before the measured request.
func BenchmarkSnapshotMetricListHTTP(b *testing.B) {
	ti, _ := snapshotListBenchmarkIndex(b)
	reference := &protov3.ListMetricsResponse{Metrics: snapshotListReference(ti, '.')}
	wire, err := reference.MarshalVT()
	if err != nil {
		b.Fatal(err)
	}
	want, size := sha256.Sum256(wire), int64(len(wire))
	runtime.GC()
	listener := newTrieServer(nil, b)
	listener.metrics = &metricStruct{}
	logFile, err := os.Create(b.TempDir() + "/list.log")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = logFile.Close() })
	listener.accessLogger = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(logFile), zap.InfoLevel))
	listener.UpdateFileIndex(&fileIndex{trieIdx: ti})
	server := httptest.NewServer(http.HandlerFunc(listener.listHandler))
	b.Cleanup(server.Close)
	b.ReportAllocs()
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, err := server.Client().Get(server.URL + "/metrics/list/?format=carbonapi_v3_pb")
		if err != nil {
			b.Fatal(err)
		}
		h := sha256.New()
		n, err := io.Copy(h, response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || n != size || [sha256.Size]byte(h.Sum(nil)) != want {
			b.Fatalf("HTTP response differs: status=%d bytes=%d/%d error=%v", response.StatusCode, n, size, err)
		}
	}
	b.StopTimer()
	logs, err := os.ReadFile(logFile.Name())
	if err != nil {
		b.Fatal(err)
	}
	var listTime, formatTime, serveTime float64
	decoder := json.NewDecoder(bytes.NewReader(logs))
	for {
		var entry struct {
			Message string  `json:"msg"`
			Elapsed float64 `json:"runtime_seconds"`
		}
		if err := decoder.Decode(&entry); err != nil {
			if err != io.EOF {
				b.Fatal(err)
			}
			break
		}
		switch entry.Message {
		case "list acquired":
			listTime += entry.Elapsed
		case "list formatted":
			formatTime += entry.Elapsed
		case "list served":
			serveTime += entry.Elapsed
		}
	}
	b.ReportMetric(listTime/float64(b.N), "list_s")
	b.ReportMetric((formatTime-listTime)/float64(b.N), "encode_s")
	b.ReportMetric(serveTime/float64(b.N), "serve_s")
}

func TestSnapshotMetricListParallel(t *testing.T) {
	for _, count := range []int{0, 1, 5000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			keys := make([]string, 0, count)
			for i := 0; i < count; i++ {
				// Almost all names share a long prefix; a few sparse branches
				// include a terminal key that is also another key's prefix.
				key := fmt.Sprintf("\x00a\x00%s%08d.wsp", strings.Repeat("shared\x00", 64), i)
				switch i {
				case 1:
					key = "\x00a.wsp"
				case 2:
					key = "\x00a.wsp.more.wsp"
				case 3:
					key = "\x00z\x00空间.wsp"
				}
				keys = append(keys, key)
			}
			sort.Strings(keys)
			var data bytes.Buffer
			builder, err := vellum.New(&data, nil)
			if err != nil {
				t.Fatal(err)
			}
			for row, key := range keys {
				if err := builder.Insert([]byte(key), uint64(row)); err != nil {
					t.Fatal(err)
				}
			}
			if err := builder.Close(); err != nil {
				t.Fatal(err)
			}
			index, err := vellum.Load(data.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = index.Close() })
			s := &indexSnapshot{index: index}
			for _, sep := range []byte{'.', '/', 0x80} {
				want := make([]string, 0, count)
				_ = s.walkNamespace("/", func(name string, _ uint64) bool {
					if sep != '.' {
						name = strings.ReplaceAll(name, ".", string(sep))
					}
					want = append(want, name)
					return true
				})
				var wg sync.WaitGroup
				for _, workers := range []int{1, 2, 4} {
					wg.Go(func() {
						files := make([]string, 1, count+1)
						files[0] = "existing"
						got := s.appendMetricNamesParallel(files, sep, workers)
						if got[0] != "existing" || !reflect.DeepEqual(got[1:], want) {
							t.Errorf("workers=%d separator=%q: list differs", workers, sep)
						}
					})
				}
				wg.Wait()
			}
		})
	}
}

func TestSnapshotMetricListParity(t *testing.T) {
	hybrid, oracle, _, _ := snapshotTrieFixture(t)
	for _, sep := range []byte{'.', '/'} {
		if got, want := hybrid.allMetrics(sep), oracle.allMetrics(sep); !reflect.DeepEqual(got, want) {
			t.Fatalf("snapshot separator %q: got %v, want %v", sep, got, want)
		}
	}
	for _, path := range []string{"/a/overlay.wsp", "/a!/value.wsp", "/a-/value.wsp", "/a0/value/leaf.wsp", "/b.wsp", "/空间/new.wsp", "/z/value.wsp"} {
		for _, index := range []*trieIndex{hybrid, oracle} {
			if _, err := index.insert(path, 1, 2, 3, 12345); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, sep := range []byte{'.', '/'} {
		if got, want := hybrid.allMetrics(sep), oracle.allMetrics(sep); !reflect.DeepEqual(got, want) {
			t.Fatalf("overlay separator %q: got %v, want %v", sep, got, want)
		}
	}
}
