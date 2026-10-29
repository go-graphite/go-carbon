package carbonserver

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/dgryski/go-expirecache"
	"github.com/go-graphite/carbonzipper/zipper/httpHeaders"
)

func processCPUTime(b *testing.B) time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

type discardResponseWriter struct {
	header http.Header
}

func (w *discardResponseWriter) Header() http.Header         { return w.header }
func (w *discardResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *discardResponseWriter) WriteHeader(int)             {}

// BenchmarkQueryCacheEntry reports the bytes of a cached render response and
// the CPU to store it and to serve a hit, with and without compression.
//
//   - store: gzip encoding on a miss.
//   - hit/none: the uncompressed entry, which gziphandler compresses on every
//     hit for clients accepting gzip, such as carbonapi.
//   - hit/gzip: the gzip entry sent as is.
//   - hit/gzip/identity: the gzip entry decoded for a client without gzip.
func BenchmarkQueryCacheEntry(b *testing.B) {
	for _, shape := range renderSampleShapes {
		raw, err := renderSample(shape, renderSampleMixed, 1).MarshalVT()
		if err != nil {
			b.Fatal(err)
		}
		listener := NewCarbonserverListener(nil)
		plain := fetchResponse{data: raw, contentType: httpHeaders.ContentTypeCarbonAPIv2PB}
		gzipped := listener.gzipFetchResponse(plain)
		b.Run(shape.String(), func(b *testing.B) {
			b.Run("store", func(b *testing.B) {
				b.SetBytes(int64(len(raw)))
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						listener.gzipFetchResponse(plain)
					}
				})
				b.ReportMetric(float64(len(raw)), "raw-B/entry")
				b.ReportMetric(float64(len(gzipped.data)), "gzip-B/entry")
			})
			for _, hit := range []struct {
				name   string
				entry  fetchResponse
				accept string
			}{
				{"hit/none", plain, "gzip"},
				{"hit/gzip", gzipped, "gzip"},
				{"hit/gzip/identity", gzipped, ""},
			} {
				b.Run(hit.name, func(b *testing.B) {
					handler := gziphandler.GzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", hit.entry.contentType)
						if err := writeRenderResponse(w, r, hit.entry); err != nil {
							b.Error(err)
						}
					}))
					req := httptest.NewRequest(http.MethodGet, "/render/", http.NoBody)
					if hit.accept != "" {
						req.Header.Set("Accept-Encoding", hit.accept)
					}
					b.SetBytes(int64(len(raw)))
					b.ReportAllocs()
					b.RunParallel(func(pb *testing.PB) {
						w := &discardResponseWriter{header: http.Header{}}
						for pb.Next() {
							clear(w.header)
							handler.ServeHTTP(w, req)
						}
					})
				})
			}
		})
	}
}

// BenchmarkQueryCacheRender times protobuf renders through the HTTP server,
// gziphandler and a Go client that accepts gzip like carbonapi, for cache hits
// and misses. The response holds 100 metrics with an hour of 1s points, of
// mixed kinds or mostly absent like the largest production responses.
// cpu-ns/op is the CPU of the whole process, the client's decoding included.
func BenchmarkQueryCacheRender(b *testing.B) {
	metrics := make([]string, 100)
	for i := range metrics {
		metrics[i] = fmt.Sprintf("bench.render.m%03d", i)
	}
	for _, kind := range []int{renderSampleMixed, renderSampleSparse} {
		listener, server := newQueryCacheTestServer(b, metrics, 3600, kind)
		url := fmt.Sprintf("%s/render/?format=protobuf&target=bench.render.*&from=%d&until=%d", server.URL, queryCacheTestNow-3600, queryCacheTestNow)
		client := &http.Client{}
		render := func(b *testing.B) int64 {
			resp, err := client.Get(url)
			if err != nil {
				b.Fatal(err)
			}
			defer resp.Body.Close()
			n, err := io.Copy(io.Discard, resp.Body)
			if err != nil || resp.StatusCode != http.StatusOK {
				b.Fatalf("status %d, err %v", resp.StatusCode, err)
			}
			return n
		}
		for _, compression := range []string{"none", "gzip"} {
			for _, hit := range []bool{false, true} {
				b.Run(fmt.Sprintf("values=%s/compression=%s/hit=%t", renderSampleKindNames[kind], compression, hit), func(b *testing.B) {
					setQueryCacheCompression(b, listener, compression)
					if !hit {
						// Entries are sized larger than the cache, so each
						// is evicted as soon as it is stored.
						sizes := &cacheEntrySizes{}
						sizes.add(1 << 40)
						listener.queryCache = expireCache{ec: expirecache.New(1), sizes: sizes}
					}
					b.SetBytes(render(b))
					b.ReportAllocs()
					cpu0 := processCPUTime(b)
					b.ResetTimer()
					for range b.N {
						render(b)
					}
					b.StopTimer()
					b.ReportMetric(float64(processCPUTime(b)-cpu0)/float64(b.N), "cpu-ns/op")
				})
			}
		}
	}
}
