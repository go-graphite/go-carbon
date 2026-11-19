package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2index"
	"github.com/go-graphite/go-carbon/internal/v2read"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
)

// A reader publication is sufficient for an exact render. Keep a populated
// discovery catalog deliberately stale: no timer or full-name rebuild may be
// needed, and a cached miss must not hide the newly published point.
func TestV2LiteralRenderBeforeCatalogPublication(t *testing.T) {
	for _, trie := range []bool{false, true} {
		t.Run(fmt.Sprint(trie), func(t *testing.T) {
			r, err := v2read.New(1, 16<<20, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			e, err := v2core.Open(t.TempDir(), v2core.Config{Rollups: true, AppendParts: true, JournalIdentity: "literal-render", Shards: 1, Owners: 1, Producers: 1, MaxRows: 256, PartitionSeconds: 86400, RawCodec: "run1"}, v2core.Hooks{PublishPending: r.PublishPending})
			if err != nil {
				r.Close()
				t.Fatal(err)
			}
			defer func() {
				r.Close()
				if err := e.Close(); err != nil {
					t.Error(err)
				}
			}()
			catalog := &v2index.Catalog{}
			if err := catalog.Publish(1, []string{"already.visible"}); err != nil {
				t.Fatal(err)
			}
			l := NewCarbonserverListener(nil)
			l.SetTrieIndex(trie)
			l.SetTrigramIndex(!trie)
			l.SetMaxGlobs(100)
			l.SetMaxMetricsRendered(100)
			l.SetV2Reader(r)
			l.findCacheEnabled, l.globCacheEnabled = true, true
			l.initializeCaches()
			if err := l.SetV2Catalog(catalog); err != nil {
				t.Fatal(err)
			}
			mux := l.newHTTPMux()
			now := time.Now().Unix()
			const name = "fresh.literal.metric"
			get := func(format, target string) *httptest.ResponseRecorder {
				t.Helper()
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/render/?format=%s&target=%s&from=%d&until=%d", format, url.QueryEscape(target), now-11, now-10), nil))
				return w
			}
			if w := get("json", name); w.Code != http.StatusNotFound {
				t.Fatalf("unpublished reader invented a metric: %d %s", w.Code, w.Body)
			}
			policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: name, Group: "local", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 3600}}}})
			if err != nil {
				t.Fatal(err)
			}
			b := v2core.Batch{ID: 1, Order: 1, Clock: now, Policy: policy, Points: []v2core.Row{{ID: 1, Time: now - 10, Bits: math.Float64bits(42.25), Seq: 1}}}
			if err := e.AppendBatches(0, []v2core.Batch{b}, 1, true); err != nil {
				t.Fatal(err)
			}
			// Exercise the directory release that motivated this change too.
			if _, err := r.Names(context.Background()); err != nil {
				t.Fatal(err)
			}
			before := make(map[string][]byte)
			for _, format := range []string{"json", "carbonapi_v2_pb"} {
				w := get(format, name)
				if w.Code != http.StatusOK {
					t.Fatalf("reader-visible literal waits for catalog: %s %d %s", format, w.Code, w.Body)
				}
				before[format] = bytes.Clone(w.Body.Bytes())
			}
			stream := &restartReadStream{ctx: context.Background()}
			if err := l.Render(&protov2.MultiFetchRequest{Metrics: []*protov2.FetchRequest{{Name: name, StartTime: int32(now - 11), StopTime: int32(now - 10)}}}, stream); err != nil || len(stream.responses) != 1 {
				t.Fatalf("gRPC literal waits for catalog: %v responses=%d", err, len(stream.responses))
			}
			response := stream.responses[0]
			if len(response.Values) != 1 || response.IsAbsent[0] || math.Float64bits(response.Values[0]) != math.Float64bits(42.25) {
				t.Fatalf("gRPC literal changed point bits: %v", response)
			}
			for _, target := range []string{"fresh.literal.*", "fresh.literal", "missing.metric"} {
				if w := get("json", target); w.Code != http.StatusNotFound {
					t.Fatalf("invented discovery or missing metric: %s %d %s", target, w.Code, w.Body)
				}
			}
			if err := catalog.Apply(2, []string{name}, nil); err != nil {
				t.Fatal(err)
			}
			for format, want := range before {
				if w := get(format, name); w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), want) {
					t.Fatalf("%s encoded render differs after catalog publication: %d", format, w.Code)
				}
			}
			want := http.StatusNotFound
			if trie {
				want = http.StatusOK
			}
			if w := get("json", " "+name+" "); w.Code != want {
				t.Fatalf("whitespace expansion changed: got %d want %d", w.Code, want)
			}
		})
	}
}
