package carbonserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/vellum"
	"github.com/go-faster/city"
	"go.uber.org/zap"
)

// namespaceHashesOracle follows events_generate_metric_hashes.pl: read the full
// metric list, strip the prefix, assign each name to its longest enabled
// "<namespace>." prefix (Regexp::Trie's greedy optional suffixes), then write
// each namespace's name count and sorted unique CityHash64 values.
func namespaceHashesOracle(names []string, prefix string, enabled []string) []byte {
	if prefix != "" && !strings.HasSuffix(prefix, ".") {
		prefix += "."
	}
	enabledSet := make(map[string]bool)
	for _, ns := range enabled {
		enabledSet[ns] = true
	}
	groups := make(map[string][]uint64)
	for _, name := range names {
		rest, ok := strings.CutPrefix(name, prefix)
		if !ok {
			continue
		}
		match := ""
		for i := range len(rest) {
			if rest[i] == '.' && (enabled == nil && i > 0 && !strings.Contains(rest[:i], ".") || enabledSet[rest[:i]]) {
				match = rest[:i]
			}
		}
		if match != "" {
			groups[match] = append(groups[match], city.Hash64([]byte(rest)))
		}
	}
	if enabled == nil {
		for ns := range groups {
			enabled = append(enabled, ns)
		}
	}
	sort.Strings(enabled)
	enabled = slices.Compact(enabled)
	out := append([]byte("NSHF\x01"), make([]byte, 8)...)
	for _, ns := range enabled {
		hashes := groups[ns]
		if len(hashes) == 0 {
			continue
		}
		count := len(hashes)
		slices.Sort(hashes)
		hashes = slices.Compact(hashes)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(ns)))
		out = append(out, ns...)
		out = binary.LittleEndian.AppendUint64(out, uint64(count))
		out = binary.LittleEndian.AppendUint64(out, uint64(len(hashes)))
		for _, h := range hashes {
			out = binary.LittleEndian.AppendUint64(out, h)
		}
	}
	return out
}

func namespaceHashesSnapshot(t testing.TB, names []string) *indexSnapshot {
	t.Helper()
	keys := make([]string, len(names))
	for i, name := range names {
		keys[i] = "\x00" + strings.ReplaceAll(name, ".", "\x00") + ".wsp"
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
	return &indexSnapshot{index: index}
}

func namespaceHashesServer(t testing.TB, ti *trieIndex) (*CarbonserverListener, *httptest.Server) {
	t.Helper()
	listener := newTrieServer(nil, t)
	listener.metrics = &metricStruct{}
	listener.accessLogger = zap.NewNop()
	listener.UpdateFileIndex(&fileIndex{trieIdx: ti})
	server := httptest.NewServer(http.HandlerFunc(listener.namespaceHashesHandler))
	t.Cleanup(server.Close)
	return listener, server
}

func requestNamespaceHashes(t testing.TB, server *httptest.Server, prefix string, body *string) (int, []byte) {
	t.Helper()
	url := server.URL + "/metrics/namespace-hashes/?prefix=" + prefix
	var response *http.Response
	var err error
	if body == nil {
		response, err = server.Client().Get(url)
	} else {
		response, err = server.Client().Post(url, "text/plain", strings.NewReader(*body))
	}
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, data
}

func TestNamespaceHashesMatchesMetricList(t *testing.T) {
	defer func(v uint64) { namespaceHashesSplitMin = v }(namespaceHashesSplitMin)
	namespaceHashesSplitMin = 1

	base := []string{
		"aggregations.secondly.web.a.b",
		"aggregations.secondly.web.c",
		"aggregations.secondly.web_x.a",
		"aggregations.secondly.web0.q",
		"aggregations.secondly.leaf",
		"aggregations.secondly.空间.x.y",
		"aggregations.secondly2.web.a",
		"aggregations.secondlyweb.a",
		"aggregations.other.x.y",
		"top",
	}
	// Skewed namespaces so the range split cuts through a namespace.
	for i := range 6000 {
		ns := "big"
		if i%7 == 0 {
			ns = fmt.Sprintf("ns%02d", i%23)
		}
		base = append(base, fmt.Sprintf("aggregations.secondly.%s.by_host.host%03d.metric%d", ns, i%311, i))
	}
	ti := newTrie(".wsp", 0, nil)
	ti.snapshot = namespaceHashesSnapshot(t, base)
	for _, name := range []string{
		"aggregations.secondly.web.new1",
		"aggregations.secondly.big.by_host.new.metric",
		"aggregations.secondly.brandnew.x",
		"aggregations.secondly.leaf2",
		"other.z.w",
	} {
		if _, err := ti.insertMutable("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	_, server := namespaceHashesServer(t, ti)
	all := ti.allMetrics('.')

	enabled := "web\n  web_x..\n\nbrandnew\nunknown\nbig\n空间\n"
	enabledList := []string{"web", "web_x", "brandnew", "unknown", "big", "空间"}
	for _, tc := range []struct {
		prefix  string
		body    *string
		enabled []string
	}{
		{"aggregations.secondly.", nil, nil},
		{"aggregations.secondly", nil, nil},
		{"aggregations.secondly.", &enabled, enabledList},
		{"", nil, nil},
		{"missing.prefix.", nil, nil},
	} {
		t.Run(fmt.Sprintf("prefix=%q/filtered=%v", tc.prefix, tc.body != nil), func(t *testing.T) {
			// The second request is served from the cached snapshot hashes.
			for range 2 {
				code, got := requestNamespaceHashes(t, server, tc.prefix, tc.body)
				want := namespaceHashesOracle(all, tc.prefix, tc.enabled)
				if code != http.StatusOK || len(got) < 13 || !bytes.Equal(got[:5], want[:5]) || !bytes.Equal(got[13:], want[13:]) {
					t.Fatalf("status %d: response differs from metric list oracle (%d vs %d bytes)", code, len(got), len(want))
				}
			}
		})
	}

	// New overlay metrics are visible with a cached base.
	if _, err := ti.insertMutable("/aggregations/secondly/web/new2.wsp", 0, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	_, got := requestNamespaceHashes(t, server, "aggregations.secondly.", nil)
	if want := namespaceHashesOracle(ti.allMetrics('.'), "aggregations.secondly.", nil); !bytes.Equal(got[13:], want[13:]) {
		t.Fatal("overlay update not reflected")
	}
}

func TestNamespaceHashesDottedNamespaces(t *testing.T) {
	names := []string{
		"p.a.x", "p.a.y.z", "p.a.b.c", "p.a.b.d.e", "p.a.b.d.f", "p.a.bc.x",
		"p.a.b", "p.c.d.e", "p.c.x", "p.e.f.g", "q.a.b.c",
	}
	ti := newTrie(".wsp", 0, nil)
	ti.snapshot = namespaceHashesSnapshot(t, names)
	for _, name := range []string{"p.a.b.new", "p.a.new", "p.c.d.new", "p.new.x.y"} {
		if _, err := ti.insertMutable("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	_, server := namespaceHashesServer(t, ti)
	plain := newTrie(".wsp", 0, nil)
	for _, name := range append(names, "p.a.b.new", "p.a.new", "p.c.d.new", "p.new.x.y") {
		if _, err := plain.insertMutable("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	_, plainServer := namespaceHashesServer(t, plain)
	all := ti.allMetrics('.')
	for _, enabled := range [][]string{
		{"a", "a.b"},          // a.b.* only in a.b
		{"a", "a.b", "a.b.d"}, // nested chain
		{"a.b", "c"},          // dotted without its top
		{"a.b.", "a", "e", "new"},
		{"c.d", "a.bc", "zz.y"},
	} {
		body := strings.Join(enabled, "\n") + "\n"
		trimmed := make([]string, len(enabled))
		for i, ns := range enabled {
			trimmed[i] = strings.TrimRight(ns, ".")
		}
		want := namespaceHashesOracle(all, "p.", trimmed)
		for label, srv := range map[string]*httptest.Server{"snapshot": server, "trie": plainServer} {
			code, got := requestNamespaceHashes(t, srv, "p.", &body)
			if code != http.StatusOK || !bytes.Equal(got[13:], want[13:]) {
				t.Errorf("%s %q: status %d, response differs from metric list oracle", label, enabled, code)
			}
		}
	}
}

func TestNamespaceHashesCachedPrefixLimit(t *testing.T) {
	ti := newTrie(".wsp", 0, nil)
	ti.snapshot = namespaceHashesSnapshot(t, []string{"a.x.y", "b.x.y", "c.x.y", "d.x.y", "e.x.y", "f.x.y"})
	listener, server := namespaceHashesServer(t, ti)
	all := ti.allMetrics('.')
	for _, prefix := range []string{"a", "b", "c", "d", "e", "f", "a"} {
		code, got := requestNamespaceHashes(t, server, prefix, nil)
		if want := namespaceHashesOracle(all, prefix, nil); code != http.StatusOK || !bytes.Equal(got[13:], want[13:]) {
			t.Fatalf("prefix %q: response differs", prefix)
		}
	}
	if n := ti.snapshot.hashesCached.Load(); n != maxCachedNamespaceHashPrefixes {
		t.Fatalf("cached %d prefixes, want %d", n, maxCachedNamespaceHashPrefixes)
	}
	// Prewarming a new generation computes every requested prefix.
	next := namespaceHashesSnapshot(t, []string{"a.x.y"})
	listener.prewarmNamespaceHashes(next)
	var prefixes []string
	listener.namespaceHashPrefixes.Range(func(key, _ any) bool {
		prefixes = append(prefixes, key.(string))
		return true
	})
	sort.Strings(prefixes)
	if !reflect.DeepEqual(prefixes, []string{"a", "b", "c", "d"}) {
		t.Fatalf("remembered prefixes %q", prefixes)
	}
	for _, prefix := range prefixes {
		if _, err := next.namespaceHashes(prefix); err != nil {
			t.Fatal(err)
		}
	}
	if n := next.hashesCached.Load(); n != maxCachedNamespaceHashPrefixes {
		t.Fatalf("prewarmed %d prefixes, want %d", n, maxCachedNamespaceHashPrefixes)
	}
}

func TestNamespaceHashesWithoutSnapshot(t *testing.T) {
	ti := newTrie(".wsp", 0, nil)
	for _, name := range []string{"p.a.b", "p.a.c", "p.b.c", "p.leaf", "q.a.b"} {
		if _, err := ti.insertMutable("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	_, server := namespaceHashesServer(t, ti)
	code, got := requestNamespaceHashes(t, server, "p.", nil)
	if want := namespaceHashesOracle(ti.allMetrics('.'), "p.", nil); code != http.StatusOK || !bytes.Equal(got[13:], want[13:]) {
		t.Fatal("response differs from metric list oracle")
	}
}

// GO_CARBON_SNAPSHOT_FST optionally selects a captured immutable index and
// GO_CARBON_NSHASH_PREFIX its namespace prefix. GO_CARBON_NSHASH_VERIFY=1
// also compares the response with the full metric list oracle.
func BenchmarkNamespaceHashes(b *testing.B) {
	ti, _ := snapshotListBenchmarkIndex(b)
	prefix := "metrics."
	if p := os.Getenv("GO_CARBON_NSHASH_PREFIX"); p != "" {
		prefix = p
	}
	_, server := namespaceHashesServer(b, ti)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	t0 := time.Now()
	if _, err := ti.snapshot.namespaceHashes(prefix); err != nil {
		b.Fatal(err)
	}
	compute := time.Since(t0)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	var size int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		code, got := requestNamespaceHashes(b, server, prefix, nil)
		if code != http.StatusOK {
			b.Fatalf("status %d", code)
		}
		size = len(got)
		if i == 0 && os.Getenv("GO_CARBON_NSHASH_VERIFY") == "1" {
			b.StopTimer()
			if want := namespaceHashesOracle(ti.allMetrics('.'), prefix, nil); !bytes.Equal(got[13:], want[13:]) {
				b.Fatal("response differs from metric list oracle")
			}
			b.StartTimer()
		}
	}
	b.StopTimer()
	b.SetBytes(int64(size))
	b.ReportMetric(compute.Seconds(), "compute_s")
	b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/(1<<20), "retained_MiB")
}
