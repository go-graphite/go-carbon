package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"syscall"
	"testing"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"go.uber.org/zap"
)

func TestCanonicalRenderCacheKey(t *testing.T) {
	tr := timeRange{100, 200}
	base := map[timeRange][]target{tr: {{"a&b", "alias(a)"}, {"c", "alias(c)"}, {"c", "alias(c)"}}, {50, 150}: {{"d", "d"}}}
	want := canonicalRenderCacheKey(base, "json")
	permuted := map[timeRange][]target{{50, 150}: {{"d", "d"}}, tr: slices.Clone(base[tr])}
	slices.Reverse(permuted[tr])
	for range 1000 {
		if got := canonicalRenderCacheKey(permuted, "json"); got != want {
			t.Fatal("range/target permutation changed key")
		}
	}
	if !reflect.DeepEqual(base[tr], []target{{"a&b", "alias(a)"}, {"c", "alias(c)"}, {"c", "alias(c)"}}) {
		t.Fatal("key construction mutated the request")
	}
	for name, other := range map[string]map[timeRange][]target{
		"multiplicity":  {tr: {{"a&b", "alias(a)"}, {"c", "alias(c)"}}, {50, 150}: {{"d", "d"}}},
		"expression":    {tr: {{"a&b", "a&b"}, {"c", "alias(c)"}, {"c", "alias(c)"}}, {50, 150}: {{"d", "d"}}},
		"range":         {{101, 200}: base[tr], {50, 150}: {{"d", "d"}}},
		"range binding": {tr: {{"d", "d"}}, {50, 150}: base[tr]},
	} {
		if canonicalRenderCacheKey(other, "json") == want {
			t.Fatal("key lost", name)
		}
	}
	for _, format := range []string{"pickle", "carbonapi_v2_pb", "carbonapi_v3_pb", "json&store=2"} {
		if canonicalRenderCacheKey(base, format) == want {
			t.Fatal("key lost format/epoch", format)
		}
	}
	// A delimiter, empty expression, NUL and negative timestamps are literal
	// inputs, not field framing. Distinct tuple lists must stay distinct.
	keys := make(map[string]bool)
	for _, name := range []string{"", "a", "a&b", "a\x00b", "a&0&1"} {
		for _, expression := range []string{"", "a", "&", "\x00", "b&json"} {
			for _, from := range []int32{-1, 0, 1, 100} {
				k := canonicalRenderCacheKey(map[timeRange][]target{{from, 200}: {{name, expression}}}, "json")
				if keys[k] {
					t.Fatal("framing collision")
				}
				keys[k] = true
			}
		}
	}
}

func TestCanonicalRenderCacheReplies(t *testing.T) {
	l, _ := newQueryCacheTestServer(t, queryCacheTestMetrics, 120, renderSampleMixed)
	l.SetQueryCacheCanonicalKeys(true)
	setQueryCacheCompression(t, l, "none")
	tr := timeRange{queryCacheTestNow - 60, queryCacheTestNow}
	a, b := queryCacheTestMetrics[0], queryCacheTestMetrics[1]
	first := map[timeRange][]target{tr: {{a, "alias"}, {b, "other"}, {a, "alias"}}}
	second := map[timeRange][]target{tr: {{b, "other"}, {a, "alias"}, {a, "alias"}}}
	fetch := func(targets map[timeRange][]target, wantHit bool) fetchResponse {
		t.Helper()
		r, hit, err := l.fetchWithCache(context.Background(), zap.NewNop(), protoV3Format, targets, &traceLogEntries{})
		if err != nil || hit != wantHit {
			t.Fatalf("hit=%v want=%v err=%v", hit, wantHit, err)
		}
		return r
	}
	want := fetch(first, false)
	got := fetch(second, true)
	if !bytes.Equal(want.data, got.data) {
		t.Fatal("cache changed bytes")
	}
	l.queryCacheEnabled = false
	oracle := fetch(second, false)
	decode := func(data []byte) [][]byte {
		t.Helper()
		var multi protov3.MultiFetchResponse
		if err := multi.UnmarshalVT(data); err != nil {
			t.Fatal(err)
		}
		var wire [][]byte
		for _, m := range multi.Metrics {
			b, err := m.MarshalVT()
			if err != nil {
				t.Fatal(err)
			}
			wire = append(wire, b)
		}
		slices.SortFunc(wire, bytes.Compare)
		return wire
	}
	if !reflect.DeepEqual(decode(got.data), decode(oracle.data)) || got.metricsFetched != 3 {
		t.Fatal("canonical hit lost series, duplicates, expressions or request range")
	}
}

func TestCanonicalStreamingCacheReplies(t *testing.T) {
	l := NewCarbonserverListener(nil)
	l.SetQueryCacheCanonicalKeys(true)
	l.SetStreamingQueryCacheEnabled(true)
	l.initializeCaches()
	tr := timeRange{100, 200}
	requests := []map[timeRange][]target{{tr: {{"a", "a"}, {"b", "b"}, {"a", "a"}}}, {tr: {{"b", "b"}, {"a", "a"}, {"a", "a"}}}}
	calls := 0
	for i, request := range requests {
		ch, hit, err := l.renderResponseChannel(request, protoV2Format, &traceLogEntries{}, func() (chan response, error) {
			calls++
			ch := make(chan response, 3)
			for _, name := range []string{"a", "b", "a"} {
				ch <- response{Name: name, Values: []float64{1}}
			}
			close(ch)
			return ch, nil
		})
		if err != nil || hit != (i == 1) {
			t.Fatalf("hit=%v err=%v", hit, err)
		}
		var names []string
		for r := range ch {
			names = append(names, r.Name)
		}
		if !slices.Equal(names, []string{"a", "b", "a"}) {
			t.Fatal(names)
		}
	}
	if calls != 1 {
		t.Fatal("permutation missed streaming cache")
	}
}

// These are synthetic assumptions, not a traffic trace. Shapes follow the
// read-replay fixtures: numeric 1h ranges, a*b and repeated target parameters.
// Extra v3 ranges model timeShift/timeStack. Requests arrive eight times in a
// 60s TTL, then all ranges move by 60s; every fifth panel moves on every read.
func renderKeyMix(seed int64) []map[timeRange][]target {
	rng := rand.New(rand.NewSource(seed))
	var requests []map[timeRange][]target
	for minute := range 20 {
		for read := range 8 {
			for panel := range 100 {
				ranges, names := 1, 1
				switch {
				case panel >= 90:
					ranges, names = 5, 2
				case panel >= 75:
					ranges, names = 2, 2
				case panel >= 60:
					names = 4
				}
				until := int32(1700000000 + minute*60)
				if panel%5 == 0 {
					until += int32(read * 7)
				}
				targets := make(map[timeRange][]target)
				for shift := range ranges {
					tr := timeRange{until - 3600 - int32(shift*86400), until - int32(shift*86400)}
					for n := range names {
						name := fmt.Sprintf("replay.panel%d.metric%d.*", panel, n)
						targets[tr] = append(targets[tr], target{name, name})
					}
					rng.Shuffle(names, func(i, j int) { targets[tr][i], targets[tr][j] = targets[tr][j], targets[tr][i] })
				}
				requests = append(requests, targets)
			}
		}
	}
	return requests
}

func TestRenderKeyMix(t *testing.T) {
	requests := renderKeyMix(1)
	for _, canonical := range []bool{false, true} {
		l := NewCarbonserverListener(nil)
		l.SetQueryCacheCanonicalKeys(canonical)
		hits, keyBytes := 0, 0
		seen := make(map[string]bool)
		for i, request := range requests {
			if i%800 == 0 {
				clear(seen)
			} // expire at each 60s boundary
			key, _ := l.getRenderCacheKeyAndSize(request, protoV3Format.String())
			if seen[key] {
				hits++
			} else {
				seen[key] = true
				keyBytes += len(key)
			}
		}
		t.Logf("canonical=%v requests=%d hits=%d hit_rate=%.4f unique_key_bytes=%d", canonical, len(requests), hits, float64(hits)/float64(len(requests)), keyBytes)
		if canonical && hits != 11200 {
			t.Fatal("unexpected canonical hit count", hits)
		}
	}
}

var renderKeySink string

func BenchmarkRenderCacheKeys(b *testing.B) {
	for _, shape := range []struct{ ranges, names int }{{1, 1}, {1, 4}, {2, 2}, {5, 2}, {1, 256}} {
		targets := make(map[timeRange][]target)
		for r := range shape.ranges {
			tr := timeRange{1700000000 - 3600 - int32(r*86400), 1700000000 - int32(r*86400)}
			for n := range shape.names {
				name := fmt.Sprintf("service.api.requests.by_host.app-%04d.by_status.200.count", n)
				targets[tr] = append(targets[tr], target{name, name})
			}
		}
		for _, canonical := range []bool{false, true} {
			b.Run(fmt.Sprintf("ranges%d/targets%d/canonical%v", shape.ranges, shape.names, canonical), func(b *testing.B) {
				l := NewCarbonserverListener(nil)
				l.SetQueryCacheCanonicalKeys(canonical)
				b.ReportAllocs()
				b.ResetTimer()
				var before, after syscall.Rusage
				_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
				for range b.N {
					renderKeySink, _ = l.getRenderCacheKeyAndSize(targets, protoV3Format.String())
				}
				_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
				b.StopTimer()
				cpuNS := after.Utime.Nano() + after.Stime.Nano() - before.Utime.Nano() - before.Stime.Nano()
				b.ReportMetric(float64(cpuNS)/float64(b.N), "cpu-ns/op")
			})
		}
	}
}
