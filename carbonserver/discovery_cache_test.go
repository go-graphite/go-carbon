package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
)

func TestDiscoveryCacheTTL(t *testing.T) {
	listener := NewCarbonserverListener(nil)
	if err := listener.SetDiscoveryCacheTTLs(map[string]time.Duration{
		"aggregations.secondly._meta.":      15 * time.Second,
		"aggregations.secondly._meta.debug": 5 * time.Second,
		"other":                             time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		queries []string
		want    int32
	}{
		{nil, 300},
		{[]string{"aggregations.secondly._meta"}, 15},
		{[]string{"aggregations.secondly._meta.*.build_sha.*"}, 15},
		{[]string{"aggregations.secondly._meta.debug.*"}, 5},
		{[]string{"aggregations.secondly._metadata.*"}, 300},
		{[]string{"aggregations.*._meta.*"}, 300},
		{[]string{"aggregations.secondly.{_meta,other}.*"}, 300},
		{[]string{"aggregations.secondly._meta.*", "ordinary.*"}, 300},
		{[]string{"aggregations.secondly._meta.*", "other.*"}, 60},
	} {
		t.Run(fmt.Sprint(tt.queries), func(t *testing.T) {
			if got := listener.discoveryCacheTTL(tt.queries); got != tt.want {
				t.Fatalf("TTL=%d; want %d", got, tt.want)
			}
		})
	}
	for _, overrides := range []map[string]time.Duration{
		{"": time.Second}, {".meta": time.Second}, {"meta..child": time.Second},
		{"meta.*": time.Second}, {"meta.{one,two}": time.Second},
		{"meta": 0}, {"meta": -time.Second}, {"meta": time.Millisecond},
		{"meta": 1500 * time.Millisecond}, {"meta": time.Duration(math.MaxInt32+1) * time.Second},
		{"meta": time.Second, "meta.": 2 * time.Second},
	} {
		if err := listener.SetDiscoveryCacheTTLs(overrides); err == nil {
			t.Errorf("accepted invalid overrides: %v", overrides)
		}
		if got := listener.discoveryCacheTTL([]string{"aggregations.secondly._meta.*"}); got != 15 {
			t.Fatal("invalid rules replaced the previous configuration")
		}
	}
}

func TestDiscoveryCacheTTLRefreshesNewMetrics(t *testing.T) {
	for _, protocol := range []string{"http-json", "http-v2", "http-v3", "grpc", "render-globs"} {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/empty=%t", protocol, empty), func(t *testing.T) {
				t.Parallel()
				listener := newDiscoveryCacheTestListener(t)
				if err := listener.SetDiscoveryCacheTTLs(map[string]time.Duration{"aggregations.secondly._meta": time.Second}); err != nil {
					t.Fatal(err)
				}
				const normalQuery = "ordinary.*"
				if listener.insertRealtimeMetric(listener.CurrentFileIndex().trieIdx, "ordinary.old") == nil {
					t.Fatal("insert failed")
				}
				ctx, logger := context.Background(), zap.NewNop()
				initialNormal, _, err := listener.findResponse(ctx, logger, time.Now(), jsonFormat, "json", []string{normalQuery})
				if err != nil {
					t.Fatal(err)
				}
				query := "aggregations.secondly._meta.by_queue_and_host.*.*.build_sha.*"
				initialCount := 40
				if empty {
					query = "aggregations.secondly._meta.new_host.build_sha.*"
					initialCount = 0
				}
				format := jsonFormat
				if protocol == "http-v2" {
					format = protoV2Format
				}
				if protocol == "http-v3" {
					format = protoV3Format
				}
				fetch := func() (int, []byte, error) {
					switch protocol {
					case "grpc":
						r, err := listener.Find(ctx, &protov2.GlobRequest{Query: query})
						if r == nil {
							return 0, nil, err
						}
						return len(r.Matches), nil, err
					case "render-globs":
						r, _, err := listener.getExpandedGlobsWithCache(ctx, logger, "render", []string{query})
						if len(r) == 0 {
							return 0, nil, err
						}
						return len(r[0].Files), nil, err
					default:
						r, _, err := listener.findResponse(ctx, logger, time.Now(), format, format.String(), []string{query})
						if r == nil {
							return 0, nil, err
						}
						return r.files, r.data, err
					}
				}
				if got, _, _ := fetch(); got != initialCount {
					t.Fatalf("initial count=%d; want %d", got, initialCount)
				}
				name := "aggregations.secondly._meta.by_queue_and_host.region.worker-00.build_sha.fedcba"
				if empty {
					name = "aggregations.secondly._meta.new_host.build_sha.fedcba"
				}
				if listener.insertRealtimeMetric(listener.CurrentFileIndex().trieIdx, name) == nil {
					t.Fatal("insert failed")
				}
				if listener.insertRealtimeMetric(listener.CurrentFileIndex().trieIdx, "ordinary.new") == nil {
					t.Fatal("insert failed")
				}
				if got, _, _ := fetch(); got != initialCount {
					t.Fatalf("cache did not retain results within its TTL: %d", got)
				}
				time.Sleep(1100 * time.Millisecond)
				got, data, err := fetch()
				if err != nil || got != initialCount+1 {
					t.Fatalf("after expiry: count=%d, error=%v; want %d", got, err, initialCount+1)
				}
				// Compare serialized HTTP responses with the uncached implementation.
				if protocol != "grpc" && protocol != "render-globs" {
					listener.globCacheEnabled = false
					oracle, err := listener.findMetrics(ctx, logger, time.Now(), format, []string{query})
					listener.globCacheEnabled = true
					if err != nil || !bytes.Equal(data, oracle.data) {
						t.Fatalf("cached result differs from uncached result: %v", err)
					}
				}
				normal, hit, err := listener.findResponse(ctx, logger, time.Now(), jsonFormat, "json", []string{normalQuery})
				if err != nil || !hit || !bytes.Equal(normal.data, initialNormal.data) {
					t.Fatalf("unrelated cached query changed: hit=%t error=%v", hit, err)
				}
			})
		}
	}
}

func BenchmarkScopedDiscoveryCacheHit(b *testing.B) {
	benchmarkDiscoveryCacheHit(b, func(listener *CarbonserverListener) error {
		return listener.SetDiscoveryCacheTTLs(map[string]time.Duration{"aggregations.secondly._meta": 15 * time.Second})
	})
}
