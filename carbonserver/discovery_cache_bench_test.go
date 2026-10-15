package carbonserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
)

func newDiscoveryCacheTestListener(tb testing.TB) *CarbonserverListener {
	tb.Helper()
	listener := NewCarbonserverListener(nil)
	listener.logger = zap.NewNop()
	listener.accessLogger = zap.NewNop()
	listener.trieIndex = true
	listener.maxMetricsGlobbed = 10000
	listener.findCacheEnabled = true
	listener.globCacheEnabled = true
	listener.UpdateFileIndex(&fileIndex{trieIdx: newTrie(".wsp", 0, nil)})
	for i := 0; i < 40; i++ {
		name := fmt.Sprintf("aggregations.secondly._meta.by_queue_and_host.region.worker-%02d.build_sha.abcdef", i)
		if listener.insertRealtimeMetric(listener.CurrentFileIndex().trieIdx, name) == nil {
			tb.Fatalf("could not insert %s", name)
		}
	}
	return listener
}

func BenchmarkDiscoveryCacheHit(b *testing.B) {
	benchmarkDiscoveryCacheHit(b, nil)
}

func benchmarkDiscoveryCacheHit(b *testing.B, configure func(*CarbonserverListener) error) {
	const query = "aggregations.secondly._meta.by_queue_and_host.*.*.build_sha.*"
	for _, handler := range []string{"http", "grpc", "glob"} {
		b.Run(handler, func(b *testing.B) {
			listener := newDiscoveryCacheTestListener(b)
			if configure != nil {
				if err := configure(listener); err != nil {
					b.Fatal(err)
				}
			}
			ctx, logger := context.Background(), zap.NewNop()
			fetch := func() error {
				switch handler {
				case "http":
					_, _, err := listener.findResponse(ctx, logger, time.Now(), jsonFormat, "json", []string{query})
					return err
				case "grpc":
					_, err := listener.Find(ctx, &protov2.GlobRequest{Query: query})
					return err
				default:
					_, _, err := listener.getExpandedGlobsWithCache(ctx, logger, "render", []string{query})
					return err
				}
			}
			if err := fetch(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := fetch(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
