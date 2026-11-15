package stagetime

import (
	"context"
	"testing"
	"time"
)

// Model an eight-metric batch, including three nested codec operations per metric.
func BenchmarkEightMetricBatchTiming(b *testing.B) {
	ctx := context.Background()
	var scratch Update
	b.ReportAllocs()
	for range b.N {
		start := time.Now()
		_, u := StartReusable(ctx, start, &scratch)
		u.Enter(LockWait)
		u.Enter(CachePop)
		for range 8 {
			u.Enter(Route)
			u.Enter(LockWait)
			u.Enter(Route)
			u.Enter(Load)
			u.Enter(Decode)
			for range 3 {
				u.Unnest(Encode, u.Nested(Encode))
			}
			u.Enter(Encode)
		}
		u.Enter(Commit)
		u.Enter(Publish)
		u.Enter(Acknowledge)
		u.FinishUpdate(start)
	}
}
