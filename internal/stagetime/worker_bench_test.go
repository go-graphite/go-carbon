package stagetime

import (
	"context"
	"testing"
	"time"
)

func BenchmarkWorkerTiming(b *testing.B) {
	ctx := context.Background()
	var scratch Update
	b.ReportAllocs()
	for range b.N {
		start := time.Now()
		_, u := StartReusable(ctx, start, &scratch)
		for _, s := range []Stage{LockWait, CachePop, Route, LockWait, Route, Load, Decode} {
			u.Enter(s)
		}
		for range 3 {
			u.Unnest(Encode, u.Nested(Encode))
		}
		u.Enter(Encode)
		u.Enter(Commit)
		u.Enter(Publish)
		u.Enter(Acknowledge)
		u.FinishUpdate(start)
	}
}
