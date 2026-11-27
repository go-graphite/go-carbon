package v2recv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

type pendingBenchOwner struct{}

func (pendingBenchOwner) Append([]string, []uint64, []points.Point, []uint64, int64) error {
	return nil
}
func (pendingBenchOwner) Seal() error  { return nil }
func (pendingBenchOwner) Drain() error { return nil }
func BenchmarkReceiverPendingBatches(b *testing.B) {
	for _, names := range []int{16, 1024} {
		b.Run(fmt.Sprintf("names%d", names), func(b *testing.B) {
			var payload strings.Builder
			for i := range 1024 {
				fmt.Fprintf(&payload, "pending.batch.metric.%04d 1.25 %d\n", i*names/1024, 1000+i)
			}
			wire := []byte(payload.String())
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			s, err := Start(l, []Owner{pendingBenchOwner{}}, Config{BatchSize: 1024, SyncInterval: time.Hour, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				b.Fatal(err)
			}
			c, err := net.Dial("tcp", l.Addr().String())
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(wire)))
			b.ResetTimer()
			for range b.N {
				if _, err = c.Write(wire); err != nil {
					b.Fatal(err)
				}
			}
			c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err = s.Wait(ctx); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if s.Stats().Accepted != uint64(b.N)*1024 {
				b.Fatal(s.Stats())
			}
		})
	}
}
