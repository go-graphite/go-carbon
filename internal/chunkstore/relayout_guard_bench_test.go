package chunkstore

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func BenchmarkRelayoutPressure(b *testing.B) {
	s, err := Open("/store", Options{fs: vfs.NewMem(), RelayoutMinFreeBytes: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.waitRelayout(context.Background()); err != nil {
				b.Error(err)
			}
		}
	})
}
