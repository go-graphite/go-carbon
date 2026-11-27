package namefp

import (
	"fmt"
	"strings"
	"testing"
)

var routeSink uint64

func BenchmarkRoute(b *testing.B) {
	for _, n := range []int{16, 64, 259, 1024} {
		name := strings.Repeat("namespace.metric.", (n+16)/17)[:n]
		b.Run(fmt.Sprintf("string/%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			var h uint64
			for i := 0; i < b.N; i++ {
				h = Route(name)
			}
			routeSink = h
		})
		raw := []byte(name)
		b.Run(fmt.Sprintf("bytes/%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			var h uint64
			for i := 0; i < b.N; i++ {
				h = Route(raw)
			}
			routeSink = h
		})
	}
}
