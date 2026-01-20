package helper

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkHashString(b *testing.B) {
	for _, n := range []int{16, 17, 32, 33, 64, 65, 96, 97, 128, 129, 240, 241} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			var acc uint64
			d := string(make([]byte, n))

			b.SetBytes(int64(n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				acc = HashString(d)
			}
			runtime.KeepAlive(acc)
		})
	}
}
