package cache

import (
	"bufio"
	"fmt"
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
)

type NopWriter struct {
}

func (w *NopWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func fillCacheForDump() *Cache {
	metrics := 1000000
	pointsCount := 5

	c := New()
	c.SetMaxSize(uint32(pointsCount*metrics + 1))

	baseTimestamp := time.Now().Unix()

	for i := 0; i < metrics; i++ {
		for j := 0; j < pointsCount; j++ {
			c.Add(points.OnePoint(
				fmt.Sprintf("carbon.localhost.cache.size%d", i),
				42.15*float64(j),
				baseTimestamp+int64(j),
			))
		}
	}

	return c
}

func BenchmarkDump(b *testing.B) {
	c := fillCacheForDump()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w := bufio.NewWriterSize(&NopWriter{}, 1048576) // 1Mb
		c.Dump(w)
		w.Flush()
	}
}

func BenchmarkDumpBinary(b *testing.B) {
	c := fillCacheForDump()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w := bufio.NewWriterSize(&NopWriter{}, 1048576) // 1Mb
		c.DumpBinary(w)
		w.Flush()
	}

}
