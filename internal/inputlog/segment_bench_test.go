package inputlog

import (
	"fmt"
	"os"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func BenchmarkSegmentTaggedBatch(b *testing.B) {
	var records []*points.Points
	for i := range 8192 {
		records = append(records, points.OnePoint(fmt.Sprintf("metric.%03d;a=first;z=last", i%256), float64(i), int64(i%8)))
	}
	f, err := os.CreateTemp(b.TempDir(), "segment")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	w, err := NewSegmentWriter(f, 1)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err = w.Append(records, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*8192), "ns/point")
}
