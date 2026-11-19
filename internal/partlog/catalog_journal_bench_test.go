package partlog

import (
	"fmt"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func BenchmarkCatalogJournalPoints(b *testing.B) {
	benchmarkCatalogJournalPoints(b, false)
}

func BenchmarkCatalogJournalChangingPoints(b *testing.B) {
	benchmarkCatalogJournalPoints(b, true)
}

func benchmarkCatalogJournalPoints(b *testing.B, changing bool) {
	c := newTestCatalog(b)
	ids := make([]uint64, 4096)
	data := make([]points.Point, len(ids))
	for i := range ids {
		var err error
		ids[i], err = c.Intern(fmt.Sprintf("metrics/system/component/%d/requests", i), nil)
		if err != nil {
			b.Fatal(err)
		}
		data[i] = points.Point{Timestamp: 1794800000, Value: float64(i) / 7}
	}
	e := CatalogEncoder{Catalog: c, SeriesClocks: true, SparseClocks: true}
	defer e.Close()
	if _, err := e.EncodePoints(ids, data, 1794800000123456789); err != nil {
		b.Fatal(err)
	}
	var size int
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if changing {
			for j := range data {
				data[j].Timestamp = 1794800000 + int64(i)
				data[j].Value = float64(i+j) / 7
			}
		}
		buf, err := e.EncodePoints(ids, data, int64(1794800000123456789+i))
		if err != nil {
			b.Fatal(err)
		}
		size += len(buf)
	}
	b.ReportMetric(float64(size)/float64(b.N*len(ids)), "journal-B/point")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(ids)), "ns/point")
}
