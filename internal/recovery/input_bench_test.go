package recovery

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/inputlog/loadgen"
	"github.com/go-graphite/go-carbon/points"
)

// BenchmarkInputFileWritePoints writes one-point records of loadgen names
// from parallel receivers, as diverted input does during a dump stop.
func BenchmarkInputFileWritePoints(b *testing.B) {
	mix := loadgen.New(100000, 1)
	for _, c := range []struct {
		size int
		f    Format
	}{{4 << 10, FormatLegacy}, {InputFileBuffer, FormatLegacy}, {InputFileBuffer, FormatCompact}} {
		b.Run(fmt.Sprintf("%v/buffer=%dKB", c.f, c.size>>10), func(b *testing.B) {
			w, err := NewWriterFormat(filepath.Join(b.TempDir(), InputName(c.f, "1.2.bin")), 1, c.size, NewBuilder(nil), c.f)
			if err != nil {
				b.Fatal(err)
			}
			b.RunParallel(func(pb *testing.PB) {
				p := points.Points{Data: make([]points.Point, 1)}
				for i := 0; pb.Next(); i++ {
					p.Metric = string(mix.Names[i%len(mix.Names)])
					p.Data[0] = points.Point{Value: float64(i), Timestamp: 1790000000}
					if err := w.WritePoints(&p); err != nil {
						b.Error(err)
						return
					}
				}
			})
			f, err := w.Close()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(f.Size)/float64(b.N), "B/pt")
		})
	}
}
