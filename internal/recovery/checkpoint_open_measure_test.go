package recovery

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// TestMeasureCheckpointOpen times OpenBundle over a compact checkpoint of
// production shape: a cache dump of the given number of metrics with 10-29
// points each, a tenth of them new to the read index, and input diverted for
// a quarter of them. Run it explicitly, with the metric count:
//
//	RECOVERY_MEASURE=11000000 go test -run TestMeasureCheckpointOpen -v ./internal/recovery/
func TestMeasureCheckpointOpen(t *testing.T) {
	metrics, _ := strconv.Atoi(os.Getenv("RECOVERY_MEASURE"))
	if metrics <= 0 {
		t.Skip("set RECOVERY_MEASURE=metrics to run")
	}
	dir, root := t.TempDir(), t.TempDir()
	builder := NewConcurrentBuilder(func() func(string) bool {
		return func(name string) bool { return name[len(name)-1] != '7' }
	}, 8)
	builder.Reserve(metrics)
	name := func(i int) string {
		return fmt.Sprintf("dc.cluster%03d.host%05d.service.metric_name_%d", i%300, i%40000, i)
	}
	started := time.Now()
	dump, err := NewWriterFormat(filepath.Join(dir, CacheName(FormatCompact, "1.2.bin")), 0, 1<<20, builder, FormatCompact)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := NewWriterFormat(filepath.Join(dir, InputName(FormatCompact, "1.2.bin")), 1, InputFileBuffer, builder, FormatCompact)
	if err != nil {
		t.Fatal(err)
	}
	const segments = 1024
	err = dump.WriteSegments(segments, 8, func(seg int, out *Segment) error {
		r := rand.New(rand.NewPCG(uint64(seg), 1))
		data := make([]points.Point, 0, 32)
		for i := seg; i < metrics; i += segments {
			data = data[:0]
			for j := range 10 + i%20 {
				data = append(data, points.Point{Timestamp: 1_790_000_000 + int64(j)*60, Value: float64(r.IntN(1000))})
			}
			if err := out.WriteCompact(name(i), AppendCompactPoints(nil, data), len(data)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cacheFile, err := dump.Close()
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := w; i < metrics/4; i += 8 {
				if err := wal.WritePoints(points.OnePoint(name(i*4), 1, 1_790_010_000)); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	index, err := WriteCompactIndex(dir, builder)
	if err != nil {
		t.Fatal(err)
	}
	walFile, err := wal.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = Publish(dir, root, cacheFile, walFile, index, "read-id"); err != nil {
		t.Fatal(err)
	}
	builder = nil
	t.Logf("checkpoint: cache %d MB, input %d MB, index %d MB, written in %v", cacheFile.Size>>20, walFile.Size>>20, index.Size>>20, time.Since(started).Round(time.Millisecond))
	var best time.Duration
	for range 5 {
		started = time.Now()
		b, err := OpenBundle(dir, root)
		if err != nil {
			t.Fatal(err)
		}
		took := time.Since(started)
		if best == 0 || took < best {
			best = took
		}
		t.Logf("OpenBundle %v: %d metrics, %d points, %d new", took.Round(time.Millisecond), b.Metrics(), b.Points(), b.NewNameCount())
		_ = b.Close()
	}
	t.Logf("best OpenBundle %v", best.Round(time.Millisecond))
}
