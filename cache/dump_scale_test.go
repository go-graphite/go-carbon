package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// Opt-in: SCALE_METRICS=26000000 dumps a production-sized cache (12 points per
// metric) through the segmented writer and reports the wall time.
func TestScaleDump(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("SCALE_METRICS"))
	if n == 0 {
		t.Skip("set SCALE_METRICS")
	}
	c := New()
	c.SetMaxSize(0)
	for i := 0; i < n; i++ {
		p := &points.Points{Metric: fmt.Sprintf("general.tuning.secondly.per_persona.svc%04d.summary.az.zone%d.status.counts.m%08d", i%5000, i%7, i)}
		for j := 0; j < 12; j++ {
			p.Data = append(p.Data, points.Point{Value: float64(i + j), Timestamp: 1700000000 + int64(j*10)})
		}
		c.Add(p)
	}
	dir := t.TempDir()
	segments := ShardCount
	b := recovery.NewConcurrentBuilder(nil, 16)
	b.Reserve(int(c.Len()))
	w, err := recovery.NewWriter(filepath.Join(dir, "cache.bin"), 0, 1<<20, b)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = w.WriteSegments(2*segments, 16, func(seg int, out *recovery.Segment) error {
		if seg < segments {
			return c.DumpPendingRange(seg, segments, out)
		}
		return c.DumpShards(seg-segments, seg-segments+1, out.WritePoints)
	})
	if err != nil {
		t.Fatal(err)
	}
	f, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("metrics=%d bytes=%d dump=%v", n, f.Size, time.Since(start))
}
