package mergepass

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func cpuSeconds() float64 {
	var u syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
	return float64(u.Utime.Nano()+u.Stime.Nano()) / 1e9
}

// TestMergeFixtureRate reports names/s and process CPU-s per 1k names for the
// digest-gated and the no-digest pass. MERGEPASS_RATE=<names> enables it.
func TestMergeFixtureRate(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("MERGEPASS_RATE"))
	if n == 0 {
		t.Skip("set MERGEPASS_RATE=<names>")
	}
	for _, noDigest := range []bool{false, true} {
		target, donor := newStore(t), newStore(t)
		ctx := context.Background()
		for i := range n {
			name := fmt.Sprintf("fixture.service.%06d.cpu", i)
			config := store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 10, Count: 1024}}, AggregationMethod: store.Sum}
			for _, s := range []*store.Store{target, donor} {
				if _, err := s.Create(ctx, config); err != nil {
					t.Fatal(err)
				}
			}
			var old, recent []store.Point
			for j := range 600 { // donor history; the target holds the same history...
				old = append(old, store.Point{Timestamp: testNow - 10000 + int64(j)*10, Value: float64(i*j%97) + 0.5})
			}
			for j := range 300 { // ...plus points written after the cutover
				recent = append(recent, store.Point{Timestamp: testNow - 3000 + int64(j)*10, Value: float64(j)})
			}
			if err := donor.UpdateMany(ctx, name, old); err != nil {
				t.Fatal(err)
			}
			kept := old
			if i%10 == 0 { // 10% of names have a donor-only hole on the target
				kept = old[:450]
			}
			if err := target.UpdateMany(ctx, name, append(append([]store.Point(nil), kept...), recent...)); err != nil {
				t.Fatal(err)
			}
		}
		o := options(t, serve(t, target, nil), serve(t, donor, nil))
		o.NoDigest, o.Batch, o.Streams, o.Sample, o.Until = noDigest, 500, 4, 0, testNow-3500
		cpu, began := cpuSeconds(), time.Now()
		r, err := Run(ctx, o)
		if err != nil {
			t.Fatal(err)
		}
		seconds, used := time.Since(began).Seconds(), cpuSeconds()-cpu
		t.Logf("noDigest=%v names=%d done=%d pulled=%d identical=%d seconds=%.2f names/s=%.0f cpu_s_per_1k=%.3f", noDigest, n, r.Done, r.Pulled, r.Identical, seconds, float64(n)/seconds, used/float64(n)*1000)
	}
}
