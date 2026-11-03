//go:build darwin || linux

package inputlog

import (
	"fmt"
	"syscall"
	"testing"

	testseries "github.com/go-graphite/go-carbon/internal/testutil/series"
)

// Warm dictionary and reusable row/frame scratch isolate staging consumption
// plus the unchanged durable encoder from compression, filesystem and sync.
func BenchmarkStagingEncodeCPU(b *testing.B) {
	batch := slimEncodeFixture()
	var enc streamEncoder
	var w streamScratch
	enc.begin(1)
	emit := func(raw []byte, _ int) []byte { return raw }
	slimConsumeBatch(&enc, &w, &batch)
	raw := enc.flush(&w, nil, emit)
	b.ReportAllocs()
	b.ResetTimer()
	before := slimEncodeCPUTime()
	for range b.N {
		slimConsumeBatch(&enc, &w, &batch)
		raw = enc.flush(&w, raw, emit)
	}
	elapsed := slimEncodeCPUTime() - before
	b.StopTimer()
	b.ReportMetric(float64(elapsed)/float64(b.N*1000000), "cpu-ns/point")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*1000000), "ns/point")
}

func slimEncodeFixture() Batch {
	var batch Batch
	for id := 0; id < 100000; id++ {
		ref := NewStreamRef(fmt.Sprintf("services.namespace.host-%08d.component.requests.value", id))
		kind := 0
		switch shape := id % 100; {
		case shape >= 90:
			kind = 3
		case shape >= 70:
			kind = 1
		case shape >= 50:
			kind = 2
		}
		for p := 0; p < 10; p++ {
			value := testseries.Value(kind, id%100, p+90)
			batch.Append(ref, value, 1700000000+int64(p)*60)
		}
	}
	return batch
}

func slimConsumeBatch(enc *streamEncoder, w *streamScratch, batch *Batch) { enc.addBatch(w, batch) }
func slimEncodeCPUTime() int64 {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return (int64(usage.Utime.Sec)+int64(usage.Stime.Sec))*1000000000 + (int64(usage.Utime.Usec)+int64(usage.Stime.Usec))*1000
}
