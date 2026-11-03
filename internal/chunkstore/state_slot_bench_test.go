package chunkstore

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"

	"github.com/go-graphite/go-carbon/helper"
)

// BenchmarkStateSlotHeap measures retained slot capacity, including append
// growth slack, separately from records and the open-addressed index.
func BenchmarkStateSlotHeap(b *testing.B) {
	const count = 100_000
	b.ReportAllocs()
	for range b.N {
		c := &stateCache{}
		sh := &stateShard{freeSlot: -1}
		for range count {
			c.newSlot(sh)
		}
		b.ReportMetric(float64(cap(sh.slots))*float64(unsafe.Sizeof(stateSlot{}))/count, "retained-B/slot")
		runtime.KeepAlive(sh)
	}
}

// Hashes vary above bit 31 as well as below it, like the production hash.
func BenchmarkStateSlotLookup(b *testing.B) {
	const count = 16_384
	c := newStateCache(1<<30, nil)
	sh := &c.shards[0]
	names := make([]string, count)
	hashes := make([]uint64, count)
	for i := range names {
		names[i] = fmt.Sprintf("service.namespace.metric.%08d", i)
		hashes[i] = helper.HashString(names[i])
		c.put(sh, hashes[i], names[i], testState(names[i], uint64(i+1)), 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		n := i % count
		if _, slot := sh.find(hashes[n], names[n]); slot < 0 {
			b.Fatal("missing state")
		}
	}
}
