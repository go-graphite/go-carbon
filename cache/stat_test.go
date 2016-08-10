package cache

import (
	"sync/atomic"
	"testing"
)

func BenchmarkCounterAdd(b *testing.B) {
	// run the Fib function b.N times
	var counter int64
	for n := 0; n < b.N; n++ {
		counter++
	}
}

func BenchmarkCounterAtomicAdd(b *testing.B) {
	// run the Fib function b.N times
	var counter int64
	for n := 0; n < b.N; n++ {
		atomic.AddInt64(&counter, 1)
	}
}

func BenchmarkCounterAtomicAddWithDelta(b *testing.B) {
	// run the Fib function b.N times
	var counter int64
	var delta int64
	for n := 0; n < b.N; n++ {
		delta++
		if delta > 10 {
			atomic.AddInt64(&counter, delta)
			delta = 0
		}
	}
}

func BenchmarkCounterAtomicAddWithDeltaAndCallback(b *testing.B) {
	// run the Fib function b.N times
	var counter int64
	var delta int64

	addCounter := func() {
		delta++
		if delta > 10 {
			atomic.AddInt64(&counter, delta)
			delta = 0
		}
	}

	for n := 0; n < b.N; n++ {
		addCounter()
	}
}
