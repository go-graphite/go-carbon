package cache

import (
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkCounterAdd(b *testing.B) {
	var counter int64
	for n := 0; n < b.N; n++ {
		counter++
	}
}

func BenchmarkCounterAtomicAdd(b *testing.B) {
	var counter int64
	for n := 0; n < b.N; n++ {
		atomic.AddInt64(&counter, 1)
	}
}

func BenchmarkCounterMutexAdd(b *testing.B) {
	var counter int64
	var m sync.Mutex
	for n := 0; n < b.N; n++ {
		m.Lock()
		counter++
		m.Unlock()
	}
}

func BenchmarkCounterAtomicAddWithDelta(b *testing.B) {
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

type TestPeriodicSync struct {
	shared  uint32
	local   uint32
	changes uint32
}

func (s *TestPeriodicSync) Add(delta uint32) {
	s.local += delta
	s.changes++
	if s.changes > 10 {
		atomic.StoreUint32(&s.shared, s.local)
		s.changes = 0
	}
}

func BenchmarkCounterPeriodicSync(b *testing.B) {
	var t TestPeriodicSync

	for n := 0; n < b.N; n++ {
		t.Add(1)
	}
}
