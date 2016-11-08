package cache

import (
	"io"
	"sync"
	"testing"
)

func BenchmarkXlogGetRWMutex(b *testing.B) {
	cache := struct {
		xlogMutex sync.RWMutex
		xlog      io.Writer
	}{}

	cnt := 0 // avoid optimizations

	for n := 0; n < b.N; n++ {
		cache.xlogMutex.RLock()
		xlog := cache.xlog
		cache.xlogMutex.RUnlock()

		if xlog != nil {
			cnt++
		}
	}

	if cnt != 0 {
		b.FailNow()
	}
}
