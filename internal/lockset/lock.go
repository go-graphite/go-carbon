// Package lockset acquires mutation stripes without holding siblings while waiting.
package lockset

import "sync"

// Lock acquires every unique index. Callers must not already hold any index.
// The uncontended path takes one Lock and n-1 TryLocks. On conflict, release
// the acquired subset and wait only for the conflicting stripe, then retry
// starting there. No goroutine sleeps while owning an incomplete lock set.
// This also works alongside ordinary single-stripe writers and ordered lockers.
func Lock(mutexes []sync.Mutex, indices []uint64) {
	if len(indices) == 0 {
		return
	}
	first := 0
	for {
		mutexes[indices[first]].Lock()
		acquired := 1
		for acquired < len(indices) && mutexes[indices[(first+acquired)%len(indices)]].TryLock() {
			acquired++
		}
		if acquired == len(indices) {
			return
		}
		for j := acquired - 1; j >= 0; j-- {
			mutexes[indices[(first+j)%len(indices)]].Unlock()
		}
		first = (first + acquired) % len(indices)
	}
}

// TryLock acquires the complete set immediately, or releases the acquired
// prefix before returning false. Indices must be unique and not already held.
func TryLock(mutexes []sync.Mutex, indices []uint64) bool {
	for i, index := range indices {
		if !mutexes[index].TryLock() {
			for j := i - 1; j >= 0; j-- {
				mutexes[indices[j]].Unlock()
			}
			return false
		}
	}
	return true
}
