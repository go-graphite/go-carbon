package persister

import (
	"slices"

	"github.com/go-graphite/go-carbon/internal/lockset"
)

// lockStoreMetrics reserves exact names through pop, write and confirmation.
// Stripes protect only the reservation maps, never storage reads or encoding.
// Acquiring the complete set before reserving avoids overlapping-batch deadlocks.
func (p *Whisper) lockStoreMetrics(names []string) func() {
	stripes := make([]uint64, 0, len(names))
	for _, name := range names {
		stripes = append(stripes, storeMutexIndex(name))
	}
	slices.Sort(stripes)
	stripes = slices.Compact(stripes)
	unlock := func() {
		for i := len(stripes) - 1; i >= 0; i-- {
			p.storeMutex[stripes[i]].Unlock()
		}
	}
	for {
		lockset.Lock(p.storeMutex[:], stripes)
		var pending chan struct{}
		for _, name := range names {
			if pending = p.storePending[storeMutexIndex(name)][name]; pending != nil {
				break
			}
		}
		if pending == nil {
			break
		}
		unlock()
		<-pending
	}
	return p.reserveStoreMetricsLocked(names, stripes)
}

// reserveStoreMetricsLocked exchanges owned stripes for exact-name ownership.
func (p *Whisper) reserveStoreMetricsLocked(names []string, stripes []uint64) func() {
	unlock := func() {
		for i := len(stripes) - 1; i >= 0; i-- {
			p.storeMutex[stripes[i]].Unlock()
		}
	}
	done := make(chan struct{})
	for _, name := range names {
		stripe := storeMutexIndex(name)
		if p.storePending[stripe] == nil {
			p.storePending[stripe] = make(map[string]chan struct{})
		}
		p.storePending[stripe][name] = done
	}
	unlock()
	return func() {
		lockset.Lock(p.storeMutex[:], stripes)
		for _, name := range names {
			delete(p.storePending[storeMutexIndex(name)], name)
		}
		unlock()
		close(done)
	}
}
