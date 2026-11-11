package persister

import (
	"sync/atomic"
	"time"
)

// backendWriteStats accumulates committed writes for one backend. Unlike the
// periodic stats counters these never reset, so Prometheus rates them.
type backendWriteStats struct {
	updates, points, failures, nanos atomic.Uint64
}

func (s *backendWriteStats) observe(points int, took time.Duration, err error) {
	if err != nil {
		s.failures.Add(1)
		return
	}
	s.updates.Add(1)
	s.points.Add(uint64(points))
	s.nanos.Add(uint64(took.Nanoseconds()))
}

type persisterWriteStats struct {
	shared, original backendWriteStats
	transferRequeued [4]atomic.Uint64
}

// BackendWrites reports cumulative updates, points, failures and seconds spent
// writing to the shared store and to original whisper files.
type BackendWrites struct {
	Updates, Points, Failures uint64
	Seconds                   float64
}

func (s *backendWriteStats) snapshot() BackendWrites {
	return BackendWrites{Updates: s.updates.Load(), Points: s.points.Load(), Failures: s.failures.Load(), Seconds: float64(s.nanos.Load()) / 1e9}
}

func (p *Whisper) WriteStats() (pebble, whisper BackendWrites) {
	return p.writeStats.shared.snapshot(), p.writeStats.original.snapshot()
}

// Workers is the number of persister workers; dividing a stage's summed time by
// it estimates the wall time the stage takes per worker.
func (p *Whisper) Workers() int { return p.workersCount }

var transferFlushReasons = [...]string{"canceled", "deadline", "absent", "storage_error"}

// TransferFlushRequeues counts groups returned to the cache, once per group,
// only after all unconfirmed batches have been requeued. It is disjoint from
// write failures for expected request/absence errors; storage_error counts both.
func (p *Whisper) TransferFlushRequeues() map[string]uint64 {
	result := make(map[string]uint64, len(transferFlushReasons))
	for i, reason := range transferFlushReasons {
		result[reason] = p.writeStats.transferRequeued[i].Load()
	}
	return result
}

// NameTooLongDroppedPoints reports cumulative points discarded by name or path limits.
func (p *Whisper) NameTooLongDroppedPoints() uint64 { return p.nameTooLongTotal.Load() }
