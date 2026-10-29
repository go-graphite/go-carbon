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
