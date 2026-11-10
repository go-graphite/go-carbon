package carbonserver

import "github.com/go-graphite/go-carbon/internal/hostcapacity"

// SetCapacityReadLatency observes per-series storage fetch time, excluding
// glob expansion, response assembly/network sends and separate point-cache waits.
// Connect before Start; the persister can start before this listener.
func (listener *CarbonserverListener) SetCapacityReadLatency(reads *hostcapacity.Latency) {
	listener.capacityReadLatency = reads
}
