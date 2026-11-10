// Package hostcapacity samples local pressure without shell commands or a metrics scrape.
package hostcapacity

import (
	"runtime"
	runtimemetrics "runtime/metrics"
	"time"
)

// Sample is local resource pressure; invalid observations never authorize growth.
type Sample struct {
	Valid, MemoryValid, DiskValid      bool
	Idle, IOWait, Memory, GC, DiskBusy float64
	Available                          uint64  // bytes left after host, Go and cgroup safety reserves
	ProcessCPU                         float64 // cumulative whole-process CPU seconds
}

type Sampler struct {
	previous hostSample
	gc       float64
	at       time.Time
	disks    map[string]diskSample
	diskAt   time.Time
}

func (r *Sampler) Sample(now time.Time) Sample {
	samples := []runtimemetrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/gc/gomemlimit:bytes"}, {Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"}}
	runtimemetrics.Read(samples)
	host := readHostSample()
	gc := samples[0].Value.Float64()
	s := Sample{Memory: host.memory, ProcessCPU: processCPU()}
	if host.valid && r.previous.valid && now.After(r.at) && host.total > r.previous.total && host.idle >= r.previous.idle && host.ioWait >= r.previous.ioWait {
		total := float64(host.total - r.previous.total)
		s.Valid = true
		s.Idle = float64(host.idle-r.previous.idle) / total
		s.IOWait = float64(host.ioWait-r.previous.ioWait) / total
		s.GC = max(0, gc-r.gc) / now.Sub(r.at).Seconds() / float64(max(1, runtime.GOMAXPROCS(0)))
	}
	limit := samples[1].Value.Uint64()
	used := samples[2].Value.Uint64() - samples[3].Value.Uint64()
	if limit > 0 {
		s.Memory = min(s.Memory, max(0, 1-float64(used)/float64(limit)))
	}
	s.Available = min(host.available, remaining(limit, used))
	s.MemoryValid = host.valid
	s.DiskValid, s.DiskBusy = r.diskSample(now)
	r.previous, r.gc, r.at = host, gc, now
	return s
}

type hostSample struct {
	total, idle, ioWait uint64
	available           uint64
	memory              float64
	valid               bool
}

// ReadMemory is also used by recovery before a persister is running. It does
// not start a goroutine or retain a cache/store pointer.
func ReadMemory() (available uint64, valid bool) {
	h := readHostSample()
	samples := []runtimemetrics.Sample{{Name: "/gc/gomemlimit:bytes"}, {Name: "/memory/classes/total:bytes"}, {Name: "/memory/classes/heap/released:bytes"}}
	runtimemetrics.Read(samples)
	used := samples[1].Value.Uint64() - samples[2].Value.Uint64()
	return min(h.available, remaining(samples[0].Value.Uint64(), used)), h.valid
}

func remaining(limit, used uint64) uint64 {
	reserve := max(uint64(64<<20), limit/20)
	if used >= limit || limit-used <= reserve {
		return 0
	}
	return limit - used - reserve
}
