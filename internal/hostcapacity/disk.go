package hostcapacity

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type diskSample struct{ busy uint64 }

func (r *Sampler) diskSample(now time.Time) (bool, float64) {
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return false, 0
	}
	next := make(map[string]diskSample)
	valid, busy := !r.diskAt.IsZero() && now.After(r.diskAt), 0.
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 14 || strings.HasPrefix(fields[2], "loop") || strings.HasPrefix(fields[2], "ram") {
			continue
		}
		ticks, err := strconv.ParseUint(fields[12], 10, 64)
		if err != nil {
			return false, 0
		}
		next[fields[2]] = diskSample{busy: ticks}
		if previous, ok := r.disks[fields[2]]; valid && ok && ticks >= previous.busy {
			busy = max(busy, float64(ticks-previous.busy)/float64(now.Sub(r.diskAt)/time.Millisecond))
		}
	}
	if len(next) == 0 {
		valid = false
	}
	r.disks, r.diskAt = next, now
	return valid, min(1, busy)
}
