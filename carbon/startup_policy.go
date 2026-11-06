package carbon

import (
	"math"

	"path/filepath"
	"runtime"

	"time"

	"github.com/go-graphite/go-carbon/internal/hostcapacity"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Ceilings are failure containment, not normal capacity targets. Shard count
// stays fixed for a capture epoch so metric ordering survives CPU changes.
type startupCapturePolicy struct {
	cpus, shards, readBuffer, writeBuffer                                          int
	available, recoveryBytes, recoveryPoints, spoolBytes, replayRate, segmentBytes int64
	limit                                                                          time.Duration
}

func startupPolicy(path string) startupCapturePolicy {
	// The shared sampler includes MemAvailable, Go memory limit, cgroup
	// siblings, and a physical reserve. Non-Linux local fixtures use host RAM.
	available := int64(1 << 30)
	if memory, valid := hostcapacity.ReadMemory(); valid {
		available = int64(min(memory, math.MaxInt64))
	} else if host, err := availableMemory(); err == nil {
		available = int64(min(host/8, math.MaxInt64))
	}

	free, _ := migrationDiskFree(filepath.Dir(path))
	return chooseStartupPolicy(runtime.GOMAXPROCS(0), available, int64(min(free, math.MaxInt64)))
}

func chooseStartupPolicy(cpus int, available, free int64) startupCapturePolicy {
	cpus = max(1, cpus)
	shards := 1
	// Two I/O streams per runnable core overlap decode/cache work and disk I/O.
	for shards < cpus*2 {
		shards *= 2
	}
	// Reserve most available memory for the restored cache and normal runtime.
	// Each reader owns bufio/name arenas and each appender owns a coalescing arena.
	for shards > 2 && available/int64(shards) < 64<<20 {
		shards /= 2
	}
	readBuffer := int(min(4<<20, max(64<<10, available/int64(shards)/128)))
	writeBuffer := int(min(2<<20, max(64<<10, available/int64(shards)/128)))
	recoveryBytes := max(16<<20, available/2)
	// Account conservatively for uncompressed values, name/item overhead and slack.
	recoveryPoints := max(1, recoveryBytes/32)
	spoolBytes := max(0, free/4)
	p := startupCapturePolicy{cpus: cpus, shards: shards, readBuffer: readBuffer, writeBuffer: writeBuffer, available: available, recoveryBytes: recoveryBytes, recoveryPoints: recoveryPoints, spoolBytes: spoolBytes, segmentBytes: max(1<<20, spoolBytes/1024), replayRate: int64(cpus) * 1250000, limit: 30 * time.Second}
	return p
}

func (p startupCapturePolicy) log() {
	zapwriter.Logger("app").Info("startup capture capacity selected",
		zap.Int("gomaxprocs", p.cpus), zap.Int("spool_shards", p.shards), zap.Int("parallel_readers", p.shards),
		zap.Int("reader_bytes", p.readBuffer), zap.Int("appender_buffer_bytes", p.writeBuffer),
		zap.Int64("available_memory_bytes", p.available), zap.Int64("recovery_headroom_bytes", p.recoveryBytes),
		zap.Int64("recovery_headroom_points", p.recoveryPoints), zap.Int64("initial_replay_points_per_second", p.replayRate), zap.Int64("spool_limit_bytes", p.spoolBytes),
		zap.Int64("segment_bytes", p.segmentBytes))
}

// Capture duration is an observation, never a reason to pause input. A
// nonpositive convergence margin means continued capture, represented by -1.
func captureTimeLimit(backlog, margin int64) time.Duration {
	if margin <= 0 {
		return -1
	}
	seconds := float64(max(0, backlog)) / float64(margin)
	if seconds > float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds * float64(time.Second))
}
