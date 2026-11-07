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
	cpus, shards, readBuffer, writeBuffer                              int
	available, recoveryBytes, recoveryPoints, spoolBytes, segmentBytes int64
}

func startupPolicy(path string) startupCapturePolicy {
	// The shared sampler includes MemAvailable, Go memory limit, cgroup
	// siblings, and a physical reserve. Non-Linux local fixtures use host RAM.
	available := int64(0)
	if memory, valid := hostcapacity.ReadMemory(); valid {
		available = startupCapacityBytes(memory)
	} else if host, err := availableMemory(); runtime.GOOS != "linux" && err == nil {
		available = startupCapacityBytes(host / 8)
	}

	free, _ := migrationDiskFree(filepath.Dir(path))
	return chooseStartupPolicy(runtime.GOMAXPROCS(0), available, startupCapacityBytes(free))
}

// startupCapacityBytes bounds an unsigned capacity before converting it to the
// signed byte budgets used by the capture policy.
func startupCapacityBytes(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
}

func chooseStartupPolicy(cpus int, available, free int64) startupCapturePolicy {
	cpus = max(1, cpus)
	shards := 1
	// Bound socket fanout on large hosts. At 192 CPUs, 256 partitions turn
	// ordinary flushes into tiny compression frames and delay acknowledgements.
	// 64 leaves parallel capacity for both append and replay while coalescing
	// enough input per frame. Smaller hosts retain one FIFO per runnable core.
	for shards < min(cpus, 64) {
		shards *= 2
	}
	// Reserve most available memory for the restored cache and normal runtime.
	// Each reader owns decompression/name arenas and each appender owns a coalescing arena.
	for shards > 2 && (available/int64(shards) < 64<<20 || free/4/int64(shards) < 1<<20) {
		shards /= 2
	}
	readBuffer := int(min(4<<20, max(64<<10, available/int64(shards)/128)))
	writeBuffer := int(min(2<<20, max(64<<10, available/int64(shards)/128)))
	recoveryBytes := max(0, available/4)
	// Account conservatively for uncompressed values, name/item overhead and slack.
	recoveryPoints := max(1, recoveryBytes/64)
	spoolBytes := max(0, free/4)
	p := startupCapturePolicy{cpus: cpus, shards: shards, readBuffer: readBuffer, writeBuffer: writeBuffer, available: available, recoveryBytes: recoveryBytes, recoveryPoints: recoveryPoints, spoolBytes: spoolBytes, segmentBytes: max(1<<20, spoolBytes/1024)}
	return p
}

func (p startupCapturePolicy) log() {
	zapwriter.Logger("app").Info("startup capture capacity selected",
		zap.Int("gomaxprocs", p.cpus), zap.Int("spool_shards", p.shards), zap.Int("parallel_readers", p.shards),
		zap.Int("reader_bytes", p.readBuffer), zap.Int("appender_buffer_bytes", p.writeBuffer),
		zap.Int64("available_memory_bytes", p.available), zap.Int64("recovery_headroom_bytes", p.recoveryBytes),
		zap.Int64("recovery_headroom_points", p.recoveryPoints), zap.Int64("spool_limit_bytes", p.spoolBytes),
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
