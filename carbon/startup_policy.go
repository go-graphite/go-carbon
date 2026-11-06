package carbon

import (
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// Ceilings are failure containment, not normal capacity targets. Shard count
// stays fixed for a capture epoch so metric ordering survives CPU changes.
type startupCapturePolicy struct {
	cpus, shards, readBuffer, writeBuffer                            int
	available, recoveryBytes, recoveryPoints, spoolBytes, replayRate int64
	limit                                                            time.Duration
}

func startupPolicy(path string) startupCapturePolicy {
	// Linux MemAvailable includes reclaimable cache, unlike MemFree. The existing
	// host/cgroup budget is an upper bound. Other platforms use 1/8 of host RAM.
	available := int64(1 << 30)
	hostLimit := int64(math.MaxInt64)
	if host, err := availableMemory(); err == nil {
		hostLimit = int64(min(host, math.MaxInt64))
		available = hostLimit / 8
	}
	if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "MemAvailable:" {
				if kb, err := strconv.ParseInt(fields[1], 10, 53); err == nil {
					available = kb << 10
				}
			}
		}
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	available = min(available, max(0, hostLimit-int64(mem.Sys-mem.HeapReleased)))
	if limit := debug.SetMemoryLimit(-1); limit < math.MaxInt64 {
		available = min(available, max(0, limit-int64(mem.Sys-mem.HeapReleased)))
	}
	free, _ := migrationDiskFree(filepath.Dir(path))
	return chooseStartupPolicy(runtime.GOMAXPROCS(0), available, int64(min(free, math.MaxInt64)))
}

func chooseStartupPolicy(cpus int, available, free int64) startupCapturePolicy {
	cpus = max(1, cpus)
	shards := 1
	// Two I/O streams per runnable core overlap decode/cache work and disk I/O.
	for shards < min(64, cpus*2) {
		shards *= 2
	}
	// Reserve most available memory for the restored cache and normal runtime.
	// Each reader owns bufio/name arenas and each appender owns a coalescing arena.
	for shards > 2 && available/int64(shards) < 64<<20 {
		shards /= 2
	}
	readBuffer := int(min(4<<20, max(64<<10, available/int64(shards)/128)))
	writeBuffer := int(min(2<<20, max(64<<10, available/int64(shards)/128)))
	recoveryBytes := min(8<<30, max(16<<20, available/8))
	// Account conservatively for uncompressed values, name/item overhead and slack.
	recoveryPoints := max(1, recoveryBytes/32)
	spoolBytes := min(128<<30, max(0, free/8))
	p := startupCapturePolicy{cpus: cpus, shards: shards, readBuffer: readBuffer, writeBuffer: writeBuffer, available: available, recoveryBytes: recoveryBytes, recoveryPoints: recoveryPoints, spoolBytes: spoolBytes, replayRate: int64(cpus) * 1250000, limit: 30 * time.Second}
	return p
}

func (p startupCapturePolicy) log() {
	zapwriter.Logger("app").Info("startup capture capacity selected",
		zap.Int("gomaxprocs", p.cpus), zap.Int("spool_shards", p.shards), zap.Int("parallel_readers", p.shards),
		zap.Int("reader_bytes", p.readBuffer), zap.Int("appender_buffer_bytes", p.writeBuffer),
		zap.Int64("available_memory_bytes", p.available), zap.Int64("recovery_headroom_bytes", p.recoveryBytes),
		zap.Int64("recovery_headroom_points", p.recoveryPoints), zap.Int64("initial_replay_points_per_second", p.replayRate), zap.Int64("spool_limit_bytes", p.spoolBytes),
		zap.Duration("initial_capture_limit", p.limit), zap.Duration("absolute_capture_limit", 120*time.Second))
}

// A measured rate turns the available disk budget into an outage-time budget.
// The absolute two-minute ceiling prevents unlimited capture even on huge disks.
func captureTimeLimit(bytes, rate int64) time.Duration {
	if rate <= 0 {
		return 30 * time.Second
	}
	seconds := min(120.0, max(10.0, float64(bytes)/float64(rate)))
	return time.Duration(seconds * float64(time.Second))
}
