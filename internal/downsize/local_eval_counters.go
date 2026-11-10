package downsize

import (
	"os"
	"runtime"
	"strconv"
	"time"
)

// These observations bracket only the candidate copy, excluding fixture seed,
// reference import and Fetch auditing. Disk/NIC counters are whole-host; process
// CPU and I/O belong to this private evaluator (including its Pebble workers).
func evalCopyCounters() map[string]string {
	result := map[string]string{"unix_nano": strconv.FormatInt(time.Now().UnixNano(), 10), "gomaxprocs": strconv.Itoa(runtime.GOMAXPROCS(0))}
	if runtime.GOOS != "linux" {
		return result
	}
	for _, name := range []string{"stat", "diskstats", "net/dev", "self/stat", "self/io", "self/status", "meminfo"} {
		data, err := os.ReadFile("/proc/" + name)
		if err != nil {
			result[name+"_error"] = err.Error()
		} else {
			result[name] = string(data)
		}
	}
	result["end_unix_nano"] = strconv.FormatInt(time.Now().UnixNano(), 10)
	return result
}
