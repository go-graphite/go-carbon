package downsize

import "os"

// Private evaluator evidence only. Read counters at the apply boundaries so
// fixture seeding and the expensive differential oracle cannot inflate its I/O.
func ingestSystemCounters() map[string]string {
	out := make(map[string]string)
	for _, path := range []string{"/proc/stat", "/proc/diskstats", "/proc/net/dev", "/proc/self/io", "/proc/self/stat"} {
		if b, err := os.ReadFile(path); err == nil {
			out[path] = string(b)
		}
	}
	return out
}
