//go:build !linux && !darwin

package cache

// Retain the portable elapsed-time benchmarks on other platforms.
func compactCPUTime() int64 { return 0 }
