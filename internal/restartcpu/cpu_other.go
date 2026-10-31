//go:build !darwin && !linux

package restartcpu

// Process accounting is supported on the deployment platforms only.
func CPU() float64 { return 0 }
