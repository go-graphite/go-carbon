//go:build !darwin && !linux

package whisperio

func prependProcessCPU() int64 { return 0 }
