package helper

import (
	"math"
	"math/rand"
	"runtime"
)

func CoinToss(p float64) bool {
	if p <= 0.0 {
		return false
	}
	if p >= 1.0 {
		return true
	}
	return float64(rand.Intn(math.MaxInt32))/float64(math.MaxInt32) < p
}

func FastwalkDefaultNumWorkers() int {
	numCPU := runtime.GOMAXPROCS(-1)
	if numCPU < 4 {
		return 4
	}
	// Darwin IO performance on APFS slows with more workers.
	// Stat performance is best around 2-4 and file IO is best
	// around 4-6. More workers only benefit CPU intensive tasks.
	if runtime.GOOS == "darwin" {
		if numCPU <= 8 {
			return 4
		}
		return 6
	}
	if numCPU > 32 {
		return 32
	}
	return numCPU
}
