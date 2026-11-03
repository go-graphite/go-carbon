// Package series supplies deterministic value shapes for codec benchmarks.
package series

import "math"

// Value returns a counter (0), hundredth-rounded gauge (1), stable gauge (2),
// or full-mantissa random float (3). Keeping these shapes shared makes memory
// and stream codec measurements comparable.
func Value(kind, id, epoch int) float64 {
	switch kind {
	case 0:
		return float64(id + epoch*100)
	case 1:
		return float64((id+epoch)%10000) / 100
	case 2:
		return float64(id % 100) // stable gauges
	default:
		// Deterministic full-mantissa random floats; no PRNG work in the benchmark.
		x := uint64(id) + uint64(epoch)*0x9e3779b97f4a7c15
		x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
		x = (x ^ (x >> 27)) * 0x94d049bb133111eb
		x ^= x >> 31
		return math.Float64frombits(x&((1<<52)-1) | 0x3ff0000000000000)
	}
}
