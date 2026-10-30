//go:build !linux && !darwin

package carbon

import "fmt"

func availableMemory() (uint64, error) {
	return 0, fmt.Errorf("percentage memory-budget is supported on Linux and macOS; use bytes")
}
