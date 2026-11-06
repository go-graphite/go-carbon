//go:build !linux && !darwin

package hostcapacity

import "errors"

func DiskSpace(string) (uint64, uint64, error) {
	return 0, 0, errors.New("disk capacity sampling unavailable")
}

func processCPU() float64 { return 0 }
