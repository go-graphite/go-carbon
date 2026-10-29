//go:build linux

package main

import (
	"os"
)

func diskStats(devices []string) (map[string]diskStat, error) {
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return nil, err
	}
	return parseDiskStats(string(data), devices)
}
