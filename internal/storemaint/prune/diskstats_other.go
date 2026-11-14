//go:build !linux

package prune

import "errors"

func diskStats([]string) (map[string]diskStat, error) {
	return nil, errors.New("auto mode requires Linux /proc/diskstats; use a fixed rate on this platform")
}
