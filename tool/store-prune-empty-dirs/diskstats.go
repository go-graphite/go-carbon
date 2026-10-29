package main

import (
	"fmt"
	"strconv"
	"strings"
)

func parseDiskStats(data string, devices []string) (map[string]diskStat, error) {
	wanted := make(map[string]bool)
	for _, name := range devices {
		wanted[name] = true
	}
	result := make(map[string]diskStat)
	for _, line := range strings.Split(data, "\n") {
		f := strings.Fields(line)
		if len(f) < 14 || !wanted[f[2]] {
			continue
		}
		values := make([]float64, len(f)-3)
		for i, field := range f[3:] {
			n, err := strconv.ParseUint(field, 10, 64)
			if err != nil {
				return nil, err
			}
			values[i] = float64(n)
		}
		stat := diskStat{operations: values[0] + values[4], millis: values[3] + values[7], busy: values[9]}
		if len(values) >= 15 {
			stat.operations += values[11]
			stat.millis += values[14]
		}
		if len(values) >= 17 {
			stat.operations += values[15]
			stat.millis += values[16]
		}
		result[f[2]] = stat
	}
	for name := range wanted {
		if _, ok := result[name]; !ok {
			return nil, fmt.Errorf("diskstats device %q missing", name)
		}
	}
	return result, nil
}
