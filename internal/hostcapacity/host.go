package hostcapacity

import (
	"strconv"
	"strings"
)

func parseHostSample(cpu, memory string) hostSample {
	var s hostSample
	line, _, _ := strings.Cut(cpu, "\n")
	fields := strings.Fields(line)
	if len(fields) < 9 || fields[0] != "cpu" {
		return s
	}
	for i, field := range fields[1:9] { // guest time is already included in user
		n, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return hostSample{}
		}
		s.total += n
		if i == 3 {
			s.idle = n // I/O wait is pressure, never idle CPU to borrow
		}
		if i == 4 {
			s.ioWait = n
		}
	}
	var total, available uint64
	for _, line := range strings.Split(memory, "\n") {
		f := strings.Fields(line)
		if len(f) != 3 || f[2] != "kB" || f[0] != "MemTotal:" && f[0] != "MemAvailable:" {
			continue
		}
		n, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			return hostSample{}
		}
		if f[0] == "MemTotal:" {
			total = n
		} else {
			available = n
		}
	}
	s.valid = total > 0 && available <= total
	if s.valid {
		s.memory = float64(available) / float64(total)
		s.available = remaining(total*1024, (total-available)*1024)
	}
	return s
}
