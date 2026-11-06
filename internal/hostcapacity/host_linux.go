package hostcapacity

import (
	"os"

	"github.com/go-graphite/go-carbon/internal/hostmemory"
)

func readHostSample() hostSample {
	cpu, err := os.ReadFile("/proc/stat")
	if err != nil {
		return hostSample{}
	}
	memory, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return hostSample{}
	}
	s := parseHostSample(string(cpu), string(memory))
	groups, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return hostSample{}
	}
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return hostSample{}
	}
	headroom, err := hostmemory.Headroom(string(groups), string(mounts), os.ReadFile)
	if err != nil {
		return hostSample{}
	}
	available, err := hostmemory.Available(s.available, string(groups), string(mounts), os.ReadFile)
	if err != nil {
		return hostSample{}
	}
	s.available = available
	s.memory = min(s.memory, headroom)
	return s
}
