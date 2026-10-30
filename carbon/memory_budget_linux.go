package carbon

import (
	"os"

	"golang.org/x/sys/unix"
)

func availableMemory() (uint64, error) {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0, err
	}
	groups, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0, err
	}
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return 0, err
	}
	return cgroupMemory(uint64(info.Totalram)*uint64(info.Unit), string(groups), string(mounts), os.ReadFile)
}
