package carbon

import "golang.org/x/sys/unix"

func availableMemory() (uint64, error) {
	return unix.SysctlUint64("hw.memsize")
}
