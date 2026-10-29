package main

import "golang.org/x/sys/unix"

func openChildSameDevice(parent int, name string, device uint64) (int, error) {
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		unix.Close(fd)
		return -1, err
	}
	if uint64(st.Dev) != device {
		unix.Close(fd)
		return -1, unix.EXDEV
	}
	return fd, nil
}
