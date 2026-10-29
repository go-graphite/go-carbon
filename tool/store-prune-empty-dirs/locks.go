package main

import (
	"errors"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// Only lock candidates need stat. The missing target, age and filename
// predicates are checked against GNU find -mmin +60 on Linux.
func staleLock(parent int, name string, cutoff time.Time, dry bool) (bool, bool, error) {
	if !strings.HasSuffix(name, ".lock") {
		return false, false, nil
	}
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ELOOP) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	defer unix.Close(fd)
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return false, false, nil
		}
		return false, false, err
	}
	var held, named, target unix.Stat_t
	if err := unix.Fstat(fd, &held); err != nil {
		return false, false, err
	}
	if held.Mode&unix.S_IFMT != unix.S_IFREG || held.Size != 0 || time.Unix(held.Mtim.Sec, held.Mtim.Nsec).After(cutoff) {
		return false, false, nil
	}
	if err := unix.Fstatat(parent, strings.TrimSuffix(name, ".lock"), &target, 0); err == nil {
		return false, false, nil
	} else if !errors.Is(err, unix.ENOENT) {
		return false, false, err
	}
	if err := unix.Fstatat(parent, name, &named, unix.AT_SYMLINK_NOFOLLOW); errors.Is(err, unix.ENOENT) {
		return false, false, nil
	} else if err != nil {
		return false, false, err
	}
	if named.Dev != held.Dev || named.Ino != held.Ino || named.Mode&unix.S_IFMT != unix.S_IFREG || named.Size != 0 || time.Unix(named.Mtim.Sec, named.Mtim.Nsec).After(cutoff) {
		return false, false, nil
	}
	if dry {
		return true, false, nil
	}
	err = unix.Unlinkat(parent, name, 0)
	if errors.Is(err, unix.ENOENT) {
		return false, false, nil
	}
	return err == nil, err == nil, err
}
