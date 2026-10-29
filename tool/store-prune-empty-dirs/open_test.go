package main

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
)

func TestOpenChildRefusesOtherDevice(t *testing.T) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	var root, other unix.Stat_t
	if err := unix.Fstat(fd, &root); err != nil {
		t.Fatal(err)
	}
	if err := unix.Fstatat(fd, "dev", &other, 0); err != nil {
		t.Skip(err)
	}
	if root.Dev == other.Dev {
		t.Skip("no device boundary at /dev")
	}
	child, err := openChildDir(fd, "dev", uint64(root.Dev))
	if err == nil {
		unix.Close(child)
		t.Fatal("crossed the data-root device")
	}
	if !errors.Is(err, unix.EXDEV) {
		t.Fatal(err)
	}
}
