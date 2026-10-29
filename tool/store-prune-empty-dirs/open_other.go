//go:build !linux

package main

func openChildDir(parent int, name string, device uint64) (int, error) {
	return openChildSameDevice(parent, name, device)
}
