//go:build !linux

package prune

func openChildDir(parent int, name string, device uint64) (int, error) {
	return openChildSameDevice(parent, name, device)
}
