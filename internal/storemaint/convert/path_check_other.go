//go:build !linux

package convert

func provePlainSourcePath(string) (bool, error) { return false, nil }
