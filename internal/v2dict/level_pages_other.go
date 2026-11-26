//go:build !linux

package v2dict

func releaseLevelPages([]byte) error { return nil }
