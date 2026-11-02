//go:build !linux && !darwin

package chunkstore

import "syscall"

func incrementalBirthIdentity(string, *syscall.Stat_t) (string, error) { return "", nil }
