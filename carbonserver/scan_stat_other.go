//go:build !linux || !(amd64 || arm64)

package carbonserver

import "golang.org/x/sys/unix"

// scanStatPath stats path, which ends with a NUL byte.
func scanStatPath(dirfd int, path []byte, follow bool, st *unix.Stat_t) error {
	return scanStat(dirfd, string(path[:len(path)-1]), follow, st)
}
