//go:build linux || darwin

package partsconvert

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

// SpaceGuard must be shared by parallel vshards on one filesystem. Reserve is
// conservative: existing staging bytes and complete in-flight reservations are
// both charged. MinFree is left available to the live writer and filesystem.
type SpaceGuard struct {
	Path     string
	MinFree  uint64
	mu       sync.Mutex
	reserved uint64
}

func (g *SpaceGuard) Reserve(n uint64) (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var fs unix.Statfs_t
	if err := unix.Statfs(g.Path, &fs); err != nil {
		return nil, err
	}
	available := uint64(fs.Bavail) * uint64(fs.Bsize)
	if available < g.MinFree || available-g.MinFree < g.reserved || available-g.MinFree-g.reserved < n {
		return nil, fmt.Errorf("conversion free-space guard: available=%d reserve=%d in-flight=%d required=%d", available, g.MinFree, g.reserved, n)
	}
	g.reserved += n
	var once sync.Once
	return func() { once.Do(func() { g.mu.Lock(); g.reserved -= n; g.mu.Unlock() }) }, nil
}
func lockDirectory(dir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("conversion journal locked: %w", err)
	}
	return func() { _ = unix.Flock(int(f.Fd()), unix.LOCK_UN); _ = f.Close() }, nil
}
