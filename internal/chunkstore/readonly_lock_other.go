//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package chunkstore

import (
	"fmt"
	"github.com/cockroachdb/pebble/vfs"
	"io"
)

func lockExistingStoreFile(fs vfs.FS, name string) (io.Closer, error) {
	if _, ok := fs.(*vfs.MemFS); ok {
		return fs.Lock(name)
	}
	return nil, fmt.Errorf("read-only store locks are unsupported on this platform")
}
