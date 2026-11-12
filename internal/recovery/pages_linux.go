//go:build linux

package recovery

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// discardMappedPages drops resident PTEs for complete pages in an immutable,
// read-only file mapping. The mapping and backing file remain valid; future
// reads fault the same bytes back in. It does not discard anonymous memory or
// promise eviction of the kernel's reclaimable file cache. Never pass Go memory.
// This is a residency hint; an unsupported kernel leaves correctness unchanged.
func discardMappedPages(data []byte) {
	if len(data) == 0 {
		return
	}
	page := os.Getpagesize()
	address := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	start := int((uintptr(page) - address%uintptr(page)) % uintptr(page))
	end := len(data) - int((address+uintptr(len(data)))%uintptr(page))
	if end > start {
		_ = unix.Madvise(data[start:end], unix.MADV_DONTNEED)
	}
}
