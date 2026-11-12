//go:build !linux

package recovery

func discardMappedPages([]byte) {}
