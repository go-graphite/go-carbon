//go:build !darwin && !linux

package chunkstore

import (
	"errors"

	"github.com/cockroachdb/pebble/vfs"
)

func checkpointBenchmarkNoCache(vfs.File) error {
	return errors.New("uncached checkpoint benchmark unsupported on this platform")
}
