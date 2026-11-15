//go:build !linux

package main

import (
	"errors"

	"github.com/cockroachdb/pebble/vfs"
)

func coreWriteDirect(f vfs.File, data, scratch []byte) ([]byte, error) {
	return scratch, errors.New("direct SST output requires Linux")
}
