package chunkstore

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"github.com/cockroachdb/pebble/vfs"
)

// readStoreMarker returns the trimmed contents of a small marker file in the
// store directory, or "" when it does not exist.
func readStoreMarker(fs vfs.FS, dir, name string) (string, error) {
	f, err := fs.Open(fs.PathJoin(dir, name))
	if err != nil {
		if isNotExist(err) {
			return "", nil
		}
		return "", err
	}
	data, err := io.ReadAll(io.LimitReader(f, 4096))
	if err := errors.Join(err, f.Close()); err != nil {
		return "", fmt.Errorf("read store marker %s: %w", name, err)
	}
	return strings.TrimSpace(string(data)), nil
}

// writeStoreMarker durably replaces a marker file: write a temporary file,
// sync it, rename it over the marker and sync the directory.
func writeStoreMarker(fs vfs.FS, dir, name, value string) error {
	temporary := fs.PathJoin(dir, name+".tmp")
	f, err := fs.Create(temporary)
	if err != nil {
		return err
	}
	_, writeErr := f.Write([]byte(value + "\n"))
	if err := errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		return fmt.Errorf("write store marker %s: %w", name, err)
	}
	if err := fs.Rename(temporary, fs.PathJoin(dir, name)); err != nil {
		return fmt.Errorf("replace store marker %s: %w", name, err)
	}
	return syncStoreDirectory(fs, dir)
}

func isNotExist(err error) bool { return errors.Is(err, fs.ErrNotExist) }
