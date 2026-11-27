package whisperpath

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

// CreateAtomic publishes a fully initialized, synced file without replacing an
// existing one. A killed creator leaves only an unindexed temporary file; the
// final path remains absent and replay can create it again. Reopening gives the
// handle its final name, including for Whisper sidecars and path locks.
// The caller creates the parent and serializes writers for the target. Obsolete
// sidecars belong to a previous incarnation and are removed before publication.
func CreateAtomic[T io.Closer](path string, create, open func(string) (T, error), obsolete ...string) (zero T, err error) {
	dir, err := os.MkdirTemp(filepath.Dir(path), ".whisper-create-")
	if err != nil {
		return zero, err
	}
	defer os.RemoveAll(dir)
	tmp := filepath.Join(dir, "data") // No .wsp suffix: discovery must ignore it.
	w, err := create(tmp)
	if err != nil {
		return zero, err
	}
	if err := w.Close(); err != nil {
		return zero, err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return zero, err
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return zero, err
	}
	if _, err := os.Stat(path); err == nil {
		return open(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return zero, err
	}
	for _, sidecar := range obsolete {
		if err := os.Remove(sidecar); err != nil && !errors.Is(err, os.ErrNotExist) {
			return zero, err
		}
	}
	// Link is an atomic no-replace publication on the same filesystem. Another
	// creator's complete file wins; never truncate it with a rename.
	if err := os.Link(tmp, path); err != nil && !errors.Is(err, os.ErrExist) {
		return zero, err
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return zero, err
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		return zero, err
	}
	return open(path)
}
