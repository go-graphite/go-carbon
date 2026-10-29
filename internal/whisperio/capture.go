package whisperio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-graphite/go-carbon/internal/whisperpath"
	whisper "github.com/go-graphite/go-whisper"
)

// Capture copies one main file and its OOO sidecar while holding the same stable
// path and inode locks as the production writer. Reflinking avoids reading the
// whole metric under its writer lock, though dirty extents can still delay the
// clone. Parsing, OOO merging and syncing happen after the source locks are
// released. Separate metrics have separate capture times.
func Capture(ctx context.Context, source, destination string, requireReflink bool) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if source == destination {
		return errors.New("capture destination equals source")
	}
	parent := filepath.Dir(destination)
	resolvedSource, err := filepath.EvalSymlinks(source)
	if err != nil {
		return err
	}
	dir, err := whisperpath.Create(parent, 0750, func() (string, error) {
		resolvedParent, err := filepath.EvalSymlinks(parent)
		if err != nil {
			return "", err
		}
		if resolvedSource == filepath.Join(resolvedParent, filepath.Base(destination)) {
			return "", errors.New("capture destination aliases source")
		}
		return os.MkdirTemp(parent, ".capture-")
	})
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	copyPath := filepath.Join(dir, filepath.Base(destination))
	w, err := openCapture(ctx, source)
	if err != nil {
		return fmt.Errorf("lock capture source: %w", err)
	}
	err = cloneFile(w.File(), copyPath, requireReflink)
	var sidecar bool
	if err == nil && w.OutOfOrderPath() != "" {
		sidecar = true
		f, openErr := os.Open(w.OutOfOrderPath())
		if openErr != nil {
			err = openErr
		} else {
			err = errors.Join(cloneFile(f, whisper.OutOfOrderSidecarPath(copyPath), requireReflink), f.Close())
		}
	}
	err = errors.Join(err, w.Close())
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A completed capture is published only after both private files exist.
	// The destination must be owned by this converter and not served directly.
	for _, path := range []string{copyPath, whisper.OutOfOrderSidecarPath(copyPath)} {
		f, openErr := os.Open(path)
		if errors.Is(openErr, os.ErrNotExist) && path != copyPath {
			continue
		}
		if openErr != nil {
			return openErr
		}
		if err := errors.Join(f.Sync(), f.Close()); err != nil {
			return err
		}
	}
	destinationSidecar := whisper.OutOfOrderSidecarPath(destination)
	if sidecar {
		if err := os.Rename(whisper.OutOfOrderSidecarPath(copyPath), destinationSidecar); err != nil {
			return err
		}
	} else if err := os.Remove(destinationSidecar); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(copyPath, destination); err != nil {
		return err
	}
	d, err := os.Open(parent)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
