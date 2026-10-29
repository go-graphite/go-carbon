package chunkstore

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// walDirMarker records where the store last kept its write-ahead logs. Pebble
// replays WALs only from its configured WAL directory, so moving the WAL needs
// to know where the unflushed logs are: a log left behind in the store
// directory fails Open, and one left in a former WAL directory is never even
// listed, losing its writes silently.
const walDirMarker = "WALDIR"

// moveWAL makes target the store's WAL directory. When the logs still live
// elsewhere, it opens the store once with the old location, flushes every
// memtable so no log holds unflushed writes, closes it and removes the logs
// before recording the new location. opts must be the options the store is
// about to open with.
func moveWAL(fs vfs.FS, dir, target string, opts *pebble.Options) error {
	dir, target = filepath.Clean(dir), filepath.Clean(target)
	if target != dir {
		// The WAL usually lives on its own volume. Refuse to create it: an
		// unmounted volume would otherwise put the WAL on the root filesystem.
		if _, err := fs.Stat(target); err != nil {
			return fmt.Errorf("WAL directory %s must exist on the WAL volume: %w", target, err)
		}
	}
	previous, err := readWALDir(fs, dir)
	if err != nil {
		return err
	}
	if previous == target {
		return nil
	}
	logs, err := walLogs(fs, previous)
	if err != nil {
		return err
	}
	if len(logs) > 0 {
		old := opts.Clone()
		old.WALDir = walDirOption(dir, previous)
		db, err := pebble.Open(dir, old)
		if err != nil {
			return fmt.Errorf("replay WAL from %s before moving it: %w", previous, err)
		}
		if err := errors.Join(db.Flush(), db.Close()); err != nil {
			return fmt.Errorf("flush WAL from %s before moving it: %w", previous, err)
		}
		// Every log is now obsolete, including the empty one the flush opened.
		if logs, err = walLogs(fs, previous); err != nil {
			return err
		}
		for _, name := range logs {
			if err := fs.Remove(fs.PathJoin(previous, name)); err != nil {
				return err
			}
		}
	}
	return writeStoreMarker(fs, dir, walDirMarker, target)
}

// walDirOption is the Pebble WALDir for a WAL in path: empty for the store
// directory itself.
func walDirOption(dir, path string) string {
	if filepath.Clean(path) == filepath.Clean(dir) {
		return ""
	}
	return path
}

func readWALDir(fs vfs.FS, dir string) (string, error) {
	value, err := readStoreMarker(fs, dir, walDirMarker)
	if err != nil || value == "" {
		return dir, err
	}
	return filepath.Clean(value), nil
}

func walLogs(fs vfs.FS, path string) ([]string, error) {
	names, err := fs.List(path)
	if err != nil {
		if errors.Is(err, io.EOF) || isNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var logs []string
	for _, name := range names {
		if strings.HasSuffix(name, ".log") {
			logs = append(logs, name)
		}
	}
	return logs, nil
}
