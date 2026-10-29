// Package whisperpath handles original-tree directories shared by creators and reclaim.
package whisperpath

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const Quarantine = ".ns-quota-trim"

// Create retries once when reclaim removed an empty parent between mkdir and
// create. Once a file exists, rmdir cannot remove its directory.
func Create[T any](dir string, mode os.FileMode, create func() (T, error)) (v T, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		err = os.MkdirAll(dir, mode)
		if err == nil {
			v, err = create()
		}
		if !errors.Is(err, os.ErrNotExist) {
			break
		}
	}
	return v, err
}

// OpenDir lets the kernel resolve the host prefix (e.g. Darwin /var), but
// refuses a symlink root. Descendants use pinned fds and O_NOFOLLOW.
func OpenDir(path string) (int, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return -1, err
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	return fd, nil
}

// RemoveDir never unlinks files or symlinks. Missing and occupied directories
// are normal when another reclaimer or creator wins the race.
func RemoveDir(parent int, name string) (bool, error) {
	err := unix.Unlinkat(parent, name, unix.AT_REMOVEDIR)
	if errors.Is(err, unix.ENOENT) {
		return false, nil
	}
	if errors.Is(err, unix.ENOTEMPTY) || errors.Is(err, unix.EEXIST) {
		return false, nil
	}
	return err == nil, err
}

// Prune removes empty ancestors below root after the caller has synced its
// unlinks. before can cancel a chain before each rmdir. Directory
// removal is best effort: a crash can leave empty directories for the next pass.
func Prune(root, dir string, before func() error) error {
	names, err := pruneNames(root, dir)
	if err != nil || len(names) == 0 {
		return err
	}
	fd, err := OpenDir(root)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return pruneAt(fd, names, before)
}

// PruneAt borrows an O_NOFOLLOW root descriptor, keeping repeated reclaim
// batches from reopening the same root. The caller retains and closes rootFD.
func PruneAt(rootFD int, root, dir string, before func() error) error {
	names, err := pruneNames(root, dir)
	if err != nil || len(names) == 0 {
		return err
	}
	return pruneAt(rootFD, names, before)
}

func pruneNames(root, dir string) ([]string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(root, dir)
	if err != nil {
		return nil, err
	}
	if rel == "." {
		return nil, nil
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("prune outside data root: %s", dir)
	}
	names := strings.Split(rel, string(filepath.Separator))
	for _, name := range names {
		if name == Quarantine {
			return nil, nil
		}
	}
	return names, nil
}

func pruneAt(rootFD int, names []string, before func() error) error {
	fds := []int{rootFD}
	defer func() {
		for _, fd := range fds[1:] {
			if fd >= 0 {
				unix.Close(fd)
			}
		}
	}()
	// The last component needs no open: unlinkat(AT_REMOVEDIR) itself refuses
	// symlinks. Open ancestor components relative to pinned fds.
	for _, name := range names[:len(names)-1] {
		next, err := unix.Openat(fds[len(fds)-1], name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if errors.Is(err, unix.ENOENT) {
			break
		}
		if err != nil {
			return err
		}
		fds = append(fds, next)
	}
	for i := len(fds) - 1; i >= 0; i-- {
		if before != nil {
			if err := before(); err != nil {
				return err
			}
		}
		// Release a removed child's directory fd before removing its own name;
		// otherwise the filesystem must keep the unlinked inode alive until close.
		if i+1 < len(fds) && fds[i+1] >= 0 {
			unix.Close(fds[i+1])
			fds[i+1] = -1
		}
		err := unix.Unlinkat(fds[i], names[i], unix.AT_REMOVEDIR)
		if errors.Is(err, unix.ENOTEMPTY) || errors.Is(err, unix.EEXIST) {
			return nil
		}
		if err != nil && !errors.Is(err, unix.ENOENT) {
			return err
		}
	}
	return nil
}
