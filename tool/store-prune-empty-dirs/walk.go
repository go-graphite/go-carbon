package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/whisperpath"
	"golang.org/x/sys/unix"
)

type counts struct {
	Passes, Dirs, EmptyDirs, RemovableDirs, Removed, Occupied, Files, Symlinks, Other, Excluded uint64
	LocksEligible, LocksRemoved                                                                 uint64
	Suffixes                                                                                    map[string]uint64
}

type walker struct {
	dispatch func(int, string, string) error
	maxDepth int
	rootDev  uint64
	cutoff   time.Time
	check    func() error
	buffers  [][]byte
	dry      bool
	exclude  map[string]bool
	counts   counts
	before   func() error
}

type dirReader struct {
	check        func() error
	fd           int
	buf          []byte
	offset, used int
}

// ReadDirent exposes getdents d_type on Linux (getdirentries on Darwin).
// Unknown types fail closed: even their classification would require stat.
func (r *dirReader) next() (string, byte, error) {
	for {
		if r.offset == r.used {
			if r.check != nil {
				if err := r.check(); err != nil {
					return "", 0, err
				}
			}
			n, err := unix.ReadDirent(r.fd, r.buf)
			if err != nil {
				return "", 0, err
			}
			if n == 0 {
				return "", 0, io.EOF
			}
			r.offset, r.used = 0, n
		}
		b := r.buf[r.offset:r.used]
		const nameOffset = int(unsafe.Offsetof(unix.Dirent{}.Name))
		if len(b) < nameOffset {
			return "", 0, errors.New("short directory record")
		}
		reclen := int(binary.NativeEndian.Uint16(b[unsafe.Offsetof(unix.Dirent{}.Reclen):]))
		if reclen <= nameOffset || reclen > len(b) {
			return "", 0, errors.New("invalid directory record length")
		}
		r.offset += reclen
		const inodeOffset = int(unsafe.Offsetof(unix.Dirent{}.Ino))
		var inode uint64
		switch unsafe.Sizeof(unix.Dirent{}.Ino) {
		case 4:
			inode = uint64(binary.NativeEndian.Uint32(b[inodeOffset:]))
		case 8:
			inode = binary.NativeEndian.Uint64(b[inodeOffset:])
		}
		if inode == 0 {
			continue
		}
		name := b[nameOffset:reclen]
		if i := bytes.IndexByte(name, 0); i >= 0 {
			name = name[:i]
		}
		if bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte("..")) {
			continue
		}
		// The name is borrowed until this directory reader refills its buffer.
		return unsafe.String(unsafe.SliceData(name), len(name)), b[unsafe.Offsetof(unix.Dirent{}.Type)], nil
	}
}

func (w *walker) walk(ctx context.Context, root string) error {
	fd, err := whisperpath.OpenDir(root)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	w.counts.Suffixes = make(map[string]uint64)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return err
	}
	w.rootDev = uint64(st.Dev)
	if w.cutoff.IsZero() {
		w.cutoff = time.Now().Add(-time.Hour)
	}
	for {
		w.counts.Passes++
		removed := w.counts.Removed
		_, err = w.visit(ctx, fd, -1, "", "", 0)
		if err != nil || w.dry || w.counts.Removed == removed {
			return err
		}
		// APFS directory cookies can skip entries after our own deletions. A
		// removal-free pass proves this walk did not invalidate its own listing.
		if _, err = unix.Seek(fd, 0, io.SeekStart); err != nil {
			return err
		}
	}
}

func (w *walker) visit(ctx context.Context, fd, parent int, name, rel string, depth int) (bool, error) {
	defer func() {
		if parent >= 0 && fd >= 0 {
			unix.Close(fd)
		}
	}()
	if depth > 512 {
		return false, errors.New("directory nesting exceeds 512")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if w.before != nil {
		if err := w.before(); err != nil {
			return false, err
		}
	}
	w.counts.Dirs++
	for len(w.buffers) <= depth {
		w.buffers = append(w.buffers, make([]byte, 32<<10))
	}
	r := dirReader{fd: fd, buf: w.buffers[depth], check: func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if w.check != nil {
			return w.check()
		}
		return nil
	}}
	empty, removable := true, true
	for {
		child, kind, err := r.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return false, err
		}
		empty = false
		var childRel string
		if len(w.exclude) > 0 || w.dispatch != nil {
			childRel = filepath.Join(rel, child)
		}
		if w.exclude[childRel] || child == whisperpath.Quarantine {
			w.counts.Excluded++
			removable = false
			continue
		}
		switch kind {
		case unix.DT_DIR:
			if w.dispatch != nil && depth+1 == 3 {
				if err := w.dispatch(fd, child, childRel); err != nil {
					return false, err
				}
				removable = false
				continue
			}
			// Empty leaves need no open/read/close and no unlinked inode kept alive
			// by an open fd. Occupied directories are still removed after children.
			if !w.dry {
				if w.before != nil {
					if err := w.before(); err != nil {
						return false, err
					}
				}
				removed, err := whisperpath.RemoveDir(fd, child)
				if errors.Is(err, unix.EBUSY) || errors.Is(err, unix.EXDEV) {
					w.counts.Excluded++
					removable = false
					continue
				}
				if errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.ELOOP) {
					removable = false
					continue
				}
				if err != nil {
					return false, err
				}
				if removed {
					w.counts.Dirs++
					w.counts.EmptyDirs++
					w.counts.RemovableDirs++
					w.counts.Removed++
					continue
				}
			}
			if w.maxDepth > 0 && depth+1 >= w.maxDepth {
				removable = false
				continue
			}
			if childRel == "" {
				childRel = filepath.Join(rel, child)
			}
			childFD, err := openChildDir(fd, child, w.rootDev)
			if errors.Is(err, unix.ENOENT) {
				continue
			}
			if errors.Is(err, unix.EXDEV) {
				w.counts.Excluded++
				removable = false
				continue
			}
			if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
				removable = false
				continue
			}
			if err != nil {
				return false, fmt.Errorf("open %s: %w", childRel, err)
			}
			gone, visitErr := w.visit(ctx, childFD, fd, child, childRel, depth+1)
			if visitErr != nil {
				return false, visitErr
			}
			removable = removable && gone
		case unix.DT_REG:
			if strings.HasSuffix(child, ".lock") {
				eligible, removed, err := staleLock(fd, child, w.cutoff, w.dry)
				if err != nil {
					return false, err
				}
				if eligible {
					w.counts.LocksEligible++
				}
				if removed {
					w.counts.LocksRemoved++
					continue
				}
			}
			w.counts.Files++
			w.counts.Suffixes[fileSuffix(child)]++
			removable = false
		case unix.DT_LNK:
			w.counts.Symlinks++
			removable = false
		case unix.DT_UNKNOWN:
			return false, fmt.Errorf("d_type unavailable for %s; refusing a stat-based walk", filepath.Join(rel, child))
		default:
			w.counts.Other++
			removable = false
		}
	}
	if empty {
		w.counts.EmptyDirs++
	}
	if removable && parent >= 0 {
		w.counts.RemovableDirs++
		if !w.dry {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if w.before != nil {
				if err := w.before(); err != nil {
					return false, err
				}
			}
			unix.Close(fd)
			fd = -1
			removed, err := whisperpath.RemoveDir(parent, name)
			if err != nil {
				return false, fmt.Errorf("rmdir %s: %w", rel, err)
			}
			if removed {
				w.counts.Removed++
			} else {
				w.counts.Occupied++
			}
			// A concurrent create may have made this subtree occupied. The parent's
			// rmdir is still authoritative; it can never delete that file.
		}
	}
	return removable, nil
}

func fileSuffix(name string) string {
	for _, suffix := range []string{".wsp.ooo.lock", ".wsp.lock", ".wsp.ooo", ".wsp"} {
		if strings.HasSuffix(name, suffix) {
			return suffix
		}
	}
	if ext := filepath.Ext(name); ext != "" {
		return strings.Clone(ext)
	}
	return "(none)"
}
