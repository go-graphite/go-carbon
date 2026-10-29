package persister

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const incrementalArchiveMarker = "INCREMENTAL-SOURCE-ARCHIVE.json"

type originalArchiveIdentity struct {
	Version int
	Path    string
	Device  uint64
	Inode   uint64
}

// SetIncrementalMigrationArchive is called before receivers/workers start.
// The policy is separate from writer state: active sources disappear after
// durable ownership, while an optional archive retains their original inodes.
func (p *Whisper) SetIncrementalMigrationArchive(path string) error {
	if p.incrementalStore == nil {
		return errors.New("original archive requires incremental migration")
	}
	marker := filepath.Join(filepath.Dir(p.incrementalScratch), incrementalArchiveMarker)
	if info, err := os.Lstat(marker); err == nil {
		if !info.Mode().IsRegular() || info.Size() > 4096 {
			return errors.New("invalid original archive policy file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data, err := os.ReadFile(marker)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if path == "" {
		if err == nil {
			return errors.New("original archive is bound to this store; retain store-migration-archive-dir")
		}
		return nil
	}
	if !filepath.IsAbs(path) {
		return errors.New("original archive requires an absolute directory")
	}
	if err := os.MkdirAll(path, 0750); err != nil {
		return err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	for _, root := range []string{p.rootPath, filepath.Dir(p.incrementalScratch)} {
		root, err := filepath.EvalSymlinks(root)
		if err != nil {
			return err
		}
		for _, pair := range [][2]string{{root, path}, {path, root}} {
			relative, err := filepath.Rel(pair[0], pair[1])
			if err != nil || relative == "." || filepath.IsLocal(relative) {
				return errors.New("original archive must be disjoint from source and shared storage")
			}
		}
	}
	source, err := os.Stat(p.rootPath)
	if err != nil {
		return err
	}
	archive, err := os.Stat(path)
	if err != nil {
		return err
	}
	src, srcOK := source.Sys().(*syscall.Stat_t)
	dst, dstOK := archive.Sys().(*syscall.Stat_t)
	if !srcOK || !dstOK || src.Dev != dst.Dev {
		return errors.New("original archive must be on the source filesystem")
	}
	want := originalArchiveIdentity{Version: 1, Path: path, Device: uint64(dst.Dev), Inode: uint64(dst.Ino)}
	// The archive root may itself have new parents. Sync through the common
	// source/archive ancestor before recording the policy or removing a source.
	boundary, err := filepath.EvalSymlinks(p.rootPath)
	if err != nil {
		return err
	}
	for {
		relative, err := filepath.Rel(boundary, path)
		if err != nil {
			return err
		}
		if relative == "." || filepath.IsLocal(relative) {
			break
		}
		boundary = filepath.Dir(boundary)
	}
	if err := syncOriginalArchiveParents(path, boundary); err != nil {
		return err
	}
	if data != nil {
		var got originalArchiveIdentity
		if err := json.Unmarshal(data, &got); err != nil || got != want {
			return errors.New("original archive directory identity differs from its store policy")
		}
	} else {
		data, err := json.Marshal(want)
		if err != nil {
			return err
		}
		file, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, writeErr := file.Write(data)
		if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
			return err
		}
		if err := syncReclaimedDirectory(filepath.Dir(marker), filepath.Dir(marker)); err != nil {
			return err
		}
	}
	p.incrementalArchive = path
	return nil
}

// Link and sync before unlinking the active name. A crash can leave both
// names; resumption accepts only the very same inode, never a replacement.
func (p *Whisper) archiveOriginal(source string, info os.FileInfo) error {
	relative, err := filepath.Rel(p.rootPath, source)
	if err != nil || !filepath.IsLocal(relative) {
		return errors.New("noncanonical original archive path")
	}
	target := filepath.Join(p.incrementalArchive, relative)
	if err := os.MkdirAll(filepath.Dir(target), 0750); err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(target))
	if err != nil || parent != filepath.Dir(target) {
		return errors.New("original archive namespace contains a symbolic link")
	}
	if err := os.Link(source, target); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		archived, err := os.Lstat(target)
		if err != nil || !archived.Mode().IsRegular() || !os.SameFile(info, archived) {
			return fmt.Errorf("refuse to replace an existing archived original: %s", target)
		}
	}
	file, err := os.Open(target)
	if err != nil {
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	// Every newly-created parent entry must be durable before the source name
	// is removed, including deeper namespace directories and the archive root.
	return syncOriginalArchiveParents(filepath.Dir(target), p.incrementalArchive)
}

func syncOriginalArchiveParents(path, root string) error {
	for directory := path; ; directory = filepath.Dir(directory) {
		if err := syncReclaimedDirectory(directory, directory); err != nil {
			return err
		}
		if directory == root {
			return nil
		}
	}
}
