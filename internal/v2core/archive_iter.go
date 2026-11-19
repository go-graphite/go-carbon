package v2core

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// This is a local locator, never a portable descriptor or copy identity. Open
// rebinds it before recovery when a complete owner directory has moved.
var rollupDirectoryKey = []byte{255, 'V', '2', 'D', 0}

type archiveDirectoryContext struct{}

// WithRollupDirectory selects the local installation of a pinned manifest.
// This is useful for read-only copies; portable descriptors contain no paths.
func WithRollupDirectory(ctx context.Context, directory string) context.Context {
	return context.WithValue(ctx, archiveDirectoryContext{}, directory)
}

// archiveIter merges the legacy LSM namespace and descriptor-published immutable
// originals. No row is inserted into the LSM. Iterator ownership pins all file
// handles until Close; publication is determined exclusively by the snapshot.
type archiveCursor interface {
	First() bool
	SeekGE([]byte) bool
	SeekLT([]byte) bool
	Next() bool
	Valid() bool
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

type archiveIter struct {
	iterators  []archiveCursor
	selected   int
	reverse    bool
	err        error
	keyScratch []byte
}

// archiveFile also detects a failed table open in any external level. The
// vendored external iterator may overwrite an earlier level-open error.
type archiveFile struct {
	*os.File
	closed  bool
	statErr error
}

func (f *archiveFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	return f.File.Close()
}
func (f *archiveFile) Stat() (os.FileInfo, error) {
	info, err := f.File.Stat()
	if err != nil {
		f.statErr = err
	}
	return info, err
}

type archivePartIdentity struct {
	group v2part.Group
	part  v2part.PartID
}

func newArchiveIter(ctx context.Context, snapshot *pebble.Snapshot, opts *pebble.IterOptions) (*archiveIter, error) {
	return newArchiveIterSelected(ctx, snapshot, opts, nil)
}
func newArchiveIterSelected(ctx context.Context, snapshot *pebble.Snapshot, opts *pebble.IterOptions, selected map[archivePartIdentity]bool, exact ...v2part.Descriptor) (_ *archiveIter, err error) {
	if parts, _ := ctx.Value(openedPartsContext{}).(*OpenedRollupParts); parts != nil {
		if len(exact) > 0 {
			selected = make(map[archivePartIdentity]bool, len(exact))
			for _, d := range exact {
				selected[archivePartIdentity{d.Scope.Group, d.Scope.Part}] = true
			}
		}
		if selected != nil {
			return parts.archiveIter(ctx, snapshot, selected)
		}
	}
	out := &archiveIter{selected: -1}
	defer func() {
		if err != nil {
			_ = out.Close()
		}
	}()
	legacy, err := snapshot.NewIter(opts)
	if err != nil {
		return nil, err
	}
	var upper []byte
	if opts != nil {
		upper = opts.UpperBound
	}
	out.iterators = append(out.iterators, newHeaderCursor(legacy, upper))
	directory, _ := ctx.Value(archiveDirectoryContext{}).(string)
	if directory == "" {
		value, closer, e := snapshot.Get(rollupDirectoryKey)
		if errors.Is(e, pebble.ErrNotFound) {
			return out, nil
		}
		if e != nil {
			return nil, e
		}
		directory = string(value)
		if e = closer.Close(); e != nil {
			return nil, e
		}
	}
	var manifests *pebble.Iterator
	if len(exact) == 0 {
		manifests, err = snapshot.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
		if err != nil {
			return nil, err
		}
		defer manifests.Close()
	}
	var files [][]sstable.ReadableFile
	var opened []*archiveFile
	defer func() {
		if err != nil {
			for _, level := range files {
				for _, f := range level {
					_ = f.Close()
				}
			}
		}
	}()
	// Directory loading already validated exact descriptors against its pinned
	// inventory. Reuse them instead of decoding the whole manifest per new file.
	for n := 0; ; n++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		var d v2part.Descriptor
		if manifests == nil {
			if n >= len(exact) {
				break
			}
			d = exact[n]
		} else {
			valid := false
			if n == 0 {
				valid = manifests.First()
			} else {
				valid = manifests.Next()
			}
			if !valid {
				break
			}
			var e error
			d, e = v2part.ParseDescriptor(manifests.Value())
			if e != nil {
				return nil, e
			}
			if !bytes.Equal(manifests.Key()[len(sealedPartPrefix):], d.SHA256[:]) {
				return nil, errors.New("archive descriptor binding mismatch")
			}
		}
		if selected != nil && !selected[archivePartIdentity{d.Scope.Group, d.Scope.Part}] {
			continue
		}
		// Open only files whose immutable key bounds intersect the requested range.
		if opts != nil && ((opts.LowerBound != nil && bytes.Compare(d.LastKey, opts.LowerBound) < 0) || (opts.UpperBound != nil && bytes.Compare(d.FirstKey, opts.UpperBound) >= 0)) {
			continue
		}
		path := filepath.Join(directory, "parts", hex.EncodeToString(d.SHA256[:])+".sst")
		original, e := os.Open(path)
		if e != nil {
			return nil, e
		}
		f := &archiveFile{File: original}
		opened = append(opened, f)
		stat, e := f.Stat()
		if e != nil || uint64(stat.Size()) != d.Bytes {
			_ = f.Close()
			if e != nil {
				return nil, e
			}
			return nil, errors.New("archive part size mismatch")
		}
		files = append(files, []sstable.ReadableFile{f})
	}
	if manifests != nil {
		if err = manifests.Error(); err != nil {
			return nil, err
		}
	}
	if len(files) > 0 {
		external, e := pebble.NewExternalIterWithContext(ctx, (&pebble.Options{}).EnsureDefaults(), opts, files)
		// A failed reader closes its file. Check every level even if the
		// external constructor returned success, and retain cleanup ownership
		// until every input has been accepted.
		if e == nil {
			for _, f := range opened {
				if f.closed || f.statErr != nil {
					e = errors.New("archive table initialization failed")
					break
				}
			}
		}
		if e != nil {
			if external != nil {
				_ = external.Close()
			}
			return nil, e
		}
		files = nil
		out.iterators = append(out.iterators, newHeaderCursor(external, upper))
	}
	return out, nil
}
func (i *archiveIter) selectKey(reverse bool) bool {
	i.selected = -1
	i.reverse = reverse
	for n, it := range i.iterators {
		if !it.Valid() {
			continue
		}
		if i.selected < 0 {
			i.selected = n
			continue
		}
		other := i.iterators[i.selected]
		c := bytes.Compare(it.Key(), other.Key())
		if c == 0 && !bytes.Equal(it.Value(), other.Value()) {
			i.err = errors.New("conflicting immutable archive key")
			i.selected = -1
			return false
		}
		if (!reverse && c < 0) || (reverse && c > 0) {
			i.selected = n
		}
	}
	return i.selected >= 0
}
func (i *archiveIter) First() bool {
	for _, it := range i.iterators {
		it.First()
	}
	return i.selectKey(false)
}
func (i *archiveIter) SeekGE(key []byte) bool {
	for _, it := range i.iterators {
		it.SeekGE(key)
	}
	return i.selectKey(false)
}
func (i *archiveIter) SeekLT(key []byte) bool {
	for _, it := range i.iterators {
		it.SeekLT(key)
	}
	return i.selectKey(true)
}
func (i *archiveIter) Next() bool {
	if !i.Valid() {
		return false
	}
	i.keyScratch = append(i.keyScratch[:0], i.Key()...)
	key := i.keyScratch
	for _, it := range i.iterators {
		if i.reverse {
			it.SeekGE(key)
		}
		if it.Valid() && bytes.Equal(it.Key(), key) {
			it.Next()
		}
	}
	return i.selectKey(false)
}
func (i *archiveIter) Valid() bool   { return i.selected >= 0 && i.err == nil }
func (i *archiveIter) Key() []byte   { return i.iterators[i.selected].Key() }
func (i *archiveIter) Value() []byte { return i.iterators[i.selected].Value() }
func (i *archiveIter) Error() error {
	err := i.err
	for _, it := range i.iterators {
		err = errors.Join(err, it.Error())
	}
	return err
}
func (i *archiveIter) Close() error {
	var err error
	for _, it := range i.iterators {
		err = errors.Join(err, it.Close())
	}
	i.iterators = nil
	return err
}
