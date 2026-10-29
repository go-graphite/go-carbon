package whisper

import (
	"io"
	"os"
)

// StorageFile is the random-access surface used by the Whisper state machine.
// External filesystems use their owning database transaction for synchronization;
// OS flock and the package-global in-memory filesystem cannot be combined with it.
type StorageFile interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	io.Seeker
	io.Closer
	Name() string
	Fd() uintptr
	Truncate(int64) error
}

// StorageFile returns the underlying storage without requiring an OS file.
func (w *Whisper) StorageFile() StorageFile { return w.file }

// StorageFileSystem scopes names, main-file rewrites and OOO sidecars to an
// externally owned transaction. This lets another storage engine preserve the
// compressed writer's behavior without keeping one operating-system file per
// metric. Implementations must keep open handles valid across Rename/Remove.
type StorageFileSystem interface {
	OpenFile(string, int, os.FileMode) (StorageFile, error)
	Stat(string) (os.FileInfo, error)
	Remove(string) error
	Rename(string, string) error
}

type StorageArchiveLayout struct {
	Offset, Step, Count, BlockSize, BlockCount int
}

type StorageFileLayout struct {
	Compressed       bool
	HeaderSize, Size int
	Archives         []StorageArchiveLayout
}

// StorageLayout describes physical regions independently of the host filesystem.
func (w *Whisper) StorageLayout() StorageFileLayout {
	layout := StorageFileLayout{Compressed: w.compressed, HeaderSize: w.MetadataSize(), Size: w.Size(),
		Archives: make([]StorageArchiveLayout, len(w.archives))}
	for i, a := range w.archives {
		layout.Archives[i] = StorageArchiveLayout{Offset: a.offset, Step: a.secondsPerPoint, Count: a.numberOfPoints}
		if w.compressed {
			layout.Archives[i].BlockSize, layout.Archives[i].BlockCount = a.blockSize, a.blockCount
		}
	}
	return layout
}

type StorageLayoutFile interface {
	NeedsStorageLayout() bool
	ConfigureStorage(StorageFileLayout) error
}

func (w *Whisper) configureStorageFile() error {
	if file, ok := w.file.(StorageLayoutFile); ok && file.NeedsStorageLayout() {
		return file.ConfigureStorage(w.StorageLayout())
	}
	return nil
}

// Typed block transports avoid reconstructing Gorilla bytes only to decode them
// again. The state machine retains its original encoder metadata and buffers.
type StorageBlockReader interface {
	ReadStorageBlock(ContinuationBlock) ([]TimeSeriesPoint, error)
}

type StorageBlockAppender interface {
	AppendStorageBlock(ContinuationBlock, int, []TimeSeriesPoint) error
}

// StorageBlockPatcher changes the points of a stored compressed block in place,
// so a correction writes only what it changes (see planInPlace). points are
// the block's complete new points. ReplaceStoragePoints changes the values at
// positions, ascending, whose timestamps stay; the block keeps its count.
// RewriteStorageBlock replaces the points from position from onward, block
// describing the result; the points before from stay. Both keep the block's
// layout. CanReplaceStoragePoints reports whether either may be used.
type StorageBlockPatcher interface {
	CanReplaceStoragePoints() bool
	ReplaceStoragePoints(block ContinuationBlock, positions []int, points []TimeSeriesPoint) error
	RewriteStorageBlock(block ContinuationBlock, from int, points []TimeSeriesPoint) error
}

// StorageExtentReader skips holes in virtual classic/OOO archives. Returned
// intervals contain complete records aligned relative to archiveStart.
type StorageExtentReader interface {
	NextStorageExtent(start, end, archiveStart int64) (int64, int64, error)
}

func statStorageFile(options *Options, path string) (os.FileInfo, error) {
	if options != nil && options.FileSystem != nil {
		return options.FileSystem.Stat(path)
	}
	return os.Stat(path)
}

func openStorageFile(options *Options, path string, flags int, mode os.FileMode) (StorageFile, error) {
	if options != nil && options.FileSystem != nil {
		return options.FileSystem.OpenFile(path, flags, mode)
	}
	return os.OpenFile(path, flags, mode)
}

func (w *Whisper) removeStorageFile(path string) error {
	if w.opts != nil && w.opts.FileSystem != nil {
		return w.opts.FileSystem.Remove(path)
	}
	return os.Remove(path)
}

func (w *Whisper) renameStorageFile(source, destination string) error {
	if w.opts != nil && w.opts.FileSystem != nil {
		return w.opts.FileSystem.Rename(source, destination)
	}
	return os.Rename(source, destination)
}
