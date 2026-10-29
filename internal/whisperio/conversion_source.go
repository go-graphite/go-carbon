package whisperio

import (
	"bytes"
	"context"
	"errors"
	"os"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

type conversionSourceKey struct{}

// WithConversionSource reuses physical source decodes during one conversion.
// The caller still fences the source pair until publication. Each reuse checks
// the complete block descriptor and its current encoded bytes; headers and
// classic/OOO slots are always read afresh. The context is used sequentially and
// must be discarded when conversion finishes, including on a staged restart.
func WithConversionSource(ctx context.Context) context.Context {
	return context.WithValue(ctx, conversionSourceKey{}, &conversionSource{files: make(map[string]map[int]*conversionBlock)})
}

type conversionSource struct {
	conversionReadOnly
	files map[string]map[int]*conversionBlock
	view  *conversionSourceView
}

type conversionSourceView struct {
	path     string
	header   []byte
	blocks   map[int]*conversionBlock
	snapshot store.Snapshot
}

func (s *conversionSource) rememberView(path string, header []byte, snapshot store.Snapshot) {
	v := &conversionSourceView{path: path, header: header, blocks: make(map[int]*conversionBlock), snapshot: snapshot}
	for offset, block := range s.files[path] {
		v.blocks[offset] = block
	}
	// Keep a private view: callers may change the expected snapshot. Physical
	// verification certifies this source view, never the caller's expectation.
	v.snapshot.Metadata.Retentions = append([]store.Retention(nil), snapshot.Metadata.Retentions...)
	v.snapshot.Archives = append([]store.Archive(nil), snapshot.Archives...)
	for i := range v.snapshot.Archives {
		v.snapshot.Archives[i].Points = append([]store.Point(nil), snapshot.Archives[i].Points...)
	}
	s.view = v
}

func (s *conversionSource) verifiedView(path string, header []byte, m store.Metadata) (store.Snapshot, bool) {
	v := s.view
	if v == nil || v.path != path || !bytes.Equal(v.header, header) {
		return store.Snapshot{}, false
	}
	blocks := s.files[path]
	if len(v.blocks) != len(blocks) {
		return store.Snapshot{}, false
	}
	for offset, block := range v.blocks {
		if blocks[offset] != block {
			return store.Snapshot{}, false
		}
	}
	result := v.snapshot
	result.Metadata = m
	return result, true
}

type conversionBlock struct {
	block  whisper.ContinuationBlock
	data   []byte
	points []whisper.TimeSeriesPoint
}

func conversionSourceOptions(ctx context.Context) *whisper.Options {
	readOnly := os.O_RDONLY
	options := &whisper.Options{OpenFileFlag: &readOnly}
	if source, ok := ctx.Value(conversionSourceKey{}).(*conversionSource); ok {
		options.FileSystem = source
	}
	return options
}

func (s *conversionSource) OpenFile(path string, flags int, mode os.FileMode) (whisper.StorageFile, error) {
	if flags != os.O_RDONLY {
		return nil, errors.New("conversion source is read-only")
	}
	f, err := os.OpenFile(path, flags, mode)
	if err != nil {
		return nil, err
	}
	blocks := s.files[path]
	if blocks == nil {
		blocks = make(map[int]*conversionBlock)
		s.files[path] = blocks
	}
	return &conversionSourceFile{File: f, blocks: blocks, checked: make(map[whisper.ContinuationBlock]*conversionBlock)}, nil
}

type conversionReadOnly struct{}

func (conversionReadOnly) Stat(path string) (os.FileInfo, error) { return os.Stat(path) }
func (conversionReadOnly) Remove(string) error                   { return errors.New("conversion source is read-only") }
func (conversionReadOnly) Rename(string, string) error {
	return errors.New("conversion source is read-only")
}

type conversionSourceFile struct {
	*os.File
	blocks  map[int]*conversionBlock
	checked map[whisper.ContinuationBlock]*conversionBlock
	data    []byte
	decoder *whisper.Whisper
	input   conversionBlockInput
	scratch whisper.ContinuationBlockBuffer
}

func (f *conversionSourceFile) Close() error {
	var err error
	if f.decoder != nil {
		err = f.decoder.Close()
	}
	return errors.Join(err, f.File.Close())
}

func (f *conversionSourceFile) ReadStorageBlock(block whisper.ContinuationBlock) ([]whisper.TimeSeriesPoint, error) {
	return f.ReadStorageBlockInto(block, nil)
}

func (f *conversionSourceFile) ReadStorageBlockInto(block whisper.ContinuationBlock, dst []whisper.TimeSeriesPoint) ([]whisper.TimeSeriesPoint, error) {
	// One open Whisper handle is a fenced, immutable read. A new open at
	// import/verification checks disk again, including for cache hits.
	if cached := f.checked[block]; cached != nil {
		return append(dst[:0], cached.points...), nil
	}
	if block.Offset < 0 || block.Size < whisper.PointSize || block.Size > int(^uint(0)>>1)/4 || block.Count <= 0 || block.Count > 4*block.Size {
		return nil, errors.New("invalid conversion source block")
	}
	if cap(f.data) < block.Size {
		f.data = make([]byte, block.Size)
	}
	data := f.data[:block.Size]
	if _, err := f.File.ReadAt(data, int64(block.Offset)); err != nil {
		return nil, err
	}
	cached, ok := f.blocks[block.Offset]
	if !ok || cached.block != block || !bytes.Equal(cached.data, data) {
		// Decode precisely the bytes whose identity we retain, rather than
		// opening a second disk read between the identity check and decode.
		f.input.path, f.input.offset, f.input.data = f.Name(), int64(block.Offset), data
		if f.decoder == nil {
			readOnly := os.O_RDONLY
			w, err := whisper.OpenWithOptions(f.Name(), &whisper.Options{OpenFileFlag: &readOnly, FileSystem: &f.input})
			if err != nil {
				return nil, err
			}
			f.decoder = w
		}
		points, err := f.decoder.ContinuationBlockPointsBuffered(block, &f.scratch)
		if err != nil {
			return nil, err
		}
		cached = &conversionBlock{block: block, data: bytes.Clone(data), points: append([]whisper.TimeSeriesPoint(nil), points...)}
		f.blocks[block.Offset] = cached
	}
	f.checked[block] = cached
	return append(dst[:0], cached.points...), nil
}

// conversionBlockInput gives the ordinary Whisper decoder an immutable block
// image. It intentionally has no typed block interface, so a miss uses the
// original compressed decoder. All other reads, including headers, use disk.
type conversionBlockInput struct {
	conversionReadOnly
	path   string
	offset int64
	data   []byte
}

func (s *conversionBlockInput) OpenFile(path string, flags int, mode os.FileMode) (whisper.StorageFile, error) {
	if flags != os.O_RDONLY {
		return nil, errors.New("conversion source is read-only")
	}
	f, err := os.OpenFile(path, flags, mode)
	if err != nil {
		return nil, err
	}
	return &conversionBlockFile{File: f, input: s}, nil
}

type conversionBlockFile struct {
	*os.File
	input *conversionBlockInput
}

func (f *conversionBlockFile) ReadAt(dst []byte, offset int64) (int, error) {
	s := f.input
	if f.Name() == s.path && offset >= s.offset && offset-s.offset <= int64(len(s.data)) && int64(len(dst)) <= int64(len(s.data))-(offset-s.offset) {
		return copy(dst, s.data[offset-s.offset:]), nil
	}
	return f.File.ReadAt(dst, offset)
}
