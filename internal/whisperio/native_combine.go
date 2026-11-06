package whisperio

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// NativeClassicBlocks exposes physical circular groups only when the complete
// continuation is a single classic file. Compressed headers, OOO files and
// legacy raw pages require the archive-aware path; they cannot be flattened
// by assuming their physical block indices are circular slots.
func (c *ContinuationCodec) NativeClassicBlocks(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time) ([]store.RelayoutKV, bool, error) {
	fs := newContinuationFiles(kv, true).withClock(clock)
	if err := fs.prefetchNames(); err != nil {
		return nil, false, err
	}
	if len(fs.names) != 1 || fs.names[filepath.Base(fs.main())] == 0 {
		return nil, false, nil
	}
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	if err != nil {
		return nil, false, err
	}
	defer w.Close()
	if w.IsCompressed() {
		return nil, false, nil
	}
	if err := continuationPolicy(w, m); err != nil {
		return nil, false, err
	}
	f := w.StorageFile().(*continuationFile)
	var groups []store.RelayoutKV
	err = kv.Visit(continuationFilePrefix(f.data.id), func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(key) < 10 {
			return store.ErrFormat
		}
		if key[9] == 'h' || key[9] == 'H' {
			return nil
		}
		if len(key) != 20 || key[9] != 'p' || binary.BigEndian.Uint32(key[12:16]) != 0 {
			return errNativeNonClassicGroup
		}
		archive, index := binary.BigEndian.Uint16(key[10:12]), binary.BigEndian.Uint32(key[16:])
		if int(archive) >= len(m.Retentions) {
			return store.ErrFormat
		}
		k := binary.BigEndian.AppendUint16(nil, archive)
		k = binary.BigEndian.AppendUint32(k, index)
		groups = append(groups, store.RelayoutKV{Key: k, Value: value})
		return nil
	})
	if errors.Is(err, errNativeNonClassicGroup) {
		return nil, false, nil
	}
	return groups, err == nil, err
}

var errNativeNonClassicGroup = errors.New("native group requires archive materialization")

// RestoreNativeClassicBlocks builds only the file's structural metadata. The
// independently combined circular groups retain their encoded value bytes.
func (c *ContinuationCodec) RestoreNativeClassicBlocks(ctx context.Context, kv store.ContinuationIO, m store.Metadata, groups []store.RelayoutKV, clock func() time.Time) error {
	fs := newContinuationFiles(kv, false).withClock(clock)
	options := continuationOptions(fs, clock)
	options.Compressed = false
	retentions := make([]whisper.Retention, len(m.Retentions))
	for i, r := range m.Retentions {
		retentions[i] = whisper.NewRetention(r.Step, r.Count)
	}
	w, err := whisper.CreateWithOptions(fs.main(), whisper.NewRetentionsNoPointer(retentions), whisper.AggregationMethod(m.AggregationMethod), m.XFilesFactor, options)
	if err != nil {
		return err
	}
	defer w.Close()
	file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	f := file.(*continuationFile)
	layout := w.StorageLayout()
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(group.Key) != 6 {
			return store.ErrFormat
		}
		archive, index := int(binary.BigEndian.Uint16(group.Key)), int(binary.BigEndian.Uint32(group.Key[2:]))
		if archive >= len(layout.Archives) {
			return store.ErrFormat
		}
		var block store.PointBlock
		if err := store.DecodePointBlock(group.Value, &block); err != nil {
			return err
		}
		for offset := range block.Points {
			if block.Present[offset/64]&(uint64(1)<<uint(offset%64)) != 0 {
				slot := index*128 + offset
				if slot >= m.Retentions[archive].Count {
					return store.ErrFormat
				}
				f.allocateClassicPages(layout.Archives[archive].Offset+slot*12, layout.Archives[archive].Offset+slot*12+12)
			}
		}
		if err := kv.Set(continuationBodyKey(f.data.id, 'p', archive, 0, index), group.Value); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	return fs.flush()
}
