package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
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
	// Classic physical slot zero supplies the ring anchor. Translate only
	// whole-group rotations here; other anchors need exact regrouping.
	shifts := make([]int, len(m.Retentions))
	anchored := make([]bool, len(shifts))
	for ai, retention := range m.Retentions {
		value, err := kv.Get(continuationBodyKey(f.data.id, 'p', ai, 0, 0))
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		var block store.PointBlock
		if err := store.DecodePointBlock(value, &block); err != nil {
			return nil, false, err
		}
		if block.Present[0]&1 == 0 {
			continue
		}
		p := block.Points[0]
		if p.Timestamp <= 0 || p.Timestamp%int64(retention.Step) != 0 {
			return nil, false, nil
		}
		shifts[ai] = int(p.Timestamp / int64(retention.Step) % int64(retention.Count))
		anchored[ai] = true
	}
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
		shift, retention := shifts[archive], m.Retentions[archive]
		if !anchored[archive] {
			return errNativeNonClassicGroup
		}
		var block store.PointBlock
		if err := store.DecodePointBlock(value, &block); err != nil {
			return err
		}
		destination := -1
		for offset, p := range block.Points {
			if block.Present[offset/64]&(uint64(1)<<uint(offset%64)) == 0 {
				continue
			}
			physical := int(index)*128 + offset
			if physical >= retention.Count {
				return store.ErrFormat
			}
			absolute := (physical + shift) % retention.Count
			if p.Timestamp <= 0 || p.Timestamp%int64(retention.Step) != 0 || p.Timestamp/int64(retention.Step)%int64(retention.Count) != int64(absolute) || absolute%128 != offset {
				return errNativeNonClassicGroup
			}
			if destination >= 0 && destination != absolute/128 {
				return errNativeNonClassicGroup
			}
			destination = absolute / 128
		}
		if destination < 0 {
			return nil
		}
		index = uint32(destination)
		k := binary.BigEndian.AppendUint16(nil, archive)
		k = binary.BigEndian.AppendUint32(k, index)
		groups = append(groups, store.RelayoutKV{Key: k, Value: value})
		return nil
	})
	if errors.Is(err, errNativeNonClassicGroup) {
		return nil, false, nil
	}
	slices.SortFunc(groups, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	return groups, err == nil, err
}

var errNativeNonClassicGroup = errors.New("native group requires archive materialization")

// RestoreNativeClassicBlocks builds only the file's structural metadata. The
// independently combined circular groups retain their encoded value bytes.
func (c *ContinuationCodec) RestoreNativeClassicBlocks(ctx context.Context, kv store.ContinuationIO, m store.Metadata, groups []store.RelayoutKV, clock func() time.Time) (bool, error) {
	type groupPosition struct {
		archive, index int
		present        [2]uint64
	}
	positions := make([]groupPosition, len(groups))
	bases := make([]int64, len(m.Retentions))
	seen := make(map[string]bool, len(groups))
	// Validate and determine anchors before creating any output. Absolute
	// chunks stay absolute; only classic file positions rotate around base.
	for i, group := range groups {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if len(group.Key) != 6 || seen[string(group.Key)] {
			return false, store.ErrFormat
		}
		seen[string(group.Key)] = true
		archive, index := int(binary.BigEndian.Uint16(group.Key)), int(binary.BigEndian.Uint32(group.Key[2:]))
		if archive >= len(m.Retentions) {
			return false, store.ErrFormat
		}
		r := m.Retentions[archive]
		if r.Step <= 0 || r.Count <= 0 {
			return false, store.ErrFormat
		}
		var block store.PointBlock
		if err := store.DecodePointBlock(group.Value, &block); err != nil {
			return false, err
		}
		for offset, p := range block.Points {
			if block.Present[offset/64]&(uint64(1)<<uint(offset%64)) == 0 {
				continue
			}
			if p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || p.Timestamp%int64(r.Step) != 0 || p.Timestamp/int64(r.Step)%int64(r.Count) != int64(index*128+offset) {
				return false, store.ErrFormat
			}
			if bases[archive] == 0 || p.Timestamp < bases[archive] {
				bases[archive] = p.Timestamp
			}
		}
		positions[i] = groupPosition{archive, index, block.Present}
	}
	for i, p := range positions {
		r := m.Retentions[p.archive]
		shift := int(bases[p.archive] / int64(r.Step) % int64(r.Count))
		destination := -1
		for offset := range 128 {
			if p.present[offset/64]&(uint64(1)<<uint(offset%64)) == 0 {
				continue
			}
			relative := (p.index*128 + offset - shift + r.Count) % r.Count
			if relative%128 != offset || destination >= 0 && destination != relative/128 {
				return false, nil
			}
			destination = relative / 128
		}
		if destination < 0 {
			destination = 0
		}
		positions[i].index = destination
	}
	fs := newContinuationFiles(kv, false).withClock(clock)
	options := continuationOptions(fs, clock)
	options.Compressed = false
	retentions := make([]whisper.Retention, len(m.Retentions))
	for i, r := range m.Retentions {
		retentions[i] = whisper.NewRetention(r.Step, r.Count)
	}
	w, err := whisper.CreateWithOptions(fs.main(), whisper.NewRetentionsNoPointer(retentions), whisper.AggregationMethod(m.AggregationMethod), m.XFilesFactor, options)
	if err != nil {
		return false, err
	}
	defer w.Close()
	file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
	if err != nil {
		return false, err
	}
	defer file.Close()
	f := file.(*continuationFile)
	layout := w.StorageLayout()
	for i, group := range groups {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		p := positions[i]
		archive, index := p.archive, p.index
		for offset := range 128 {
			if p.present[offset/64]&(uint64(1)<<uint(offset%64)) != 0 {
				slot := index*128 + offset
				f.allocateClassicPages(layout.Archives[archive].Offset+slot*12, layout.Archives[archive].Offset+slot*12+12)
			}
		}
		if err := kv.Set(continuationBodyKey(f.data.id, 'p', archive, 0, index), group.Value); err != nil {
			return false, err
		}
	}
	if err := w.Close(); err != nil {
		return false, err
	}
	return true, fs.flush()
}
