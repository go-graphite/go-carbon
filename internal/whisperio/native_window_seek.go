package whisperio

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// The optional index certifies classic slot addressing and an upper bound on
// timestamps. An upper bound may outlive a deleted point: in that case export
// uses the reference path rather than rejecting a potentially valid window.
// Old images without the index remain readable and use the reference path.
type classicWindowArchive struct{ base, high int64 }
type classicWindowIndex struct {
	archives []classicWindowArchive
	revision uint64
}

func classicWindowKey(id uint64) []byte { return append(continuationFilePrefix(id), 'w') }
func encodeClassicWindowIndex(index *classicWindowIndex) []byte {
	data := make([]byte, 9)
	data[0] = 2
	if index == nil {
		return data
	}
	binary.BigEndian.PutUint64(data[1:], index.revision)
	for _, a := range index.archives {
		data = binary.BigEndian.AppendUint32(data, uint32(a.base))
		data = binary.BigEndian.AppendUint32(data, uint32(a.high))
	}
	return data
}

func (c *ContinuationCodec) BindIndex(key, value []byte, revision uint64) []byte {
	if len(key) == 10 && key[0] == 'f' && key[9] == 'w' && len(value) >= 9 && value[0] == 2 {
		value = slices.Clone(value)
		binary.BigEndian.PutUint64(value[1:], revision)
	}
	return value
}

func (c *ContinuationCodec) CheckIndex(key, value []byte, revision uint64) []byte {
	if len(key) == 10 && key[0] == 'f' && key[9] == 'w' && len(value) > 9 && value[0] == 2 && binary.BigEndian.Uint64(value[1:]) != revision {
		return value[:9]
	}
	return value
}
func (f *continuationFile) loadClassicWindow() error {
	if f.data.windowLoaded {
		return nil
	}
	f.data.windowLoaded = true
	data, err := f.fs.kv.Get(classicWindowKey(f.data.id))
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(data) == 9 && data[0] == 2 {
		return nil
	}
	if len(data) != 9+8*len(f.data.layout.Archives) || data[0] != 2 {
		return store.ErrFormat
	}
	index := &classicWindowIndex{archives: make([]classicWindowArchive, len(f.data.layout.Archives)), revision: binary.BigEndian.Uint64(data[1:])}
	if v, ok := f.fs.kv.(store.ContinuationRevision); ok && index.revision != v.ContinuationRevision() {
		return nil
	}
	for ai := range index.archives {
		index.archives[ai] = classicWindowArchive{int64(binary.BigEndian.Uint32(data[9+8*ai:])), int64(binary.BigEndian.Uint32(data[13+8*ai:]))}
		if index.archives[ai].base > index.archives[ai].high {
			return store.ErrFormat
		}
	}
	f.data.windowIndex = index
	return nil
}

func (f *continuationFile) trackClassicWindow(key []byte, block *store.PointBlock) error {
	if f.data.layout.Compressed || len(key) != 20 || key[9] != 'p' {
		return nil
	}
	if err := f.loadClassicWindow(); err != nil {
		return err
	}
	if f.data.windowIndex == nil {
		return nil
	}
	f.data.windowDirty = true
	ai, group := int(binary.BigEndian.Uint16(key[10:])), int(binary.BigEndian.Uint32(key[16:]))
	if ai >= len(f.data.layout.Archives) {
		return store.ErrFormat
	}
	r := f.data.layout.Archives[ai]
	a := &f.data.windowIndex.archives[ai]
	for i, p := range block.Points {
		if block.Present[i/64]&(1<<uint(i%64)) == 0 {
			continue
		}
		if a.base == 0 && group == 0 && i == 0 {
			a.base = p.Timestamp
			f.data.windowDirty = true
		}
		slot := ((p.Timestamp-a.base)/int64(r.Step)%int64(r.Count) + int64(r.Count)) % int64(r.Count)
		if a.base <= 0 || p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || p.Timestamp%int64(r.Step) != 0 || slot != int64(group*128+i) {
			f.data.windowIndex, f.data.windowDirty = nil, true
			return nil
		}
		if p.Timestamp > a.high {
			a.high = p.Timestamp
			f.data.windowDirty = true
		}
	}
	return nil
}

func openWindowClassic(kv store.ContinuationIO, m store.Metadata, clock func() time.Time) (*continuationFile, bool, error) {
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
	if err := continuationPolicy(w, m); err != nil {
		return nil, false, err
	}
	f := w.StorageFile().(*continuationFile)
	if f.data.layout.Compressed {
		return nil, false, nil
	}
	if err := f.loadClassicWindow(); err != nil {
		return nil, false, err
	}
	return f, f.data.windowIndex != nil, nil
}

// Read at most the circular groups covering the requested time interval. The
// index certifies the mapping for every stored cell, including referenced ones.
func classicWindowPoints(ctx context.Context, f *continuationFile, ai int, from, until int64) ([]store.Point, error) {
	a := f.data.layout.Archives[ai]
	base := f.data.windowIndex.archives[ai].base
	if base == 0 {
		return nil, nil
	}
	step, count := int64(a.Step), int64(a.Count)
	first := (from + step - 1) / step * step
	groups := make(map[int]bool)
	var points []store.Point
	for ts, n := first, int64(0); ts < until && n < count; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		slot := int(((ts-base)/step%count + count) % count)
		group := slot / 128
		if !groups[group] {
			groups[group] = true
			block, err := f.classicGroup(ai, a, group, false)
			if err != nil {
				return nil, err
			}
			for i, p := range block.Points {
				if block.Present[i/64]&(1<<uint(i%64)) != 0 && p.Timestamp >= from && p.Timestamp < until {
					points = append(points, p)
				}
			}
		}
		advance := min(int64(128-slot%128), count-int64(slot), count-n)
		ts += advance * step
		n += advance
	}
	slices.SortFunc(points, func(a, b store.Point) int {
		if a.Timestamp < b.Timestamp {
			return -1
		}
		if a.Timestamp > b.Timestamp {
			return 1
		}
		return 0
	})
	return points, nil
}

func (c *ContinuationCodec) seekNativeWindow(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time, window store.NativeWindow) (store.Snapshot, []store.NativeWindowSkip, bool, error) {
	f, ok, err := openWindowClassic(kv, m, clock)
	if err != nil || !ok {
		return store.Snapshot{}, nil, false, err
	}
	if len(f.data.layout.Archives) != len(m.Retentions) {
		return store.Snapshot{}, nil, false, store.ErrFormat
	}
	snapshot := store.Snapshot{Metadata: m, Archives: make([]store.Archive, len(m.Retentions))}
	maxStep := int64(m.Retentions[len(m.Retentions)-1].Step)
	from, until := window.From/maxStep*maxStep, (window.Until+maxStep-1)/maxStep*maxStep
	for ai, r := range m.Retentions {
		a := f.data.layout.Archives[ai]
		if a.Step != r.Step || a.Count != r.Count {
			return store.Snapshot{}, nil, false, store.ErrFormat
		}
		if f.data.windowIndex.archives[ai].high >= window.From+int64(r.Step)*int64(r.Count) {
			return store.Snapshot{}, nil, false, nil
		}
		points, err := classicWindowPoints(ctx, f, ai, from, until)
		if err != nil {
			return store.Snapshot{}, nil, false, err
		}
		snapshot.Archives[ai] = store.Archive{Retention: r, Points: points}
	}
	snapshot, skips, err := store.SelectNativeWindow(snapshot, window, clock().Unix())
	return snapshot, skips, true, err
}

// PlanNativeWindowPatch keeps the existing generation and writes only groups
// that contain incoming timestamps. It does not propagate across archives.
// The store applies these private writes together with a revision-checked piece.
func (c *ContinuationCodec) PlanNativeWindowPatch(ctx context.Context, kv store.ContinuationIO, source store.Snapshot, clock func() time.Time, window store.NativeWindow) ([]store.RelayoutKV, store.NativeCombineClass, bool, error) {
	class := store.NativeCombineClass{Archives: len(source.Archives)}
	f, ok, err := openWindowClassic(kv, source.Metadata, clock)
	if err != nil || !ok {
		return nil, class, false, err
	}
	if len(f.data.layout.Archives) != len(source.Archives) {
		return nil, class, false, store.ErrFormat
	}
	overlay := newContinuationOverlay(kv)
	f.fs.kv, f.fs.readOnly = overlay, false
	for ai, archive := range source.Archives {
		a := f.data.layout.Archives[ai]
		if a.Step != archive.Retention.Step || a.Count != archive.Retention.Count {
			return nil, class, false, store.ErrFormat
		}
		base := f.data.windowIndex.archives[ai].base
		if base == 0 {
			if len(archive.Points) == 0 {
				continue
			}
			base = archive.Points[0].Timestamp
			f.data.windowIndex.archives[ai].base = base
		}
		groups := make(map[int]*store.PointBlock)
		// The reference union removes target NaNs inside the window, including
		// timestamps absent from the donor. Outside NaN payloads remain exact.
		targetPoints, err := classicWindowPoints(ctx, f, ai, window.From, window.Until)
		if err != nil {
			return nil, class, false, err
		}
		for _, p := range targetPoints {
			if !math.IsNaN(p.Value) {
				continue
			}
			slot := int(((p.Timestamp-base)/int64(a.Step)%int64(a.Count) + int64(a.Count)) % int64(a.Count))
			block := groups[slot/128]
			if block == nil {
				block, err = f.classicGroup(ai, a, slot/128, true)
				if err != nil {
					return nil, class, false, err
				}
				groups[slot/128] = block
			}
			block.Present[slot%128/64] &^= 1 << uint(slot%64)
			block.Points[slot%128] = store.Point{}
		}
		for _, p := range archive.Points {
			if err := ctx.Err(); err != nil {
				return nil, class, false, err
			}
			if !window.Contains(p.Timestamp) || p.Timestamp%int64(a.Step) != 0 || math.IsNaN(p.Value) {
				return nil, class, false, nil
			}
			slot := int(((p.Timestamp-base)/int64(a.Step)%int64(a.Count) + int64(a.Count)) % int64(a.Count))
			block := groups[slot/128]
			if block == nil {
				block, err = f.classicGroup(ai, a, slot/128, true)
				if err != nil {
					return nil, class, false, err
				}
				groups[slot/128] = block
			}
			i := slot % 128
			if block.Present[i/64]&(1<<uint(i%64)) != 0 {
				old := block.Points[i]
				if !window.Contains(old.Timestamp) {
					if old.Timestamp > p.Timestamp {
						return nil, class, false, fmt.Errorf("window would evict newer out-of-window archive %d point %d: %w", ai, old.Timestamp, store.ErrConflict)
					}
					class.StaleEvictedPoints++
				}
				if old.Timestamp > p.Timestamp || old.Timestamp == p.Timestamp && !math.IsNaN(old.Value) {
					continue
				}
			}
			block.Present[i/64] |= 1 << uint(i%64)
			block.Points[i] = p
			f.allocateClassicPages(a.Offset+slot*12, a.Offset+slot*12+12)
		}
		for group, block := range groups {
			if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, group), block); err != nil {
				return nil, class, false, err
			}
		}
	}
	f.data.windowDirty = true // Publication advances the owning revision even for an empty patch.
	if err := f.fs.flush(); err != nil {
		return nil, class, false, err
	}
	if len(overlay.prefixes) != 0 {
		return nil, class, false, nil
	}
	writes := make([]store.RelayoutKV, 0, len(overlay.values))
	for key, value := range overlay.values {
		writes = append(writes, store.RelayoutKV{Key: []byte(key), Value: value})
	}
	for key := range overlay.deleted {
		writes = append(writes, store.RelayoutKV{Key: []byte(key)})
	}
	class.CombinedBlocks = len(writes)
	return writes, class, true, nil
}
