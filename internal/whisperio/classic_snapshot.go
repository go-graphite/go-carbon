package whisperio

import (
	"context"
	"errors"
	"fmt"
	"sort"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// classicContinuationSnapshot reads physical classic slots directly. The byte
// reader otherwise serializes these same groups to Whisper records, decodes
// them, then copies the points twice before producing a store snapshot.
func classicContinuationSnapshot(ctx context.Context, fs *continuationFiles, m store.Metadata, buffers *snapshotScratch) (store.Snapshot, error) {
	d, err := fs.load("main.wsp")
	if err != nil {
		return store.Snapshot{}, err
	}
	f := &continuationFile{fs: fs, data: d}
	result := store.Snapshot{Metadata: m, Archives: make([]store.Archive, len(m.Retentions))}
	var scratch store.PointBlock
	for i, r := range m.Retentions {
		if err := ctx.Err(); err != nil {
			return store.Snapshot{}, err
		}
		if i >= len(d.layout.Archives) {
			return store.Snapshot{}, fmt.Errorf("archive index %d out of range", i)
		}
		a := d.layout.Archives[i]
		points := buffers.buffer(i, 0)
		for group := 0; group*128 < a.Count; group++ {
			if err := ctx.Err(); err != nil {
				return store.Snapshot{}, err
			}
			if !f.classicGroupAllocated(a, group) {
				continue
			}
			block := &scratch
			if d.references {
				block, err = f.classicGroup(i, a, group, false)
			} else {
				var value []byte
				value, err = fs.kv.Get(continuationBodyKey(d.id, 'p', i, 0, group))
				if errors.Is(err, store.ErrNotFound) {
					continue
				}
				if err == nil {
					err = store.DecodePointBlock(value, block)
				}
			}
			if err != nil {
				return store.Snapshot{}, err
			}
			for slot := 0; slot < min(128, a.Count-group*128); slot++ {
				if block.Present[slot/64]&(uint64(1)<<uint(slot%64)) == 0 {
					continue
				}
				p := block.Points[slot]
				// Match the classic byte format, including uint32 timestamp truncation.
				p.Timestamp = int64(uint32(p.Timestamp))
				if p.Timestamp > 0 {
					points = append(points, p)
				}
			}
		}
		buffers.keep(i, points)
		sortClassicSnapshotPoints(points)
		result.Archives[i] = store.Archive{Retention: r, Points: points}
	}
	return result, nil
}

type classicSnapshotPoints []store.Point

func (p classicSnapshotPoints) Len() int           { return len(p) }
func (p classicSnapshotPoints) Less(i, j int) bool { return p[i].Timestamp < p[j].Timestamp }
func (p classicSnapshotPoints) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Match go-whisper's ordering, including its unstable sort for duplicate times.
func sortClassicSnapshotPoints(points []store.Point) {
	cut := 0
	for i := 1; i < len(points); i++ {
		if points[i].Timestamp <= points[i-1].Timestamp {
			if cut != 0 {
				cut = -1
				break
			}
			cut = i
		}
	}
	if cut == 0 {
		return
	}
	if cut > 0 && points[len(points)-1].Timestamp < points[0].Timestamp {
		// Rotate the two strictly increasing runs without an auxiliary point slice.
		reverseClassicSnapshot(points[:cut])
		reverseClassicSnapshot(points[cut:])
		reverseClassicSnapshot(points)
		return
	}
	sort.Sort(classicSnapshotPoints(points))
}
func reverseClassicSnapshot(p []store.Point) {
	for i, j := 0, len(p)-1; i < j; i, j = i+1, j-1 {
		p[i], p[j] = p[j], p[i]
	}
}
