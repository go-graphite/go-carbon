package whisperio

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type classicSlotConflict struct{ archive int }

func (e *classicSlotConflict) Error() string {
	return fmt.Sprintf("inconsistent classic archive %d", e.archive)
}

var classicReanchorCounters struct{ archives, aliases atomic.Uint64 }

// ClassicReanchorStats counts completed codec repairs, including work discarded
// if the owning store transaction subsequently fails. AliasDrops counts older
// distinct timestamps displaced by a newer timestamp one or more rings away.
type ClassicReanchorStats struct{ ReanchoredArchives, AliasDrops uint64 }

func ClassicReanchors() ClassicReanchorStats {
	return ClassicReanchorStats{classicReanchorCounters.archives.Load(), classicReanchorCounters.aliases.Load()}
}

func (c *ContinuationCodec) updateReanchoring(ctx context.Context, kv store.ContinuationIO, m store.Metadata, input []store.Point, retention int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	base := kv
	var repairs *continuationOverlay
	var repaired map[int]uint64
	// A metric already known to be damaged skips the attempt that would only
	// rediscover the conflict: defer it outright, or repair it first.
	known, granted := c.Reanchor.known(ctx, m.Name)
	if !granted {
		return store.ErrReanchorDeferred
	}
	for _, archive := range known {
		if repairs == nil {
			repairs = newContinuationOverlay(kv)
			base = repairs
			repaired = make(map[int]uint64)
		}
		drops, err := reanchorClassicArchive(ctx, repairs, archive)
		if err != nil {
			return err
		}
		repaired[archive] = drops
	}
	for {
		var attempt *continuationOverlay
		err := c.updateAttempt(ctx, base, m, input, retention, clock, maintenance, repairs == nil, &attempt)
		if err == nil {
			if attempt != nil {
				err = attempt.commit()
			}
			if err == nil && repairs != nil {
				err = repairs.commit()
			}
			if err != nil {
				return err
			}
			if len(repaired) != 0 {
				c.Reanchor.repaired(ctx, m.Name)
			}
			for archive, drops := range repaired {
				classicReanchorCounters.archives.Add(1)
				classicReanchorCounters.aliases.Add(drops)
				zapwriter.Logger("storage").Warn("prepared classic archive re-anchor",
					zap.String("metric", m.Name), zap.Int("archive", archive), zap.Uint64("alias_drops", drops))
			}
			return nil
		}
		var conflict *classicSlotConflict
		if !errors.As(err, &conflict) {
			return err
		}
		if _, ok := repaired[conflict.archive]; ok {
			return fmt.Errorf("classic archive %d still conflicts after re-anchor: %w", conflict.archive, store.ErrFormat)
		}
		if repairs == nil {
			if !c.Reanchor.admit(ctx, m.Name, conflict.archive) {
				return store.ErrReanchorDeferred
			}
			repairs = newContinuationOverlay(kv)
			base = repairs
			repaired = make(map[int]uint64)
		}
		// Nothing from the failed attempt is retained, including propagation to
		// sibling archives. Reopen Whisper after changing slot zero: its previous
		// offsets were calculated from the old anchor.
		drops, err := reanchorClassicArchive(ctx, repairs, conflict.archive)
		if err != nil {
			return err
		}
		repaired[conflict.archive] = drops
	}
}

// reanchorClassicArchive normalizes exactly one main-file archive. The caller
// owns a private overlay inside the store's indexed batch; no intermediate
// layout can become visible, even if retrying the ordinary write fails.
//
// Any timestamp present in the repaired ring may sit in slot zero, so the
// repair picks the anchor that leaves the most points where they already are,
// and writes only the groups whose points change. The surviving points are the
// ones the original min-anchored repair kept; only their rotation differs.
func reanchorClassicArchive(ctx context.Context, kv store.ContinuationIO, ai int) (uint64, error) {
	fs := newContinuationFiles(kv, false)
	file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	f := file.(*continuationFile)
	if f.data.layout.Compressed || ai < 0 || ai >= len(f.data.layout.Archives) {
		return 0, store.ErrFormat
	}
	a := f.data.layout.Archives[ai]
	step, count := int64(a.Step), int64(a.Count)
	groups := (a.Count + 127) / 128
	prefix := continuationBlockPrefix(f.data.id, 'p', ai, 0)
	old := make([]store.PointBlock, groups)
	had := make([]bool, groups)
	var stray []int // Empty groups past the archive end, deleted as before.
	// A fresh repair overlay holds nothing under this archive yet: read the
	// batch directly instead of cloning and sorting every block in the overlay.
	src, direct := kv, false
	if o, ok := kv.(*continuationOverlay); ok && o.untouched(prefix) {
		src, direct = o.base, true
	}
	var scratch store.PointBlock
	err = src.Visit(prefix, func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(key) != len(prefix)+4 {
			return store.ErrFormat
		}
		index := int(binary.BigEndian.Uint32(key[len(prefix):]))
		block := &scratch
		if index < groups {
			block = &old[index]
		}
		if err := store.DecodePointBlock(value, block); err != nil {
			return err
		}
		for i := range block.Points {
			if block.Present[i/64]&(uint64(1)<<uint(i%64)) == 0 {
				continue
			}
			p := block.Points[i]
			if index*128+i >= a.Count || p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || p.Timestamp%step != 0 {
				return fmt.Errorf("invalid classic point in archive %d: %w", ai, store.ErrFormat)
			}
		}
		if index < groups {
			had[index] = true
		} else {
			stray = append(stray, index)
		}
		return nil
	})
	if direct && errors.Is(err, store.ErrNotFound) {
		err = nil
	}
	if err != nil {
		return 0, err
	}
	present := func(slot int) bool {
		return old[slot/128].Present[slot%128/64]&(uint64(1)<<uint(slot%64)) != 0
	}
	oldBase := int64(0)
	if present(0) {
		oldBase = old[0].Points[0].Timestamp
	}
	// Congruence is independent of the chosen base. A ring cannot represent
	// two distinct timestamps in one congruence class; retain the newer one.
	// For duplicate timestamps prefer the copy visible at the existing anchor.
	// winner holds slot+1 of each congruence class's survivor, 0 for none.
	winner := make([]int32, a.Count)
	var drops uint64
	for slot := 0; slot < a.Count; slot++ {
		if !present(slot) {
			continue
		}
		ts := old[slot/128].Points[slot%128].Timestamp
		absolute := ts / step % count
		previous := int(winner[absolute]) - 1
		if previous < 0 {
			winner[absolute] = int32(slot + 1)
			continue
		}
		prev := old[previous/128].Points[previous%128].Timestamp
		if prev != ts {
			drops++
		}
		visible := oldBase != 0 && int(((ts-oldBase)/step%count+count)%count) == slot
		if ts > prev || ts == prev && visible {
			winner[absolute] = int32(slot + 1)
		}
	}
	// A survivor in class c anchors the ring at c: every survivor whose
	// current slot already equals (class - c) mod count stays in place. Pick
	// the anchor class that keeps the most survivors still, preferring the
	// original min-timestamp anchor on ties.
	stays := make([]int32, a.Count)
	minClass, minTs := -1, int64(math.MaxInt64)
	for class, w := range winner {
		if w == 0 {
			continue
		}
		slot := int(w) - 1
		ts := old[slot/128].Points[slot%128].Timestamp
		stays[((int64(class)-int64(slot))%count+count)%count]++
		if ts < minTs {
			minClass, minTs = class, ts
		}
	}
	anchor := minClass
	for class, w := range winner {
		if w != 0 && anchor >= 0 && stays[class] > stays[anchor] {
			anchor = class
		}
	}
	fresh := make([]store.PointBlock, groups)
	filled := make([]bool, groups)
	if anchor >= 0 {
		runStart, runEnd := -1, -1
		for i := 0; i < a.Count; i++ {
			// Visit the new slots in order so page allocation coalesces runs.
			class := (anchor + i) % a.Count
			w := int(winner[class]) - 1
			if w < 0 {
				continue
			}
			p := old[w/128].Points[w%128]
			slot := i
			block := &fresh[slot/128]
			filled[slot/128] = true
			block.Present[slot%128/64] |= uint64(1) << uint(slot%64)
			block.Points[slot%128] = p // Preserve IEEE bits, including NaN payloads.
			if slot != runEnd {
				if runStart >= 0 {
					f.allocateClassicPages(a.Offset+runStart*12, a.Offset+runEnd*12)
				}
				runStart = slot
			}
			runEnd = slot + 1
		}
		if runStart >= 0 {
			f.allocateClassicPages(a.Offset+runStart*12, a.Offset+runEnd*12)
		}
	}
	// The optional seek certificate covered the old phase. Invalidate it;
	// ordinary reads safely fall back without touching sibling archive data.
	f.data.windowIndex = nil
	f.data.windowLoaded, f.data.windowDirty = true, true
	for _, index := range stray {
		if err := kv.Delete(continuationBodyKey(f.data.id, 'p', ai, 0, index)); err != nil {
			return 0, err
		}
	}
	for index := range fresh {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		key := continuationBodyKey(f.data.id, 'p', ai, 0, index)
		switch {
		case !filled[index]:
			if had[index] {
				if err := kv.Delete(key); err != nil {
					return 0, err
				}
			}
		case had[index] && !f.data.references && samePointBlock(&old[index], &fresh[index]):
			// Unchanged: keep the stored bytes, saving the encode and the write.
		default:
			if err := f.setPointBlock(key, &fresh[index]); err != nil {
				return 0, err
			}
		}
	}
	return drops, fs.flush()
}

// samePointBlock reports whether two groups hold the same present slots with
// bit-identical timestamps and values; absent slots' contents are ignored.
func samePointBlock(a, b *store.PointBlock) bool {
	if a.Present != b.Present {
		return false
	}
	for i := range a.Points {
		if a.Present[i/64]&(uint64(1)<<uint(i%64)) == 0 {
			continue
		}
		if a.Points[i].Timestamp != b.Points[i].Timestamp || math.Float64bits(a.Points[i].Value) != math.Float64bits(b.Points[i].Value) {
			return false
		}
	}
	return true
}
