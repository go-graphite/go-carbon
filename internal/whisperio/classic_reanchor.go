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
	prefix := continuationBlockPrefix(f.data.id, 'p', ai, 0)
	type locatedPoint struct {
		point store.Point
		slot  int
	}
	var points []locatedPoint
	var oldGroups []int
	err = kv.Visit(prefix, func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(key) != len(prefix)+4 {
			return store.ErrFormat
		}
		index := int(binary.BigEndian.Uint32(key[len(prefix):]))
		var block store.PointBlock
		if err := store.DecodePointBlock(value, &block); err != nil {
			return err
		}
		oldGroups = append(oldGroups, index)
		for i, p := range block.Points {
			if block.Present[i/64]&(uint64(1)<<uint(i%64)) == 0 {
				continue
			}
			if index*128+i >= a.Count || p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 || p.Timestamp%int64(a.Step) != 0 {
				return fmt.Errorf("invalid classic point in archive %d: %w", ai, store.ErrFormat)
			}
			points = append(points, locatedPoint{p, index*128 + i})
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	oldBase := int64(0)
	for _, p := range points {
		if p.slot == 0 {
			oldBase = p.point.Timestamp
			break
		}
	}
	// Congruence is independent of the chosen base. A ring cannot represent
	// two distinct timestamps in one congruence class; retain the newer one.
	// For duplicate timestamps prefer the copy visible at the existing anchor.
	winners := make(map[int64]locatedPoint, len(points))
	var drops uint64
	for _, p := range points {
		absolute := p.point.Timestamp / int64(a.Step) % int64(a.Count)
		previous, exists := winners[absolute]
		if exists && previous.point.Timestamp != p.point.Timestamp {
			drops++
		}
		visible := oldBase != 0 && int(((p.point.Timestamp-oldBase)/int64(a.Step)%int64(a.Count)+int64(a.Count))%int64(a.Count)) == p.slot
		if !exists || p.point.Timestamp > previous.point.Timestamp || p.point.Timestamp == previous.point.Timestamp && visible {
			winners[absolute] = p
		}
	}
	base := int64(math.MaxInt64)
	for _, p := range winners {
		base = min(base, p.point.Timestamp)
	}
	fresh := make(map[int]*store.PointBlock)
	for _, p := range winners {
		slot := int((p.point.Timestamp - base) / int64(a.Step) % int64(a.Count))
		block := fresh[slot/128]
		if block == nil {
			block = &store.PointBlock{}
			fresh[slot/128] = block
		}
		block.Present[slot%128/64] |= uint64(1) << uint(slot%64)
		block.Points[slot%128] = p.point // Preserve IEEE bits, including NaN payloads.
		f.allocateClassicPages(a.Offset+slot*12, a.Offset+slot*12+12)
	}
	// The optional seek certificate covered the old phase. Invalidate it;
	// ordinary reads safely fall back without touching sibling archive data.
	f.data.windowIndex = nil
	f.data.windowLoaded, f.data.windowDirty = true, true
	for _, index := range oldGroups {
		if fresh[index] == nil {
			if err := kv.Delete(continuationBodyKey(f.data.id, 'p', ai, 0, index)); err != nil {
				return 0, err
			}
		}
	}
	for index, block := range fresh {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', ai, 0, index), block); err != nil {
			return 0, err
		}
	}
	return drops, fs.flush()
}
