package whisperio

import (
	"context"
	"math"
	"sort"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Frozen pre-fast-path implementation: differential and benchmark oracle.
func (c *ContinuationCodec) updateWithLegacyMutationProof(ctx context.Context, kv store.ContinuationIO, m store.Metadata, input []store.Point, retention int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	recorder, ok := kv.(store.ContinuationMutationRecorder)
	if !ok {
		return c.updateWithSumCache(ctx, kv, m, input, retention, clock, maintenance)
	}
	baseline, closeBase := recorder.MutationBaseline()
	defer closeBase()
	if baseline == nil {
		return c.updateWithSumCache(ctx, kv, m, input, retention, clock, maintenance)
	}
	before, err := readSumWriteView(baseline, clock)
	if err != nil {
		return err
	}
	if err = c.updateWithSumCache(ctx, kv, m, input, retention, clock, maintenance); err != nil {
		return err
	}
	after, err := readSumWriteView(kv, clock)
	if err != nil {
		return err
	}
	windows := make([][]whisper.ArchiveWindow, len(m.Retentions))
	mark := func(ai, from, until int) {
		if ai < 0 || ai >= len(windows) {
			return
		}
		// Proof requests use positive signed 32-bit Unix seconds. Include old
		// owners here even when the incoming timestamp is far in the future.
		from = max(from, 0)
		until = min(until, math.MaxInt32)
		if until > from {
			for _, scope := range recorder.MutationWindows() {
				lo, hi := max(from, int(scope.From)), min(until, int(scope.Until))
				// Proof scopes are arbitrary half-open seconds, while Whisper
				// reads aligned slots. Match DigestBounds: include exactly the
				// slot timestamps in the intersection, including lower rollups.
				step := m.Retentions[ai].Step
				lo = (lo + step - 1) / step * step
				hi = (hi + step - 1) / step * step
				if hi > lo {
					windows[ai] = append(windows[ai], whisper.ArchiveWindow{Archive: ai, From: lo, Until: hi})
				}
			}
		}
	}
	if !before.compressed || !after.compressed {
		now := int(clock().Unix())
		for ai, r := range m.Retentions {
			mark(ai, now-r.MaxRetention(), now+1)
		}
	} else {
		for _, w := range append(before.buffers, after.buffers...) {
			mark(w.Archive, w.From, w.Until)
		}
		type blockID struct{ archive, index int }
		old := make(map[blockID]whisper.ContinuationBlock, len(before.blocks))
		for _, b := range before.blocks {
			old[blockID{b.Archive, b.Index}] = b
		}
		for _, b := range after.blocks {
			id := blockID{b.Archive, b.Index}
			prev, ok := old[id]
			if ok && b.First == prev.First && b.Last > prev.Last && b.Count > prev.Count {
				mark(b.Archive, prev.Last+b.Step, b.Last+b.Step)
			} else if !ok || b != prev {
				mark(b.Archive, b.First, b.Last+b.Step)
				if ok {
					mark(prev.Archive, prev.First, prev.Last+prev.Step)
				}
			}
			delete(old, id)
		}
		for _, b := range old {
			mark(b.Archive, b.First, b.Last+b.Step)
		}
		for _, p := range input {
			if p.Timestamp > 0 && p.Timestamp < math.MaxInt32 {
				for ai, r := range m.Retentions {
					ts := int(p.Timestamp) / r.Step * r.Step
					mark(ai, ts, ts+r.Step)
				}
			}
		}
	}
	slots := 0
	for ai, ws := range windows {
		sort.Slice(ws, func(i, j int) bool { return ws[i].From < ws[j].From })
		merged := ws[:0]
		for _, w := range ws {
			if len(merged) > 0 && w.From <= merged[len(merged)-1].Until {
				merged[len(merged)-1].Until = max(merged[len(merged)-1].Until, w.Until)
			} else {
				merged = append(merged, w)
			}
		}
		windows[ai] = merged
		for _, w := range merged {
			// Discontinuous owners can span more than one circular archive.
			// Keep the revision fence unknown instead of rejecting the write.
			if w.Until-w.From > m.Retentions[ai].MaxRetention() {
				return nil
			}
			slots += (w.Until-w.From)/m.Retentions[ai].Step + 1
			if slots > mutationCompareSlots {
				return nil
			}
		}
	}
	open := func(source store.ContinuationIO) (*whisper.Whisper, error) {
		fs := newContinuationFiles(source, true).withClock(clock)
		fs.rangeReads = true
		return whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	}
	old, err := open(baseline)
	if err != nil {
		return err
	}
	defer old.Close()
	current, err := open(kv)
	if err != nil {
		return err
	}
	defer current.Close()
	for _, ws := range windows {
		for _, w := range ws {
			a, err := old.FetchArchiveWindow(w)
			if err != nil {
				return err
			}
			b, err := current.FetchArchiveWindow(w)
			if err != nil {
				return err
			}
			if a.FromTime() != b.FromTime() || a.Step() != b.Step() || len(a.Values()) != len(b.Values()) {
				return nil
			}
			for i, v := range a.Values() {
				if math.Float64bits(v) != math.Float64bits(b.Values()[i]) {
					ts := int64(a.FromTime() + i*a.Step())
					recorder.RecordMutation(w.Archive, ts, ts+1)
				}
			}
		}
	}
	recorder.CompleteMutation()
	return nil
}
