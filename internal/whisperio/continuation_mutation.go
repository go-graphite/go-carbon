package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"reflect"
	"sort"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Refinement is deliberately bounded. Large rewrites and codecs without this
// seam keep the whole-revision UNKNOWN fence rather than guessing from input
// timestamps or physical block bounds.
const mutationCompareSlots = 65536

func (c *ContinuationCodec) updateWithMutationProof(ctx context.Context, kv store.ContinuationIO, m store.Metadata, input []store.Point, retention int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	recorder, ok := kv.(store.ContinuationMutationRecorder)
	if !ok {
		return c.updateWithSumCache(ctx, kv, m, input, retention, clock, maintenance)
	}
	baseline, closeBase := recorder.MutationBaseline()
	defer closeBase()
	if baseline == nil {
		return c.updateWithSumCache(ctx, kv, m, input, retention, clock, maintenance)
	}
	if c.updateState != nil {
		c.updateState.reuseManifests = true
	}
	before, err := readSumWriteView(baseline, clock, c.updateState)
	if err != nil {
		return err
	}
	// Classic updates already stage private attempts. Retain their final write
	// set to refine only changed groups, including writes from propagation.
	var changes *continuationOverlay
	writeKV := kv
	if !before.compressed {
		changes = newContinuationOverlay(kv)
		writeKV = changes
	}
	if err = c.updateWithSumCache(ctx, writeKV, m, input, retention, clock, maintenance); err != nil {
		return err
	}
	if changes != nil {
		if err := changes.commit(); err != nil {
			return err
		}
	}
	after, err := readSumWriteView(kv, clock, c.updateState)
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
	if changes != nil {
		return refineClassicMutation(baseline, kv, changes, before, after, windows, recorder, clock, c.updateState)
	}
	open := func(source store.ContinuationIO) (*whisper.Whisper, error) {
		fs := newContinuationFiles(source, true).withClock(clock)
		fs.update = c.updateState
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

// refineClassicMutation compares physical slots only in the final write set.
// Both owners of a replaced/deleted slot matter. A stable classic header and
// circular anchor ensure untouched groups retain the same logical meaning.
// Unknown key kinds and structural rewrites keep the whole-revision fence.
func refineClassicMutation(baseline, current store.ContinuationIO, changes *continuationOverlay, before, after sumWriteView, windows [][]whisper.ArchiveWindow, recorder store.ContinuationMutationRecorder, clock func() time.Time, manifests *continuationUpdateState) error {
	if before.file != after.file || after.compressed || before.sidecar || after.sidecar {
		return nil
	}
	for _, prefix := range changes.prefixes {
		if !bytes.Equal(prefix, sumRevisionKey) {
			return nil
		}
	}
	oldFS, newFS := before.fs, after.fs
	if oldFS == nil {
		oldFS = newContinuationFiles(baseline, true).withClock(clock)
	}
	if newFS == nil {
		newFS = newContinuationFiles(current, true).withClock(clock)
	}
	oldFS.update, newFS.update = manifests, manifests
	oldData, err := oldFS.load("main.wsp")
	if err != nil {
		return err
	}
	newData, err := newFS.load("main.wsp")
	if err != nil {
		return err
	}
	if !bytes.Equal(oldData.header, newData.header) || !reflect.DeepEqual(oldData.layout, newData.layout) {
		return nil
	}
	oldFile := &continuationFile{fs: oldFS, data: oldData}
	newFile := &continuationFile{fs: newFS, data: newData}
	groups := make(map[string]bool)
	check := func(key string) bool {
		k := []byte(key)
		if bytes.HasPrefix(k, sumRevisionKey) {
			return true // Disposable digest cache, never point data.
		}
		if len(k) < 10 || !bytes.Equal(k[:9], continuationFilePrefix(before.file)) {
			return false
		}
		if len(k) == 10 && (k[9] == 'h' || k[9] == 'w') {
			return true // Manifest checked above; optional window index is a cache.
		}
		if len(k) != 20 || k[9] != 'p' || binary.BigEndian.Uint32(k[12:16]) != 0 {
			return false // Includes alias retirement and sidecar/namespace changes.
		}
		groups[key] = true
		return len(groups) <= mutationCompareSlots/128
	}
	for key := range changes.values {
		if !check(key) {
			return nil
		}
	}
	for key := range changes.deleted {
		if !check(key) {
			return nil
		}
	}
	mark := func(ai int, ts int64) {
		for _, w := range windows[ai] {
			if ts >= int64(w.From) && ts < int64(w.Until) {
				recorder.RecordMutation(ai, ts, ts+1)
				return
			}
		}
	}
	for key := range groups {
		k := []byte(key)
		ai, gi := int(binary.BigEndian.Uint16(k[10:12])), int(binary.BigEndian.Uint32(k[16:20]))
		if ai >= len(oldData.layout.Archives) || gi*128 >= oldData.layout.Archives[ai].Count {
			return nil
		}
		a, err := oldFile.classicGroup(ai, oldData.layout.Archives[ai], gi, false)
		if err != nil {
			return err
		}
		b, err := newFile.classicGroup(ai, newData.layout.Archives[ai], gi, false)
		if err != nil {
			return err
		}
		if gi == 0 && (a.Present[0]&1 != b.Present[0]&1 || a.Points[0].Timestamp != b.Points[0].Timestamp) {
			return nil // Moving the circular anchor can change untouched groups.
		}
		for i, p := range a.Points {
			ap, bp := a.Present[i/64]&(1<<uint(i%64)) != 0, b.Present[i/64]&(1<<uint(i%64)) != 0
			q := b.Points[i]
			if ap == bp && (!ap || p.Timestamp == q.Timestamp && math.Float64bits(p.Value) == math.Float64bits(q.Value)) {
				continue
			}
			if ap {
				mark(ai, p.Timestamp)
			}
			if bp {
				mark(ai, q.Timestamp)
			}
		}
	}
	recorder.CompleteMutation()
	return nil
}
