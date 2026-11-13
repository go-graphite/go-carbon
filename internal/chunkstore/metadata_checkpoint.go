package chunkstore

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"iter"
	"math"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/phasecpu"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

const checkpointFile = "metadata-cache-v1"

// v2 certifies that every saved record was authoritative at the clean fence.
// v1 may contain provisional records and must still validate their revisions.
const checkpointMagic = "GCSTATE2"
const checkpointLegacyMagic = "GCSTATE1"
const maxCheckpointRecordSize = 16 << 20

// MetadataCheckpointLoadTimeout bounds the accelerator attempt after its
// caller's readiness gate. Missing or provisional records use the store.
// This bounds CPU admission waits, not an uninterruptible filesystem read.
const MetadataCheckpointLoadTimeout = 10 * time.Minute
const provisionalState = uint64(1) << 63

var checkpointIdentityKey = []byte("z/metadata-cache-identity")
var checkpointCleanKey = []byte("z/metadata-cache-clean")

type metadataCheckpoint struct {
	enabled, valid, prepared, lazy bool
	fenced                         bool
	// openHeader is immutable while preparation may refresh clean/sequence.
	openHeader [52]byte
	identity   [16]byte
	clean      [16]byte
	sequence   uint64
	saveErr    error
	// mutationEpochs and stateEpochs hold every stripe's epochs at the save
	// fence. A stripe whose epoch moved by close had a mutation begin or end
	// after the fence, so its saved records may be stale.
	mutationEpochs, stateEpochs []uint64
	// dirty marks, at load, the stripes the close-time fence excluded.
	dirty []uint64
}

// checkpointFenceFile completes a checkpoint whose store kept changing after
// the save fence: reads stay up during the stop, and some writers (deletes,
// maintenance tails, cold-pack and transfer bookkeeping) still commit then.
// It names the final sequence at close and the stripes mutated since the
// fence; the loader skips only those stripes instead of the whole file.
const checkpointFenceFile = checkpointFile + ".fence"
const checkpointFenceMagic = "GCFENCE1"

// magic, identity, clean, base and final sequence, dirty stripe bitmap, crc.
const checkpointFenceSize = 8 + 16 + 16 + 8 + 8 + metricStripes/8 + 4

func (c *metadataCheckpoint) dirtyStripe(stripe uint64) bool {
	return c.dirty != nil && c.dirty[stripe/64]&(1<<(stripe%64)) != 0
}

// readFence accepts the fence for header only when it completes this very
// save: same identity and clean stamp, the header's sequence as its base and
// the store's current sequence as its final one.
func (c *metadataCheckpoint) readFence(s *Store, header []byte) bool {
	f, err := s.fs.Open(s.fs.PathJoin(s.dir, checkpointFenceFile))
	if err != nil {
		return false
	}
	defer f.Close()
	var fence [checkpointFenceSize]byte
	if _, err := io.ReadFull(f, fence[:]); err != nil {
		return false
	}
	body := fence[:checkpointFenceSize-4]
	if string(fence[:8]) != checkpointFenceMagic || binary.LittleEndian.Uint32(fence[checkpointFenceSize-4:]) != crc32.ChecksumIEEE(body) ||
		!bytes.Equal(fence[8:24], c.identity[:]) || !bytes.Equal(fence[24:40], c.clean[:]) ||
		binary.LittleEndian.Uint64(fence[40:48]) != binary.LittleEndian.Uint64(header[40:48]) ||
		binary.LittleEndian.Uint64(fence[48:56]) != c.sequence {
		return false
	}
	c.dirty = make([]uint64, metricStripes/64)
	for i := range c.dirty {
		c.dirty[i] = binary.LittleEndian.Uint64(fence[56+8*i:])
	}
	return true
}

// captureFence records every stripe's epochs before the save reads the
// cache. A mutation that ended before its stripe was captured installed or
// invalidated its state first, so the snapshot reflects it; any other
// mutation moves an epoch after the capture.
func (s *Store) captureFence() {
	c := &s.checkpoint
	c.mutationEpochs = make([]uint64, metricStripes)
	for i := range c.mutationEpochs {
		c.mutationEpochs[i] = s.mutationEpoch[i].Load()
	}
	if s.stateCache != nil {
		c.stateEpochs = make([]uint64, metricStripes)
		for i := range c.stateEpochs {
			c.stateEpochs[i] = s.stateCache.shards[i].epoch.Load()
		}
	}
}

// finishMetadataCheckpoint runs in close once every writer has stopped. If
// the store changed after the save fence, it publishes the fence that lets
// the next open trust the unchanged stripes. Never call it with writers
// running: the final sequence must be the one the next open observes.
func (s *Store) finishMetadataCheckpoint() error {
	c := &s.checkpoint
	final := s.db.VisibleSequence()
	var bitmap [metricStripes / 64]uint64
	dirty := 0
	for i := range metricStripes {
		e := s.mutationEpoch[i].Load()
		changed := e == math.MaxUint64 || e != c.mutationEpochs[i]
		if c.stateEpochs != nil && s.stateCache.shards[i].epoch.Load() != c.stateEpochs[i] {
			changed = true
		}
		if changed {
			bitmap[i/64] |= 1 << (i % 64)
			dirty++
		}
	}
	if final == c.sequence && dirty == 0 {
		return nil
	}
	var fence [checkpointFenceSize]byte
	copy(fence[:8], checkpointFenceMagic)
	copy(fence[8:24], c.identity[:])
	copy(fence[24:40], c.clean[:])
	binary.LittleEndian.PutUint64(fence[40:48], c.sequence)
	binary.LittleEndian.PutUint64(fence[48:56], final)
	for i, word := range bitmap {
		binary.LittleEndian.PutUint64(fence[56+8*i:], word)
	}
	binary.LittleEndian.PutUint32(fence[checkpointFenceSize-4:], crc32.ChecksumIEEE(fence[:checkpointFenceSize-4]))
	path := s.fs.PathJoin(s.dir, checkpointFenceFile)
	tmp := path + ".tmp"
	f, err := s.fs.Create(tmp)
	if err != nil {
		return err
	}
	_, err = f.Write(fence[:])
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err == nil {
		err = s.fs.Rename(tmp, path)
	}
	if err != nil {
		_ = s.fs.Remove(tmp)
		return err
	}
	if err := syncStoreDirectory(s.fs, s.dir); err != nil {
		return err
	}
	zapwriter.Logger("storage").Info("metadata checkpoint fenced after writes during the stop",
		zap.Uint64("sequence_delta", final-c.sequence), zap.Int("dirty_stripes", dirty), zap.Int("stripes", metricStripes))
	return nil
}

// MetadataCheckpointStats describes the advisory checkpoint, not durable data.
type MetadataCheckpointStats struct {
	Entries, Bytes, ReadBytes, Validated int64
	// Fenced records need no revision read: the v2 clean fence and the
	// mutation filter establish that their saved state is still current.
	Fenced int64
	// SkippedDirty counts records in stripes mutated after the save fence;
	// they stay cache misses and refill from the store.
	SkippedDirty int64
	Duration     time.Duration
	Valid        bool
}

// openMetadataCheckpoint consumes the clean stamp before any store writer can
// run. A crash or an intervening divergent write invalidates the next
// load, even if an old accelerator file remains on disk.
func (s *Store) openMetadataCheckpoint() error {
	c := &s.checkpoint
	c.enabled = true
	v, closer, err := s.db.Get(checkpointIdentityKey)
	if err == nil {
		if len(v) == len(c.identity) {
			copy(c.identity[:], v)
		}
		closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	if c.identity == [16]byte{} {
		if _, err := rand.Read(c.identity[:]); err != nil {
			return err
		}
		if err := s.db.Set(checkpointIdentityKey, c.identity[:], pebble.Sync); err != nil {
			return err
		}
	}
	c.sequence = s.db.VisibleSequence()
	v, closer, err = s.db.Get(checkpointCleanKey)
	if err == nil {
		if len(v) == len(c.clean) {
			copy(c.clean[:], v)
		}
		closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	f, err := s.fs.Open(s.fs.PathJoin(s.dir, checkpointFile))
	if err == nil {
		var header [52]byte
		_, readErr := io.ReadFull(f, header[:])
		f.Close()
		c.fenced = string(header[:8]) == checkpointMagic
		c.valid = readErr == nil && c.matches(header[:])
		if !c.valid && readErr == nil && c.fenced && c.matchesExceptSequence(header[:]) {
			// The store changed after the save fence; a fence written at
			// close names the final sequence and the stripes to skip.
			c.valid = c.readFence(s, header[:])
		}
		c.openHeader = header
	}
	if err := s.db.Delete(checkpointCleanKey, pebble.Sync); err != nil {
		return err
	}
	if c.valid {
		// Some state-only mutations (relayout) leave the revision unchanged.
		// Never refill such a name from the older checkpoint. False positives
		// only skip fills; retain this bounded filter for repeat load calls.
		bytes := max(int64(4096), min(int64(8<<20), s.stateCache.budget()/256))
		changed := newCatalogMembership(uint64(bytes / 4))
		changed.ready.Store(true)
		s.stateCache.checkpointChanges = changed
	}

	return nil
}

func (c *metadataCheckpoint) matches(header []byte) bool {
	return c.matchesExceptSequence(header) && binary.LittleEndian.Uint64(header[40:48]) == c.sequence
}

func (c *metadataCheckpoint) matchesExceptSequence(header []byte) bool {
	return (string(header[:8]) == checkpointMagic || string(header[:8]) == checkpointLegacyMagic) &&
		bytes.Equal(header[8:24], c.identity[:]) && bytes.Equal(header[24:40], c.clean[:]) && c.clean != [16]byte{} &&
		binary.LittleEndian.Uint32(header[48:]) == crc32.ChecksumIEEE(header[:48])
}

// LoadMetadataCheckpoint streams raw slab records after caller readiness. It
// never evicts live entries and shares the existing metadata budget. A v2
// record for an unchanged name is authoritative under the clean fence.
// Legacy records stay provisional until a bounded validation window proves
// their revision. First use treats a provisional record as a cache miss.
func (s *Store) LoadMetadataCheckpoint(ctx context.Context, wait func(context.Context) error) (stats MetadataCheckpointStats, err error) {
	return s.LoadMetadataCheckpointAfter(ctx, PrewarmOptions{Wait: wait})
}

// LoadMetadataCheckpointAfter waits for readiness while registered for shutdown.
func (s *Store) LoadMetadataCheckpointAfter(ctx context.Context, options PrewarmOptions) (stats MetadataCheckpointStats, err error) {
	ready, progress := options.StartAfter, options.Progress
	started := time.Now()
	defer func() {
		stats.Duration = time.Since(started)
		if progress != nil && !stats.Valid {
			progress.work.Store(0)
			progress.total.Store(0)
			progress.passes.Store(0)
		}
	}()
	ctx, end, ok := s.prewarm.begin(ctx)
	if !ok {
		return stats, context.Canceled
	}
	defer end()
	if ready != nil {
		select {
		case <-ready:
		case <-ctx.Done():
			return stats, ctx.Err()
		}
	}
	started = time.Now()
	ctx, cancel := context.WithTimeout(ctx, MetadataCheckpointLoadTimeout)
	defer cancel()
	if !s.checkpoint.valid || s.stateCache == nil {
		return stats, nil
	}
	if progress != nil {
		progress.checkpointLoading.Add(1)
		defer progress.checkpointLoading.Add(-1)
	}
	phasecpu.Set(phasecpu.MetadataCache)
	defer phasecpu.Set(phasecpu.Other)
	f, err := s.fs.Open(s.fs.PathJoin(s.dir, checkpointFile))
	if err != nil {
		return stats, err
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 64<<10)
	var header [52]byte
	if _, err = io.ReadFull(reader, header[:]); err != nil {
		return stats, err
	}
	// The path may have been replaced since Open; certify the very file we
	// are about to stream, using the fence captured before writers started.
	if !bytes.Equal(header[:], s.checkpoint.openHeader[:]) {
		return stats, errors.New("metadata checkpoint fence changed since open")
	}
	stats.ReadBytes = int64(len(header))
	if progress != nil {
		if info, err := f.Stat(); err == nil {
			progress.work.Store(uint64(len(header)))
			progress.total.Store(uint64(info.Size()))
		}
	}
	err = s.loadCheckpointRecords(ctx, reader, options, &stats)
	stats.Valid = err == nil
	return stats, err
}

// ValidateMetadataCheckpoint validates the offered names next, in bounded
// ID-sorted batches sharing one read view and revision iterator. A writeout
// coordinator can offer its upcoming names before admitting their writes.
// Nil names finishes all provisional records without retaining all cache names.
// Already authoritative, missing and pending names need no work here.
//
// Wait uses the shared restart allowance. It runs before opening a view and
// without metric or cache locks. First use never bypasses this admission with
// a revision lookup; an unfinished record is an ordinary cache miss instead.
// StopBackground cancels and joins this work before closing the database.
func (s *Store) ValidateMetadataCheckpoint(ctx context.Context, names iter.Seq[string], options PrewarmOptions) (stats MetadataCheckpointStats, err error) {
	started := time.Now()
	defer func() { stats.Duration = time.Since(started) }()
	ctx, end, ok := s.prewarm.begin(ctx)
	if !ok {
		return stats, context.Canceled
	}
	defer end()
	if options.StartAfter != nil {
		select {
		case <-options.StartAfter:
		case <-ctx.Done():
			return stats, ctx.Err()
		}
	}
	if s.stateCache == nil || !s.checkpoint.valid {
		return stats, nil
	}
	if options.Progress != nil {
		options.Progress.checkpointLoading.Add(1)
		defer options.Progress.checkpointLoading.Add(-1)
	}
	phasecpu.Set(phasecpu.MetadataCache)
	defer phasecpu.Set(phasecpu.Other)
	batch := options.Batch
	if batch <= 0 {
		batch = 1024
	}
	batch = min(batch, 4096)
	items := make([]checkpointItem, 0, batch)
	pace := prewarmPacer{rate: options.Rate, byteRate: options.ByteRate}
	flush := func() error {
		if len(items) == 0 {
			return ctx.Err()
		}
		if options.Wait != nil {
			if err := options.Wait(ctx); err != nil {
				return err
			}
		}
		before := stats.ReadBytes
		stats.Entries += int64(len(items))
		if err := s.validateCheckpointWindow(ctx, items, &stats); err != nil {
			return err
		}
		if !pace.waitBytes(ctx, len(items), int(stats.ReadBytes-before)) {
			return ctx.Err()
		}
		items = items[:0]
		return nil
	}
	c := s.stateCache
	offer := func(sh *stateShard, slot int32) {
		if slot < 0 || sh.slots[slot].n == 0 || sh.slots[slot].epoch&provisionalState == 0 {
			return
		}
		record := sh.record(slot)
		name := string(recordKey(record))
		if sh.entries[name] == nil {
			items = append(items, checkpointItem{meta: checkpointRecordMetadata(record, name), recordEpoch: sh.slots[slot].epoch})
		}
	}
	if names != nil {
		for name := range names {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			sh := &c.shards[s.metricStripe(name)]
			sh.mu.Lock()
			_, slot := sh.find(c.hash(name), name)
			offer(sh, slot)
			sh.mu.Unlock()
			if len(items) == batch {
				if err := flush(); err != nil {
					return stats, err
				}
			}
		}
	} else {
		for i := range c.shards {
			sh := &c.shards[i]
			sh.mu.Lock()
			slots := len(sh.slots)
			sh.mu.Unlock()
			for slot := 0; slot < slots; slot++ {
				if err := ctx.Err(); err != nil {
					return stats, err
				}
				sh.mu.Lock()
				offer(sh, int32(slot))
				sh.mu.Unlock()
				if len(items) == batch {
					if err := flush(); err != nil {
						return stats, err
					}
				}
			}
		}
	}
	err = flush()
	stats.Valid = err == nil
	return stats, err
}

// Decode only the fixed metadata needed for revision validation, leaving the
// larger continuation state packed in its cache arena.
func checkpointRecordMetadata(record []byte, name string) Metadata {
	_, w := binary.Uvarint(record)
	p := w + len(name)
	m := Metadata{MetricConfig: MetricConfig{Name: name}, ID: binary.LittleEndian.Uint64(record[p:]), Generation: binary.LittleEndian.Uint64(record[p+8:]), Revision: binary.LittleEndian.Uint64(record[p+16:])}
	if nanos := int64(binary.LittleEndian.Uint64(record[p+24:])); nanos != 0 {
		m.LastUpdate = time.Unix(0, nanos)
	}
	return m
}

type checkpointItem struct {
	meta               Metadata
	recordEpoch, epoch uint64
}

// Validate ahead in bounded ID-sorted windows, avoiding a cold point lookup
// per first update. Epochs exclude changes between the view and promotion.
func (s *Store) validateCheckpointWindow(ctx context.Context, items []checkpointItem, stats *MetadataCheckpointStats) error {
	if len(items) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c := s.stateCache
	for i := range items {
		items[i].epoch = c.epoch(s.metricStripe(items[i].meta.Name))
	}
	view := s.db.NewReadView()
	defer view.Close()
	it, err := view.NewIter(&pebble.IterOptions{LowerBound: revisionLower, UpperBound: revisionUpper})
	if err != nil {
		return err
	}
	defer it.Close()
	slices.SortFunc(items, func(a, b checkpointItem) int {
		if a.meta.ID < b.meta.ID {
			return -1
		}
		if a.meta.ID > b.meta.ID {
			return 1
		}
		return 0
	})
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := revisionKey(item.meta)
		countStoreRead()
		valid := it.SeekGE(key) && bytes.Equal(it.Key(), key)
		stats.ReadBytes += int64(len(key))
		if valid {
			stats.ReadBytes += int64(len(it.Value()))
		}
		m := item.meta
		valid = valid && decodeRevision(it.Value(), &m) == nil && m.Revision == item.meta.Revision && m.LastUpdate.Equal(item.meta.LastUpdate)
		sh := &c.shards[s.metricStripe(item.meta.Name)]
		sh.mu.Lock()
		pos, slot := sh.find(c.hash(item.meta.Name), item.meta.Name)
		if slot >= 0 && sh.slots[slot].epoch == item.recordEpoch && sh.epoch.Load() == item.epoch && sh.entries[item.meta.Name] == nil {
			if valid {
				sh.slots[slot].epoch &^= provisionalState
				stats.Validated++
			} else {
				c.remove(sh, pos, slot)
				c.entries.Add(-1)
			}
		}
		sh.mu.Unlock()
	}
	return it.Error()
}

// PrepareMetadataCheckpoint runs while listeners remain available, after store
// writers stop. Close checks its sequence again and never repeats this work.
func (s *Store) PrepareMetadataCheckpoint(ctx context.Context) error {
	return s.PrepareMetadataCheckpointWithOptions(ctx, PrewarmOptions{})
}

// PrepareMetadataCheckpointWithOptions bounds parallel preparation with the same
// worker count and CPU pacers used for loading. Writers must already be stopped.
func (s *Store) PrepareMetadataCheckpointWithOptions(ctx context.Context, options PrewarmOptions) error {
	if !s.checkpoint.enabled {
		return nil
	}
	s.checkpoint.prepared = true
	s.checkpoint.saveErr = s.saveMetadataCheckpoint(ctx, options)
	return s.checkpoint.saveErr
}

func (s *Store) saveMetadataCheckpoint(ctx context.Context, options PrewarmOptions) error {
	c := &s.checkpoint
	if _, err := rand.Read(c.clean[:]); err != nil {
		return err
	}
	if err := s.db.Set(checkpointCleanKey, c.clean[:], pebble.Sync); err != nil {
		return err
	}
	// A fence from an earlier save can never match the new clean stamp;
	// remove it so only this save's close can publish one.
	if err := s.fs.Remove(s.fs.PathJoin(s.dir, checkpointFenceFile)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	s.captureFence()
	c.sequence = s.db.VisibleSequence()
	path := s.fs.PathJoin(s.dir, checkpointFile)
	tmp := path + ".tmp"
	f, err := s.fs.Create(tmp)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			f.Close()
		}
		_ = s.fs.Remove(tmp)
	}()
	// Workers already produce large buffers. Write them directly rather than
	// copying the entire checkpoint through another buffered writer.
	writer := f
	var header [52]byte
	copy(header[:8], checkpointMagic)
	copy(header[8:24], c.identity[:])
	copy(header[24:40], c.clean[:])
	binary.LittleEndian.PutUint64(header[40:48], c.sequence)
	binary.LittleEndian.PutUint32(header[48:], crc32.ChecksumIEEE(header[:48]))
	if err := writeCheckpointBytes(writer, header[:]); err != nil {
		return err
	}
	if err := s.saveCheckpointRecords(ctx, writer, options); err != nil {
		return err
	}
	if err := writeCheckpointBytes(writer, make([]byte, 8)); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	err = f.Close()
	closed = true
	if err != nil {
		return err
	}
	// Writes during the save no longer fail it: close fences the stripes
	// they mutated (see finishMetadataCheckpoint).
	if err := s.fs.Rename(tmp, path); err != nil {
		return err
	}
	return syncStoreDirectory(s.fs, s.dir)
}

// MetadataCheckpointError exposes a failed optional save without failing close.
func (s *Store) MetadataCheckpointError() error { return s.checkpoint.saveErr }

// First use must not perform optional revision I/O while holding a writer's
// stripe or a reader's view. Such work cannot safely wait behind writeout's
// CPU admission. Discard this provisional entry so the caller can refill it
// through the ordinary authoritative cache-miss path. The normal reader epoch
// and writer stripe protocols still protect that fill.
func (c *stateCache) validateCheckpoint(stripe uint64, name string, _ *metricState, recordEpoch uint64) *metricState {
	sh := &c.shards[stripe]
	sh.mu.Lock()
	pos, slot := sh.find(c.hash(name), name)
	if slot >= 0 && sh.slots[slot].epoch == recordEpoch {
		c.remove(sh, pos, slot)
		if sh.entries[name] == nil {
			c.entries.Add(-1)
		}
	}
	sh.mu.Unlock()
	return nil
}
