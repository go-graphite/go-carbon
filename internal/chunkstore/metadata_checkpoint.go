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
	"iter"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/phasecpu"
	"golang.org/x/sync/errgroup"
)

const checkpointFile = "metadata-cache-v1"

// v2 certifies that every saved record was authoritative at the clean fence.
// v1 may contain provisional records and must still validate their revisions.
const checkpointMagic = "GCSTATE2"
const checkpointLegacyMagic = "GCSTATE1"
const maxCheckpointRecordSize = 16 << 20
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
}

// MetadataCheckpointStats describes the advisory checkpoint, not durable data.
type MetadataCheckpointStats struct {
	Entries, Bytes, ReadBytes, Validated int64
	// Fenced records need no revision read: the v2 clean fence and the
	// mutation filter establish that their saved state is still current.
	Fenced   int64
	Duration time.Duration
	Valid    bool
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
	return (string(header[:8]) == checkpointMagic || string(header[:8]) == checkpointLegacyMagic) &&
		bytes.Equal(header[8:24], c.identity[:]) && bytes.Equal(header[24:40], c.clean[:]) && c.clean != [16]byte{} &&
		binary.LittleEndian.Uint64(header[40:48]) == c.sequence && binary.LittleEndian.Uint32(header[48:]) == crc32.ChecksumIEEE(header[:48])
}

// LoadMetadataCheckpoint streams raw slab records after input readiness. It
// never evicts live entries and shares the existing metadata budget. A v2
// record for an unchanged name is authoritative under the clean fence.
// Legacy records stay provisional until a bounded validation window proves
// their revision. First use treats a provisional record as a cache miss.
func (s *Store) LoadMetadataCheckpoint(ctx context.Context, wait func(context.Context) error) (stats MetadataCheckpointStats, err error) {
	return s.LoadMetadataCheckpointAfter(ctx, PrewarmOptions{Wait: wait})
}

// LoadMetadataCheckpointAfter waits for readiness while registered for shutdown.
func (s *Store) LoadMetadataCheckpointAfter(ctx context.Context, options PrewarmOptions) (stats MetadataCheckpointStats, err error) {
	ready, wait, progress := options.StartAfter, options.Wait, options.Progress
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
	reader := bufio.NewReaderSize(f, 256<<10)
	var header [52]byte
	if _, err = io.ReadFull(reader, header[:]); err != nil {
		return stats, err
	}
	// The path may have been replaced since Open; certify the very file we
	// are about to stream, using the fence captured before writers started.
	if !bytes.Equal(header[:], s.checkpoint.openHeader[:]) {
		return stats, errors.New("metadata checkpoint fence changed since open")
	}
	var frame [8]byte
	var records, readBytes uint64
	var pacedBytes int64
	pace := prewarmPacer{rate: options.Rate, byteRate: options.ByteRate}
	readBytes = uint64(len(header))
	stats.ReadBytes = int64(len(header))
	if progress != nil {
		if info, err := f.Stat(); err == nil {
			progress.work.Store(readBytes)
			progress.total.Store(uint64(info.Size()))
		}
	}
	// Bound both queued validation records and concurrent read views. Reuse a
	// view/iterator across a larger ID-sorted batch, then close it before pacing.
	workers := options.Workers
	if workers == 0 {
		workers = DefaultPrewarmWorkers()
	}
	workers = max(1, min(workers, 256))
	if s.checkpoint.fenced || s.checkpoint.lazy {
		workers = 0
	}
	batch := options.Batch
	if batch <= 0 {
		batch = 1024
	}
	batch = min(batch, 4096)
	group, validationCtx := errgroup.WithContext(ctx)
	ctx = validationCtx
	windows := make(chan []checkpointItem, workers)
	results := make([]MetadataCheckpointStats, workers)
	for i := range workers {
		group.Go(func() error {
			for items := range windows {
				if wait != nil {
					if err := wait(ctx); err != nil {
						return err
					}
				}
				before := results[i].ReadBytes
				if err := s.validateCheckpointWindow(ctx, items, &results[i]); err != nil {
					return err
				}
				if !pace.waitBytes(ctx, 0, int(results[i].ReadBytes-before)) {
					return ctx.Err()
				}
			}
			return nil
		})
	}
	defer func() {
		close(windows)
		if validationErr := group.Wait(); validationErr != nil {
			stats.Valid = false
			if err == nil || errors.Is(err, context.Canceled) {
				err = validationErr
			}
		}
		for _, result := range results {
			stats.Validated += result.Validated
			stats.ReadBytes += result.ReadBytes
		}
	}()
	submit := func(items []checkpointItem) error {
		if len(items) == 0 {
			return nil
		}
		select {
		case windows <- items:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var record []byte
	window := make([]checkpointItem, 0, batch)
	for {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		if records%DefaultPrewarmBatch == 0 {
			if records > 0 {
				if !pace.waitBytes(ctx, DefaultPrewarmBatch, int(stats.ReadBytes-pacedBytes)) {
					return stats, ctx.Err()
				}
				pacedBytes = stats.ReadBytes
			}
			if wait != nil {
				if err = wait(ctx); err != nil {
					return stats, err
				}
			}
		}
		if _, err = io.ReadFull(reader, frame[:]); err != nil {
			return stats, err
		}
		n := binary.LittleEndian.Uint32(frame[:4])
		readBytes += uint64(len(frame))
		stats.ReadBytes += int64(len(frame))
		if n == 0 {
			if progress != nil {
				progress.work.Store(readBytes)
			}
			if err := submit(window); err != nil {
				return stats, err
			}
			if !pace.waitBytes(ctx, int(records%DefaultPrewarmBatch), int(stats.ReadBytes-pacedBytes)) {
				return stats, ctx.Err()
			}
			stats.Valid = true
			return stats, nil
		}
		if n > maxCheckpointRecordSize {
			return stats, errors.New("metadata checkpoint record too large")
		}
		if cap(record) < int(n) {
			record = make([]byte, n)
		}
		record = record[:n]
		if _, err = io.ReadFull(reader, record); err != nil {
			return stats, err
		}
		records++
		readBytes += uint64(n)
		stats.ReadBytes += int64(n)
		if progress != nil && records%DefaultPrewarmBatch == 0 {
			progress.work.Store(readBytes)
		}
		if crc32.ChecksumIEEE(record) != binary.LittleEndian.Uint32(frame[4:]) {
			return stats, errors.New("metadata checkpoint checksum mismatch")
		}
		keyLen, w := binary.Uvarint(record)
		if w <= 0 || keyLen == 0 || keyLen > uint64(len(record)-w) || uint64(len(record)-w)-keyLen < stateRecordFixed+1 {
			return stats, errors.New("invalid metadata checkpoint record")
		}
		// Alias records need their original catalog lookup; never promote one
		// using only the cached key's binding from the clean fence.
		if record[w+int(keyLen)+41]&stateRecordOwnName != 0 {
			continue
		}
		name := string(record[w : w+int(keyLen)])
		c := s.stateCache
		sh := &c.shards[s.metricStripe(name)]
		hash := c.hash(name)
		sh.mu.Lock()
		_, slot := sh.find(hash, name)
		changed := c.checkpointChanges != nil && c.checkpointChanges.mayContain(name)
		if !changed && slot < 0 && sh.entries[name] == nil && sh.bytes+int64(n)+int64(stateSlotOverhead) <= c.shardBudget {
			slot = c.newSlot(sh)
			chunk, off, dst := c.reserve(&sh.arena, int(n))
			copy(dst[:n], record)
			epoch := sh.epoch.Load()
			if !s.checkpoint.fenced {
				epoch |= provisionalState
			}
			sh.slots[slot] = stateSlot{hash: hash, epoch: epoch, chunk: chunk, off: off, n: n}
			c.index(sh, slot)
			sh.pushFront(slot)
			c.account(sh, int64(n)+int64(stateSlotOverhead))
			c.entries.Add(1)
			stats.Entries++
			stats.Bytes += int64(n)
			if s.checkpoint.fenced {
				stats.Fenced++
			} else if !s.checkpoint.lazy {
				window = append(window, checkpointItem{meta: checkpointRecordMetadata(record, name), recordEpoch: sh.slots[slot].epoch})
			}
		}
		sh.mu.Unlock()
		if len(window) == batch {
			if err := submit(window); err != nil {
				return stats, err
			}
			window = make([]checkpointItem, 0, batch)
		}
	}
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
	if !s.checkpoint.enabled {
		return nil
	}
	s.checkpoint.prepared = true
	s.checkpoint.saveErr = s.saveMetadataCheckpoint(ctx)
	return s.checkpoint.saveErr
}

func (s *Store) saveMetadataCheckpoint(ctx context.Context) error {
	c := &s.checkpoint
	if _, err := rand.Read(c.clean[:]); err != nil {
		return err
	}
	if err := s.db.Set(checkpointCleanKey, c.clean[:], pebble.Sync); err != nil {
		return err
	}
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
	writer := bufio.NewWriterSize(f, 256<<10)
	var header [52]byte
	copy(header[:8], checkpointMagic)
	copy(header[8:24], c.identity[:])
	copy(header[24:40], c.clean[:])
	binary.LittleEndian.PutUint64(header[40:48], c.sequence)
	binary.LittleEndian.PutUint32(header[48:], crc32.ChecksumIEEE(header[:48]))
	if _, err := writer.Write(header[:]); err != nil {
		return err
	}
	var record []byte
	var frame [8]byte
	for i := range s.stateCache.shards {
		sh := &s.stateCache.shards[i]
		// Reads still run during Prepare and may fill or evict cache slots.
		sh.mu.Lock()
		slots := len(sh.slots)
		sh.mu.Unlock()
		for slot := 0; slot < slots; slot++ {
			if slot%32 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			sh.mu.Lock()
			live := sh.slots[slot].n != 0 && sh.slots[slot].n <= maxCheckpointRecordSize && sh.slots[slot].epoch&provisionalState == 0
			if live {
				// A v1 lazy load or a mutation in progress cannot be certified
				// by the v2 fence. Omit it rather than laundering old state.
				live = sh.entries[string(recordKey(sh.record(int32(slot))))] == nil
			}
			if live {
				record = append(record[:0], sh.record(int32(slot))...)
			}
			sh.mu.Unlock()
			if !live {
				continue
			}
			binary.LittleEndian.PutUint32(frame[:4], uint32(len(record)))
			binary.LittleEndian.PutUint32(frame[4:], crc32.ChecksumIEEE(record))
			if _, err := writer.Write(frame[:]); err != nil {
				return err
			}
			if _, err := writer.Write(record); err != nil {
				return err
			}
		}
	}
	if _, err := writer.Write(make([]byte, 8)); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
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
	if s.db.VisibleSequence() != c.sequence {
		return errors.New("store changed during metadata checkpoint")
	}
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
