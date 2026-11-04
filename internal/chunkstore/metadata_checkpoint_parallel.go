package chunkstore

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

const checkpointBlockSize = 4 << 20

func writeCheckpointBytes(writer io.Writer, data []byte) error {
	n, err := writer.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

func checkpointWorkers(options PrewarmOptions) int {
	workers := options.Workers
	if workers == 0 {
		workers = DefaultPrewarmWorkers()
	}
	return max(1, min(workers, 256))
}

// Cut the existing v1/v2 stream into independently processed sections ending at
// record boundaries. This needs neither a preliminary index scan nor a format
// change: the very next restart can parallelize a predecessor's checkpoint.
// At most workers+1 blocks are in flight, each normally 4 MiB. A single large
// record can extend a block by at most maxCheckpointRecordSize+8 bytes.
func readCheckpointBlock(r io.Reader, buf []byte) ([]byte, bool, error) {
	buf = buf[:checkpointBlockSize]
	n, err := io.ReadFull(r, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, false, err
	}
	buf = buf[:n]
	extend := func(end int) error {
		if end <= len(buf) {
			return nil
		}
		start := len(buf)
		buf = append(buf, make([]byte, end-start)...)
		_, err := io.ReadFull(r, buf[start:])
		return err
	}
	for p := 0; ; {
		if err := extend(p + 8); err != nil {
			return nil, false, err
		}
		size := binary.LittleEndian.Uint32(buf[p:])
		if size == 0 {
			return buf[:p+8], true, nil
		}
		if size > maxCheckpointRecordSize {
			return nil, false, errors.New("metadata checkpoint record too large")
		}
		p += 8 + int(size)
		if err := extend(p); err != nil {
			return nil, false, err
		}
		if p >= checkpointBlockSize {
			return buf[:p], false, nil
		}
	}
}

func (s *Store) loadCheckpointRecords(ctx context.Context, reader io.Reader, options PrewarmOptions, stats *MetadataCheckpointStats) error {
	workers := checkpointWorkers(options)
	group, ctx := errgroup.WithContext(ctx)
	blocks := make(chan []byte)
	free := make(chan []byte, workers+1)
	for range workers + 1 {
		free <- make([]byte, checkpointBlockSize)
	}
	pace := &prewarmPacer{rate: options.Rate, byteRate: options.ByteRate}
	results := make([]MetadataCheckpointStats, workers)
	for i := range workers {
		wait := options.Wait
		var idle func(context.Context) error
		finish := func() {}
		if options.CheckpointWorker != nil {
			pacer := options.CheckpointWorker()
			wait, idle, finish = pacer.Wait, pacer.Idle, pacer.Finish
		}
		group.Go(func() error {
			defer finish()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case block, ok := <-blocks:
					if !ok {
						return nil
					}
					err := s.loadCheckpointBlock(ctx, block, options, wait, pace, &results[i])
					if err == nil && idle != nil {
						err = idle(ctx)
					}
					if options.Progress != nil {
						options.Progress.work.Add(uint64(len(block)))
					}
					free <- block
					if err != nil {
						return err
					}
				}
			}
		})
	}
	group.Go(func() error {
		defer close(blocks)
		for {
			if options.CheckpointWait != nil {
				if err := options.CheckpointWait(ctx); err != nil {
					return err
				}
			}
			var buf []byte
			select {
			case <-ctx.Done():
				return ctx.Err()
			case buf = <-free:
			}
			block, last, err := readCheckpointBlock(reader, buf)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case blocks <- block:
			}
			if last {
				return nil
			}
		}
	})
	err := group.Wait()
	for _, result := range results {
		stats.Entries += result.Entries
		stats.Bytes += result.Bytes
		stats.ReadBytes += result.ReadBytes
		stats.Fenced += result.Fenced
		stats.Validated += result.Validated
	}
	return err
}

func (s *Store) loadCheckpointBlock(ctx context.Context, block []byte, options PrewarmOptions, wait func(context.Context) error, pace *prewarmPacer, stats *MetadataCheckpointStats) error {
	batch := options.Batch
	if batch <= 0 {
		batch = 1024
	}
	batch = min(batch, 4096)
	var items []checkpointItem
	if !s.checkpoint.fenced && !s.checkpoint.lazy {
		items = make([]checkpointItem, 0, batch)
	}
	validate := func() error {
		if len(items) == 0 {
			return nil
		}
		if options.Wait != nil {
			if err := options.Wait(ctx); err != nil {
				return err
			}
		}
		before := stats.ReadBytes
		if err := s.validateCheckpointWindow(ctx, items, stats); err != nil {
			return err
		}
		items = items[:0]
		if !pace.waitBytes(ctx, 0, int(stats.ReadBytes-before)) {
			return ctx.Err()
		}
		return nil
	}
	c := s.stateCache
	for p := 0; p < len(block); {
		if err := ctx.Err(); err != nil {
			return err
		}
		if wait != nil {
			if err := wait(ctx); err != nil {
				return err
			}
		}
		start, records := p, 0
		var sh *stateShard
		var added, addedBytes int64
		unlock := func() {
			if sh != nil {
				c.account(sh, addedBytes+added*int64(stateSlotOverhead))
				c.entries.Add(added)
				sh.mu.Unlock()
				sh = nil
				added, addedBytes = 0, 0
			}
		}
		var recordErr error
		for records < DefaultPrewarmBatch && p-start < 64<<10 && p < len(block) {
			n := binary.LittleEndian.Uint32(block[p:])
			checksum := binary.LittleEndian.Uint32(block[p+4:])
			p += 8
			if n == 0 {
				break
			}
			record := block[p : p+int(n)]
			p += int(n)
			records++
			if crc32.ChecksumIEEE(record) != checksum {
				recordErr = errors.New("metadata checkpoint checksum mismatch")
				break
			}
			keyLen, w := binary.Uvarint(record)
			if w <= 0 || keyLen == 0 || keyLen > uint64(len(record)-w) || uint64(len(record)-w)-keyLen < stateRecordFixed+1 {
				recordErr = errors.New("invalid metadata checkpoint record")
				break
			}
			if record[w+int(keyLen)+41]&stateRecordOwnName != 0 {
				continue
			}
			// The temporary name never escapes this window. The cache copies
			// the complete record to its immutable arena, without a name object.
			name := unsafeString(record[w : w+int(keyLen)])
			next := &c.shards[s.metricStripe(name)]
			if sh != next {
				unlock()
				sh = next
				sh.mu.Lock()
			}
			hash := c.hash(name)
			_, slot := sh.find(hash, name)
			changed := c.checkpointChanges != nil && c.checkpointChanges.mayContain(name)
			if changed || slot >= 0 || sh.entries[name] != nil || sh.bytes+addedBytes+(added+1)*int64(stateSlotOverhead)+int64(n) > c.shardBudget {
				continue
			}
			slot = c.newSlot(sh)
			chunk, off, dst := c.reserve(&sh.arena, int(n))
			copy(dst[:n], record)
			epoch := sh.epoch.Load()
			if !s.checkpoint.fenced {
				epoch |= provisionalState
			}
			sh.slots[slot] = stateSlot{hash: uint32(hash), epoch: epoch, chunk: chunk, off: off, n: n}
			c.index(sh, slot)
			sh.pushFront(slot)
			added++
			addedBytes += int64(n)
			stats.Entries++
			stats.Bytes += int64(n)
			if s.checkpoint.fenced {
				stats.Fenced++
			} else if !s.checkpoint.lazy {
				// Validation is synchronous and finishes before this worker
				// returns the block to the pool, so its names can alias it too.
				items = append(items, checkpointItem{meta: checkpointRecordMetadata(record, name), recordEpoch: epoch})
				if len(items) == batch {
					break
				}
			}
		}
		unlock()
		stats.ReadBytes += int64(p - start)
		if recordErr != nil {
			return recordErr
		}
		if len(items) >= batch {
			if err := validate(); err != nil {
				return err
			}
		}
		if !pace.waitBytes(ctx, records, p-start) {
			return ctx.Err()
		}
	}
	return validate()
}

// Shards are independent preparation sections. Workers copy bounded windows
// under the shard lock, then checksum outside it. One writer appends large
// sequential buffers, avoiding competing writes to the same file/inode.
func (s *Store) saveCheckpointRecords(ctx context.Context, writer io.Writer, options PrewarmOptions) error {
	workers := checkpointWorkers(options)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)
	blocks := make(chan []byte)
	free := make(chan []byte, workers)
	for range workers {
		free <- make([]byte, 0, checkpointBlockSize)
	}
	var next atomic.Uint64
	for range workers {
		wait := options.Wait
		var idle func(context.Context) error
		finish := func() {}
		if options.CheckpointWorker != nil {
			pacer := options.CheckpointWorker()
			wait, idle, finish = pacer.Wait, pacer.Idle, pacer.Finish
		}
		group.Go(func() error {
			defer finish()
			var buf []byte
			get := func() error {
				buf = nil
				select {
				case <-ctx.Done():
					return ctx.Err()
				case buf = <-free:
					buf = buf[:0]
					return nil
				}
			}
			flush := func() error {
				if len(buf) == 0 {
					return nil
				}
				if idle != nil {
					if err := idle(ctx); err != nil {
						return err
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case blocks <- buf:
					return get()
				}
			}
			if err := get(); err != nil {
				return err
			}
			defer func() {
				if buf != nil {
					free <- buf
				}
			}()
			for i := next.Add(1) - 1; i < metricStripes; i = next.Add(1) - 1 {
				sh := &s.stateCache.shards[i]
				sh.mu.Lock()
				slots := len(sh.slots)
				sh.mu.Unlock()
				for slot := 0; slot < slots; {
					if err := ctx.Err(); err != nil {
						return err
					}
					if wait != nil {
						if err := wait(ctx); err != nil {
							return err
						}
					}
					start := len(buf)
					sh.mu.Lock()
					end := min(slot+DefaultPrewarmBatch, slots)
					for ; slot < end; slot++ {
						saved := &sh.slots[slot]
						if saved.n == 0 || saved.n > maxCheckpointRecordSize || saved.epoch&provisionalState != 0 {
							continue
						}
						record := sh.record(int32(slot))
						if sh.entries[string(recordKey(record))] != nil {
							continue
						}
						buf = binary.LittleEndian.AppendUint32(buf, saved.n)
						buf = binary.LittleEndian.AppendUint32(buf, 0)
						buf = append(buf, record...)
						if len(buf) >= checkpointBlockSize {
							slot++
							break
						}
					}
					sh.mu.Unlock()
					for p := start; p < len(buf); {
						n := int(binary.LittleEndian.Uint32(buf[p:]))
						binary.LittleEndian.PutUint32(buf[p+4:], crc32.ChecksumIEEE(buf[p+8:p+8+n]))
						p += 8 + n
					}
					if len(buf) >= checkpointBlockSize {
						if err := flush(); err != nil {
							return err
						}
					}
				}
			}
			return flush()
		})
	}
	// Closing is independent of the writer: a failed write cancels blocked
	// producers, and all producers are joined before the file can be closed.
	done := make(chan error, 1)
	go func() { done <- group.Wait(); close(blocks) }()
	var writeErr error
	for block := range blocks {
		if writeErr == nil {
			writeErr = writeCheckpointBytes(writer, block)
			if writeErr != nil {
				cancel()
			}
		}
		free <- block[:0]
	}
	err := <-done
	if writeErr != nil {
		return writeErr
	}
	return err
}
