package chunkstore

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// These are receiver limits, independent of a controller's range/batch flags or
// the machine's CPU count. Oversized names fail before decoding their archives.
// Ready intents within the metadata budget retain their original identity;
// oversized legacy metadata fails closed rather than allocating without a limit.
const (
	// NativeMaxRangeNames bounds range metadata independently of history size.
	NativeMaxRangeNames       = 4096
	nativeRangeMetadataBytes  = 16 << 20
	nativeMetricBytes         = 64 << 20
	nativeMetricPoints        = 1 << 20
	nativeTableBytes          = 8 << 20
	nativeTableWriters        = 8
	nativeIngestFiles         = 8
	nativeCombineBatch        = 32
	nativeCombineMemory       = 2 << 30
	nativeCombineWorkerMemory = 1 << 30
)

// ValidateNativeNames rejects oversized new requests before import/export work.
// It does not change durable range identities or prevent receipt recovery.
func ValidateNativeNames(names []string) error {
	if len(names) > NativeMaxRangeNames {
		return fmt.Errorf("native range has %d names (limit %d): %w", len(names), NativeMaxRangeNames, ErrTransferLimit)
	}
	var size int
	for _, name := range names {
		size += len(name)
		if size > 1<<20 {
			return fmt.Errorf("native range names exceed 1 MiB: %w", ErrTransferLimit)
		}
	}
	return nil
}

func (s *Store) initNativeContext() {
	s.nativeContextOnce.Do(func() {
		s.nativeContext, s.nativeCancel = context.WithCancel(context.Background())
		s.nativeAdmission = make(chan struct{}, 1)
		s.nativeExportAdmission = make(chan struct{}, 1)
	})
}

// CancelNativeTransfers stops native copy work before persistence drains during
// shutdown. It does not stop foreground writes or maintenance needed by the dump.
func (s *Store) CancelNativeTransfers() { s.initNativeContext(); s.nativeCancel() }

// NativeTransferContext joins request cancellation with the store stop signal.
// The caller must release the returned cancellation registration.
func (s *Store) NativeTransferContext(parent context.Context) (context.Context, context.CancelFunc) {
	s.initNativeContext()
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(s.nativeContext, cancel)
	if s.nativeContext.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}

func (s *Store) admitNative(ctx context.Context) (func(), error) {
	s.initNativeContext()
	return admitNativeWork(ctx, s.nativeAdmission)
}
func (s *Store) admitNativeExport(ctx context.Context) (func(), error) {
	s.initNativeContext()
	return admitNativeWork(ctx, s.nativeExportAdmission)
}
func admitNativeWork(ctx context.Context, admission chan struct{}) (func(), error) {
	select {
	case admission <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-admission
			return nil, err
		}
		return func() { <-admission }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func nativeMetadataSize(c MetricConfig) int64 {
	return int64(512 + 2*len(c.Name) + 32*len(c.Retentions))
}

func validateNativeMemory(r TransferMetric) error {
	if transferSize(r)+int64(len(r.Keys))*48 > nativeMetricBytes {
		return fmt.Errorf("native metric %q exceeds %d bytes: %w", r.Config.Name, nativeMetricBytes, ErrTransferLimit)
	}
	var capacity int64
	for _, retention := range r.Config.Retentions {
		if retention.Count < 0 || int64(retention.Count) > nativeMetricPoints-capacity {
			return fmt.Errorf("native metric %q exceeds %d archive slots: %w", r.Config.Name, nativeMetricPoints, ErrTransferLimit)
		}
		capacity += int64(retention.Count)
	}
	// Physical history can exceed the current retention (OOO files and old
	// rings). Count its groups too, before invoking any archive decoder.
	var points int64
	for _, kv := range r.Keys {
		key := kv.Key
		if isContinuationBucketSuffix(key) {
			key = key[5:]
		}
		n := int64(128) // classic groups; conservatively charge metadata too
		if len(key) == 20 && key[0] == 'f' && (key[9] == 'c' || key[9] == 'r') && len(kv.Value) > 0 && kv.Value[0] >= 3 {
			_, _, count, err := sealedHeaderFields(kv.Value)
			if err != nil {
				return err
			}
			n = int64(count)
		}
		points += n
		if points > nativeMetricPoints {
			return fmt.Errorf("native metric %q exceeds %d physical points: %w", r.Config.Name, nativeMetricPoints, ErrTransferLimit)
		}
	}
	return nil
}

// Receipt lookups retain the same publication/durability barrier, but a
// disconnected requester no longer waits indefinitely for another range call.
func (s *Store) lockNativeRange(ctx context.Context, identity string) (func(), error) {
	value, _ := s.nativeRanges.LoadOrStore(identity, make(chan struct{}, 1))
	lock := value.(chan struct{})
	select {
	case lock <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-lock
			return nil, err
		}
		return func() { <-lock }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Published old IDs are immutable. Stream their point tombstones through small
// WAL batches, including when resuming pieces published by an older receiver.
// Keep the intent and pieces until the final receipt so partial cleanup replays.
func (s *Store) retireNativeCombine(ctx context.Context, old Metadata) error {
	s.canonicalRun(old.ID).Clear(old.ID)
	b := s.db.NewBatch()
	defer func() { b.Close() }()
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			s.nativeWriteFailed(err)
			return err
		}
		b.Close()
		b = s.db.NewBatch()
		return s.nativeStep("combine-retire-batch")
	}
	remove := func(prefix []byte) error {
		it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if err != nil {
			return err
		}
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := b.Delete(it.Key(), nil); err != nil {
				return err
			}
			if b.Len() >= 1<<20 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return it.Error()
	}
	if old.Continuation {
		kv := s.newContinuationIO(ctx, s.db, nil, old).withIterator()
		defer kv.Close()
		if dropper, ok := s.continuation.(ContinuationDropper); ok {
			io := &nativeRetireIO{ContinuationIO: kv, remove: func(prefix []byte) error { return remove(kv.key(prefix)) }}
			if err := dropper.Drop(ctx, io); err != nil {
				return err
			}
		} else if s.timeBuckets {
			return ErrFormat
		}
		if err := remove(kv.key(nil)); err != nil {
			return err
		}
	} else if err := remove(chunkPrefix(old.ID, 0)[:9]); err != nil {
		return err
	}
	if err := b.Delete(transferReceiptKey(old), nil); err != nil {
		return err
	}
	if err := b.Delete(revisionKey(old), nil); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	return s.nativeStep("combine-name-retired")
}

type nativeRetireIO struct {
	ContinuationIO
	remove func([]byte) error
}

func (io *nativeRetireIO) DeletePrefix(prefix []byte) error { return io.remove(prefix) }

// Pebble ingestion is synchronous and has no context API. Check cancellation
// between bounded groups; never detach an ingest goroutine on cancellation.
func (s *Store) ingestNativeFiles(ctx context.Context, paths []string) error {
	for len(paths) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(nativeIngestFiles, len(paths))
		stats, err := s.db.IngestWithStats(paths[:n])
		if err != nil {
			s.nativeWriteFailed(err)
			return err
		}
		s.nativeCounters.sst.Add(stats.Bytes)
		s.ingestStats.tables.Add(uint64(n))
		s.ingestStats.bytes.Add(stats.Bytes)
		s.ingestStats.l0Bytes.Add(stats.ApproxIngestedIntoL0Bytes)
		s.ingestStats.memtableOverlaps.Add(uint64(stats.MemtableOverlappingFiles))
		if err := s.nativeStep("native-ingest-batch"); err != nil {
			return err
		}
		paths = paths[n:]
	}
	return ctx.Err()
}
