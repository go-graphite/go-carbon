package chunkstore

import (
	"context"
	"fmt"
	"runtime"

	"github.com/cockroachdb/pebble"
)

// These are receiver limits, independent of a controller's range/batch flags or
// the machine's CPU count. Oversized names are reported before decoding their archives.
// Ready intents within the metadata budget retain their original identity;
// oversized legacy metadata fails closed rather than allocating without a limit.
const (
	// NativeMaxRangeNames bounds range metadata independently of history size.
	NativeMaxRangeNames       = 4096
	nativeRangeMetadataBytes  = 16 << 20
	nativeMetricBytes         = 128 << 20
	nativeMetricPoints        = 32 << 20
	nativeTableBytes          = 8 << 20
	nativeTableWriters        = 8
	nativeIngestFiles         = 8
	nativeCombineBatch        = 32
	nativeCombineMemory       = 2 << 30
	nativeCombineWorkerMemory = 1 << 30
)

const (
	DefaultNativeImportConcurrency = 1
	DefaultNativeExportConcurrency = 0
	// Conservative planning charge for decoded points, source/output buffers,
	// range metadata and iterators at the existing per-name limits. This is
	// additional to import scratch and the store/cache baseline, not an RSS cap.
	NativeExportMemoryPerStream int64 = 17 << 30
	DefaultNativeCombineWorkers       = 2
	DefaultNativeCombineMemory        = nativeCombineMemory
)

// ValidateNativeExportConcurrency bounds independent export scratch allocations.
func ValidateNativeExportConcurrency(exports int) error {
	if exports < 0 || exports > 8 {
		return fmt.Errorf("native export concurrency must be 0..8")
	}
	return nil
}

// AutoNativeExportConcurrency scales independent export work with CPU capacity.
// Keep the existing maximum scratch bound; the application also budgets RAM.
func AutoNativeExportConcurrency() int {
	return min(8, max(1, runtime.GOMAXPROCS(0)/8))
}

// NativeExportConcurrency is fixed at Open. Waiting exporters are admitted
// before allocating range metadata, iterators or materialized history.
func (s *Store) NativeExportConcurrency() int {
	if s.nativeExportConcurrency == 0 {
		return AutoNativeExportConcurrency()
	}
	return s.nativeExportConcurrency
}

// ValidateNativeImportBudget partitions one store-wide scratch allowance across
// all admitted imports. Increasing admission must not multiply that allowance.
func ValidateNativeImportBudget(imports, workers int, memory int64) error {
	if imports < 1 || imports > 8 {
		return fmt.Errorf("native import concurrency must be 1..8")
	}
	return ValidateNativeCombineBudget(workers, memory/int64(imports))
}

// NativeImportConcurrency is fixed at Open; Buckyd uses the same admission cap
// before opening a donor so queued requests do not retain export responses.
func (s *Store) NativeImportConcurrency() int {
	if s.nativeImportConcurrency == 0 {
		return DefaultNativeImportConcurrency
	}
	return s.nativeImportConcurrency
}

// ValidateNativeCombineBudget keeps at least the original 1 GiB allowance per
// worker. The application additionally caps increases against available RAM.
func ValidateNativeCombineBudget(workers int, memory int64) error {
	if workers < 1 || workers > 32 {
		return fmt.Errorf("native combine workers must be 1..32")
	}
	if memory/int64(workers) < nativeCombineWorkerMemory {
		return fmt.Errorf("native combine memory must provide at least 1 GiB per worker")
	}
	return nil
}

func (s *Store) nativeWorkerMemory() int64 {
	if s.nativeCombineWorkerMemory == 0 {
		return nativeCombineWorkerMemory
	}
	return s.nativeCombineWorkerMemory
}

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
		s.nativeAdmission = make(chan struct{}, s.NativeImportConcurrency())
		s.nativeExportAdmission = make(chan struct{}, s.NativeExportConcurrency())
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

// NativeSkip records a name left uncopied by size admission. Sizes are measured
// without decoding history. DeferredCount includes these ordinals; they must
// never be mistaken for completed work or sent to the metric fallback silently.
type NativeSkip struct {
	Ordinal                                          int
	Name                                             string
	Bytes, PhysicalPoints, ArchiveSlots, MemoryBytes int64
	Reason                                           string
}

func (n *NativeSkip) add(k, v []byte) error {
	n.Bytes += int64(len(k) + len(v) + 56)
	if isContinuationBucketSuffix(k) {
		k = k[5:]
	}
	points := int64(128) // conservatively include classic groups and metadata
	if len(k) == 20 && k[0] == 'f' && (k[9] == 'c' || k[9] == 'r') && len(v) > 0 && v[0] >= 3 {
		_, _, count, err := sealedHeaderFields(v)
		if err != nil {
			return err
		}
		points = int64(count)
	}
	n.PhysicalPoints += points
	return nil
}
func nativeSize(c MetricConfig) NativeSkip {
	n := NativeSkip{Name: c.Name, Bytes: nativeMetadataSize(c)}
	for _, r := range c.Retentions {
		n.ArchiveSlots += int64(r.Count)
	}
	return n
}
func (n NativeSkip) exceedsLimit() bool {
	return n.Bytes > nativeMetricBytes || n.PhysicalPoints > nativeMetricPoints || n.ArchiveSlots > nativeMetricPoints
}
func (n *NativeSkip) Error() string {
	return fmt.Sprintf("native metric %q: %s (%d bytes, %d physical points, %d archive slots, %d estimated worker bytes): %v", n.Name, n.Reason, n.Bytes, n.PhysicalPoints, n.ArchiveSlots, n.MemoryBytes, ErrTransferLimit)
}
func (n *NativeSkip) Unwrap() error { return ErrTransferLimit }
func nativeRecordSize(r TransferMetric) (NativeSkip, error) {
	n := nativeSize(r.Config)
	for _, kv := range r.Keys {
		if err := n.add(kv.Key, kv.Value); err != nil {
			return n, err
		}
	}
	return n, nil
}
func validateNativeMemory(r TransferMetric) error {
	n, err := nativeRecordSize(r)
	if err != nil {
		return err
	}
	if n.exceedsLimit() {
		n.Reason = "name limit"
		return &n
	}
	return nil
}

// Input arenas, vector growth, encoded output and codec bookkeeping all belong
// to the worker until its ordered SST consumer releases it. A codec must supply
// an estimate before it can use the large-name path; reference materialization
// has a separate conservative charge before any whole-archive allocation.
type ContinuationNativeMemory interface {
	NativeCombineMemory(TransferMetric, TransferMetric) (int64, error)
}

func (s *Store) admitNativeCombineRecords(source, target TransferMetric, materialize bool) error {
	a, err := nativeRecordSize(source)
	if err != nil {
		return err
	}
	b, err := nativeRecordSize(target)
	if err != nil {
		return err
	}
	memory := 4*(a.Bytes+b.Bytes) + int64(len(source.Keys)+len(target.Keys))*256
	if materialize {
		memory += 512 * (a.PhysicalPoints + b.PhysicalPoints + a.ArchiveSlots)
	} else if source.Continuation {
		codec, ok := s.continuation.(ContinuationNativeMemory)
		if !ok {
			return s.admitNativeCombineRecords(source, target, true)
		}
		scratch, err := codec.NativeCombineMemory(source, target)
		if err != nil {
			return err
		}
		memory += scratch
	}
	if memory > s.nativeWorkerMemory() {
		a.Bytes += b.Bytes
		a.PhysicalPoints += b.PhysicalPoints
		a.MemoryBytes, a.Reason = memory, "worker memory budget"
		return &a
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

func validateNativeSkipRecord(r TransferMetric) error {
	n := r.NativeSkip
	if n == nil || len(r.Keys) != 0 || n.Name != r.Config.Name || n.Bytes < 0 || n.PhysicalPoints < 0 || n.ArchiveSlots < 0 || n.MemoryBytes != 0 || !n.exceedsLimit() || n.Reason != "name limit" {
		return ErrFormat
	}
	return nil
}
