package chunkstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/fifo"
	"github.com/go-graphite/go-carbon/internal/phasecpu"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/helper"
)

var (
	ErrClosed         = errors.New("chunk store is closing")
	ErrNotFound       = errors.New("metric not found")
	ErrExists         = errors.New("metric already exists")
	ErrFormat         = errors.New("unsupported chunk store format")
	readCacheSequence atomic.Uint64
)

const (
	formatMarkerV1           = "go-carbon-chunks-v1\n"
	formatMarkerV2           = "go-carbon-chunks-v2\n"
	formatMarkerExact        = "go-carbon-chunks-v4\n"
	formatMarkerIncremental  = "go-carbon-chunks-v5\n"
	formatMarkerContinuation = "go-carbon-chunks-v6\n"
	formatMarkerTimeBuckets  = "go-carbon-chunks-v7\n"
	formatMarkerColdPack     = "go-carbon-chunks-v9\n"
	formatMarkerFloatCodec   = "go-carbon-chunks-v11\n"
	formatMarkerReferences   = "go-carbon-chunks-v12\n"
	legacyRevisionSize       = 8
	revisionSize             = 16
)

type Options struct {
	// NativeImportConcurrency bounds admitted imports (1..8). NativeCombineWorkers
	// bounds workers per import (1..32). NativeCombineMemory is the store-wide
	// estimated scratch allowance, including results waiting for SST consumers.
	// Each worker receives total/imports/workers (at least 1 GiB). Zero selects
	// 1 import / 2 workers / 2 GiB. Changes require reopening.
	NativeImportConcurrency int
	NativeCombineWorkers    int
	NativeCombineMemory     int64
	// MaintenanceWait defers optional scans/rewrites until restart warming and
	// pacing end. The caller supplies a bounded, cancellation-aware lifetime
	// gate. Nil keeps offline tools' immediate maintenance behavior.
	MaintenanceWait func(context.Context, string) error
	ColdPack        ColdPackOptions
	Continuation    ContinuationCodec
	// TimeBuckets writes new continuation blocks to a time-partitioned key
	// space (format v7), so compactions of recent writes stop rewriting each
	// metric's cold history. It upgrades a v6 store's marker in place; older
	// binaries then refuse the store. A v7 store keeps writing v7 blocks.
	TimeBuckets bool
	// RelayoutRate > 0 moves v6 blocks of metrics converted before v7 into
	// the history bucket in the background, at about this many bytes per
	// second. It requires a v7 store and resumes after restarts.
	RelayoutRate int64
	// RelayoutWorkers splits the ID-ordered relayout into disjoint ranges.
	// Zero selects one worker; otherwise 1..32. Changing it requires reopening.
	// RelayoutRate remains the total byte budget across all workers.
	RelayoutWorkers int
	// Zero selects the defaults (200 GiB free, 32 GiB compaction debt).
	// Both limits can be changed with SetRelayoutLimits while workers run.
	RelayoutMinFreeBytes, RelayoutMaxCompactionDebt uint64
	// Zero selects 32 GiB, additionally capped by half the free reserve headroom.
	RelayoutReclaimBytes uint64
	// ExactBlocks selects integer/floating-point adaptive encoding in a separate
	// v4 store. A legacy store is never silently upgraded to this format.
	ExactBlocks  bool
	CacheSize    int64
	MemTableSize uint64
	// Zero preserves Pebble's single-compactor/default concurrency policy.
	CompactionWorkers       int
	L0CompactionConcurrency int
	// CompactionRate limits aggregate compaction output bytes per second.
	// -1 adapts to ingress, write amplification and backlog; zero disables
	// pacing. Ingest tables and WAL writes are never paced; see FlushRate.
	CompactionRate int64
	// CompactionHeadroom multiplies auto mode's modelled rate: 1..4, zero
	// keeps the default 2.
	CompactionHeadroom float64
	// CompactionKeepPacingOnDebt keeps auto pacing active while compaction
	// debt is high; stalls, L0 depth and memtable pressure still bypass it.
	CompactionKeepPacingOnDebt bool
	// CompactionWarmup is how long auto mode stays unpaced after start while
	// it learns rates: 0..24h, nil keeps the default hour. Zero paces from the
	// first sample.
	CompactionWarmup *time.Duration
	// FlushRate limits memtable flush output bytes per second. Pebble writes a
	// flush at device speed, so a large memtable on a store with little
	// ingress becomes a burst of a gigabyte in a few seconds once per fill.
	// -1 paces each flush at four times the modelled SST ingress (at least
	// 4 MiB/s), spreading it over about a quarter of the average fill time;
	// zero disables pacing. A flush that a caller waits for (Flush, Compact),
	// write stalls, memtable pressure, a second queued memtable, conversion
	// and shutdown release it in every mode. Pacing applies to exact-block
	// stores, whose memtable queue absorbs a slower flush.
	FlushRate int64
	// L0CompactionThreshold is the L0 read amplification that triggers an
	// L0 compaction; L0StopWritesThreshold stops writes. Hot blocks are
	// rewritten in every flush, so letting more sublevels accumulate drops more
	// overwritten versions per byte of Lbase rewritten. Zero keeps Pebble's
	// defaults (4 and 12).
	L0CompactionThreshold, L0StopWritesThreshold int
	// LBaseMaxBytes sizes the base level; zero keeps Pebble's 64 MiB.
	LBaseMaxBytes int64
	// MaxUnflushedMemTableBytes bounds queued memtables, and so WAL replay
	// after a crash. A deeper queue absorbs write bursts (a restart's cache
	// catch-up) instead of stalling writers. Zero means 8 GiB.
	MaxUnflushedMemTableBytes uint64
	// Zero preserves Pebble's default table-reader budget.
	MaxOpenFiles int
	// TargetFileSize sets the output table size of flushes and of every
	// compaction level. Zero keeps Pebble's 2 MiB L0 default, doubling per
	// level. Large L0 tables make each flush a sublevel spanning the keyspace:
	// on a production store, 64 MiB raised L0 sublevels from 1-4 to 14 and stalled
	// writes on the L0 limit. Prefer BytesPerSync to reduce syncs.
	TargetFileSize int64
	// L0TargetFileSize overrides the flush output table size only; deeper
	// levels keep TargetFileSize or Pebble's doubling defaults. Each L0 table
	// costs one sync, but larger ones overlap more of the other sublevels and
	// of the base level (see TargetFileSize).
	L0TargetFileSize int64
	// BloomFilterBits > 0 writes a bloom filter with that many bits per key
	// into every new table, so point reads skip tables without the key instead
	// of searching each level's index and data blocks. Existing tables gain
	// filters as compactions rewrite them; readers without filters ignore them.
	BloomFilterBits int
	// ParallelTableSyncs > 0 defers the sync of each new table and syncs the
	// finished tables that many at a time before the manifest records them,
	// instead of one after another as Pebble writes them.
	ParallelTableSyncs int
	// WALDir keeps Pebble's write-ahead logs in another directory, usually on
	// another device, so they do not share the store device's write budget.
	// Empty keeps them in the store directory. Open moves existing logs (by
	// flushing them) when this changes; the directory must already exist.
	WALDir string
	// CacheShards sets the block cache shard count (exact stores). Zero keeps
	// four per execution thread. More shards cut lock contention between
	// readers and the inserts that misses cause.
	CacheShards int
	// TableCacheShards sets the table cache shard count; zero keeps Pebble's
	// one per execution thread.
	TableCacheShards int
	// CompactOnce, when it differs from the token of the last finished run,
	// compacts the whole keyspace once in the background after Open.
	CompactOnce string
	// FetchSharedIterator serves a continuation fetch's prefix walks and probes
	// of rarely rewritten keys through one iterator over the metric's key
	// prefix (see continuationIO.withIterator) instead of a lookup or iterator
	// per read. Probes consult bloom filters; prefix walks cannot.
	FetchSharedIterator bool
	// MetadataCacheSize > 0 keeps up to that many bytes of recently written or
	// read metrics' committed metadata in memory: catalog entries and the
	// continuation codec's state keys (see ContinuationStateKeys and
	// stateCache), which every update and fetch otherwise reads back from the
	// LSM. Zero disables it.
	MetadataCacheSize int64
	// MetadataCacheCheckpoint saves advisory slab records on clean close.
	MetadataCacheCheckpoint bool
	// MetadataCacheCheckpointLazy leaves revision validation to first use.
	// Normally load validates bounded parallel batches ahead of writeout.
	MetadataCacheCheckpointLazy bool
	// UpdatePointLookups gives each read of a continuation update its own LSM
	// lookup or iterator, which opens a table in every level it reaches. By
	// default an update's prefix walks and probes share one iterator over the
	// metric's key prefix and batch, which opens each level's table once.
	// SetUpdatePointLookups changes it on an open store.
	UpdatePointLookups bool
	// BytesPerSync bounds dirty table data between incremental syncs during
	// flushes and compactions. Zero keeps Pebble's 512 KiB default; with a large
	// host dirty-page backlog every incremental sync waits behind writeback.
	BytesPerSync int
	// SyncInterval controls WAL syncing for successful store mutations. Zero
	// syncs each mutation; a positive interval permits loss of mutations since
	// the last sync.
	SyncInterval time.Duration
	// IncrementalSource enables verified per-metric ownership and reclamation.
	// It must identify the original canonical directory for every restart.
	IncrementalSource string
	// IngestFileStages writes fresh file conversions as ingested tables
	// instead of memtable batches. It requires IncrementalSource and Continuation.
	IngestFileStages bool
	// IngestLinger bounds how long a fresh stage waits for others to share its
	// table. Zero selects one second, suited to hundreds of conversion workers.
	IngestLinger time.Duration
	// IngestGroupBytes is the stage bytes that fill one ingested table; zero
	// selects 64 MiB. Pebble ingests one table at a time, so conversion
	// throughput is about one group per ingestion. Larger groups hold more
	// staged conversions in memory.
	IngestGroupBytes int
	// IngestParallel is how many groups build and ingest at once; zero
	// selects four.
	IngestParallel int
	Now            func() time.Time
	fs             vfs.FS // Fault-injection tests use the same storage path as production.
	sourceIdentity func(vfs.FS, string) (incrementalIdentity, error)
}

// WithFS selects the filesystem used by the store, including its WAL and markers.
// Tests can supply a filesystem without disk syncs; nil keeps the default.
func (o Options) WithFS(fs vfs.FS) Options {
	o.fs = fs
	return o
}

type Store struct {
	readSlots             *fifo.Semaphore
	readMu                sync.Mutex // serializes read registration with cancellation
	readCount             int
	readDone              chan struct{} // closed after cancellation and the last view closes
	readCtx               context.Context
	readCancel            context.CancelFunc
	nativeCounters        nativeCounters
	nativeReady           func() error // installed before listeners; native/shared ownership only
	nativePendingSnapshot func() (map[string]struct{}, error)
	nativeFailure         atomic.Pointer[ownershipError] // native write errors require owner recovery
	nativeRanges          sync.Map                       // durable range identity -> context-aware publication lock
	nativeContextOnce     sync.Once
	nativeContext         context.Context
	nativeCancel          context.CancelFunc
	nativeAdmission       chan struct{}
	nativeExportAdmission chan struct{}
	nativeTestHook        func(string) error // private deterministic fault-injection tests
	maintenanceWait       func(context.Context, string) error
	maintenanceCtx        context.Context
	maintenanceStop       context.CancelFunc

	nativeImportConcurrency   int
	nativeCombineWorkers      int
	nativeCombineWorkerMemory int64

	// Owner-driven maintenance lives until close, independently of StopBackground.
	explicitMaintenance   operationState
	coldPack              coldPackState
	transferPendingAny    func() bool
	transferPending       func(string) bool
	transferExport        transferExportCounters
	transferApply         transferApplyCounters
	transferApplyTestHook func() // deterministic concurrent-publication tests only
	sidecars              sidecarCatalog
	transferDeleteRanges  atomic.Uint64
	compactOnce           compactOnceState
	manualCompaction      manualCompactionState
	referenceFormat       atomic.Bool
	// transferRereadMetadata disables the unchanged-epoch commit shortcut (tests).
	transferRereadMetadata bool
	// transferSplitSingles restores the single-name split of contended groups (tests).
	transferSplitSingles bool
	// fetchSharedIterator is the Options field.
	fetchSharedIterator bool
	fetchTimings        fetchTimingCounters
	// updatePointLookups starts as the Options field; see SetUpdatePointLookups.
	updatePointLookups atomic.Bool
	// levelBase and levelMultiplier size the LSM levels; see levelTargets.
	levelBase       int64
	levelMultiplier int
	continuation    ContinuationCodec
	// stateCache, when enabled, holds metrics' committed metadata.
	stateCache                     *stateCache
	checkpoint                     metadataCheckpoint
	timeBuckets                    bool
	relayout                       relayoutState
	relayoutWorkers                int
	relayoutStop                   context.CancelFunc
	relayoutDone                   chan struct{}
	deferredSync                   *deferredSyncFS
	fsStats                        *countingFS
	compactionPacing               *compactionPacingFS
	compactionControl              *compactionController
	flushPacing                    bool // FlushRate may pace this store's flushes
	compactionStop, compactionDone chan struct{}
	incrementalSource              string
	ownershipFailure               atomic.Pointer[ownershipError]
	fileReadMu                     *[32768]sync.RWMutex
	exactBlocks                    bool
	db                             *pebble.DB
	dir                            string
	fs                             vfs.FS
	pebbleOptions                  *pebble.Options
	ingest                         *fileIngester
	snapshotRelease                chan *pebble.Snapshot
	walSync                        *walSyncer
	snapshotReleased               chan struct{}
	cache                          *pebble.Cache
	rangeDelCache                  *pebble.Cache
	cacheShards                    int
	now                            func() time.Time
	writeOptions                   *pebble.WriteOptions
	pendingSync                    atomic.Bool
	syncStop                       chan struct{}
	syncDone                       chan struct{}
	// mu serializes metric ID allocation from the catalog sequence. Mutations
	// of one metric hold its metricMu stripe; readers use consistent read views.
	mu            sync.Mutex
	metricMu      [metricStripes]sync.Mutex
	finitePatchMu sync.Mutex
	// mutationEpoch advances at both ends of every mutation, even without a
	// metadata cache. Optimistic maintenance can validate its immutable view
	// under the stripe without an LSM read. Saturation disables that shortcut.
	mutationEpoch      [metricStripes]atomic.Uint64
	metadataCache      [metricStripes]metadataRing
	catalogMembership  *catalogMembership
	catalogChanges     atomic.Pointer[catalogChanges]
	materializations   atomic.Uint64
	operands           atomic.Uint64
	readCacheEpoch     atomic.Uint64
	compactionWorkers  atomic.Int64
	telemetryPebble    atomic.Pointer[pebble.Metrics]
	telemetryRange     atomic.Pointer[rangeCacheSnapshot]
	telemetryAt        atomic.Int64
	closeTimings       CloseTimings
	openTimings        OpenTimings
	stallMu            sync.Mutex
	stallCount         uint64
	stallDuration      time.Duration
	stallSince         time.Time
	stallReason        string
	expirationPressure expirationPressureCache
	reconcileCompared  atomic.Uint64
	reconcileWritten   atomic.Uint64
	reconcileDeleted   atomic.Uint64
	reconcileUnchanged atomic.Uint64
	// PurgeInvalidNames progress: refused names its scan found, and removed.
	purgeFound, purgeRemoved atomic.Uint64
	purgeRunning             atomic.Bool
	mirrorCanonical          atomic.Uint64
	mirrorBlind              atomic.Uint64
	canonicalRuns            [64]IDSet
	ingestStats              struct{ tables, entries, bytes, l0Bytes, memtableOverlaps atomic.Uint64 }
	transferBatchStats       struct{ commits, entries, bytes atomic.Uint64 }
	transferMergeStats       transferMergeStats
	// transferMergeAppendOff merges every present continuation name with
	// RestoreArchives (differential tests).
	transferMergeAppendOff bool
	// transferBatchBytes overrides transferBatchLimit (tests/benchmarks): 0
	// keeps the default, negative always ingests SSTs.
	transferBatchBytes int
	// backgroundStopped is set once StopBackground completed.
	backgroundStopped atomic.Bool
	// prewarm tracks running PrewarmMetadata calls.
	prewarm operationState
	// walSyncMu makes the WAL byte count and the adjustment for empty sync
	// records one observation. Ordinary writes do not acquire this mutex.
	walSyncMu sync.Mutex
	// syncWALBytes counts the WAL bytes of logSync's records.
	syncWALBytes uint64
}

// storeComparer is Pebble's default bytewise comparer, under the same name,
// with a Split that covers the whole key. Tables written either way filter on
// whole keys, so filters stay valid in both directions; Split lets SeekPrefixGE
// make iterator point lookups that consult them (see continuationIO.Get).
var storeComparer = func() *pebble.Comparer {
	c := *pebble.DefaultComparer
	c.Split = func(key []byte) int { return len(key) }
	return &c
}()

// Open accepts only a new directory or an explicitly identified chunk store.
// A legacy Pebble directory is rejected before Pebble can modify its manifest.
func Open(dir string, options Options) (*Store, error) {
	if options.NativeImportConcurrency == 0 {
		options.NativeImportConcurrency = DefaultNativeImportConcurrency
	}
	if options.NativeCombineWorkers == 0 {
		options.NativeCombineWorkers = DefaultNativeCombineWorkers
	}
	if options.NativeCombineMemory == 0 {
		options.NativeCombineMemory = DefaultNativeCombineMemory
	}
	if err := ValidateNativeImportBudget(options.NativeImportConcurrency, options.NativeCombineWorkers, options.NativeCombineMemory); err != nil {
		return nil, err
	}
	if options.RelayoutWorkers < 0 || options.RelayoutWorkers > 32 {
		return nil, errors.New("relayout workers must be 1..32 (zero selects one)")
	}
	coldOptions, coldErr := NormalizeColdPack(options.ColdPack)
	if coldErr != nil {
		return nil, coldErr
	}
	if coldOptions.Mode != "off" && (!options.TimeBuckets || options.Continuation == nil || !options.ExactBlocks) {
		return nil, errors.New("cold pack requires exact time-bucketed continuation storage")
	}
	if dir == "" {
		return nil, errors.New("store directory is empty")
	}
	if options.IncrementalSource != "" && !options.ExactBlocks {
		return nil, errors.New("incremental migration requires exact blocks")
	}
	if options.Continuation != nil && !options.ExactBlocks {
		return nil, errors.New("CWhisper continuation requires exact point blocks")
	}
	if options.SyncInterval < 0 {
		return nil, errors.New("store sync interval must be non-negative")
	}
	if options.CompactionWorkers < 0 || options.CompactionWorkers > 64 || options.L0CompactionConcurrency < 0 || options.L0CompactionConcurrency > 64 {
		return nil, errors.New("compaction worker and L0 concurrency budgets must be 0..64")
	}
	if options.TargetFileSize < 0 || options.TargetFileSize > 1<<30 || options.BytesPerSync < 0 || options.BytesPerSync > 1<<30 {
		return nil, errors.New("store target file size and bytes per sync must be 0..1 GiB")
	}
	if options.MaxOpenFiles < 0 || options.MaxOpenFiles > 1<<20 || options.MaxOpenFiles > 0 && options.MaxOpenFiles < 64 {
		return nil, errors.New("table-reader budget must be zero or 64..1048576")
	}
	if options.L0TargetFileSize < 0 || options.L0TargetFileSize > 1<<30 || options.ParallelTableSyncs < 0 || options.ParallelTableSyncs > 1024 {
		return nil, errors.New("L0 table size must be 0..1 GiB and parallel table syncs 0..1024")
	}
	if options.IngestGroupBytes < 0 || options.IngestGroupBytes > 1<<30 || options.IngestParallel < 0 || options.IngestParallel > 64 {
		return nil, errors.New("invalid file ingestion group size or parallelism")
	}
	if options.BloomFilterBits < 0 || options.BloomFilterBits > 32 {
		return nil, errors.New("bloom filter bits per key must be 0..32")
	}
	if options.CacheShards < 0 || options.CacheShards > 1<<16 || options.TableCacheShards < 0 || options.TableCacheShards > 1<<16 {
		return nil, errors.New("cache and table cache shards must be 0..65536")
	}
	if options.MetadataCacheSize < 0 {
		return nil, errors.New("metadata cache size must not be negative")
	}
	if options.CompactionRate < -1 {
		return nil, errors.New("compaction rate must be -1 (auto), 0 (off), or positive")
	}
	headroom, warmup := options.CompactionHeadroom, compactionWarmupDefault
	if headroom == 0 {
		headroom = compactionHeadroomDefault
	}
	if options.CompactionWarmup != nil {
		warmup = *options.CompactionWarmup
	}
	if !validCompactionHeadroom(headroom) {
		return nil, errCompactionHeadroom
	}
	if !validCompactionWarmup(warmup) {
		return nil, errCompactionWarmup
	}
	if options.FlushRate < -1 {
		return nil, errors.New("flush rate must be -1 (auto), 0 (off), or positive")
	}
	fs := options.fs
	if fs == nil {
		fs = vfs.Default
	}
	// Count real filesystem I/O innermost, below deferred syncs, so a batched
	// sync is counted when it happens.
	counting := newCountingFS(fs)
	fs = counting
	var deferredSync *deferredSyncFS
	if options.ParallelTableSyncs > 0 {
		deferredSync = newDeferredSyncFS(fs, dir, options.ParallelTableSyncs)
		fs = deferredSync
	}
	pacing := newCompactionPacingFS(fs, options.CompactionRate)
	fs = pacing
	if options.sourceIdentity == nil {
		options.sourceIdentity = readIncrementalIdentity
	}
	if _, err := checkIncrementalMarker(fs, dir, options.IncrementalSource, options.sourceIdentity); err != nil {
		return nil, err
	}
	// An existing v6 store becomes v7 only once Pebble holds its lock and has
	// accepted the options: a failed open must leave older readers working.
	format, created, upgradeMarker, err := checkFormatForContinuation(fs, dir, options)
	if err != nil {
		return nil, err
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.MemTableSize == 0 {
		options.MemTableSize = 64 << 20
	}
	s := &Store{readSlots: fifo.NewSemaphore(maxConcurrentStoreReads), now: options.Now, writeOptions: pebble.Sync, exactBlocks: options.ExactBlocks, fileReadMu: new([32768]sync.RWMutex), deferredSync: deferredSync, fsStats: counting, compactionPacing: pacing}
	s.readCtx, s.readCancel = context.WithCancel(context.Background())
	s.readDone = make(chan struct{})
	s.maintenanceWait = options.MaintenanceWait
	s.maintenanceCtx, s.maintenanceStop = context.WithCancel(context.Background())
	// Flush pacing starts once the store is open: WAL recovery and WAL moves
	// flush at full speed.
	s.flushPacing = options.ExactBlocks
	s.compactionControl = newPacingController(pacing.pacer, pacing.flush, options.CompactionRate, 0, time.Now())
	s.compactionControl.setHeadroom(headroom)
	s.compactionControl.setDebtBypass(!options.CompactionKeepPacingOnDebt)
	s.compactionControl.setWarmup(warmup)
	s.continuation = options.Continuation
	s.nativeImportConcurrency = options.NativeImportConcurrency
	s.nativeCombineWorkers = options.NativeCombineWorkers
	s.nativeCombineWorkerMemory = options.NativeCombineMemory / int64(options.NativeImportConcurrency) / int64(options.NativeCombineWorkers)
	s.fetchSharedIterator = options.FetchSharedIterator
	stateKeys, _ := options.Continuation.(ContinuationStateKeys)
	if size := options.MetadataCacheSize; size > 0 {
		s.stateCache = newStateCache(size, stateKeys)
	} else if metadataCacheTestSize > 0 {
		s.stateCache = newStateCache(metadataCacheTestSize, stateKeys)
	}
	s.updatePointLookups.Store(options.UpdatePointLookups)
	s.timeBuckets = format >= 7
	s.relayoutWorkers = max(1, options.RelayoutWorkers)
	if options.RelayoutMinFreeBytes == 0 {
		options.RelayoutMinFreeBytes = DefaultRelayoutMinFreeBytes
	}
	if options.RelayoutMaxCompactionDebt == 0 {
		options.RelayoutMaxCompactionDebt = DefaultRelayoutMaxCompactionDebt
	}
	_ = s.SetRelayoutLimits(options.RelayoutMinFreeBytes, options.RelayoutMaxCompactionDebt)
	if options.RelayoutReclaimBytes == 0 {
		options.RelayoutReclaimBytes = DefaultRelayoutReclaimBytes
	}
	_ = s.SetRelayoutReclaimBytes(options.RelayoutReclaimBytes)
	s.coldPack.format.Store(format >= 9)
	s.coldPack.floatFormat.Store(format >= 11)
	s.referenceFormat.Store(format >= 12)
	s.compactionWorkers.Store(int64(max(1, options.CompactionWorkers)))
	opts := &pebble.Options{MemTableSize: options.MemTableSize, Comparer: storeComparer, Merger: chunkMerger, FS: fs,
		LoadBlockSema:            fifo.NewSemaphore(maxConcurrentBlockLoads),
		MaxConcurrentCompactions: func() int { return int(s.compactionWorkers.Load()) },
		EventListener:            &pebble.EventListener{WriteStallBegin: s.writeStallBegin, WriteStallEnd: s.writeStallEnd, TableCreated: pacing.tableCreated},
	}
	// Deferred table syncs complete at the directory sync that precedes each
	// manifest commit; Pebble must not skip it (see deferredSyncFS).
	opts.Local.AlwaysSyncDir = deferredSync != nil
	opts.Experimental.L0CompactionConcurrency = options.L0CompactionConcurrency
	if options.L0CompactionThreshold > 0 {
		opts.L0CompactionThreshold = options.L0CompactionThreshold
	}
	if options.L0StopWritesThreshold > 0 {
		opts.L0StopWritesThreshold = options.L0StopWritesThreshold
	}
	if options.LBaseMaxBytes > 0 {
		opts.LBaseMaxBytes = options.LBaseMaxBytes
	}
	s.levelBase, s.levelMultiplier = opts.LBaseMaxBytes, opts.Experimental.LevelMultiplier
	if s.levelBase <= 0 {
		s.levelBase = 64 << 20
	}
	if s.levelMultiplier <= 0 {
		s.levelMultiplier = 10
	}
	opts.MaxOpenFiles = options.MaxOpenFiles
	if options.TableCacheShards > 0 {
		opts.Experimental.TableCacheShards = options.TableCacheShards
	}
	if options.BytesPerSync > 0 {
		opts.BytesPerSync = options.BytesPerSync
	}
	if options.TargetFileSize > 0 || options.L0TargetFileSize > 0 || options.BloomFilterBits > 0 {
		opts.Levels = make([]pebble.LevelOptions, numLevels)
		for i := range opts.Levels {
			opts.Levels[i].TargetFileSize = options.TargetFileSize
			if options.TargetFileSize == 0 {
				opts.Levels[i].TargetFileSize = 2 << 20 << i // Pebble's default
			}
		}
		if options.L0TargetFileSize > 0 {
			opts.Levels[0].TargetFileSize = options.L0TargetFileSize
		}
		if options.BloomFilterBits > 0 {
			policy := bloom.FilterPolicy(options.BloomFilterBits)
			opts.Filters = map[string]pebble.FilterPolicy{policy.Name(): policy}
			for i := range opts.Levels {
				opts.Levels[i].FilterPolicy = policy
				opts.Levels[i].FilterType = pebble.TableFilter
			}
		}
	}
	if options.ExactBlocks {
		opts.Merger = exactChunkMerger
		opts.WALRotationAsync = true
		// Bulk conversion fills memtables faster than one flush drains them on a
		// busy device; a deeper queue absorbs that instead of stalling all writes.
		// Flushes there wait on per-table syncs behind host writeback in bursts.
		// Bound the queue by bytes too: every unflushed memtable is replayed from
		// the WAL before a restarted instance serves, and sixteen 2 GiB memtables
		// once took nearly seven minutes to replay.
		budget := options.MaxUnflushedMemTableBytes
		if budget == 0 {
			budget = maxUnflushedMemTableBytes
		}
		opts.MemTableStopWritesThreshold = max(4, min(16, int(budget/options.MemTableSize)))
	}
	if options.CacheSize > 0 {
		if options.ExactBlocks {
			s.cache, s.rangeDelCache, s.cacheShards = newExactBlockCaches(options.CacheSize, options.CacheShards)
			opts.RangeDelCache = s.rangeDelCache
		} else {
			s.cache = pebble.NewCache(options.CacheSize)
		}
		opts.Cache = s.cache
	}
	walDir := dir
	if options.WALDir != "" {
		walDir = options.WALDir
	}
	opts.WALDir = walDirOption(dir, walDir)
	opened := time.Now()
	if err := moveWAL(fs, dir, walDir, opts); err != nil {
		unrefCaches(s)
		return nil, fmt.Errorf("move chunk store WAL: %w", err)
	}
	s.openTimings.MoveWAL = time.Since(opened)
	opened = time.Now()
	db, err := pebble.Open(dir, opts)
	s.openTimings.Pebble = time.Since(opened)
	opened = time.Now()
	defer func() { s.openTimings.Configure = time.Since(opened) }()
	if err != nil {
		if s.cache != nil {
			s.cache.Unref()
		}
		if s.rangeDelCache != nil {
			s.rangeDelCache.Unref()
		}
		return nil, fmt.Errorf("open chunk store: %w", err)
	}
	if upgradeMarker {
		if err := replaceFormatMarker(fs, dir, formatMarkerTimeBuckets); err != nil {
			closeErr := db.Close()
			unrefCaches(s)
			return nil, errors.Join(err, closeErr)
		}
	}
	s.db, s.dir, s.fs, s.pebbleOptions = db, dir, fs, opts.Clone().EnsureDefaults()
	if options.MetadataCacheCheckpoint && s.stateCache != nil {
		s.checkpoint.lazy = options.MetadataCacheCheckpointLazy
		if err := s.openMetadataCheckpoint(); err != nil {
			return nil, s.openFailure(db, err)
		}
	}
	s.startSnapshotRelease()
	s.startWALSyncer()
	if err := s.configureIncrementalSource(fs, dir, options.IncrementalSource, options.sourceIdentity); err != nil {
		return nil, s.openFailure(db, err)
	}
	if err := s.loadSidecars(); err != nil {
		return nil, s.openFailure(db, fmt.Errorf("load sidecar catalog: %w", err))
	}
	if err := s.recoverNative(); err != nil {
		return nil, s.openFailure(db, fmt.Errorf("recover native bulk: %w", err))
	}
	if err := s.recoverNativeCombines(); err != nil {
		return nil, s.openFailure(db, fmt.Errorf("recover native combine: %w", err))
	}
	if options.IngestFileStages {
		if s.incrementalSource == "" || s.continuation == nil {
			return nil, s.openFailure(db, errors.New("file stage ingestion requires incremental continuation migration"))
		}
		linger := options.IngestLinger
		if linger <= 0 {
			linger = defaultFileIngestLinger
		}
		target, parallel := options.IngestGroupBytes, options.IngestParallel
		if target <= 0 {
			target = defaultFileIngestTargetBytes
		}
		if parallel <= 0 {
			parallel = defaultFileIngestParallel
		}
		if s.ingest, err = newFileIngester(s, linger, target, parallel); err != nil {
			return nil, s.openFailure(db, err)
		}
	}
	if format == 1 {
		if err := upgradeFormatMarker(fs, dir); err != nil {
			return nil, s.openFailure(db, fmt.Errorf("upgrade chunk store format: %w", err))
		}
	}
	// An interrupted upgrade may have completed the rename while its directory
	// sync failed. Re-sync an accepted v2 marker while Pebble owns the database
	// lock, before allowing any v2 revision record to be written. A marker
	// created by this Open was already synced by createFormatMarker.
	if !created {
		if err := syncFormatMarker(fs, dir); err != nil {
			return nil, s.openFailure(db, fmt.Errorf("sync chunk store marker: %w", err))
		}
	}
	if options.SyncInterval > 0 {
		s.writeOptions = pebble.NoSync
		s.syncStop = make(chan struct{})
		s.syncDone = make(chan struct{})
		go s.syncLoop(options.SyncInterval)
	}
	s.invalidateReadCache()
	s.startCatalogMembership()
	if options.RelayoutRate > 0 && s.timeBuckets {
		ctx, cancel := context.WithCancel(s.maintenanceCtx)
		s.relayoutStop, s.relayoutDone = cancel, make(chan struct{})
		go func() {
			defer close(s.relayoutDone)
			if err := s.waitMaintenance(ctx, "relayout"); err != nil {
				return
			}
			s.runRelayoutBackground(ctx, options.RelayoutRate)
		}()
	}
	if err := s.startCompactOnce(options.CompactOnce); err != nil {
		return nil, s.openFailure(db, err)
	}
	if err := s.ConfigureColdPack(coldOptions); err != nil {
		return nil, s.openFailure(db, err)
	}
	s.startCompactionController()
	_ = s.SetFlushRate(options.FlushRate) // validated above
	if s.continuation != nil {
		s.startColdPack()
	}
	return s, nil
}

// Keep ordinary data/index concurrency while isolating the large range-deletion
// blocks produced by reconciliation. Shared larger shards made eviction of a
// large block hold up ordinary metadata reads. The combined budget is unchanged.
func newExactBlockCaches(size int64, shards int) (*pebble.Cache, *pebble.Cache, int) {
	ranges := min(size/8, int64(1<<30))
	if shards <= 0 {
		shards = exactBlockCacheShards(size-ranges, runtime.GOMAXPROCS(0))
	}
	return pebble.NewCacheWithShards(size-ranges, shards), pebble.NewCacheWithShards(ranges, 4), shards
}

// Pebble's default jumps from four shards per execution thread to only four
// shards when their individual budgets would be below 4 MiB. Choose the largest
// affordable count instead, preserving small-cache overhead and total capacity.
// Large range-deletion blocks keep their separate, less-partitioned cache.
func exactBlockCacheShards(size int64, procs int) int {
	return max(4, int(min(int64(4*max(1, procs)), size/(4<<20))))
}

// ReadCacheEpoch identifies a process-local cache namespace. Ordinary ingestion
// retains the configured Graphite cache TTL. Archive imports and deletions
// invalidate cached replies before returning to their caller. A new Store also
// receives a distinct token, including when a listener replaces its handle.
func (s *Store) ReadCacheEpoch() uint64 { return s.readCacheEpoch.Load() }
func (s *Store) invalidateReadCache()   { s.readCacheEpoch.Store(readCacheSequence.Add(1)) }

func unrefCaches(s *Store) {
	if s.cache != nil {
		s.cache.Unref()
	}
	if s.rangeDelCache != nil {
		s.rangeDelCache.Unref()
	}
}

func (s *Store) openFailure(db *pebble.DB, err error) error {
	s.maintenanceStop()
	s.StopCompactionPacing()
	s.stopRelayout()
	if f := s.catalogMembership; f != nil {
		f.halt()
	}
	if s.syncStop != nil {
		close(s.syncStop)
		<-s.syncDone
	}
	s.stopSnapshotRelease()
	s.stopWALSyncer()
	closeErr := db.Close()
	if s.compactOnce.done != nil {
		<-s.compactOnce.done
	}
	if s.cache != nil {
		s.cache.Unref()
	}
	if s.rangeDelCache != nil {
		s.rangeDelCache.Unref()
	}
	return errors.Join(err, closeErr)
}

func (s *Store) syncLoop(interval time.Duration) {
	phasecpu.Set(phasecpu.PebbleWAL)
	defer phasecpu.Set(phasecpu.Other)
	defer close(s.syncDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.syncStop:
			return
		case <-ticker.C:
			if !s.pendingSync.Swap(false) {
				continue
			}
			// LogData flushes Pebble's userspace WAL buffer and syncs all prior
			// records, without forcing a memtable flush or growing an idle WAL.
			if err := s.logSync(); err != nil {
				// Pebble already fails fatally on WAL I/O errors. Do not continue
				// accepting mutations if another sync error is ever returned.
				panic(fmt.Errorf("periodic store WAL sync: %w", err))
			}
		}
	}
}

// checkFormat returns the on-disk format version and whether this call
// created (and synced) the marker for a new store.
func checkFormat(fs vfs.FS, dir string) (format int, created bool, err error) {
	return checkFormatFor(fs, dir, false)
}

func checkFormatFor(fs vfs.FS, dir string, exact bool) (format int, created bool, err error) {
	return checkFormatForMode(fs, dir, exact, false)
}

func checkFormatForMode(fs vfs.FS, dir string, exact, incremental bool) (format int, created bool, err error) {
	names, err := fs.List(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, false, fmt.Errorf("list store directory: %w", err)
	}
	if len(names) > 0 {
		format, err = checkFormatMarker(fs, dir)
		wanted := 4
		if incremental {
			wanted = 5
		}
		if err == nil && (exact && format != wanted || !exact && format != 1 && format != 2) {
			err = fmt.Errorf("%w: store encoding does not match marker; use a separate destination", ErrFormat)
		}
		return format, false, err
	}
	marker := formatMarkerV2
	format = 2
	if exact {
		marker, format = formatMarkerExact, 4
	}
	if incremental {
		marker, format = formatMarkerIncremental, 5
	}
	if err := createFormatMarkerFor(fs, dir, marker); err != nil {
		return 0, false, err
	}
	return format, true, nil
}

// checkFormatForContinuation reports the store format, whether it was just
// created, and whether Open must upgrade an existing v6 marker to v7 once
// Pebble has opened. The upgrade decision comes from this single marker read.
func checkFormatForContinuation(fs vfs.FS, dir string, options Options) (int, bool, bool, error) {
	if options.Continuation == nil {
		format, created, err := checkFormatForMode(fs, dir, options.ExactBlocks, options.IncrementalSource != "")
		return format, created, false, err
	}
	names, err := fs.List(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, false, false, err
	}
	if len(names) != 0 {
		format, err := checkFormatMarker(fs, dir)
		if err != nil || format != 6 && format != 7 && format != 9 && format != 11 && format != 12 {
			return 0, false, false, fmt.Errorf("%w: continuation mode requires its own v6 or v7 destination", ErrFormat)
		}
		if format == 12 {
			reader, ok := options.Continuation.(ContinuationReferenceReader)
			if !ok || !reader.NativeReferenceReader() {
				return 0, false, false, fmt.Errorf("%w: continuation codec lacks v12 reference reader", ErrFormat)
			}
		}
		if format == 6 && options.TimeBuckets {
			// v6 data stays readable; only new blocks use the v7 layout. Open
			// replaces the marker after Pebble opens.
			return 7, false, true, nil
		}
		return format, false, false, nil
	}
	marker, format := formatMarkerContinuation, 6
	if options.TimeBuckets {
		marker, format = formatMarkerTimeBuckets, 7
	}
	if err := createFormatMarkerFor(fs, dir, marker); err != nil {
		return 0, false, false, err
	}
	return format, true, false, nil
}

func checkFormatMarker(fs vfs.FS, dir string) (int, error) {
	f, err := fs.Open(fs.PathJoin(dir, "CHUNKSTORE"))
	if err != nil {
		return 0, fmt.Errorf("%w: missing marker; migrate legacy data through buckyd", ErrFormat)
	}
	data, readErr := io.ReadAll(io.LimitReader(f, int64(len(nativeAbandonFormatPrefix)+len(formatMarkerFloatCodec)+1)))
	closeErr := f.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return 0, fmt.Errorf("read store marker: %w", err)
	}
	switch strings.TrimPrefix(string(data), nativeAbandonFormatPrefix) {
	case formatMarkerV1:
		return 1, nil
	case formatMarkerV2:
		return 2, nil
	case formatMarkerExact:
		return 4, nil
	case formatMarkerIncremental:
		return 5, nil
	case formatMarkerContinuation:
		return 6, nil
	case formatMarkerTimeBuckets:
		return 7, nil
	case formatMarkerColdPack:
		return 9, nil
	case formatMarkerFloatCodec:
		return 11, nil
	case formatMarkerReferences:
		return 12, nil
	default:
		return 0, ErrFormat
	}
}

func createFormatMarker(fs vfs.FS, dir string) error {
	return createFormatMarkerFor(fs, dir, formatMarkerV2)
}

func createFormatMarkerFor(fs vfs.FS, dir, marker string) error {
	// Sync newly created directory entries too: syncing the WAL and store
	// directory alone cannot make a missing ancestor survive a crash.
	var parents []string
	for path := dir; ; path = fs.PathDir(path) {
		if _, err := fs.Stat(path); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		parent := fs.PathDir(path)
		if parent == path {
			return fmt.Errorf("store directory has no existing ancestor: %s", dir)
		}
		parents = append(parents, parent)
	}
	if err := fs.MkdirAll(dir, 0755); err != nil {
		return err
	}
	f, err := fs.Create(fs.PathJoin(dir, "CHUNKSTORE"))
	if err != nil {
		return err
	}
	_, writeErr := f.Write([]byte(marker))
	syncErr := f.Sync()
	if err := errors.Join(writeErr, syncErr, f.Close()); err != nil {
		return fmt.Errorf("write store marker: %w", err)
	}
	if err := syncStoreDirectory(fs, dir); err != nil {
		return err
	}
	for _, path := range parents {
		if err := syncStoreDirectory(fs, path); err != nil {
			return err
		}
	}
	return nil
}

func upgradeFormatMarker(fs vfs.FS, dir string) error {
	return replaceFormatMarker(fs, dir, formatMarkerV2)
}

func replaceFormatMarker(fs vfs.FS, dir, marker string) error {
	// A cleanup fence is permanent even when a later codec upgrades the base
	// format. Readers predating ABANDON must never resume its partial cleanup.
	fence, err := fs.Open(fs.PathJoin(dir, "CHUNKSTORE"))
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(fence, int64(len(nativeAbandonFormatPrefix))))
	if err := errors.Join(readErr, fence.Close()); err != nil {
		return err
	}
	if string(data) == nativeAbandonFormatPrefix && !strings.HasPrefix(marker, nativeAbandonFormatPrefix) {
		marker = nativeAbandonFormatPrefix + marker
	}
	temporary := fs.PathJoin(dir, "CHUNKSTORE.upgrade")
	f, err := fs.Create(temporary)
	if err != nil {
		return err
	}
	_, writeErr := f.Write([]byte(marker))
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("write upgraded store marker: %w", err)
	}
	if err := fs.Rename(temporary, fs.PathJoin(dir, "CHUNKSTORE")); err != nil {
		return fmt.Errorf("replace store marker: %w", err)
	}
	if err := syncStoreDirectory(fs, dir); err != nil {
		return fmt.Errorf("sync upgraded store marker: %w", err)
	}
	return nil
}

func syncStoreDirectory(fs vfs.FS, dir string) error {
	d, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func syncFormatMarker(fs vfs.FS, dir string) error {
	f, err := fs.Open(fs.PathJoin(dir, "CHUNKSTORE"))
	if err != nil {
		return err
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return err
	}
	return syncStoreDirectory(fs, dir)
}

// CloseTimings reports how long each step of the last Close took. A restart
// cannot serve reads while the store closes, so each step is on that path.
type CloseTimings struct {
	Ingest, Membership, PeriodicSync, SnapshotRelease, WALSyncer, Pebble time.Duration
	// PebbleSteps breaks down the Pebble close.
	PebbleSteps []string
}

// OpenTimings reports how long the steps of Open took: moving the WAL to its
// directory, opening Pebble (manifest and WAL replay) and starting the
// store's own workers. A restart serves nothing until Open returns.
type OpenTimings struct {
	MoveWAL, Pebble, Configure time.Duration
}

// OpenTimings returns the step durations of Open.
func (s *Store) OpenTimings() OpenTimings { return s.openTimings }

// LastCloseTimings returns the step durations of the completed Close.
func (s *Store) LastCloseTimings() CloseTimings { return s.closeTimings }

// StopBackground stops the store's background maintenance, the catalog
// membership scan, the continuation relayout and any metadata prewarm, and
// waits for them to end.
// Close stops them too, while the store serves nothing; a stop calls this
// early instead, while input still flows, so its close does not wait for
// them. Afterwards the store takes no writes but its callers'. It is safe to
// call more than once, and concurrently with Close. Owner-driven Compact,
// cold-pack and purge calls remain available until Close.
func (s *Store) StopBackground() {
	s.maintenanceStop()
	s.prewarm.stop()
	s.stopRelayout()
	if s.compactOnce.done != nil {
		<-s.compactOnce.done
	}
	if f := s.catalogMembership; f != nil {
		f.halt()
	}
	s.backgroundStopped.Store(true)
}

// BackgroundStopped reports whether StopBackground has completed.
func (s *Store) BackgroundStopped() bool { return s.backgroundStopped.Load() }

func (s *Store) stopRelayout() {
	// Signal both workers before joining either: a cold-pack block must not
	// delay cancellation of an unrelated running reclaim.
	if s.relayoutStop != nil {
		s.relayoutStop()
	}
	s.stopColdPack()
	if s.relayoutStop != nil {
		<-s.relayoutDone
	}
}

// WALBytes returns the bytes that writes have added to the store's WAL, not
// counting the empty records of its own syncs. A flush started after reading
// n holds every write made before it; if WALBytes still returns n later,
// nothing has been written since. Observations wait for empty sync records
// to be counted, so a completed sync cannot cancel out a later real write.
func (s *Store) WALBytes() uint64 {
	s.walSyncMu.Lock()
	defer s.walSyncMu.Unlock()
	return s.db.Metrics().WAL.BytesIn - s.syncWALBytes
}

// logSync syncs the WAL, and every commit before it, by committing an empty
// LogData record, which WALBytes leaves out.
func (s *Store) logSync() error {
	s.walSyncMu.Lock()
	defer s.walSyncMu.Unlock()
	b := s.db.NewBatch()
	_ = b.LogData(nil, nil)
	size := uint64(len(b.Repr()))
	if err := b.Commit(pebble.Sync); err != nil {
		return err // like DB.LogData, keep a failed batch out of the pool
	}
	s.syncWALBytes += size
	return b.Close()
}

// CloseForExit closes the store like Close but leaves cached table readers
// for process exit to release. Use it only when the process exits right after.
func (s *Store) CloseForExit() error { return s.close(true) }

func (s *Store) Close() error { return s.close(false) }

func (s *Store) close(forExit bool) error {
	// Cancel admission before joining readers. Held admission slots are not
	// active views and must never keep Close waiting for their owners.
	s.readMu.Lock()
	s.readCancel()
	if s.readCount == 0 {
		close(s.readDone)
	}
	s.readMu.Unlock()
	// Refuse new owner operations and cancel every admitted caller before any
	// joins. Cancellation reaches each span's admission check synchronously.
	s.explicitMaintenance.cancel()
	s.maintenanceStop()
	// Closing must not wait on an operational compaction rate budget.
	s.StopCompactionPacing()
	close(s.compactionStop)
	<-s.compactionDone
	step := time.Now()
	lap := func(d *time.Duration) {
		now := time.Now()
		*d, step = now.Sub(step), now
	}
	t := &s.closeTimings
	s.explicitMaintenance.stop()
	s.prewarm.stop()
	s.stopRelayout()
	// Also checkpoint callers of ColdPackStep/RunColdPack which stopped the
	// background worker before advancing the sweep themselves.
	coldPackErr := s.checkpointColdPackOnStop()
	if s.ingest != nil {
		s.ingest.close()
	}
	lap(&t.Ingest)
	if f := s.catalogMembership; f != nil {
		f.halt()
	}
	lap(&t.Membership)
	if s.syncStop != nil {
		close(s.syncStop)
		<-s.syncDone
	}
	lap(&t.PeriodicSync)
	s.stopSnapshotRelease()
	lap(&t.SnapshotRelease)
	s.stopWALSyncer()
	lap(&t.WALSyncer)
	// Checkpoints are optional accelerators; a failed save never changes durability.
	if s.checkpoint.enabled {
		if !s.checkpoint.prepared && !forExit {
			_ = s.PrepareMetadataCheckpoint(context.Background())
		}
		if s.checkpoint.prepared && s.db.VisibleSequence() != s.checkpoint.sequence {
			s.checkpoint.saveErr = errors.New("store changed after metadata checkpoint")
		}
	}
	// All admitted reads release their views before Pebble closes. New calls
	// reject readCtx cancellation under the registration lock.
	<-s.readDone
	// Writers must be stopped before Close. Pebble flushes and syncs the WAL
	// on close, including mutations made after the final periodic sync.
	var err error
	if forExit {
		err = s.db.CloseForExit()
	} else {
		err = s.db.Close()
	}
	if s.compactOnce.done != nil {
		<-s.compactOnce.done
	}
	if s.deferredSync != nil {
		// Close the tables of jobs that never reached their manifest commit.
		err = errors.Join(err, s.deferredSync.syncPending())
	}
	lap(&t.Pebble)
	t.PebbleSteps = s.db.CloseSteps()
	if s.cache != nil {
		s.cache.Unref()
	}
	if s.rangeDelCache != nil {
		s.rangeDelCache.Unref()
	}
	return errors.Join(err, coldPackErr)
}

// Sync makes completed mutations durable without flushing the memtables.
// Recovery sources may be retired only after this boundary, even when ordinary
// mutations use periodic WAL syncing.
func (s *Store) Sync() error { return s.syncWAL() }

// Flush writes the memtables at full speed: its caller waits for them.
func (s *Store) Flush() error {
	s.compactionControl.urgentFlush(1)
	defer s.compactionControl.urgentFlush(-1)
	return s.db.Flush()
}

// AsyncFlush starts a flush nobody waits for, which flush pacing may spread.
func (s *Store) AsyncFlush() (<-chan struct{}, error) { return s.db.AsyncFlush() }
func (s *Store) Compact() error {
	// Explicit/offline compactions share the close lifetime. Close
	// must not race a caller admitting its next file span after cancellation.
	ctx, finish, ok := s.explicitMaintenance.begin(context.Background())
	if !ok {
		return context.Canceled
	}
	defer finish()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.Flush(); err != nil {
		return err
	}
	return s.compactSpans(ctx, []byte{0}, []byte{255}, nil)
}

// CompactionKinds counts completed compactions by kind.
type CompactionKinds struct {
	Default, DeleteOnly, ElisionOnly, Move, Read, Rewrite, MultiLevel uint64
}

type Stats struct {
	ColdPack                                                                                           ColdPackStats
	TransferExport                                                                                     TransferExportStats
	MirrorCanonicalBatches, MirrorBlindBatches                                                         uint64
	DiskBytes, WALBytes, MemTableBytes, Materializations, Operands                                     uint64
	CacheBytes, CacheHits, CacheMisses                                                                 uint64
	RangeDelCacheBytes, RangeDelCacheHits, RangeDelCacheMisses                                         uint64
	Flushes, Compactions, CompactionDebt                                                               uint64
	CompactionRate, CompactionPacerWaitNanos                                                           int64
	CompactionPacerBytes                                                                               uint64
	CompactionPacerTargetRate                                                                          int64
	CompactionPacerAuto                                                                                bool
	CompactionPacerHeadroom                                                                            float64
	CompactionPacerWarmup                                                                              time.Duration
	CompactionPacerBypass                                                                              string
	CompactionPacerBypasses                                                                            map[string]uint64
	FlushRate, FlushPacerTargetRate, FlushPacerWaitNanos                                               int64
	FlushPacerBytes                                                                                    uint64
	FlushPacerAuto                                                                                     bool
	FlushPacerBypass                                                                                   string
	FlushPacerBypasses                                                                                 map[string]uint64
	CompactionsInProgress                                                                              int64
	CompactionWorkers, L0Sublevels, L0Files, L0Bytes                                                   int64
	CompactionReadBytes, CompactionWriteBytes, FlushWriteBytes                                         uint64
	WriteStalls                                                                                        uint64
	WriteStallNanos                                                                                    int64
	WriteStallReason                                                                                   string
	WriteStallActive                                                                                   bool
	ReconcileComparedChunks, ReconcileWrittenChunks, ReconcileDeletedChunks, ReconcileUnchangedMetrics uint64
	TableCacheEntries, TableCacheHits, TableCacheMisses                                                int64
	CatalogFilterBytes                                                                                 uint64
	CatalogFilterReady                                                                                 bool
	CacheShards, RangeDelCacheShards                                                                   int
	// CompactOnceRunning is true while a CompactOnce full compaction is pending or running.
	CompactOnceRunning bool
	// StateCache reports the metadata cache (see Options.MetadataCacheSize).
	StateCache StateCacheStats
	// UpdatePointLookups reports the read path continuation updates now start
	// with (see SetUpdatePointLookups).
	UpdatePointLookups                                                              bool
	IngestTables, IngestEntries, IngestBytes, IngestL0Bytes, IngestMemtableOverlaps uint64
	// Transfers committed as one batch instead of SST ingestion: commits,
	// imported names, and batch bytes.
	TransferBatchCommits, TransferBatchEntries, TransferBatchBytes uint64
	// Present continuation names merged by appending the target's newer
	// points to the staged source (of which classic sources merged in place),
	// and those merged by RestoreArchives, by reason
	// (TransferMergeFallbackReasons).
	TransferMergeAppended, TransferMergeAppendedClassic uint64
	TransferMergeFallbacks                              map[string]uint64
	// Per LSM level: bytes compactions wrote into it, bytes the compactions
	// into it read (inputs from the level above plus its own overlapping
	// tables), and its current size.
	LevelCompactedBytes [7]uint64
	LevelReadBytes      [7]uint64
	LevelBytes          [7]int64
	// LevelTargetBytes is each level's size target (zero for L0 and levels
	// above the base level); LevelScores is Pebble's compaction score, the
	// level's size over its target adjusted by the next level's.
	LevelTargetBytes [7]int64
	LevelScores      [7]float64
	Relayout         RelayoutStats
	// PurgeInvalidNames progress since start: names its scan found, names it
	// removed so far, and whether it is still running.
	PurgeFound, PurgeRemoved uint64
	PurgeRunning             bool
	// ContinuationRangeDeletes counts continuation prefix deletes written as
	// range tombstones (cumulative, process-wide).
	ContinuationRangeDeletes uint64
	// CompactionKinds counts compactions by kind (MultiLevel is a subset of
	// Default).
	CompactionKinds CompactionKinds
	// FS counts filesystem operations and bytes by file kind.
	FS FSStats
}

func (s *Store) Stats() Stats {
	m := s.db.Metrics()
	pacing := s.compactionControl.stats()
	s.stallMu.Lock()
	stalls, duration, reason := s.stallCount, s.stallDuration, s.stallReason
	active := !s.stallSince.IsZero()
	if active {
		duration += time.Since(s.stallSince)
	}
	s.stallMu.Unlock()
	return s.statsFrom(m, pacing, s.ColdPackStats(), s.RelayoutStats(), stalls, duration, reason, active, s.rangeCacheSnapshot())
}

func (s *Store) statsFrom(m *pebble.Metrics, pacing compactionPacingStats, cold ColdPackStats, relayout RelayoutStats,
	stalls uint64, duration time.Duration, reason string, active bool, rangeCache rangeCacheSnapshot) Stats {
	var read, written, flushed uint64
	var levelCompacted, levelRead [7]uint64
	var levelBytes [7]int64
	var levelScores [7]float64
	for i, level := range m.Levels {
		read += level.BytesRead
		written += level.BytesCompacted
		flushed += level.BytesFlushed
		levelCompacted[i], levelRead[i], levelBytes[i], levelScores[i] = level.BytesCompacted, level.BytesRead, level.Size, level.Score
	}
	stats := Stats{ColdPack: cold,
		MirrorCanonicalBatches: s.mirrorCanonical.Load(), MirrorBlindBatches: s.mirrorBlind.Load(),
		DiskBytes: uint64(m.DiskSpaceUsage()), WALBytes: m.WAL.Size, MemTableBytes: m.MemTable.Size,
		Materializations: s.materializations.Load(), Operands: s.operands.Load(),
		CacheBytes: uint64(m.BlockCache.Size), CacheHits: uint64(m.BlockCache.Hits), CacheMisses: uint64(m.BlockCache.Misses),
		Flushes: uint64(m.Flush.Count), Compactions: uint64(m.Compact.Count), CompactionDebt: m.Compact.EstimatedDebt,
		CompactionRate: pacing.rate, CompactionPacerBytes: s.compactionPacing.pacer.bytes.Load(), CompactionPacerWaitNanos: s.compactionPacing.pacer.wait.Load(),
		CompactionPacerTargetRate: pacing.target, CompactionPacerAuto: pacing.auto, CompactionPacerHeadroom: pacing.headroom, CompactionPacerWarmup: pacing.warmup, CompactionPacerBypass: pacing.bypass, CompactionPacerBypasses: pacing.bypasses,
		FlushRate: pacing.flush.rate, FlushPacerTargetRate: pacing.flush.target, FlushPacerAuto: pacing.flush.auto, FlushPacerBypass: pacing.flush.bypass,
		FlushPacerBypasses: pacing.flush.bypasses, FlushPacerBytes: s.compactionPacing.flush.bytes.Load(), FlushPacerWaitNanos: s.compactionPacing.flush.wait.Load(),
		CompactionsInProgress: m.Compact.NumInProgress,
		CompactionWorkers:     s.compactionWorkers.Load(), L0Sublevels: int64(m.Levels[0].Sublevels), L0Files: m.Levels[0].NumFiles, L0Bytes: m.Levels[0].Size,
		CompactionReadBytes: read, CompactionWriteBytes: written, FlushWriteBytes: flushed,
		WriteStalls: stalls, WriteStallNanos: int64(duration), WriteStallReason: reason, WriteStallActive: active,
		TableCacheEntries: m.TableCache.Count, TableCacheHits: m.TableCache.Hits, TableCacheMisses: m.TableCache.Misses,
		LevelCompactedBytes: levelCompacted, LevelReadBytes: levelRead, LevelBytes: levelBytes, Relayout: relayout,
		LevelTargetBytes: levelTargets(levelBytes, s.levelBase, s.levelMultiplier), LevelScores: levelScores,
		PurgeFound: s.purgeFound.Load(), PurgeRemoved: s.purgeRemoved.Load(), PurgeRunning: s.purgeRunning.Load(),
		ReconcileComparedChunks: s.reconcileCompared.Load(), ReconcileWrittenChunks: s.reconcileWritten.Load(), ReconcileDeletedChunks: s.reconcileDeleted.Load(), ReconcileUnchangedMetrics: s.reconcileUnchanged.Load(),
	}
	if f := s.catalogMembership; f != nil {
		stats.CatalogFilterBytes = uint64(len(f.words)) * 8
		stats.CatalogFilterReady = f.ready.Load()
	}
	stats.CacheShards = s.cacheShards
	stats.StateCache = s.stateCache.stats()
	stats.TransferExport = s.transferExport.stats()
	stats.CompactOnceRunning = s.compactOnce.running.Load()
	stats.UpdatePointLookups = s.updatePointLookups.Load()
	if s.rangeDelCache != nil {
		stats.RangeDelCacheShards = 4
		stats.RangeDelCacheBytes, stats.RangeDelCacheHits, stats.RangeDelCacheMisses = rangeCache.bytes, rangeCache.hits, rangeCache.misses
		stats.CacheBytes += stats.RangeDelCacheBytes
		stats.CacheHits += stats.RangeDelCacheHits
		stats.CacheMisses += stats.RangeDelCacheMisses
	}
	stats.IngestTables, stats.IngestEntries, stats.IngestBytes = s.ingestStats.tables.Load(), s.ingestStats.entries.Load(), s.ingestStats.bytes.Load()
	stats.IngestL0Bytes, stats.IngestMemtableOverlaps = s.ingestStats.l0Bytes.Load(), s.ingestStats.memtableOverlaps.Load()
	stats.TransferBatchCommits, stats.TransferBatchEntries, stats.TransferBatchBytes = s.transferBatchStats.commits.Load(), s.transferBatchStats.entries.Load(), s.transferBatchStats.bytes.Load()
	stats.TransferMergeAppended, stats.TransferMergeAppendedClassic, stats.TransferMergeFallbacks = s.transferMergeStats.snapshot()
	stats.ContinuationRangeDeletes = continuationRangeDeletes.Load()
	stats.CompactionKinds = CompactionKinds{Default: uint64(m.Compact.DefaultCount), DeleteOnly: uint64(m.Compact.DeleteOnlyCount), ElisionOnly: uint64(m.Compact.ElisionOnlyCount),
		Move: uint64(m.Compact.MoveCount), Read: uint64(m.Compact.ReadCount), Rewrite: uint64(m.Compact.RewriteCount), MultiLevel: uint64(m.Compact.MultiLevelCount)}
	if s.fsStats != nil {
		stats.FS = s.fsStats.Stats()
	}
	return stats
}

// SetUpdatePointLookups selects the read path of continuation updates that
// start after it returns: a lookup or iterator per read (true), or reads
// shared through one iterator (false, see continuationIO.withIterator). An
// update in flight keeps the path it started with and its own iterator, so
// switching on a live host lets the two paths be compared in one process,
// without the restart transient that confounds a comparison across restarts.
func (s *Store) SetUpdatePointLookups(enabled bool) {
	s.updatePointLookups.Store(enabled)
}

// SetCompactionRate changes the shared compaction output budget immediately.
// -1 enables automatic pacing; zero disables pacing and releases waiting writes.
func (s *Store) SetCompactionRate(rate int64) error {
	if rate < -1 {
		return errors.New("compaction rate must be -1 (auto), 0 (off), or positive")
	}
	s.compactionControl.setMode(rate)
	return nil
}

var (
	errCompactionHeadroom = errors.New("compaction rate headroom must be 1..4")
	errCompactionWarmup   = errors.New("compaction pacing warmup must be 0..24h")
)

// SetCompactionHeadroom changes the multiplier auto mode applies to its
// modelled compaction rate, immediately.
func (s *Store) SetCompactionHeadroom(headroom float64) error {
	if !validCompactionHeadroom(headroom) {
		return errCompactionHeadroom
	}
	s.compactionControl.setHeadroom(headroom)
	return nil
}

// SetCompactionDebtBypass chooses whether excess compaction debt alone makes
// auto mode stop pacing, immediately.
func (s *Store) SetCompactionDebtBypass(enabled bool) {
	s.compactionControl.setDebtBypass(enabled)
}

// SetCompactionWarmup changes how long auto mode stays unpaced after the store
// started. It never restarts the warm-up.
func (s *Store) SetCompactionWarmup(warmup time.Duration) error {
	if !validCompactionWarmup(warmup) {
		return errCompactionWarmup
	}
	s.compactionControl.setWarmup(warmup)
	return nil
}

// SetFlushRate changes memtable flush pacing (see Options.FlushRate),
// including for a flush already writing. Stores without exact blocks keep
// flushes unpaced.
func (s *Store) SetFlushRate(rate int64) error {
	if rate < -1 {
		return errors.New("flush rate must be -1 (auto), 0 (off), or positive")
	}
	if !s.flushPacing {
		rate = 0
	}
	s.compactionControl.setFlushMode(rate)
	return nil
}

// SetCompactionWorkers changes Pebble's supported dynamic concurrency budget.
// Existing compactions finish; subsequent scheduling uses the new limit.
func (s *Store) SetCompactionWorkers(workers int) error {
	if workers < 1 || workers > 64 {
		return errors.New("compaction workers must be 1..64")
	}
	s.compactionWorkers.Store(int64(workers))
	return nil
}

func (s *Store) writeStallBegin(info pebble.WriteStallBeginInfo) {
	s.compactionControl.stall(true)
	s.stallMu.Lock()
	defer s.stallMu.Unlock()
	s.stallCount++
	s.stallSince = time.Now()
	s.stallReason = info.Reason
}

func (s *Store) writeStallEnd() {
	s.compactionControl.stall(false)
	s.stallMu.Lock()
	defer s.stallMu.Unlock()
	if !s.stallSince.IsZero() {
		s.stallDuration += time.Since(s.stallSince)
		s.stallSince = time.Time{}
	}
}

func (s *Store) lockMetric(name string) func() {
	// The writeout queue groups metrics by low hash bits. Use high bits so
	// unrelated metrics in one queued cache shard can make progress together.
	mu := &s.metricMu[s.metricStripe(name)]
	mu.Lock()
	return mu.Unlock
}

// Thousands of concurrent conversions share these stripes with persister
// metadata lookups; 256 stripes made the persister queue behind conversions.
const metricStripes = 4096

func (s *Store) metricStripe(name string) uint64 {
	return (helper.HashString(name) >> 32) % uint64(len(s.metricMu))
}

func (s *Store) commit(batch *pebble.Batch) error {
	if err := batch.Commit(s.writeOptions); err != nil {
		return err
	}
	if !s.writeOptions.Sync {
		s.pendingSync.Store(true)
	}
	return nil
}

// maxUnflushedMemTableBytes is the default bound on queued memtables.
const maxUnflushedMemTableBytes = 8 << 30

func catalogKey(name string) []byte { return append([]byte("m/"), name...) }
func sequenceKey() []byte           { return []byte("z/sequence") }
func uint64Bytes(v uint64) []byte   { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }
func revisionBytes(m Metadata) []byte {
	b := make([]byte, revisionSize)
	binary.BigEndian.PutUint64(b[:8], m.Revision)
	if !m.LastUpdate.IsZero() {
		binary.BigEndian.PutUint64(b[8:], uint64(m.LastUpdate.UnixNano()))
	}
	return b
}
func decodeRevision(value []byte, m *Metadata) error {
	switch len(value) {
	case legacyRevisionSize:
		m.Revision = binary.BigEndian.Uint64(value)
		m.LastUpdate = time.Time{}
	case revisionSize:
		m.Revision = binary.BigEndian.Uint64(value[:8])
		nanos := int64(binary.BigEndian.Uint64(value[8:]))
		if nanos == 0 {
			m.LastUpdate = time.Time{}
		} else {
			m.LastUpdate = time.Unix(0, nanos)
		}
	default:
		return errors.New("invalid metric revision")
	}
	return nil
}
func (s *Store) nextActivity(previous time.Time) time.Time {
	return nextActivityAt(previous, s.now())
}

func nextActivityAt(previous, observed time.Time) time.Time {
	now := time.Unix(0, observed.UnixNano())
	if !previous.IsZero() && now.Before(previous) {
		return previous
	}
	// An all-zero encoded timestamp means "activity unknown"; a clock that
	// reads exactly the Unix epoch must not collide with that sentinel.
	if now.UnixNano() == 0 {
		return time.Unix(0, 1)
	}
	return now
}
func revisionKey(m Metadata) []byte {
	b := make([]byte, 17)
	b[0] = 'r'
	binary.BigEndian.PutUint64(b[1:], m.ID)
	binary.BigEndian.PutUint64(b[9:], m.Generation)
	return b
}

func (s *Store) Create(ctx context.Context, config MetricConfig) (Metadata, error) {
	if err := s.fileOwnershipError(); err != nil {
		return Metadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if err := errors.Join(validateName(config.Name), validate(config)); err != nil {
		return Metadata{}, err
	}
	unlock := s.lockMetricMutation(config.Name)
	defer unlock()
	// Every catalog publisher holds this name's mutation stripe. A complete
	// membership filter proves absence; otherwise keep the potentially cold
	// LSM read outside the global ID-allocation/commit lock.
	if s.catalogMembership.mayContain(config.Name) {
		if _, closer, err := s.db.Get(catalogKey(config.Name)); err == nil {
			closer.Close()
			return Metadata{}, ErrExists
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return Metadata{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := uint64(1)
	if v, closer, err := s.db.Get(sequenceKey()); err == nil {
		if len(v) != 8 {
			closer.Close()
			return Metadata{}, errors.New("invalid catalog sequence")
		}
		previous := binary.BigEndian.Uint64(v)
		closer.Close()
		if previous == math.MaxUint64 {
			return Metadata{}, errors.New("metric IDs exhausted")
		}
		id = previous + 1
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return Metadata{}, err
	}
	m := Metadata{MetricConfig: cloneConfig(config), ID: id, Generation: 1, Revision: 1, LastUpdate: s.nextActivity(time.Time{})}
	m.Continuation = s.continuation != nil
	if s.incrementalSource != "" {
		m.FileMigration = FileReclaimed // New metrics never had an original file.
	}
	encoded, err := s.encodeCatalog(m)
	if err != nil {
		return Metadata{}, err
	}
	b := s.newStateBatch()
	defer b.Close()
	var state *stateView
	if m.Continuation {
		io := s.newFreshContinuationIO(ctx, b, m)
		io.state = s.newStateView(nil, m)
		state = io.state
		if err := s.continuation.Create(ctx, io, m, s.now); err != nil {
			return Metadata{}, err
		}
	}
	for _, kv := range []struct{ k, v []byte }{{catalogKey(config.Name), encoded}, {sequenceKey(), uint64Bytes(id)}, {revisionKey(m), revisionBytes(m)}} {
		if err := b.Set(kv.k, kv.v, nil); err != nil {
			return Metadata{}, err
		}
	}
	if err := s.commit(b); err != nil {
		return Metadata{}, fmt.Errorf("commit create: %w", err)
	}
	s.catalogChanged(m.Name)
	s.installState(state, m)
	return m, nil
}

func (s *Store) Metadata(ctx context.Context, name string) (Metadata, error) {
	ctx, finish, err := s.beginRead(ctx)
	if err != nil {
		return Metadata{}, err
	}
	defer finish()
	if err := s.fileOwnershipError(); err != nil {
		return Metadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if !s.catalogMembership.mayContain(name) {
		return Metadata{}, ErrNotFound
	}
	releaseSlot, err := s.acquireReadSlot(ctx)
	if err != nil {
		return Metadata{}, err
	}
	defer releaseSlot()
	// Like Fetch, read at one committed view without waiting for a writer's
	// stripe. The epoch prevents a cached state newer than that view from
	// being used, and prevents a concurrent mutation from accepting a fill.
	cache := s.stateCache
	var stripe, epoch uint64
	if cache != nil {
		stripe = s.metricStripe(name)
		epoch = cache.epoch(stripe)
	}
	view := s.db.NewReadView()
	defer view.Close()
	if cache != nil {
		if state := cache.get(stripe, name, epoch); state != nil {
			m := state.meta
			m.MetricConfig = cloneConfig(m.MetricConfig)
			return m, s.checkFileOwnership(m)
		}
	}
	m, err := metadataFrom(view, name)
	if err == nil {
		if cache != nil {
			cache.fill(stripe, name, epoch, newMetricState(m).seal())
		}
		err = s.checkFileOwnership(m)
	}
	return m, err
}
func metadataFrom(reader pebble.Reader, name string) (Metadata, error) {
	countStoreRead()
	v, closer, err := reader.Get(catalogKey(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return Metadata{}, ErrNotFound
	}
	if err != nil {
		return Metadata{}, fmt.Errorf("read catalog: %w", err)
	}
	m, err := decodeCatalog(name, v)
	closer.Close()
	if err != nil {
		return Metadata{}, fmt.Errorf("decode catalog: %w", err)
	}
	countStoreRead()
	v, closer, err = reader.Get(revisionKey(m))
	if err != nil {
		return Metadata{}, fmt.Errorf("read revision: %w", err)
	}
	defer closer.Close()
	if err := decodeRevision(v, &m); err != nil {
		return Metadata{}, err
	}
	return m, nil
}
