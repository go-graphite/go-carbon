package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
)

// Converting an original file writes its whole history once. Through the
// memtable that history is written again by the WAL, the flush and every
// compaction level, and its flushes stall live writers on a busy device.
// Fresh stages are instead grouped into one sorted table under a durably
// reserved, contiguous ID range that no other writer can allocate, then
// ingested. Pebble places such a table directly in the lowest level whose files
// it does not overlap, normally the bottom level.
const (
	defaultFileIngestTargetBytes = 64 << 20
	// Each ingestion holds a commit sequence number unpublished while its
	// manifest edit syncs, so every later write waits behind it. Most stale
	// originals convert to about 1 KiB; lingering bounds ingestion to about one
	// per second instead of one per handful of metrics. With only a few
	// conversion workers the linger dominates their latency; configure less.
	defaultFileIngestLinger = time.Second
	fileIngestDir           = "ingest-stage"
	// Table syncs wait behind host writeback, so several groups build and
	// ingest concurrently; their ID ranges are disjoint.
	defaultFileIngestParallel = 4
	// One durable reservation covers many groups. Unused IDs are only gaps.
	fileIngestIDBlock = 1024
	// Pebble runs one ingestion at a time (about one per second behind a busy
	// device), so throughput is the group size per ingestion. Groups flush as
	// soon as they fill; with workers several times the group size, one group
	// ingests while the previous publishes and the next fills.
	// Entries per group scale with its byte target: 4096 at 64 MiB.
	fileIngestBytesPerEntry = 16 << 10
)

var errFileIngestClosed = errors.New("file ingestion stopped")

type fileIngestEntry struct {
	m      Metadata
	keys   [][]byte // continuation suffixes in ascending order
	values [][]byte
	bytes  int
	result Metadata
	err    error
	done   chan struct{}
}

type fileIngester struct {
	s       *Store
	mu      sync.Mutex
	pending []*fileIngestEntry
	bytes   int
	closed  bool
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	slots   chan struct{}
	linger  time.Duration
	// target is the group size in bytes that starts a table at once;
	// maxEntries bounds its stages.
	target, maxEntries int
	inflight           sync.WaitGroup
	idMu               sync.Mutex
	// [nextID, endID) is durably reserved and not yet assigned.
	nextID, endID uint64
}

func newFileIngester(s *Store, linger time.Duration, target, parallel int) (*fileIngester, error) {
	dir := s.fs.PathJoin(s.dir, fileIngestDir)
	// Tables left by an interrupted ingestion were never linked into the
	// database; their reserved IDs simply remain unused.
	if err := s.fs.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := s.fs.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	g := &fileIngester{s: s, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), slots: make(chan struct{}, parallel), linger: linger,
		target: target, maxEntries: max(1, target/fileIngestBytesPerEntry)}
	go g.run()
	return g, nil
}

func (g *fileIngester) close() {
	close(g.stop)
	<-g.done
}

// submit blocks until the entry is ingested and its catalog record committed,
// or ingestion fails. Every submitted entry is answered exactly once.
func (g *fileIngester) submit(e *fileIngestEntry) (Metadata, error) {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return Metadata{}, errFileIngestClosed
	}
	g.pending = append(g.pending, e)
	g.bytes += e.bytes
	g.mu.Unlock()
	select {
	case g.wake <- struct{}{}:
	default:
	}
	<-e.done
	return e.result, e.err
}

func (g *fileIngester) take() []*fileIngestEntry {
	g.mu.Lock()
	defer g.mu.Unlock()
	n, bytes := 0, 0
	for n < len(g.pending) && n < g.maxEntries && (n == 0 || bytes+g.pending[n].bytes <= 2*g.target) {
		bytes += g.pending[n].bytes
		n++
	}
	entries := g.pending[:n:n]
	g.pending = g.pending[n:]
	g.bytes -= bytes
	return entries
}

func (g *fileIngester) pendingBytes() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.bytes
}

// groupReady reports whether a full table is already waiting.
func (g *fileIngester) groupReady() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.bytes >= g.target || len(g.pending) >= g.maxEntries
}

func (g *fileIngester) run() {
	defer close(g.done)
	defer func() {
		g.inflight.Wait()
		g.mu.Lock()
		g.closed = true
		pending := g.pending
		g.pending, g.bytes = nil, 0
		g.mu.Unlock()
		finishFileIngest(pending, errFileIngestClosed)
	}()
	for {
		select {
		case <-g.stop:
			return
		case <-g.wake:
		}
		// Stages keep accumulating while every table builder is busy.
		select {
		case g.slots <- struct{}{}:
		case <-g.stop:
			return
		}
		// Linger briefly so concurrent conversions share one table and one
		// manifest update, unless a full table is already waiting.
		linger := time.NewTimer(g.linger)
	wait:
		for !g.groupReady() {
			select {
			case <-g.wake:
			case <-linger.C:
				break wait
			case <-g.stop:
				linger.Stop()
				<-g.slots
				return
			}
		}
		linger.Stop()
		entries := g.take()
		if len(entries) == 0 {
			<-g.slots
			continue
		}
		g.inflight.Add(1)
		go func() {
			defer g.inflight.Done()
			defer func() { <-g.slots }()
			g.s.ingestFileStages(entries)
		}()
		if g.pendingBytes() > 0 {
			select {
			case g.wake <- struct{}{}:
			default:
			}
		}
	}
}

func (g *fileIngester) allocate(n uint64) (uint64, error) {
	g.idMu.Lock()
	defer g.idMu.Unlock()
	if g.endID-g.nextID < n {
		block := max(n, fileIngestIDBlock)
		first, err := g.s.reserveFileMetricIDs(block)
		if err != nil {
			return 0, err
		}
		g.nextID, g.endID = first, first+block
	}
	first := g.nextID
	g.nextID += n
	return first, nil
}

func finishFileIngest(entries []*fileIngestEntry, err error) {
	for _, e := range entries {
		if e.err == nil {
			e.err = err
		}
		close(e.done)
	}
}

// stageFileIngest builds a fresh candidate privately and hands it to the
// ingester, which assigns its ID. Only metrics absent from the catalog use it.
func (s *Store) stageFileIngest(ctx context.Context, snapshot Snapshot, m Metadata) (Metadata, error) {
	m.ID, m.Generation, m.Revision = 0, 1, 1
	if m.LastUpdate.IsZero() {
		m.LastUpdate = s.nextActivity(time.Time{})
	}
	batch := s.db.NewIndexedBatch()
	defer batch.Close()
	// The continuation codec stores only metric-local identities, so the
	// private prefix is rewritten to the assigned ID when the table is built.
	io := s.newFreshContinuationIO(ctx, batch, m)
	if err := s.continuation.Import(ctx, io, snapshot.SourceState, m, s.now); err != nil {
		return Metadata{}, err
	}
	e := &fileIngestEntry{m: m, done: make(chan struct{})}
	if err := io.Visit(nil, func(key, value []byte) error {
		e.keys, e.values = append(e.keys, key), append(e.values, value)
		e.bytes += len(io.prefix) + len(key) + len(value)
		return nil
	}); err != nil {
		return Metadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return Metadata{}, err
	}
	if y, ok := ctx.Value(ingestYieldKey{}).(IngestYield); ok {
		// Waiting for a group holds no file or thread; let another
		// conversion use the caller's I/O admission meanwhile.
		y.Release()
		defer y.Acquire()
	}
	return s.ingest.submit(e)
}

// IngestYield is an admission a caller holds around file I/O. A fresh stage
// releases it while waiting for its ingestion group and reacquires it after.
type IngestYield interface {
	Release()
	Acquire()
}

type ingestYieldKey struct{}

// WithIngestYield attaches an admission released during ingestion waits.
func WithIngestYield(ctx context.Context, y IngestYield) context.Context {
	return context.WithValue(ctx, ingestYieldKey{}, y)
}

func (s *Store) ingestFileStages(entries []*fileIngestEntry) {
	first, err := s.ingest.allocate(uint64(len(entries)))
	if err != nil {
		finishFileIngest(entries, err)
		return
	}
	for i, e := range entries {
		e.m.ID = first + uint64(i)
	}
	base := s.fs.PathJoin(s.dir, fileIngestDir, fmt.Sprintf("stage-%020d", first))
	paths, err := s.writeFileStageTables(base, entries)
	if err != nil {
		// Partial tables were never ingested.
		if names, listErr := s.fs.List(s.fs.PathDir(base)); listErr == nil {
			for _, name := range names {
				if strings.HasPrefix(name, s.fs.PathBase(base)) {
					_ = s.fs.Remove(s.fs.PathJoin(s.fs.PathDir(base), name))
				}
			}
		}
		finishFileIngest(entries, err)
		return
	}
	stats, err := s.db.IngestWithStats(paths)
	// Pebble links or copies the tables into the database; the staging names
	// are never read again.
	for _, path := range paths {
		_ = s.fs.Remove(path)
	}
	if err != nil {
		finishFileIngest(entries, fmt.Errorf("ingest file stages: %w", err))
		return
	}
	s.ingestStats.tables.Add(1)
	s.ingestStats.entries.Add(uint64(len(entries)))
	s.ingestStats.bytes.Add(stats.Bytes)
	s.ingestStats.l0Bytes.Add(stats.ApproxIngestedIntoL0Bytes)
	s.ingestStats.memtableOverlaps.Add(uint64(stats.MemtableOverlappingFiles))
	s.commitIngestedStages(entries)
	finishFileIngest(entries, nil)
}

// The reservation is synced before any table that uses it becomes durable, so
// a restart can never allocate an ID whose ingested history already exists.
func (s *Store) reserveFileMetricIDs(n uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	high, err := s.CatalogHighWatermark(context.Background())
	if err != nil {
		return 0, err
	}
	if n == 0 || high > math.MaxUint64-n {
		return 0, errors.New("metric IDs exhausted")
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(sequenceKey(), uint64Bytes(high+n), nil); err != nil {
		return 0, err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, err
	}
	return high + 1, nil
}

// writeFileStageTables writes the group's metric-prefixed keys to base.sst and
// its time-partitioned keys to one base-t<bucket>.sst per bucket, normally a
// single one. Time-partitioned keys order by bucket before metric, so a table
// spanning two buckets, or holding both kinds, would cover other metrics'
// keys; each of these tables covers only the group's reserved IDs.
func (s *Store) writeFileStageTables(base string, entries []*fileIngestEntry) ([]string, error) {
	type kv struct{ key, value []byte }
	var main, bucketed []kv
	for _, e := range entries {
		prefix := continuationPrefix(e.m.ID, e.m.Generation)
		for i, suffix := range e.keys {
			key := appendContinuationKey(nil, prefix, suffix)
			if isContinuationBucketSuffix(suffix) {
				bucketed = append(bucketed, kv{key, e.values[i]})
			} else {
				main = append(main, kv{key, e.values[i]})
			}
		}
	}
	slices.SortFunc(bucketed, func(a, b kv) int { return bytes.Compare(a.key, b.key) })
	type table struct {
		path string
		keys []kv
	}
	tables := []table{{base + ".sst", main}}
	for len(bucketed) > 0 {
		n := 1
		for n < len(bucketed) && bytes.Equal(bucketed[n].key[:5], bucketed[0].key[:5]) {
			n++
		}
		tables = append(tables, table{fmt.Sprintf("%s-t%d.sst", base, binary.BigEndian.Uint32(bucketed[0].key[1:5])), bucketed[:n]})
		bucketed = bucketed[n:]
	}
	var paths []string
	for _, table := range tables {
		if len(table.keys) == 0 {
			continue
		}
		f, err := s.fs.Create(table.path)
		if err != nil {
			return nil, err
		}
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), s.pebbleOptions.MakeWriterOptions(numLevels-1, s.db.FormatMajorVersion().MaxTableFormat()))
		for _, k := range table.keys {
			if err := w.Set(k.key, k.value); err != nil {
				_ = w.Close()
				return nil, err
			}
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		paths = append(paths, table.path)
	}
	return paths, nil
}

const numLevels = 7

// commitIngestedStages publishes one table's staged catalog records, each
// under its own stripe with the conflict rule of an ordinary fresh stage. The
// entries are independent (their data is already ingested under unpublished
// IDs), so holding every stripe of a table at once only made the commit wait
// for each persister update on any of them while stalling those writers. Free
// stripes commit in turn; entries whose stripe is busy wait concurrently. A
// conflicting stage leaves only unreferenced history under its unique ID.
func (s *Store) commitIngestedStages(entries []*fileIngestEntry) {
	var busy []*fileIngestEntry
	for _, e := range entries {
		if unlock, ok := s.tryLockMetricMutation(e.m.Name); ok {
			s.commitIngestedStage(e, unlock)
		} else {
			busy = append(busy, e)
		}
	}
	var wg sync.WaitGroup
	for _, e := range busy {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.commitIngestedStage(e, s.lockMetricMutation(e.m.Name))
		}()
	}
	wg.Wait()
}

// commitIngestedStage publishes e's catalog record while the caller holds its
// stripe, then releases it with unlock.
func (s *Store) commitIngestedStage(e *fileIngestEntry, unlock func()) {
	defer unlock()
	// Check before encoding: encoding adds the name to the membership filter.
	if _, err := s.metadataLockedOrAbsent(e.m.Name); err == nil {
		e.err = ErrConflict
		return
	} else if !errors.Is(err, ErrNotFound) {
		e.err = err
		return
	}
	encoded, err := s.encodeCatalog(e.m)
	if err != nil {
		e.err = err
		return
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(catalogKey(e.m.Name), encoded, nil); err != nil {
		e.err = err
		return
	}
	if err := batch.Set(revisionKey(e.m), revisionBytes(e.m), nil); err != nil {
		e.err = err
		return
	}
	var sidecar *SidecarSummary
	for i, key := range e.keys {
		if bytes.Equal(key, SidecarSummaryKey) {
			summary, err := DecodeSidecarSummary(e.values[i])
			if err != nil {
				e.err = err
				return
			}
			sidecar = &summary
		}
	}
	if err := s.commitSidecars(batch, e.m.Name, sidecar, s.commitFileStage); err != nil {
		e.err = err
		return
	}
	s.catalogChanged(e.m.Name)
	e.result = e.m
}
