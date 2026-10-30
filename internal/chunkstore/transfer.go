package chunkstore

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

const MaxTransferMetrics = 10000

var ErrTransferLimit = errors.New("transfer exceeds byte budget")

// TransferVersion fences deletion against writes, recreation and replacement.
type TransferVersion struct {
	Name                     string
	ID, Generation, Revision uint64
}

func transferVersion(m Metadata) TransferVersion {
	return TransferVersion{m.Name, m.ID, m.Generation, m.Revision}
}

// TransferMetric carries generation-local keys. Exact blocks and continuation
// descriptors are copied verbatim; store-local identities never cross the wire.
type TransferMetric struct {
	Config       MetricConfig
	Version      TransferVersion
	Continuation bool
	Keys         []RelayoutKV
	PointDigest  string
	Counts       []int
}

// TransferProof describes the complete staged result before durable publication.
// Digest is over raw keys for a disjoint copy, logical archive points for a merge.
type TransferProof struct {
	KeyCount int
	Sampled  bool
	Source   TransferVersion
	Target   TransferVersion
	Digest   string
	Counts   []int
	Merged   bool
}

func (s *Store) ExportTransfer(ctx context.Context, names []string) ([]TransferMetric, error) {
	return s.ExportTransferLimit(ctx, names, 160<<20)
}
func (s *Store) ExportTransferLimit(ctx context.Context, names []string, budget int64) ([]TransferMetric, error) {
	if len(names) == 0 || len(names) > MaxTransferMetrics {
		return nil, errors.New("transfer needs 1..10000 metrics")
	}
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return nil, err
	}
	var epochs [metricStripes]uint64
	if s.stateCache != nil {
		for i := range epochs {
			epochs[i] = s.stateCache.epoch(uint64(i))
		}
	}
	view := s.db.NewReadView()
	defer view.Close()
	walk := &transferReader{reader: view, ordered: true, export: &s.transferExport}
	defer walk.close()
	s.transferExport.requests.Add(1)
	s.transferExport.metrics.Add(uint64(len(names)))
	metas := make([]Metadata, len(names))
	states := make([]*metricState, len(names))
	order := make([]int, len(names))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return strings.Compare(names[a], names[b]) })
	for j, i := range order {
		name := names[i]
		if j > 0 && name == names[order[j-1]] {
			return nil, errors.New("duplicate transfer name")
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if cache := s.stateCache; cache != nil {
			stripe := s.metricStripe(name)
			states[i] = cache.get(stripe, name, epochs[stripe])
		}
		if states[i] != nil {
			s.transferExport.metadataHits.Add(1)
			metas[i] = states[i].meta
			metas[i].MetricConfig = cloneConfig(metas[i].MetricConfig)
		} else {
			value, err := walk.lookup(&walk.catalog, catalogLower, catalogUpper, catalogKey(name))
			if err != nil {
				return nil, fmt.Errorf("export %s: %w", name, err)
			}
			metas[i], err = decodeCatalog(name, value)
			if err != nil {
				return nil, err
			}
		}
	}
	// Catalog keys are names; revisions and raw histories are ordered by ID.
	slices.SortFunc(order, func(a, b int) int { return cmp.Compare(metas[a].ID, metas[b].ID) })
	result := make([]TransferMetric, len(names))
	for _, i := range order {
		m := metas[i]
		if states[i] == nil {
			var err error
			m, err = walk.revision(m)
			if err != nil {
				return nil, err
			}
		}
		if err := s.checkFileOwnership(m); err != nil {
			return nil, err
		}
		if m.FileMigration == FileCleanupPending {
			return nil, ErrConflict
		}
		walk.state = states[i]
		record, err := s.transferFromLimit(ctx, view, m, budget, walk)
		if err != nil {
			return nil, err
		}
		budget -= transferSize(record)
		result[i] = record
	}
	return result, nil
}

// Reuse the codec's bucket enumeration without performing its deletes. The
// engine owns the physical prefixes; only the codec knows the live buckets.
type transferBuckets struct {
	ContinuationIO
	prefixes [][]byte
}

func (b *transferBuckets) DeletePrefix(prefix []byte) error {
	b.prefixes = append(b.prefixes, bytes.Clone(prefix))
	return nil
}
func (*transferBuckets) Set([]byte, []byte) error { return errors.New("read-only transfer") }
func (*transferBuckets) Delete([]byte) error      { return errors.New("read-only transfer") }

func (s *Store) transferFrom(ctx context.Context, reader pebble.Reader, m Metadata) (TransferMetric, error) {
	return s.transferFromLimit(ctx, reader, m, 160<<20)
}
func transferSize(r TransferMetric) int64 {
	n := int64(512 + len(r.Config.Name)*2 + len(r.Config.Retentions)*32)
	for _, kv := range r.Keys {
		n += int64(len(kv.Key) + len(kv.Value) + 8)
	}
	return n
}
func (s *Store) transferFromLimit(ctx context.Context, reader pebble.Reader, m Metadata, budget int64, shared ...*transferReader) (TransferMetric, error) {
	record := TransferMetric{Config: m.MetricConfig, Version: transferVersion(m), Continuation: m.Continuation, Keys: make([]RelayoutKV, 0, 16)}
	used := int64(512 + len(m.Name)*2 + len(m.Retentions)*32)
	if used > budget {
		return TransferMetric{}, ErrTransferLimit
	}
	var arena transferArena
	memory := &arena
	if len(shared) > 0 {
		memory = &shared[0].arena
	}
	add := func(k, v []byte) error {
		used += int64(len(k) + len(v) + 8)
		if used > budget {
			return ErrTransferLimit
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		data := memory.alloc(len(k) + len(v))
		copy(data, k)
		copy(data[len(k):], v)
		record.Keys = append(record.Keys, RelayoutKV{data[:len(k):len(k)], data[len(k):]})
		return nil
	}
	if m.Continuation {
		kv := s.newContinuationIO(ctx, reader, nil, m).withIterator()
		if len(shared) > 0 {
			it, err := shared[0].iterator(&shared[0].continuation, continuationLower, continuationUpper)
			if err != nil {
				return TransferMetric{}, err
			}
			kv.withBorrowedIterator(it)
		}
		defer kv.Close()
		if len(shared) > 0 {
			prefix := continuationPrefix(m.ID, m.Generation)
			if err := shared[0].visit(ctx, &shared[0].continuation, continuationLower, continuationUpper, prefix, func(key, value []byte) error { return add(key[len(prefix):], value) }); err != nil {
				return TransferMetric{}, err
			}
		} else if err := kv.Visit(nil, add); err != nil {
			return TransferMetric{}, err
		}
		if dropper, ok := s.continuation.(ContinuationDropper); ok {
			b := &transferBuckets{ContinuationIO: transferRecordIO(record)}
			if err := dropper.Drop(ctx, b); err != nil {
				return TransferMetric{}, err
			}
			for _, prefix := range b.prefixes {
				visit := func() error { return kv.Visit(prefix, add) }
				if len(shared) > 0 {
					visit = func() error {
						start := kv.key(prefix)
						return shared[0].visitBucket(ctx, start, func(key, value []byte) error { return add(kv.suffix(key), value) })
					}
				}
				if err := visit(); err != nil {
					return TransferMetric{}, err
				}
			}
		} else if s.timeBuckets {
			return TransferMetric{}, ErrFormat
		}
	} else {
		prefix := chunkPrefix(m.ID, m.Generation)
		if len(shared) > 0 {
			if err := shared[0].visit(ctx, &shared[0].chunks, []byte{'c'}, []byte{'d'}, prefix, func(key, value []byte) error { return add(key[len(prefix):], value) }); err != nil {
				return TransferMetric{}, err
			}
		} else {
			it, err := reader.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
			if err != nil {
				return TransferMetric{}, err
			}
			defer it.Close()
			for it.First(); it.Valid(); it.Next() {
				if err := add(it.Key()[len(prefix):], it.Value()); err != nil {
					return TransferMetric{}, err
				}
			}
			if err := it.Error(); err != nil {
				return TransferMetric{}, err
			}
		}
	}
	slices.SortFunc(record.Keys, func(a, b RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	if crc32.ChecksumIEEE([]byte(m.Name))%64 == 0 {
		var state *metricState
		if len(shared) > 0 {
			state = shared[0].state
		}
		snapshot, err := s.transferSnapshotCached(ctx, reader, m, state)
		if err != nil {
			return TransferMetric{}, err
		}
		record.PointDigest, record.Counts = snapshotDigest(snapshot)
	}
	return record, nil
}

func transferDigest(record TransferMetric) string {
	h := sha256.New()
	var size [4]byte
	for _, kv := range record.Keys {
		binary.BigEndian.PutUint32(size[:], uint32(len(kv.Key)))
		_, _ = h.Write(size[:])
		_, _ = h.Write(kv.Key)
		binary.BigEndian.PutUint32(size[:], uint32(len(kv.Value)))
		_, _ = h.Write(size[:])
		_, _ = h.Write(kv.Value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func snapshotDigest(snapshot Snapshot) (string, []int) {
	h := sha256.New()
	counts := make([]int, len(snapshot.Archives))
	var data [16]byte
	for i, a := range snapshot.Archives {
		counts[i] = len(a.Points)
		binary.BigEndian.PutUint64(data[:8], uint64(len(a.Points)))
		_, _ = h.Write(data[:8])
		for _, p := range a.Points {
			binary.BigEndian.PutUint64(data[:8], uint64(p.Timestamp))
			binary.BigEndian.PutUint64(data[8:], math.Float64bits(p.Value))
			_, _ = h.Write(data[:])
		}
	}
	return hex.EncodeToString(h.Sum(nil)), counts
}

func (s *Store) transferSnapshot(ctx context.Context, reader pebble.Reader, m Metadata) (Snapshot, error) {
	return s.transferSnapshotCached(ctx, reader, m, nil)
}
func (s *Store) transferSnapshotCached(ctx context.Context, reader pebble.Reader, m Metadata, state *metricState) (Snapshot, error) {
	if m.Continuation {
		kv := s.newContinuationIO(ctx, reader, nil, m).withIterator()
		kv.state = s.newStateView(state, m)
		defer kv.Close()
		return s.continuation.Snapshot(ctx, kv, m, s.now)
	}
	return snapshotFrom(reader, m.Name)
}

// ImportTransfer reserves disjoint IDs and ingests whole SSTs before catalog
// publication. A target created meanwhile is merged under its writer lock.
// Publication and merge use bounded stripe groups and a final WAL barrier.
func (s *Store) ImportTransfer(ctx context.Context, records []TransferMetric) ([]TransferProof, error) {
	if len(records) == 0 || len(records) > MaxTransferMetrics {
		return nil, errors.New("transfer needs 1..10000 metrics")
	}
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(records))
	for _, r := range records {
		if err := errors.Join(validateName(r.Config.Name), validate(r.Config)); err != nil {
			return nil, err
		}
		if seen[r.Config.Name] || r.Version.Name != r.Config.Name {
			return nil, errors.New("duplicate or mismatched transfer name")
		}
		seen[r.Config.Name] = true
		if r.Continuation != (s.continuation != nil) {
			return nil, ErrFormat
		}
		for i, kv := range r.Keys {
			if r.Continuation && !s.coldPack.format.Load() && sealedTransferKey(kv) {
				return nil, fmt.Errorf("%w: cold blocks require v9 destination", ErrFormat)
			}

			if len(kv.Key) == 0 || len(kv.Key) > 4096 || len(kv.Value) > 16<<20 || (i > 0 && bytes.Compare(r.Keys[i-1].Key, kv.Key) >= 0) {
				return nil, errors.New("invalid transfer keys")
			}
			if !r.Continuation && len(kv.Key) != 6 {
				return nil, ErrFormat
			}
			if isContinuationBucketSuffix(kv.Key) && !s.timeBuckets {
				return nil, ErrFormat
			}
		}
	}
	first, err := s.reserveFileMetricIDs(uint64(len(records)))
	if err != nil {
		return nil, err
	}
	metas := make([]Metadata, len(records))
	for i, r := range records {
		metas[i] = Metadata{MetricConfig: r.Config, ID: first + uint64(i), Generation: 1, Revision: 1, LastUpdate: s.nextActivity(time.Time{}), Continuation: r.Continuation}
		if s.incrementalSource != "" {
			metas[i].FileMigration = FileReclaimed
		}
	}
	paths, err := s.writeTransferTables(ctx, first, records, metas)
	defer func() {
		for _, p := range paths {
			_ = s.fs.Remove(p)
		}
	}()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(paths) > 0 {
		stats, err := s.db.IngestWithStats(paths)
		if err != nil {
			return nil, err
		}
		s.ingestStats.tables.Add(uint64(len(paths)))
		s.ingestStats.entries.Add(uint64(len(records)))
		s.ingestStats.bytes.Add(stats.Bytes)
		s.ingestStats.l0Bytes.Add(stats.ApproxIngestedIntoL0Bytes)
		s.ingestStats.memtableOverlaps.Add(uint64(stats.MemtableOverlappingFiles))
	}
	proofs := make([]TransferProof, 0, len(records))
	// Small groups bound the time foreground writers wait on a stripe and the
	// number of tombstones accumulated by retirement of old target generations.
	for start := 0; start < len(records); start += 64 {
		end := min(start+64, len(records))
		group, err := s.publishTransfer(ctx, records[start:end], metas[start:end])
		if err != nil {
			return nil, err
		}
		proofs = append(proofs, group...)
	}
	// The per-stripe commits already published their receipts and catalogs.
	// One coalesced barrier covers all of them without holding writer stripes
	// across a filesystem sync, regardless of the foreground sync policy.
	if err := s.syncWAL(); err != nil {
		return nil, err
	}
	s.invalidateReadCache()
	return proofs, nil
}

func (s *Store) lockTransfer(names []string, states []*metricState) func() {
	stripes := make([]uint64, 0, len(names))
	for _, name := range names {
		stripes = append(stripes, s.metricStripe(name))
	}
	slices.Sort(stripes)
	stripes = slices.Compact(stripes)
	for _, stripe := range stripes {
		s.metricMu[stripe].Lock()
	}
	// Capture committed target state before mutation invalidates the cache.
	// Writer stripes keep it consistent with the snapshot used by the merge.
	if states != nil && s.stateCache != nil {
		for i, name := range names {
			if s.catalogMembership.mayContain(name) {
				stripe := s.metricStripe(name)
				states[i] = s.stateCache.get(stripe, name, s.stateCache.epoch(stripe))
			}
		}
	}
	for _, name := range names {
		s.beginStateMutation(name)
	}
	return func() {
		for _, name := range names {
			ring := &s.metadataCache[s.metricStripe(name)]
			for i := range ring.entries {
				if ring.entries[i].Name == name {
					ring.entries[i] = Metadata{}
				}
			}
			s.endStateMutation(name)
		}
		for i := len(stripes) - 1; i >= 0; i-- {
			s.metricMu[stripes[i]].Unlock()
		}
	}
}

// ContinuationArchiveRestorer reconstructs independent authoritative archives.
// Used only for overlapping transfers; the usual raw copy needs no codec work.
type ContinuationArchiveRestorer interface {
	RestoreArchives(context.Context, ContinuationIO, Snapshot, func() time.Time) error
}

func (s *Store) publishTransfer(ctx context.Context, records []TransferMetric, metas []Metadata) ([]TransferProof, error) {
	names := make([]string, len(records))
	for i, r := range records {
		names[i] = r.Config.Name
	}
	var states []*metricState
	if s.stateCache != nil {
		states = make([]*metricState, len(records))
	}
	unlock := s.lockTransfer(names, states)
	defer unlock()
	b := s.db.NewIndexedBatch()
	defer b.Close()
	walk := &transferReader{reader: s.db}
	defer walk.close()
	proofs := make([]TransferProof, len(records))
	var receiptEncoder transferProofEncoder
	var sidecars []SidecarRecord
	for i, r := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m := metas[i]
		var state *metricState
		if states != nil {
			state = states[i]
		}
		old, err := Metadata{}, ErrNotFound
		if state != nil {
			old, err = state.meta, nil
		} else if s.catalogMembership.mayContain(m.Name) {
			old, err = walk.metadata(m.Name)
		}
		merged := err == nil
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		p := TransferProof{Source: r.Version, Target: transferVersion(m), Digest: transferDigest(r), Merged: merged, KeyCount: len(r.Keys), Counts: r.Counts, Sampled: r.PointDigest != ""}
		encodedStage, err := s.encodeCatalog(m)
		if err != nil {
			return nil, err
		}
		if err := b.Set(catalogKey(m.Name), encodedStage, nil); err != nil {
			return nil, err
		}
		if err := b.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
			return nil, err
		}
		if r.PointDigest != "" && !r.Continuation {
			snapshot, err := s.transferSnapshot(ctx, b, m)
			if err != nil {
				return nil, err
			}
			digest, counts := snapshotDigest(snapshot)
			if digest != r.PointDigest || !slices.Equal(counts, r.Counts) {
				return nil, fmt.Errorf("source point verification %s failed", m.Name)
			}
		}
		if merged {
			if !samePolicy(old.MetricConfig, m.MetricConfig) || old.Continuation != m.Continuation || old.FileMigration == FileCleanupPending {
				return nil, ErrConflict
			}
			if err := s.checkFileOwnership(old); err != nil {
				return nil, err
			}
			// The staged ID is unpublished. Read its continuation directly; ordinary
			// chunks need only a temporary catalog entry visible inside this batch.
			enc, err := s.encodeCatalog(m)
			if err != nil {
				return nil, err
			}
			if err = b.Set(catalogKey(m.Name), enc, nil); err != nil {
				return nil, err
			}
			if err = b.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
				return nil, err
			}
			var source Snapshot
			if m.Continuation {
				source, err = s.continuation.Snapshot(ctx, transferRecordIO(r), m, s.now)
			} else {
				source, err = s.transferSnapshot(ctx, b, m)
			}
			if err != nil {
				return nil, err
			}
			target, err := s.transferSnapshotCached(ctx, s.db, old, state)
			if err != nil {
				return nil, err
			}
			want := source
			want.Archives = make([]Archive, len(source.Archives))
			for j, a := range source.Archives {
				points, err := fillArchive(a.Points, target.Archives[j].Points, a.Retention.Step)
				if err != nil {
					return nil, err
				}
				want.Archives[j] = Archive{a.Retention, points}
			}
			if m.Continuation {
				restorer, ok := s.continuation.(ContinuationArchiveRestorer)
				if !ok {
					return nil, ErrFormat
				}
				kv := s.newContinuationIO(ctx, b, b, m)
				if err := s.dropContinuation(ctx, s.db, b, m); err != nil {
					return nil, err
				}
				if err := kv.DeletePrefix(nil); err != nil {
					return nil, err
				}
				if err := restorer.RestoreArchives(ctx, kv, want, s.now); err != nil {
					return nil, err
				}
				got, err := s.continuationSnapshot(ctx, b, m)
				if err != nil {
					return nil, err
				}
				if err := EqualSnapshots(want, got); err != nil {
					return nil, fmt.Errorf("merge %s: %w", m.Name, err)
				}
			} else {
				chunks, err := encodeSnapshotChunksWith(m.Retentions, want.Archives, s.encodeChunk)
				if err != nil {
					return nil, err
				}
				for ai, values := range chunks {
					for index, value := range values {
						if err := b.Set(chunkKey(m, ai, index), value, nil); err != nil {
							return nil, err
						}
					}
				}
				got, err := snapshotFrom(b, m.Name)
				if err != nil {
					return nil, err
				}
				if err := EqualSnapshots(want, got); err != nil {
					return nil, fmt.Errorf("merge %s: %w", m.Name, err)
				}
			}
			p.Digest, p.Counts = snapshotDigest(want)
			if err := s.retireTransfer(ctx, b, old); err != nil {
				return nil, err
			}
		}
		var summary SidecarSummary
		if merged {
			summary, err = s.transferSidecarSummary(ctx, b, m)
		} else if m.Continuation {
			io := transferRecordIO(r)
			if value, lookupErr := io.Get(SidecarSummaryKey); lookupErr == nil {
				summary, err = DecodeSidecarSummary(value)
			} else if codec, ok := s.continuation.(ContinuationSidecars); ok {
				summary, err = codec.InspectSidecar(io)
				if errors.Is(err, errTransferReadOnly) {
					// Legacy sidecars need their index persisted before publication.
					summary, err = s.transferSidecarSummary(ctx, b, m)
				}
			}
		}
		if err != nil {
			return nil, err
		}
		sidecars = append(sidecars, SidecarRecord{Name: m.Name, SidecarSummary: summary})
		encoded, err := s.encodeCatalog(m)
		if err != nil {
			return nil, err
		}
		if err = b.Set(catalogKey(m.Name), encoded, nil); err != nil {
			return nil, err
		}
		if err = b.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
			return nil, err
		}
		proofs[i] = p
		receipt, err := receiptEncoder.digest(&proofs[i])
		if err != nil {
			return nil, err
		}
		if err := b.Set(transferReceiptKey(m), receipt[:], nil); err != nil {
			return nil, err
		}
	}
	walk.close()
	// ImportTransfer performs the durability barrier after every group has
	// committed and released its stripes. Failed requests never authorize delete.
	if err := s.commitSidecarRecords(b, sidecars, func(b *pebble.Batch) error { return b.Commit(pebble.NoSync) }); err != nil {
		return nil, err
	}
	s.pendingSync.Store(true)
	for _, m := range metas {
		s.catalogChanged(m.Name)
	}
	return proofs, nil
}

func (s *Store) transferSidecarSummary(ctx context.Context, b *pebble.Batch, m Metadata) (SidecarSummary, error) {
	if !m.Continuation {
		return SidecarSummary{}, nil
	}
	kv := s.newContinuationIO(ctx, b, b, m)
	defer kv.Close()
	value, err := kv.Get(SidecarSummaryKey)
	if err == nil {
		return DecodeSidecarSummary(value)
	}
	if !errors.Is(err, ErrNotFound) {
		return SidecarSummary{}, err
	}
	if codec, ok := s.continuation.(ContinuationSidecars); ok {
		return codec.InspectSidecar(kv)
	}
	return SidecarSummary{}, nil
}

func (s *Store) retireTransfer(ctx context.Context, b *pebble.Batch, m Metadata) error {
	s.canonicalRun(m.ID).Clear(m.ID)
	if err := b.Delete(transferReceiptKey(m), nil); err != nil {
		return err
	}
	if err := b.Delete(revisionKey(m), nil); err != nil {
		return err
	}
	if m.Continuation {
		// The indexed batch observes its own writes, so the continuation IO uses
		// point deletes. Per-metric ranges here would fragment every live read.
		if err := s.dropContinuation(ctx, b, b, m); err != nil {
			return err
		}
		kv := s.newContinuationIO(ctx, b, b, m)
		return kv.DeletePrefix(nil)
	}
	prefix := chunkPrefix(m.ID, 0)[:9]
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
	}
	return it.Error()
}

// Receipts sort below history. Putting them above u/ would make metadata
// SST bounds cover the whole history keyspace and obstruct disjoint ingestion.
func transferReceiptKey(m Metadata) []byte {
	key := chunkPrefix(m.ID, m.Generation)
	key[0] = 'a'
	return key
}

type transferProofEncoder struct {
	buffer  bytes.Buffer
	encoder *json.Encoder
}

func (e *transferProofEncoder) digest(p *TransferProof) ([sha256.Size]byte, error) {
	if e.encoder == nil {
		e.encoder = json.NewEncoder(&e.buffer)
	}
	e.buffer.Reset()
	if err := e.encoder.Encode(p); err != nil {
		return [sha256.Size]byte{}, err
	}
	// Encoder reuses its scratch and adds one newline. Hash the same JSON
	// bytes as the original Marshal receipt so old journals still verify.
	data := e.buffer.Bytes()
	return sha256.Sum256(data[:len(data)-1]), nil
}

// VerifyTransfer checks the durable result of the staged full-byte/point
// verification. Foreground updates may advance the same ID/generation: they
// inherit its history and win overlaps. Replacement/deletion/recreation cannot
// inherit a receipt. The receipt and catalog publication share one synced WAL
// commit, so verification has no quiet-target requirement.
func (s *Store) VerifyTransfer(ctx context.Context, proofs []TransferProof) error {
	view := s.db.NewSnapshot()
	defer view.Close()
	walk := &transferReader{reader: view}
	defer walk.close()
	var receiptEncoder transferProofEncoder
	for i := range proofs {
		p := &proofs[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		m, err := walk.metadata(p.Target.Name)
		if err != nil {
			return err
		}
		if m.ID != p.Target.ID || m.Generation != p.Target.Generation || m.Revision < p.Target.Revision {
			return ErrConflict
		}
		receipt, err := walk.lookup(&walk.receipts, []byte{'a'}, []byte{'b'}, transferReceiptKey(m))
		if err != nil {
			return err
		}
		want, hashErr := receiptEncoder.digest(p)
		equal := bytes.Equal(want[:], receipt)
		if hashErr != nil {
			return hashErr
		}
		if !equal {
			return fmt.Errorf("verify %s receipt differs", m.Name)
		}
	}
	return nil
}

// DeleteTransfer is an atomic, durable conditional delete of a bounded batch.
// A changed source aborts the whole batch; no metric is silently skipped.
func (s *Store) DeleteTransfer(ctx context.Context, versions []TransferVersion) error {
	if len(versions) == 0 || len(versions) > 256 {
		return errors.New("delete needs 1..256 metrics")
	}
	if err := errors.Join(ctx.Err(), s.fileOwnershipError()); err != nil {
		return err
	}
	names := make([]string, len(versions))
	for i, v := range versions {
		names[i] = v.Name
	}
	unlock := s.lockTransfer(names, nil)
	defer unlock()
	ranges := &transferRanges{}
	var sidecars []SidecarRecord
	b := s.db.NewBatch()
	defer b.Close()
	for _, v := range versions {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, err := metadataFrom(s.db, v.Name)
		// A retry after a lost delete response is safe; IDs are never reused.
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if transferVersion(m) != v || m.FileMigration == FileCleanupPending || s.pendingTransfer(m.Name) {
			return ErrConflict
		}
		if err := s.checkFileOwnership(m); err != nil {
			return err
		}
		if err := s.rangeTransferDelete(ctx, b, m, ranges); err != nil {
			return err
		}
		if err := b.Delete(catalogKey(v.Name), nil); err != nil {
			return err
		}
		sidecars = append(sidecars, SidecarRecord{Name: v.Name})
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ranges.apply(b); err != nil {
		return err
	}
	if err := s.commitSidecarRecords(b, sidecars, func(b *pebble.Batch) error { return b.Commit(pebble.Sync) }); err != nil {
		return err
	}
	for _, record := range sidecars {
		s.catalogChanged(record.Name)
	}
	s.invalidateReadCache()
	n := uint64(len(ranges.spans))
	total := s.transferDeleteRanges.Add(n)
	if total/1024 != (total-n)/1024 {
		if _, err := s.db.AsyncFlush(); err != nil {
			return err
		}
	}
	return nil
}

// Transfer wire v1: magic, metric count, then JSON headers and length-delimited
// raw key/value pairs, followed by SHA256. The reader owns one bounded wire
// buffer; keys and values borrow it for the lifetime of the returned batch.
type transferHeader struct {
	Config       MetricConfig
	Version      TransferVersion
	Continuation bool
	Keys         int
	PointDigest  string
	Counts       []int
}

func headerForTransfer(r TransferMetric) transferHeader {
	return transferHeader{r.Config, r.Version, r.Continuation, len(r.Keys), r.PointDigest, r.Counts}
}

func TransferWireSize(records []TransferMetric) (int64, error) {
	n := int64(12 + sha256.Size)
	for _, r := range records {
		header, err := json.Marshal(headerForTransfer(r))
		if err != nil {
			return 0, err
		}
		n += int64(4 + len(header))
		for _, kv := range r.Keys {
			n += int64(8 + len(kv.Key) + len(kv.Value))
		}
	}
	return n, nil
}

func WriteTransfer(w io.Writer, records []TransferMetric) error {
	h := sha256.New()
	out := io.MultiWriter(w, h)
	var size [4]byte
	writeSize := func(n int) error {
		binary.BigEndian.PutUint32(size[:], uint32(n))
		_, err := out.Write(size[:])
		return err
	}
	writeBytes := func(v []byte) error {
		if err := writeSize(len(v)); err != nil {
			return err
		}
		_, err := out.Write(v)
		return err
	}
	if _, err := out.Write([]byte("BKTFR001")); err != nil {
		return err
	}
	if err := writeSize(len(records)); err != nil {
		return err
	}
	for _, r := range records {
		data, err := json.Marshal(headerForTransfer(r))
		if err != nil {
			return err
		}
		if err := writeBytes(data); err != nil {
			return err
		}
		for _, kv := range r.Keys {
			if err := writeBytes(kv.Key); err != nil {
				return err
			}
			if err := writeBytes(kv.Value); err != nil {
				return err
			}
		}
	}
	_, err := w.Write(h.Sum(nil))
	return err
}

func ReadTransfer(r io.Reader, maxBytes int64) ([]TransferMetric, error) {
	size := int64(0)
	if sized, ok := r.(interface{ Len() int }); ok {
		size = int64(sized.Len())
	}
	return ReadTransferSize(r, maxBytes, size)
}

// A known HTTP content length avoids growing/copying the batch buffer. Unknown
// lengths retain the bounded reader and all paths require an exact final EOF.
func ReadTransferSize(r io.Reader, maxBytes, size int64) ([]TransferMetric, error) {
	var buffer TransferBuffer
	return buffer.Read(r, maxBytes, size)
}

// TransferBuffer reuses the wire storage between batches. Returned keys and
// values borrow it until the next Read; callers must finish importing first.
// A buffer belongs to one reader at a time.
type TransferBuffer struct{ data []byte }

func (b *TransferBuffer) Read(r io.Reader, maxBytes, size int64) ([]TransferMetric, error) {
	if maxBytes < 44 || maxBytes == math.MaxInt64 || size > maxBytes {
		return nil, ErrTransferLimit
	}
	var data []byte
	var err error
	if size > 0 {
		if int64(cap(b.data)) < size {
			b.data = make([]byte, size)
		}
		data = b.data[:size]
		_, err = io.ReadFull(r, data)
		if err == nil {
			var extra [1]byte
			if n, end := io.ReadFull(r, extra[:]); n != 0 || end != io.EOF {
				err = ErrFormat
			}
		}
	} else {
		data, err = io.ReadAll(io.LimitReader(r, maxBytes+1))
		b.data = data
	}
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, ErrTransferLimit
	}
	if len(data) < 44 || string(data[:8]) != "BKTFR001" && string(data[:8]) != "BKTFR002" {
		return nil, ErrFormat
	}
	body, actual := data[:len(data)-sha256.Size], data[len(data)-sha256.Size:]
	expected := sha256.Sum256(body)
	if !bytes.Equal(expected[:], actual) {
		return nil, errors.New("transfer checksum mismatch")
	}
	count := binary.BigEndian.Uint32(body[8:12])
	if count == 0 || count > MaxTransferMetrics {
		return nil, errors.New("invalid metric count")
	}
	body = body[12:]
	if string(data[:8]) == "BKTFR002" {
		return readTransfer2(body, count)
	}
	readBytes := func(capacity uint32) ([]byte, error) {
		if len(body) < 4 {
			return nil, io.ErrUnexpectedEOF
		}
		n := binary.BigEndian.Uint32(body[:4])
		body = body[4:]
		if n > capacity || uint64(n) > uint64(len(body)) {
			return nil, ErrTransferLimit
		}
		v := body[:n:n]
		body = body[n:]
		return v, nil
	}
	records := make([]TransferMetric, count)
	for i := range records {
		data, err := readBytes(65536)
		if err != nil {
			return nil, err
		}
		var head transferHeader
		if err := json.Unmarshal(data, &head); err != nil {
			return nil, err
		}
		if head.Keys < 0 || int64(head.Keys)*8 > int64(len(body)) {
			return nil, errors.New("invalid key count")
		}
		record := TransferMetric{Config: head.Config, Version: head.Version, Continuation: head.Continuation, PointDigest: head.PointDigest, Counts: head.Counts, Keys: make([]RelayoutKV, head.Keys)}
		for j := range record.Keys {
			k, err := readBytes(4096)
			if err != nil {
				return nil, err
			}
			v, err := readBytes(16 << 20)
			if err != nil {
				return nil, err
			}
			record.Keys[j] = RelayoutKV{k, v}
		}
		records[i] = record
	}
	if len(body) != 0 {
		return nil, errors.New("trailing transfer bytes")
	}
	return records, nil
}

func transferDeleteKey(m Metadata) []byte {
	key := chunkPrefix(m.ID, m.Generation)
	key[0] = 'd'
	return key
}

// StageTransferDelete records a verified source version without creating range
// tombstones while other streams are still reading history. Retired-node moves
// then delete the complete store in a single conditional range transaction.
func (s *Store) StageTransferDelete(ctx context.Context, operation string, versions []TransferVersion) error {
	scope, err := transferScope(operation)
	if err != nil {
		return err
	}
	if len(versions) == 0 || len(versions) > MaxTransferMetrics {
		return errors.New("delete staging needs 1..10000 metrics")
	}
	// These are candidates, not deletes. Checking them against live metadata
	// here adds random reads and writer locks without making the final check
	// safer: a source can change immediately afterwards. Retirement alone
	// validates every current catalog member under all writer stripes.
	b := s.db.NewBatch()
	defer b.Close()
	value := make([]byte, 40)
	copy(value, scope)
	for _, v := range versions {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validateName(v.Name); err != nil {
			return err
		}
		if v.ID == 0 || v.Generation == 0 || v.Revision == 0 {
			return errors.New("invalid source version")
		}
		m := Metadata{ID: v.ID, Generation: v.Generation}
		binary.BigEndian.PutUint64(value[32:], v.Revision)
		if err := b.Set(transferDeleteKey(m), value, nil); err != nil {
			return err
		}
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return err
	}
	return s.syncWAL()
}

func transferScope(operation string) ([]byte, error) {
	scope, err := hex.DecodeString(operation)
	if err != nil || len(scope) != 32 {
		return nil, errors.New("invalid transfer operation")
	}
	return scope, nil
}

func (s *Store) PendingTransferDelete(ctx context.Context, operation string, limit int) ([]string, error) {
	scope, err := transferScope(operation)
	if err != nil {
		return nil, err
	}
	if limit < 1 || limit > 256 {
		return nil, errors.New("pending limit must be 1..256")
	}
	view := s.db.NewSnapshot()
	defer view.Close()
	walk := &transferReader{reader: view}
	defer walk.close()
	it, err := view.NewIter(&pebble.IterOptions{LowerBound: []byte("m/"), UpperBound: []byte("m0")})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	result := make([]string, 0, limit)
	for it.First(); it.Valid() && len(result) < limit; it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m, err := decodeCatalog(string(it.Key()[2:]), it.Value())
		if err == nil {
			m, err = walk.revision(m)
		}
		if err != nil {
			return nil, err
		}
		staged, err := walk.deleteStaged(m, scope)
		if err != nil {
			return nil, err
		}
		if !staged || s.pendingTransfer(m.Name) {
			result = append(result, m.Name)
		}
	}
	return result, it.Error()
}

// RetireTransferredStore is deliberately for an entirely drained physical node.
// It blocks writers only for the final catalog/version check. Every catalog
// member must have a durable verification marker for its current version;
// unknown metrics and late writes abort the whole delete. One range tombstone
// avoids the quadratic iterator fragmentation of millions of per-ID tombstones.
func (s *Store) RetireTransferredStore(ctx context.Context, operation string) (int64, error) {
	scope, err := transferScope(operation)
	if err != nil {
		return 0, err
	}
	for i := range s.metricMu {
		s.metricMu[i].Lock()
	}
	defer func() {
		for i := len(s.metricMu) - 1; i >= 0; i-- {
			s.metricMu[i].Unlock()
		}
	}()
	if err := s.fileOwnershipError(); err != nil {
		return 0, err
	}
	if s.transferPendingAny != nil && s.transferPendingAny() {
		return 0, fmt.Errorf("source write cache is not empty: %w", ErrConflict)
	}
	walk := &transferReader{reader: s.db}
	defer walk.close()
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("m/"), UpperBound: []byte("m0")})
	if err != nil {
		return 0, err
	}
	var count int64
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			it.Close()
			return 0, err
		}
		m, err := decodeCatalog(string(it.Key()[2:]), it.Value())
		if err == nil {
			m, err = walk.revision(m)
		}
		if err != nil {
			it.Close()
			return 0, err
		}
		staged, err := walk.deleteStaged(m, scope)
		if err != nil {
			it.Close()
			return 0, err
		}
		if !staged || m.FileMigration == FileCleanupPending || s.pendingTransfer(m.Name) {
			it.Close()
			return 0, fmt.Errorf("unstaged or changed source %s: %w", m.Name, ErrConflict)
		}
		if err := s.checkFileOwnership(m); err != nil {
			it.Close()
			return 0, err
		}
		count++
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}
	b := s.db.NewBatch()
	defer b.Close()
	// Receipts (a), data (c,t,u), transfer marks (d), catalog (m) and revisions (r).
	// Preserve z/sequence: even an emptied store never reuses an identity.
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := b.DeleteRange([]byte{'a'}, []byte{'b'}, nil); err != nil {
		return 0, err
	}
	if err := b.DeleteRange([]byte{'c'}, []byte{'y'}, nil); err != nil {
		return 0, err
	}
	if err := b.Set(sidecarReadyKey, []byte{1}, nil); err != nil {
		return 0, err
	}
	if c := s.stateCache; c != nil {
		for i := range c.shards {
			c.shards[i].mu.Lock()
		}
		defer func() {
			for i := len(c.shards) - 1; i >= 0; i-- {
				c.shards[i].mu.Unlock()
			}
		}()
	}
	s.sidecars.mu.Lock()
	defer s.sidecars.mu.Unlock()
	if err := b.Commit(pebble.Sync); err != nil {
		return 0, err
	}
	s.sidecars.entries, s.sidecars.oldest, s.sidecars.points = nil, nil, 0
	s.sidecars.ready = true
	s.resetCatalogChanges()
	for i := range s.metadataCache {
		s.metadataCache[i] = metadataRing{}
	}
	// Eviction under every stripe excludes all state cache readers/writers.
	if c := s.stateCache; c != nil {
		for i := range c.shards {
			sh := &c.shards[i]
			for sh.lruTail >= 0 {
				slot := sh.lruTail
				c.remove(sh, sh.position(slot), slot)
				c.entries.Add(-1)
			}
			sh.epoch.Add(1)
		}
	}
	s.invalidateReadCache()
	return count, nil
}

func (s *Store) TransferL0StopWrites() int64 {
	if s.pebbleOptions.L0StopWritesThreshold > 0 {
		return int64(s.pebbleOptions.L0StopWritesThreshold)
	}
	return 12
}

// SetTransferPending installs the app write-cache fence before admin handlers
// start. A durable store version cannot authorize dropping its queued points.
func (s *Store) SetTransferPending(pending func(string) bool, anyPending ...func() bool) {
	s.transferPending = pending
	if len(anyPending) > 0 {
		s.transferPendingAny = anyPending[0]
	}
}
func (s *Store) pendingTransfer(name string) bool {
	return s.transferPending != nil && s.transferPending(name)
}
