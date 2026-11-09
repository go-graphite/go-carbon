package chunkstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

// The original donor SSTs remain pinned by this durable record until every
// ordinal is decided. A restart never needs a newer donor snapshot to resume a
// partially published range. Pieces and catalogs commit atomically.
type nativeCombineIntent struct {
	Request     NativeRange
	Receipt     NativeReceipt
	Raw         []nativeCombineMetadata
	Positions   []int
	Origins     []nativeCombineOrigin
	Spans       []nativeSpan
	Directories []string
	Ready       bool
}

type nativeCombineOrigin struct{ ID, Generation, Revision uint64 }

type nativeCombinePiece struct {
	Position int
	Meta     nativeCombineMetadata
	Old      nativeCombineMetadata
	Class    NativeCombineClass
	Deferred bool
}

type nativeCombinePrepared struct {
	nativeCombinePiece
	Summary SidecarSummary
}

func nativeCombineIntentKey(identity string) []byte { return []byte("COMBINE/intent/" + identity) }
func nativeCombinePiecePrefix(identity string) []byte {
	return []byte("COMBINE/piece/" + identity + "/")
}
func nativeCombinePieceKey(identity string, position int) []byte {
	return fmt.Appendf(nativeCombinePiecePrefix(identity), "%010d", position)
}

// ResumeNativeCombine needs no donor connection once the original checksum-
// verified input is durable. In particular, expiry of the source pin cannot
// prevent recovery after a survivor restart or a partially lost response.
func (s *Store) ResumeNativeCombine(ctx context.Context, request NativeRange) (NativeReceipt, bool, error) {
	if !request.Combine {
		return NativeReceipt{}, false, nil
	}
	identity, _, err := request.Identity()
	if err != nil {
		return NativeReceipt{}, false, err
	}
	intent, err := s.readNativeCombineIntent(identity)
	if errors.Is(err, pebble.ErrNotFound) {
		return NativeReceipt{}, false, nil
	}
	if err != nil || !intent.Ready {
		return NativeReceipt{}, false, err
	}
	receipt, err := s.nativeCombineImport(ctx, request, nil)
	return receipt, true, err
}

func (s *Store) nativeCombineImport(ctx context.Context, request NativeRange, input io.Reader) (receipt NativeReceipt, err error) {
	identity, namesHash, err := request.Identity()
	if err != nil {
		return receipt, err
	}
	lockAny, _ := s.nativeRanges.LoadOrStore(identity, new(sync.Mutex))
	lock := lockAny.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	if failure := s.nativeFailure.Load(); failure != nil {
		return receipt, failure.err
	}
	if receipt, err = s.readNativeReceipt(ctx, request); err == nil {
		return receipt, nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return receipt, err
	}
	defer func() {
		if err != nil {
			s.nativeCounters.errors.Add(1)
		}
	}()
	if err := errors.Join(ctx.Err(), s.checkNativeReady(), s.fileOwnershipError(), s.nativeDiskCheck()); err != nil {
		return receipt, err
	}
	intent, err := s.readNativeCombineIntent(identity)
	if errors.Is(err, pebble.ErrNotFound) || err == nil && !intent.Ready {
		if err == nil {
			if err := s.discardNativeCombine(identity, intent); err != nil {
				return receipt, err
			}
		}
		intent = nativeCombineIntent{Request: request, Receipt: NativeReceipt{Identity: identity, NamesSHA256: namesHash, Count: len(request.Names), Copied: make([]byte, (len(request.Names)+7)/8)}}
		if err := s.stageNativeCombine(ctx, &intent, input); err != nil {
			return receipt, err
		}
	} else if err != nil {
		return receipt, err
	}
	if id, hash, e := intent.Request.Identity(); e != nil || id != identity || hash != namesHash {
		return receipt, ErrFormat
	}
	if err := s.nativeStep("combine-prepared"); err != nil {
		return receipt, err
	}
	pieces, err := s.readNativeCombinePieces(intent)
	if err != nil {
		return receipt, err
	}
	for {
		pending := make([]int, 0, len(intent.Raw))
		for i, m := range intent.Raw {
			if m.ID != 0 && pieces[intent.Positions[i]] == nil {
				pending = append(pending, i)
			}
		}
		if len(pending) == 0 {
			break
		}
		if err := errors.Join(ctx.Err(), s.nativeDiskCheck()); err != nil {
			return receipt, err
		}
		prepared, err := s.prepareNativeCombine(ctx, &intent, pending)
		if err != nil {
			return receipt, err
		}
		if err := s.nativeStep("combine-before-publish"); err != nil {
			return receipt, err
		}
		for _, p := range prepared {
			published, err := s.publishNativeCombineName(ctx, identity, &p)
			if err != nil {
				return receipt, err
			}
			if published {
				piece := p.nativeCombinePiece
				pieces[p.Position] = &piece
				if !p.Deferred {
					s.catalogChanged(p.Meta.Name)
					s.invalidateReadCache()
				}
				s.nativeCounters.combineCopiedBlocks.Add(uint64(p.Class.CopiedBlocks))
				s.nativeCounters.combinePointBlocks.Add(uint64(p.Class.CombinedBlocks))
				s.nativeCounters.combineMaterialized.Add(uint64(p.Class.MaterializedArchives))
				if p.Old.ID != 0 {
					if p.Class.IntersectingArchives > 0 {
						s.nativeCounters.combineIntersectingNames.Add(1)
					} else {
						s.nativeCounters.combineDisjointNames.Add(1)
					}
					if p.Class.NormalizedArchives > 0 {
						s.nativeCounters.combineNormalizedNames.Add(1)
					}
				}
				if err := s.nativeStep("combine-name-published"); err != nil {
					return receipt, err
				}
			} else {
				s.nativeCounters.combineRetries.Add(1)
			}
		}
	}
	// Neither history retirement nor the durability barrier holds a writer
	// stripe. Published IDs are excluded from all hidden-ID cleanup ranges.
	b := s.db.NewIndexedBatch()
	defer b.Close()
	for _, piece := range pieces {
		if piece == nil || piece.Old.ID == 0 {
			continue
		}
		if err := s.retireTransfer(ctx, b, Metadata(piece.Old)); err != nil {
			return receipt, err
		}
	}
	if err := nativeCombineCleanup(b, intent, pieces); err != nil {
		return receipt, err
	}
	receipt = intent.Receipt
	origins := make([]nativeCombineOrigin, receipt.Count)
	for i, position := range intent.Positions {
		origins[position] = intent.Origins[i]
	}
	for i, piece := range pieces {
		if piece != nil && !piece.Deferred {
			receipt.Copied[i/8] |= 1 << uint(i%8)
			receipt.CopiedCount++
			origin := origins[i]
			receipt.Mappings = append(receipt.Mappings, NativeMapping{Ordinal: i, SourceID: origin.ID, SourceGeneration: origin.Generation, SourceRevision: origin.Revision, TargetID: piece.Meta.ID, TargetGeneration: piece.Meta.Generation, TargetRevision: piece.Meta.Revision})
		}
	}
	receipt.DeferredCount = receipt.Count - receipt.CopiedCount
	data, err := json.Marshal(receipt)
	if err != nil {
		return receipt, err
	}
	if err := b.Set(nativeReceiptKey(identity), data, nil); err != nil {
		return receipt, err
	}
	if err := b.Delete(nativeCombineIntentKey(identity), nil); err != nil {
		return receipt, err
	}
	prefix := nativeCombinePiecePrefix(identity)
	if err := b.DeleteRange(prefix, prefixEnd(prefix), nil); err != nil {
		return receipt, err
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		s.nativeWriteFailed(err)
		return receipt, err
	}
	if err := s.nativeStep("combine-receipt"); err != nil {
		s.nativeWriteFailed(err)
		return receipt, err
	}
	if err := s.syncWAL(); err != nil {
		s.nativeWriteFailed(err)
		return receipt, err
	}
	s.invalidateReadCache()
	s.recordNativeReceipt(receipt)
	return receipt, s.nativeStep("combine-published")
}

func (s *Store) readNativeCombineIntent(identity string) (nativeCombineIntent, error) {
	value, closer, err := s.db.Get(nativeCombineIntentKey(identity))
	if err != nil {
		return nativeCombineIntent{}, err
	}
	defer closer.Close()
	var intent nativeCombineIntent
	if err := json.Unmarshal(value, &intent); err != nil {
		return intent, err
	}
	return intent, validateNativeCombineIntent(identity, intent)
}

func validateNativeCombineIntent(identity string, intent nativeCombineIntent) error {
	id, names, err := intent.Request.Identity()
	if err != nil || id != identity || intent.Receipt.Identity != identity || names != intent.Receipt.NamesSHA256 || intent.Receipt.Count != len(intent.Request.Names) || len(intent.Raw) != len(intent.Positions) || len(intent.Raw) != len(intent.Origins) {
		return ErrFormat
	}
	if intent.Ready {
		seen := make([]bool, intent.Receipt.Count)
		for i, raw := range intent.Raw {
			p := intent.Positions[i]
			if p < 0 || p >= len(seen) || seen[p] {
				return ErrFormat
			}
			seen[p] = true
			if raw.ID != 0 && (raw.Name != intent.Request.Names[p] || raw.Generation == 0 || raw.Revision == 0 || intent.Origins[i].ID == 0 || intent.Origins[i].Generation == 0 || intent.Origins[i].Revision == 0) {
				return ErrFormat
			}
		}
	}
	return nil
}

func (s *Store) saveNativeCombineIntent(intent nativeCombineIntent) error {
	data, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	err = s.db.Set(nativeCombineIntentKey(intent.Receipt.Identity), data, pebble.Sync)
	s.nativeWriteFailed(err)
	return err
}

func (s *Store) reserveNativeCombineIDs(intent *nativeCombineIntent, count int) (uint64, error) {
	began := time.Now()
	defer func() { s.nativeCounters.reserve.Add(uint64(time.Since(began))) }()
	s.mu.Lock()
	defer s.mu.Unlock()
	high, err := s.CatalogHighWatermark(context.Background())
	if err != nil {
		return 0, err
	}
	if count < 1 || high > math.MaxUint64-uint64(count) {
		return 0, ErrFormat
	}
	first := high + 1
	intent.Receipt.ReservedIDs += count
	if intent.Receipt.FirstID == 0 {
		intent.Receipt.FirstID = first
	}
	intent.Directories = append(intent.Directories, s.fs.PathJoin(s.dir, nativeDirectory(intent.Receipt.Identity, first)))
	data, err := json.Marshal(intent)
	if err != nil {
		return 0, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(sequenceKey(), uint64Bytes(high+uint64(count)), nil); err != nil {
		return 0, err
	}
	if err := b.Set(nativeCombineIntentKey(intent.Receipt.Identity), data, nil); err != nil {
		return 0, err
	}
	err = b.Commit(pebble.Sync)
	s.nativeWriteFailed(err)
	return first, err
}

func (s *Store) stageNativeCombine(ctx context.Context, intent *nativeCombineIntent, input io.Reader) (err error) {
	stream, err := NewTransferStream(input)
	if err != nil {
		return err
	}
	request := intent.Request
	expected := len(request.Names)
	if request.Candidates != nil {
		if len(request.Candidates) != (expected+7)/8 {
			return ErrFormat
		}
		expected = 0
		for i := range request.Names {
			if nativeBit(request.Candidates, i) {
				expected++
			}
		}
	}
	if stream.Count != expected {
		return errors.New("native combine stream differs from plan range")
	}
	intent.Raw, intent.Positions, intent.Origins = make([]nativeCombineMetadata, expected), make([]int, expected), make([]nativeCombineOrigin, expected)
	if expected == 0 {
		intent.Receipt.Digest, intent.Receipt.Bytes, err = stream.Finish()
		if err != nil {
			return err
		}
		intent.Ready = true
		return s.saveNativeCombineIntent(*intent)
	}
	first, err := s.reserveNativeCombineIDs(intent, expected)
	if err != nil {
		return err
	}
	if err := s.nativeStep("combine-reserved"); err != nil {
		return err
	}
	buildBegan := time.Now()
	tables, err := s.newNativeTables(intent.Receipt.Identity, first)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, tables.close()); _ = s.fs.RemoveAll(tables.directory) }()
	positions := make(map[string]int, len(request.Names))
	for i, name := range request.Names {
		positions[name] = i
	}
	seen := make([]bool, len(request.Names))
	floatCodec := false
	for i := range expected {
		record, err := stream.Next()
		if err != nil {
			return err
		}
		position, ok := positions[record.Config.Name]
		if !ok || seen[position] || record.Version.Name != record.Config.Name || request.Candidates != nil && !nativeBit(request.Candidates, position) {
			return ErrFormat
		}
		seen[position], intent.Positions[i] = true, position
		if record.Version.ID == 0 && record.Version.Generation == 0 && record.Version.Revision == 0 && len(record.Keys) == 0 && len(record.Config.Retentions) == 0 {
			continue
		}
		if err := s.validateNativeCombineRecord(record); err != nil {
			return err
		}
		m := Metadata{MetricConfig: record.Config, ID: first + uint64(i), Generation: 1, Revision: 1, LastUpdate: s.nextActivity(time.Time{}), Continuation: record.Continuation}
		if s.incrementalSource != "" {
			m.FileMigration = FileReclaimed
		}
		intent.Raw[i] = nativeCombineMetadata(m)
		intent.Origins[i] = nativeCombineOrigin{record.Version.ID, record.Version.Generation, record.Version.Revision}
		for _, kv := range record.Keys {
			floatCodec = floatCodec || record.Continuation && newFloatGroup(kv.Key, kv.Value)
			if err := tables.add(ctx, m, kv); err != nil {
				return err
			}
		}
	}
	intent.Receipt.Digest, intent.Receipt.Bytes, err = stream.Finish()
	if err != nil {
		return err
	}
	if err := s.ingestNativeCombineTables(intent, tables, floatCodec, buildBegan); err != nil {
		return err
	}
	if err := s.nativeStep("combine-ingested"); err != nil {
		return err
	}
	intent.Ready = true
	return s.saveNativeCombineIntent(*intent)
}

func (s *Store) validateNativeCombineRecord(r TransferMetric) error {
	if r.Version.ID == 0 || r.Version.Generation == 0 || r.Version.Revision == 0 || r.Version.Name != r.Config.Name {
		return ErrFormat
	}
	if err := validate(r.Config); err != nil {
		return err
	}
	if r.Continuation != (s.continuation != nil) {
		return ErrFormat
	}
	for i, kv := range r.Keys {
		if len(kv.Key) == 0 || len(kv.Key) > 4096 || len(kv.Value) > 16<<20 || i > 0 && bytes.Compare(r.Keys[i-1].Key, kv.Key) >= 0 {
			return ErrFormat
		}
		if !r.Continuation && len(kv.Key) != 6 || r.Continuation && newFloatGroup(kv.Key, kv.Value) && s.AcceptTransferFormat() < 11 || r.Continuation && sealedTransferKey(kv) && !s.coldPack.format.Load() || isContinuationBucketSuffix(kv.Key) && !s.timeBuckets {
			return ErrFormat
		}
	}
	return nil
}

func (s *Store) ingestNativeCombineTables(intent *nativeCombineIntent, tables *nativeTables, floatCodec bool, buildBegan time.Time) error {
	if err := tables.close(); err != nil {
		s.nativeWriteFailed(err)
		return err
	}
	s.nativeCounters.build.Add(uint64(time.Since(buildBegan)))
	intent.Spans = append(intent.Spans, tables.spans()...)
	if err := s.saveNativeCombineIntent(*intent); err != nil {
		return err
	}
	if err := s.nativeStep("combine-staged"); err != nil {
		return err
	}
	if err := s.nativeDiskCheck(); err != nil {
		return err
	}
	if floatCodec {
		if err := s.fenceFloatCodec(); err != nil {
			return err
		}
	}
	if len(tables.paths) != 0 {
		began := time.Now()
		stats, err := s.db.IngestWithStats(tables.paths)
		s.nativeCounters.ingest.Add(uint64(time.Since(began)))
		if err != nil {
			s.nativeWriteFailed(err)
			return err
		}
		s.nativeCounters.sst.Add(stats.Bytes)
	}
	receipts, err := tables.receipts()
	if err != nil {
		return err
	}
	intent.Receipt.Tables = append(intent.Receipt.Tables, receipts...)
	return nil
}

func (s *Store) prepareNativeCombine(ctx context.Context, intent *nativeCombineIntent, pending []int) (prepared []nativeCombinePrepared, err error) {
	first, err := s.reserveNativeCombineIDs(intent, len(pending))
	if err != nil {
		return nil, err
	}
	buildBegan := time.Now()
	tables, err := s.newNativeTables(intent.Receipt.Identity, first)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, tables.close()); _ = s.fs.RemoveAll(tables.directory) }()
	view := s.db.NewSnapshot()
	defer view.Close()
	sourceWalk := &transferReader{reader: view, native: true, ordered: true}
	targetWalk := &transferReader{reader: view, native: true}
	defer sourceWalk.close()
	defer targetWalk.close()
	floatCodec := false
	for j, i := range pending {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sourceWalk.arena.free = sourceWalk.arena.slab
		targetWalk.arena.free = targetWalk.arena.slab
		raw := Metadata(intent.Raw[i])
		source, err := s.transferFromLimit(ctx, view, raw, math.MaxInt64, sourceWalk)
		if err != nil {
			return nil, err
		}
		old, err := targetWalk.metadata(raw.Name)
		p := nativeCombinePrepared{nativeCombinePiece: nativeCombinePiece{Position: intent.Positions[i], Meta: nativeCombineMetadata(raw)}}
		if err == nil {
			if err := nativeCompatibleTarget(s, old, source); err != nil {
				return nil, err
			}
			target, err := s.transferFromLimit(ctx, view, old, math.MaxInt64, targetWalk)
			if err != nil {
				return nil, err
			}
			result, class, err := s.nativeCombineRecord(ctx, source, target)
			if err != nil {
				return nil, err
			}
			p.Old, p.Class = nativeCombineMetadata(old), class
			p.Meta.ID = first + uint64(j)
			p.Meta.LastUpdate = s.nextActivity(old.LastUpdate)
			for _, kv := range result.Keys {
				floatCodec = floatCodec || result.Continuation && newFloatGroup(kv.Key, kv.Value)
				if err := tables.add(ctx, Metadata(p.Meta), kv); err != nil {
					return nil, err
				}
			}
			source = result
		} else if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if source.Continuation {
			kv := transferRecordIO(source)
			if value, e := kv.Get(SidecarSummaryKey); e == nil {
				p.Summary, err = DecodeSidecarSummary(value)
			} else if inspector, ok := s.continuation.(ContinuationSidecars); ok {
				p.Summary, err = inspector.InspectSidecar(kv)
			} else if !errors.Is(e, ErrNotFound) {
				err = e
			}
			if err != nil {
				return nil, err
			}
		}
		prepared = append(prepared, p)
	}
	if err := s.ingestNativeCombineTables(intent, tables, floatCodec, buildBegan); err != nil {
		return nil, err
	}
	return prepared, nil
}

func (s *Store) publishNativeCombineName(ctx context.Context, identity string, p *nativeCombinePrepared) (bool, error) {
	encoded, err := s.encodeCatalog(Metadata(p.Meta))
	if err != nil {
		return false, err
	}
	data, err := json.Marshal(p.nativeCombinePiece)
	if err != nil {
		return false, err
	}
	wait := time.Now()
	unlock := s.lockTransfer([]string{p.Meta.Name}, nil)
	locked := time.Now()
	s.nativeCounters.combineWait.Add(uint64(locked.Sub(wait)))
	defer func() { unlock(); s.nativeCounters.recordCombineHold(time.Since(locked)) }()
	// Create and phase1 publication share this lock. Keep their create-if-
	// absent boundary intact when an absent candidate becomes present.
	if p.Old.ID == 0 {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	old, err := metadataFrom(s.db, p.Meta.Name)
	switch {
	case errors.Is(err, ErrNotFound):
		if p.Old.ID != 0 {
			return false, nil
		}
	case err != nil:
		return false, err
	case transferVersion(old) != transferVersion(Metadata(p.Old)) || !samePolicy(old.MetricConfig, p.Old.MetricConfig) || old.Continuation != p.Old.Continuation || old.FileMigration != p.Old.FileMigration:
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	// A checkpoint/cache-only target is not an absent history. Preserve the
	// phase1 creation fence, under the same creation lock, until its saved
	// points have reached the catalog. The existing journal imports this
	// exceptional absent name; present names still combine without a cache gate.
	if p.Old.ID == 0 && s.pendingTransfer(p.Meta.Name) {
		p.Deferred = true
		data, err = json.Marshal(p.nativeCombinePiece)
		if err != nil {
			return false, err
		}
		if err := s.db.Set(nativeCombinePieceKey(identity, p.Position), data, pebble.NoSync); err != nil {
			s.nativeWriteFailed(err)
			return false, err
		}
		s.pendingSync.Store(true)
		return true, nil
	}
	b := s.db.NewBatch()
	defer b.Close()
	for _, entry := range []struct{ key, value []byte }{{catalogKey(p.Meta.Name), encoded}, {revisionKey(Metadata(p.Meta)), revisionBytes(Metadata(p.Meta))}} {
		if err := b.Set(entry.key, entry.value, nil); err != nil {
			return false, err
		}
	}
	if err := b.Set(nativeCombinePieceKey(identity, p.Position), data, nil); err != nil {
		return false, err
	}
	if err := s.commitSidecarRecords(b, []SidecarRecord{{Name: p.Meta.Name, SidecarSummary: p.Summary}}, func(b *pebble.Batch) error { return b.Commit(pebble.NoSync) }); err != nil {
		s.nativeWriteFailed(err)
		return false, err
	}
	s.pendingSync.Store(true)
	return true, nil
}

func (s *Store) readNativeCombinePieces(intent nativeCombineIntent) ([]*nativeCombinePiece, error) {
	pieces := make([]*nativeCombinePiece, intent.Receipt.Count)
	prefix := nativeCombinePiecePrefix(intent.Receipt.Identity)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		var p nativeCombinePiece
		if err := json.Unmarshal(it.Value(), &p); err != nil {
			return nil, err
		}
		if p.Position < 0 || p.Position >= len(pieces) || p.Meta.ID == 0 || p.Meta.Name != intent.Request.Names[p.Position] || p.Deferred && p.Old.ID != 0 || !bytes.Equal(it.Key(), nativeCombinePieceKey(intent.Receipt.Identity, p.Position)) {
			return nil, ErrFormat
		}
		pieces[p.Position] = &p
	}
	return pieces, it.Error()
}

// Cut the published IDs out of each staged namespace. A piece remains valid
// after later foreground writes or another merge changes its catalog again.
func nativeCombineCleanup(b *pebble.Batch, intent nativeCombineIntent, pieces []*nativeCombinePiece) error {
	for _, span := range intent.Spans {
		if len(span.Lower) < 17 || bytes.Compare(span.Lower, span.Upper) >= 0 {
			return ErrFormat
		}
		var keep [][]byte
		for _, p := range pieces {
			if p == nil || p.Deferred {
				continue
			}
			prefix := chunkPrefix(p.Meta.ID, p.Meta.Generation)
			if span.Lower[0] == continuationBucketKeyPrefix {
				prefix[0] = continuationPrefix(p.Meta.ID, p.Meta.Generation)[0]
				prefix = append(bytes.Clone(span.Lower[:5]), prefix...)
			} else {
				prefix[0] = span.Lower[0]
			}
			if bytes.Compare(prefixEnd(prefix), span.Lower) > 0 && bytes.Compare(prefix, span.Upper) < 0 {
				keep = append(keep, prefix)
			}
		}
		slices.SortFunc(keep, bytes.Compare)
		lower := span.Lower
		for _, prefix := range keep {
			if bytes.Compare(lower, prefix) < 0 {
				if err := b.DeleteRange(lower, prefix, nil); err != nil {
					return err
				}
			}
			lower = prefixEnd(prefix)
		}
		if bytes.Compare(lower, span.Upper) < 0 {
			if err := b.DeleteRange(lower, span.Upper, nil); err != nil {
				return err
			}
		}
	}
	return nil
}
