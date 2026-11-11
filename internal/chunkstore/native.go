package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// NativeRange retains the existing controller's operation, stream and ordinals.
// Source identifies the physical donor, not its ephemeral pinned-view session.
type NativeRange struct {
	Operation, Plan, Stream, Source string
	Start, End                      int64
	Names                           []string
	Candidates                      []byte `json:",omitempty"`
	Combine                         bool   `json:",omitempty"`
	// Selection is an immutable phase-2 ordinal bitmap. Candidates is the
	// receiver's transient export filter and deliberately does not identify work.
	DeferredFrom string `json:",omitempty"`
	Selection    []byte `json:",omitempty"`
}

type NativeReceipt struct {
	Identity                          string
	NamesSHA256                       string
	Count, CopiedCount, DeferredCount int
	Copied                            []byte // plan ordinal bitmap; absent bits remain for reviewed combine
	Digest                            string
	Bytes                             int64
	FirstID                           uint64
	ReservedIDs                       int
	Tables                            []NativeSSTReceipt `json:",omitempty"`
	Mappings                          []NativeMapping    `json:",omitempty"`
	Skipped                           []NativeSkip       `json:",omitempty"`
}

// Ordinal plus the immutable stream hash resolves the source name. Keeping
// numeric identities avoids repeating long metric names in durable receipts.
type NativeMapping struct {
	Ordinal                                    int
	SourceID, SourceGeneration, SourceRevision uint64
	TargetID, TargetGeneration, TargetRevision uint64
}

func (r NativeReceipt) IsCopied(index int) bool {
	return index >= 0 && index < r.Count && index/8 < len(r.Copied) && r.Copied[index/8]&(1<<uint(index%8)) != 0
}

func (r NativeRange) Identity() (string, string, error) {
	for _, value := range []string{r.Operation, r.Plan, r.Stream} {
		if len(value) != 64 {
			return "", "", ErrFormat
		}
		if _, err := hex.DecodeString(value); err != nil {
			return "", "", ErrFormat
		}
	}
	if r.Start < 0 || r.End <= r.Start || r.End-r.Start != int64(len(r.Names)) || uint64(len(r.Names)) > math.MaxUint32 || r.Source == "" {
		return "", "", ErrFormat
	}
	h := sha256.New()
	var size [8]byte
	seen := make(map[string]bool, len(r.Names))
	for _, name := range r.Names {
		if err := validateName(name); err != nil {
			return "", "", err
		}
		if seen[name] {
			return "", "", errors.New("duplicate native plan name")
		}
		seen[name] = true
		binary.BigEndian.PutUint64(size[:], uint64(len(name)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(name))
	}
	names := hex.EncodeToString(h.Sum(nil))
	domain := "native-v1"
	if r.Combine {
		domain = "native-combine-v1"
	}
	if r.DeferredFrom != "" || r.Selection != nil {
		if !r.Combine || len(r.DeferredFrom) != 64 || len(r.Selection) != (len(r.Names)+7)/8 {
			return "", "", ErrFormat
		}
		if _, err := hex.DecodeString(r.DeferredFrom); err != nil {
			return "", "", ErrFormat
		}
		if n := len(r.Names) % 8; n != 0 && r.Selection[len(r.Selection)-1]>>uint(n) != 0 {
			return "", "", ErrFormat
		}
		domain += "/" + r.DeferredFrom + "/" + hex.EncodeToString(r.Selection)
	}
	identity := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%s/%s/%d/%d/%s", domain, r.Operation, r.Plan, r.Stream, r.Source, r.Start, r.End, names)))
	return hex.EncodeToString(identity[:]), names, nil
}

func nativeReceiptKey(identity string) []byte { return []byte("NATIVE/receipt/" + identity) }
func nativeIntentKey(identity string) []byte  { return []byte("NATIVE/intent/" + identity) }

type nativeSpan struct{ Lower, Upper []byte }
type nativeIntent struct {
	First     uint64
	Count     int
	Spans     []nativeSpan
	Directory string
}

func (s *Store) nativeDiskCheck() error {
	usage, err := s.fs.GetDiskUsage(s.dir)
	if errors.Is(err, vfs.ErrUnsupported) {
		return nil
	} // virtual filesystem tests
	if err != nil {
		return err
	}
	if usage.TotalBytes > 0 && usage.AvailBytes < usage.TotalBytes/10 {
		return fmt.Errorf("native bulk hard stop: less than 10%% free disk (%d/%d bytes)", usage.AvailBytes, usage.TotalBytes)
	}
	return nil
}

// NativeReceipt resolves a lost acknowledgement without exporting or importing
// any history again. The receipt and all visible catalogs share one WAL batch.
func (s *Store) NativeReceipt(ctx context.Context, request NativeRange) (NativeReceipt, error) {
	identity, _, err := request.Identity()
	if err != nil {
		return NativeReceipt{}, err
	}
	unlock, err := s.lockNativeRange(ctx, identity)
	if err != nil {
		return NativeReceipt{}, err
	}
	defer unlock()
	if failure := s.nativeFailure.Load(); failure != nil {
		return NativeReceipt{}, failure.err
	}
	return s.readNativeReceipt(ctx, request)
}

func (s *Store) readNativeReceipt(ctx context.Context, request NativeRange) (NativeReceipt, error) {
	if err := ctx.Err(); err != nil {
		return NativeReceipt{}, err
	}
	id, names, err := request.Identity()
	if err != nil {
		return NativeReceipt{}, err
	}
	value, closer, err := s.db.Get(nativeReceiptKey(id))
	if err != nil {
		return NativeReceipt{}, err
	}
	defer closer.Close()
	var receipt NativeReceipt
	if err := json.Unmarshal(value, &receipt); err != nil {
		return receipt, err
	}
	if receipt.Identity != id || receipt.NamesSHA256 != names || receipt.Count != len(request.Names) || len(receipt.Copied) != (receipt.Count+7)/8 || receipt.CopiedCount+receipt.DeferredCount != receipt.Count {
		return receipt, ErrFormat
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	for _, skip := range receipt.Skipped {
		if skip.Name != request.Names[skip.Ordinal] {
			return receipt, ErrFormat
		}
	}
	return receipt, nil
}

// Validate rejects inconsistent durable/client receipt bitmaps.
func (r NativeReceipt) Validate() error {
	if r.Count < 0 || r.CopiedCount < 0 || r.DeferredCount < 0 || len(r.Copied) != (r.Count+7)/8 || r.CopiedCount+r.DeferredCount != r.Count || r.Bytes < 0 {
		return ErrFormat
	}
	copied := 0
	for _, b := range r.Copied {
		copied += bits.OnesCount8(b)
	}
	if copied != r.CopiedCount || (r.Count%8 != 0 && len(r.Copied) > 0 && r.Copied[len(r.Copied)-1]>>uint(r.Count%8) != 0) {
		return ErrFormat
	}
	for _, h := range []string{r.Identity, r.NamesSHA256, r.Digest} {
		if len(h) != 64 {
			return ErrFormat
		}
		if _, err := hex.DecodeString(h); err != nil {
			return ErrFormat
		}
	}
	previous := -1
	for _, skip := range r.Skipped {
		if skip.Ordinal <= previous || skip.Ordinal >= r.Count || r.IsCopied(skip.Ordinal) || skip.Name == "" || skip.Bytes < 0 || skip.PhysicalPoints < 0 || skip.ArchiveSlots < 0 || skip.MemoryBytes < 0 || skip.Reason == "" {
			return ErrFormat
		}
		previous = skip.Ordinal
	}
	if len(r.Mappings) != 0 {
		if len(r.Mappings) != r.CopiedCount {
			return ErrFormat
		}
		previous := -1
		for _, m := range r.Mappings {
			if m.Ordinal <= previous || !r.IsCopied(m.Ordinal) || m.SourceID == 0 || m.SourceGeneration == 0 || m.SourceRevision == 0 || m.TargetID == 0 || m.TargetGeneration == 0 || m.TargetRevision == 0 {
				return ErrFormat
			}
			previous = m.Ordinal
		}
	}
	return nil
}

// NativeImport uses its own memory admission, independent of legacy transfer
// slots, tuner, compaction-debt/read/cache gates. IDs and hidden SSTs are
// prepared before the short creation lock.
// Only names still absent at that atomic publication become visible.
func (s *Store) NativeImport(ctx context.Context, request NativeRange, input io.Reader) (receipt NativeReceipt, err error) {
	ctx, cancel := s.NativeTransferContext(ctx)
	defer cancel()
	if request.Combine {
		return s.nativeCombineImport(ctx, request, input)
	}
	release, err := s.admitNative(ctx)
	if err != nil {
		return receipt, err
	}
	defer release()
	identity, namesHash, err := request.Identity()
	if err != nil {
		return receipt, err
	}
	unlock, err := s.lockNativeRange(ctx, identity)
	if err != nil {
		return NativeReceipt{}, err
	}
	defer unlock()
	if failure := s.nativeFailure.Load(); failure != nil {
		return receipt, failure.err
	}
	if receipt, err = s.readNativeReceipt(ctx, request); err == nil {
		return receipt, nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return receipt, err
	}
	if err := ValidateNativeNames(request.Names); err != nil {
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
	if err := s.cleanupNativeIntent(identity); err != nil {
		return receipt, err
	}
	stream, err := NewTransferStream(input)
	if err != nil {
		return receipt, err
	}
	stream.metricLimit = nativeMetricBytes
	expected := len(request.Names)
	if request.Candidates != nil {
		if len(request.Candidates) != (len(request.Names)+7)/8 {
			return receipt, ErrFormat
		}
		expected = 0
		for i := range request.Names {
			if nativeBit(request.Candidates, i) {
				expected++
			}
		}
	}
	if stream.Count != expected {
		return receipt, errors.New("native stream count differs from plan range")
	}
	if stream.Count == 0 {
		digest, wireBytes, e := stream.Finish()
		if e != nil {
			return receipt, e
		}
		receipt = NativeReceipt{Identity: identity, NamesSHA256: namesHash, Count: len(request.Names), Copied: make([]byte, (len(request.Names)+7)/8), Digest: digest, Bytes: wireBytes}
		if e = s.nativeStep("before-publish"); e != nil {
			return receipt, e
		}
		if e = s.publishNative(ctx, nil, nil, nil, &receipt, nil); e != nil {
			return receipt, e
		}
		s.recordNativeReceipt(receipt)
		return receipt, s.nativeStep("published")
	}
	reserveBegan := time.Now()
	var intent nativeIntent
	if stream.Count > 0 {
		intent, err = s.reserveNativeIDs(identity, stream.Count)
	} else {
		intent = nativeIntent{Directory: s.fs.PathJoin(s.dir, nativeDirectory(identity, 0))}
	}
	if err != nil {
		return receipt, err
	}
	s.nativeCounters.reserve.Add(uint64(time.Since(reserveBegan)))
	buildBegan := time.Now()
	published := false
	defer func() {
		if !published {
			err = errors.Join(err, s.cleanupNativeIntent(identity))
		}
	}()
	if err := s.nativeStep("reserved"); err != nil {
		return receipt, err
	}
	tables, err := s.newNativeTables(identity, intent.First)
	if err != nil {
		return receipt, err
	}
	intent.Directory = tables.directory
	defer func() { err = errors.Join(err, tables.close()); _ = s.fs.RemoveAll(tables.directory) }()
	metas := make([]Metadata, stream.Count)
	positions := make([]int, stream.Count)
	summaries := make([]SidecarSummary, stream.Count)
	planPositions := make(map[string]int, len(request.Names))
	for i, name := range request.Names {
		planPositions[name] = i
	}
	seen := make([]bool, len(request.Names))
	metadataBytes := int64(0)
	needsFloatFormat := false
	var skipped []NativeSkip
	for i := range stream.Count {
		if err := ctx.Err(); err != nil {
			return receipt, err
		}
		record, err := stream.Next()
		if err != nil {
			return receipt, err
		}
		metadataBytes += nativeMetadataSize(record.Config)
		if metadataBytes > nativeRangeMetadataBytes {
			return receipt, ErrTransferLimit
		}

		position, ok := planPositions[record.Config.Name]
		if !ok || seen[position] || record.Version.Name != record.Config.Name || (request.Candidates != nil && !nativeBit(request.Candidates, position)) {
			return receipt, errors.New("native stream names differ from plan")
		}
		seen[position] = true
		positions[i] = position
		if record.NativeSkip != nil {
			if err := validateNativeSkipRecord(record); err != nil {
				return receipt, err
			}
			skip := *record.NativeSkip
			skip.Ordinal = position
			skipped = append(skipped, skip)
			continue
		}
		// A zero-version empty record is an explicit snapshot absence marker.
		// It remains deferred to the existing source-absence/verification policy;
		// it never creates a catalog or silently counts as copied history.
		if record.Version.ID == 0 && record.Version.Generation == 0 && record.Version.Revision == 0 && len(record.Keys) == 0 && len(record.Config.Retentions) == 0 {
			continue
		}
		if err := validate(record.Config); err != nil {
			return receipt, err
		}
		if err := validateNativeMemory(record); err != nil {
			var skip *NativeSkip
			if !errors.As(err, &skip) {
				return receipt, err
			}
			skip.Ordinal = position
			skipped = append(skipped, *skip)
			continue
		}
		if record.Continuation != (s.continuation != nil) {
			return receipt, ErrFormat
		}
		m := Metadata{MetricConfig: record.Config, ID: intent.First + uint64(i), Generation: 1, Revision: 1, LastUpdate: s.nextActivity(time.Time{}), Continuation: record.Continuation}
		if s.incrementalSource != "" {
			m.FileMigration = FileReclaimed
		}
		metas[i] = m
		for j, kv := range record.Keys {
			if len(kv.Key) == 0 || len(kv.Key) > 4096 || len(kv.Value) > 16<<20 || (j > 0 && bytes.Compare(record.Keys[j-1].Key, kv.Key) >= 0) {
				return receipt, ErrFormat
			}
			if !record.Continuation && len(kv.Key) != 6 {
				return receipt, ErrFormat
			}
			if record.Continuation && newFloatGroup(kv.Key, kv.Value) && !s.acceptsFloatCodec() {
				return receipt, ErrFormat
			}
			if record.Continuation && newFloatGroup(kv.Key, kv.Value) {
				needsFloatFormat = true
			}
			if record.Continuation && !s.coldPack.format.Load() && sealedTransferKey(kv) {
				return receipt, ErrFormat
			}
			if isContinuationBucketSuffix(kv.Key) && !s.timeBuckets {
				return receipt, ErrFormat
			}
			if err := tables.add(ctx, m, kv); err != nil {
				return receipt, err
			}
		}
		if record.Continuation {
			summaries[i], err = s.nativeSidecarSummary(record)
			if err != nil {
				return receipt, err
			}
		}
	}
	digest, wireBytes, err := stream.Finish()
	if err != nil {
		return receipt, err
	}
	if err := tables.close(); err != nil {
		s.nativeWriteFailed(err)
		return receipt, err
	}
	s.nativeCounters.build.Add(uint64(time.Since(buildBegan)))
	intent.Spans = tables.spans()
	if err := s.saveNativeIntent(identity, intent); err != nil {
		s.nativeWriteFailed(err)
		return receipt, err
	}
	if err := s.nativeStep("staged"); err != nil {
		return receipt, err
	}
	if err := s.nativeDiskCheck(); err != nil {
		return receipt, err
	}
	ingestBegan := time.Now()
	if len(tables.paths) > 0 {
		if needsFloatFormat {
			if err := s.fenceFloatCodec(); err != nil {
				return receipt, err
			}
		}
		if err := s.ingestNativeFiles(ctx, tables.paths); err != nil {
			return receipt, err
		}
		s.ingestStats.entries.Add(uint64(len(metas)))
	}
	s.nativeCounters.ingest.Add(uint64(time.Since(ingestBegan)))
	if err := s.nativeStep("ingested"); err != nil {
		return receipt, err
	}
	if err := errors.Join(ctx.Err(), s.nativeDiskCheck()); err != nil {
		return receipt, err
	}
	tableReceipts, err := tables.receipts()
	if err != nil {
		return receipt, err
	}
	slices.SortFunc(skipped, func(a, b NativeSkip) int { return a.Ordinal - b.Ordinal })
	receipt = NativeReceipt{Skipped: skipped, FirstID: intent.First, ReservedIDs: intent.Count, Tables: tableReceipts, Identity: identity, NamesSHA256: namesHash, Count: len(request.Names), Copied: make([]byte, (len(request.Names)+7)/8), Digest: digest, Bytes: wireBytes}
	if err := s.nativeStep("before-publish"); err != nil {
		return receipt, err
	}
	if err := s.publishNative(ctx, metas, positions, summaries, &receipt, tables); err != nil {
		return receipt, err
	}
	published = true
	s.recordNativeReceipt(receipt)
	if err := s.nativeStep("published"); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (s *Store) reserveNativeIDs(identity string, count int) (nativeIntent, error) {
	s.mu.Lock()
	locked := true
	defer func() {
		if locked {
			s.mu.Unlock()
		}
	}()
	high, err := s.CatalogHighWatermark(context.Background())
	if err != nil {
		return nativeIntent{}, err
	}
	if count < 1 || high > math.MaxUint64-uint64(count) {
		return nativeIntent{}, errors.New("native metric IDs exhausted")
	}
	intent := nativeIntent{First: high + 1, Count: count, Directory: s.fs.PathJoin(s.dir, nativeDirectory(identity, high+1))}
	data, err := json.Marshal(intent)
	if err != nil {
		return intent, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(sequenceKey(), uint64Bytes(high+uint64(count)), nil); err != nil {
		return intent, err
	}
	if err := b.Set(nativeIntentKey(identity), data, nil); err != nil {
		return intent, err
	}
	// The sequence and intent must be visible before the next allocator runs,
	// but its fsync need not hold the creation lock. Wait before using the IDs;
	// concurrent reservations can share Pebble's WAL sync.
	err = s.db.ApplyNoSyncWait(b, pebble.Sync)
	s.mu.Unlock()
	locked = false
	if err == nil {
		err = b.SyncWait()
	}
	s.nativeWriteFailed(err)
	return intent, err
}
func (s *Store) saveNativeIntent(identity string, intent nativeIntent) error {
	data, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	return s.db.Set(nativeIntentKey(identity), data, pebble.Sync)
}
func (s *Store) cleanupNativeIntent(identity string) error {
	value, closer, err := s.db.Get(nativeIntentKey(identity))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var intent nativeIntent
	err = json.Unmarshal(value, &intent)
	_ = closer.Close()
	if err != nil {
		return err
	}
	b := s.db.NewBatch()
	defer b.Close()
	for _, span := range intent.Spans {
		if len(span.Lower) == 0 || bytes.Compare(span.Lower, span.Upper) >= 0 {
			return ErrFormat
		}
		if err := b.DeleteRange(span.Lower, span.Upper, nil); err != nil {
			return err
		}
	}
	if err := b.Delete(nativeIntentKey(identity), nil); err != nil {
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return err
	}
	if intent.Directory != "" {
		if intent.Directory != s.fs.PathJoin(s.dir, nativeDirectory(identity, intent.First)) {
			return ErrFormat
		}
		return s.fs.RemoveAll(intent.Directory)
	}
	return nil
}

func (s *Store) publishNative(ctx context.Context, metas []Metadata, positions []int, summaries []SidecarSummary, receipt *NativeReceipt, tables *nativeTables) error {
	waitBegan := time.Now()
	s.mu.Lock()
	lockedBegan := time.Now()
	s.nativeCounters.publishWait.Add(uint64(lockedBegan.Sub(waitBegan)))
	locked := true
	defer func() {
		if locked {
			s.mu.Unlock()
		}
	}() // same lock as normal Create
	b := s.db.NewBatch()
	defer b.Close()
	walk := &transferReader{reader: s.db}
	defer walk.close()
	var visible []Metadata
	var sidecars []SidecarRecord
	for i, m := range metas {
		if m.ID == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := walk.lookup(&walk.catalog, catalogLower, catalogUpper, catalogKey(m.Name))
		if err == nil || errors.Is(err, ErrNotFound) && s.pendingTransfer(m.Name) {
			for _, span := range tables.unusedSpans(m) {
				if err := b.DeleteRange(span.Lower, span.Upper, nil); err != nil {
					return err
				}
			}
			continue
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		data, err := s.encodeCatalog(m)
		if err != nil {
			return err
		}
		if err := b.Set(catalogKey(m.Name), data, nil); err != nil {
			return err
		}
		if err := b.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
			return err
		}
		receipt.Copied[positions[i]/8] |= 1 << uint(positions[i]%8)
		receipt.CopiedCount++
		visible = append(visible, m)
		sidecars = append(sidecars, SidecarRecord{Name: m.Name, SidecarSummary: summaries[i]})
	}
	receipt.DeferredCount = receipt.Count - receipt.CopiedCount
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if err := b.Set(nativeReceiptKey(receipt.Identity), data, nil); err != nil {
		return err
	}
	if err := b.Delete(nativeIntentKey(receipt.Identity), nil); err != nil {
		return err
	}
	if err := s.commitSidecarRecords(b, sidecars, func(b *pebble.Batch) error { return s.db.ApplyNoSyncWait(b, pebble.Sync) }); err != nil {
		s.nativeWriteFailed(err)
		return err
	}
	s.mu.Unlock()
	locked = false
	held := uint64(time.Since(lockedBegan))
	s.nativeCounters.publishHold.Add(held)
	for old := s.nativeCounters.maxPublishHold.Load(); held > old; old = s.nativeCounters.maxPublishHold.Load() {
		if s.nativeCounters.maxPublishHold.CompareAndSwap(old, held) {
			break
		}
	}
	// Catalogs, sidecar inventory and the receipt became visible together
	// under the creation lock. Keep the per-range lock until fsync completes:
	// neither this import nor a duplicate/receipt query can acknowledge early.
	// ApplyNoSyncWait retains the batch until SyncWait, including when a
	// crash hook fails. Closing it early can recycle its pending sync state.
	hookErr := s.nativeStep("publish-committed")
	syncBegan := time.Now()
	err = errors.Join(hookErr, b.SyncWait())
	s.nativeCounters.publishSync.Add(uint64(time.Since(syncBegan)))
	if err != nil {
		s.nativeWriteFailed(err)
		return err
	}
	s.nativeCatalogChanged(visible)
	s.invalidateReadCache()
	return nil
}

func nativeBit(bits []byte, index int) bool {
	return index/8 < len(bits) && bits[index/8]&(1<<uint(index%8)) != 0
}

func (s *Store) NativeCandidates(ctx context.Context, names []string) ([]byte, error) {
	if err := errors.Join(ctx.Err(), s.checkNativeReady(), s.fileOwnershipError(), s.nativeDiskCheck()); err != nil {
		return nil, err
	}
	view := s.db.NewSnapshot()
	defer view.Close()
	walk := &transferReader{reader: view, ordered: true}
	defer walk.close()
	order := make([]int, len(names))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return bytes.Compare([]byte(names[a]), []byte(names[b])) })
	bits := make([]byte, (len(names)+7)/8)
	for _, i := range order {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_, err := walk.lookup(&walk.catalog, catalogLower, catalogUpper, catalogKey(names[i]))
		if errors.Is(err, ErrNotFound) && !s.pendingTransfer(names[i]) {
			bits[i/8] |= 1 << uint(i%8)
		} else if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return bits, nil
}

func (s *Store) nativeStep(stage string) error {
	if s.nativeTestHook != nil {
		return s.nativeTestHook(stage)
	}
	return nil
}

func (s *Store) nativeWriteFailed(err error) {
	if err != nil {
		s.nativeFailure.CompareAndSwap(nil, &ownershipError{err: err})
	}
}

func (s *Store) recordNativeReceipt(r NativeReceipt) {
	s.nativeCounters.ranges.Add(1)
	s.nativeCounters.copied.Add(uint64(r.CopiedCount))
	s.nativeCounters.deferred.Add(uint64(r.DeferredCount))
	s.nativeCounters.wire.Add(uint64(r.Bytes))
}
