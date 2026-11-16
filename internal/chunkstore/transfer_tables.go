package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
)

// transferStage holds a transfer's keys under its reserved IDs, sorted, and
// the sampled point checks that run on an independent read-back of them.
type transferStage struct {
	keys    []RelayoutKV
	bytes   int
	capture func(key, value []byte) error
	verify  func(ctx context.Context) error
}

func (s *Store) stageTransfer(first uint64, records []TransferMetric, metas []Metadata) *transferStage {
	samples := make(map[int]*TransferMetric)
	for i, r := range records {
		if r.Continuation && r.PointDigest != "" {
			samples[i] = &TransferMetric{Config: r.Config, Continuation: true}
		}
	}
	var sampled transferArena
	capture := func(key, value []byte) error {
		offset := 0
		if key[0] == continuationBucketKeyPrefix {
			offset = 5
		}
		if len(key) < offset+17 {
			return ErrFormat
		}
		id := binary.BigEndian.Uint64(key[offset+1 : offset+9])
		if id < first || id-first >= uint64(len(records)) {
			return ErrFormat
		}
		i := int(id - first)
		if sample := samples[i]; sample != nil {
			var suffix []byte
			if offset == 5 {
				suffix = ContinuationBucketKey(binary.BigEndian.Uint32(key[1:5]), key[22:])
			} else {
				suffix = key[17:]
			}
			data := sampled.alloc(len(suffix) + len(value))
			copy(data, suffix)
			copy(data[len(suffix):], value)
			sample.Keys = append(sample.Keys, RelayoutKV{data[:len(suffix):len(suffix)], data[len(suffix):]})
		}
		return nil
	}
	count := 0
	for _, r := range records {
		count += len(r.Keys)
	}
	keys := make([]RelayoutKV, 0, count)
	size := 0
	var arena transferArena
	for i, r := range records {
		prefix := chunkPrefix(metas[i].ID, metas[i].Generation)
		if r.Continuation {
			prefix = continuationPrefix(metas[i].ID, metas[i].Generation)
		}
		for _, kv := range r.Keys {
			key := arena.alloc(len(prefix) + len(kv.Key))[:0]
			if r.Continuation {
				key = appendContinuationKey(key, prefix, kv.Key)
			} else {
				key = append(append(key, prefix...), kv.Key...)
			}
			value := kv.Value
			if r.Continuation {
				value = s.bindContinuationIndex(kv.Key, value, metas[i].Revision)
			}
			keys = append(keys, RelayoutKV{key, value})
			size += len(key) + len(value)
		}
	}
	slices.SortFunc(keys, func(a, b RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	// Decode the independent read-back, rather than re-reading the same
	// sampled IDs through every LSM level after publication.
	verify := func(ctx context.Context) error {
		for i, sample := range samples {
			slices.SortFunc(sample.Keys, func(a, b RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
			got, err := s.continuation.Snapshot(ctx, transferRecordIO(*sample), metas[i], s.now)
			if err != nil {
				return err
			}
			digest, counts := snapshotDigest(got)
			if digest != records[i].PointDigest || !slices.Equal(counts, records[i].Counts) {
				return fmt.Errorf("staged point verification %s failed", records[i].Config.Name)
			}
		}
		return nil
	}
	return &transferStage{keys: keys, bytes: size, capture: capture, verify: verify}
}

// Keep immutable staging commits short enough to interleave with foreground
// commits. Publication has its own, larger atomic metadata bound. A single
// validated value can exceed the staging target and is kept indivisible.
const transferStageBatchBytes = 8 << 10

// transferBatchLimit lets tests reduce the staging bound.
func (s *Store) transferBatchLimit() int {
	if s.transferBatchBytes > 0 {
		return min(transferStageBatchBytes, s.transferBatchBytes)
	}
	return transferStageBatchBytes
}

func (s *Store) verifyTransferBatch(ctx context.Context, b *pebble.Batch, stage *transferStage) error {
	if int(b.Count()) != len(stage.keys) {
		return errors.New("staged transfer batch count verification failed")
	}
	r := b.Reader()
	for _, expected := range stage.keys {
		kind, key, value, ok, err := r.Next()
		if err != nil {
			return err
		}
		if !ok || kind != pebble.InternalKeyKindSet || !bytes.Equal(key, expected.Key) {
			return errors.New("staged transfer key verification failed")
		}
		if !bytes.Equal(value, expected.Value) {
			return errors.New("staged transfer value verification failed")
		}
		if err := stage.capture(key, value); err != nil {
			return err
		}
	}
	if _, _, _, ok, err := r.Next(); ok || err != nil {
		return errors.Join(err, errors.New("unexpected staged transfer key"))
	}
	return stage.verify(ctx)
}

// A process crash may leave staged files that were never ingested. The ID
// reservation is durable and will not be reused; only Pebble's own numbered
// files can back published data. Called before listeners under process ownership.
func (s *Store) recoverTransferTables() error {
	names, err := s.fs.List(s.dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		stem, ok := strings.CutPrefix(name, "transfer-")
		if !ok || !strings.HasSuffix(stem, ".sst") {
			continue
		}
		id, index, ok := strings.Cut(strings.TrimSuffix(stem, ".sst"), "-")
		first, idErr := strconv.ParseUint(id, 10, 64)
		n, indexErr := strconv.ParseUint(index, 10, 64)
		if !ok || idErr != nil || indexErr != nil || name != fmt.Sprintf("transfer-%020d-%d.sst", first, n) {
			continue
		}
		if err := s.fs.Remove(s.fs.PathJoin(s.dir, name)); err != nil {
			return err
		}
	}
	return nil
}

// Reserved IDs have no visible catalog entry and cannot be written by live
// writers. Stage them in bounded WAL batches: SST placement scans all files
// without range keys under Pebble's DB mutex, even for disjoint point-only
// tables. The final publication barrier syncs these private writes as before.
func (s *Store) stageTransferData(ctx context.Context, stage *transferStage, entries int) error {
	return s.stageTransferWAL(ctx, stage, entries)
}

// stageTransferWAL avoids SST target-level selection under Pebble's DB mutex.
// Only reserved, unpublished IDs are written here. Import commits share the
// publication mutex, released after each byte-bounded batch; live writers do
// not acquire it. Verification and the final visibility/sync protocol remain
// identical to table staging. A single validated record stays indivisible.
func (s *Store) stageTransferWAL(ctx context.Context, stage *transferStage, entries int) error {
	b := s.db.NewBatch()
	defer b.Close()
	start := 0
	flush := func(end int) error {
		if b.Empty() {
			return ctx.Err()
		}
		part := &transferStage{keys: stage.keys[start:end], capture: stage.capture, verify: func(context.Context) error { return nil }}
		if err := s.verifyTransferBatch(ctx, b, part); err != nil {
			return err
		}
		size := uint64(len(b.Repr()))
		if err := s.commitTransferBatch(ctx, b); err != nil {
			return err
		}
		s.pendingSync.Store(true)
		s.transferBatchStats.commits.Add(1)
		s.transferBatchStats.bytes.Add(size)
		b.Reset()
		start = end
		return s.nativeStep("transfer-stage-batch")
	}
	for i, kv := range stage.keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !b.Empty() && len(b.Repr())+len(kv.Key)+len(kv.Value)+16 > s.transferBatchLimit() {
			if err := flush(i); err != nil {
				return err
			}
		}
		if err := b.Set(kv.Key, kv.Value, nil); err != nil {
			return err
		}
	}
	if err := flush(len(stage.keys)); err != nil {
		return err
	}
	if err := stage.verify(ctx); err != nil {
		return err
	}
	s.transferBatchStats.entries.Add(uint64(entries))
	return ctx.Err()
}
