package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestNativeTablesBoundLargeSyntheticRange(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{fs: nativeTestDiskFS{vfs.Default}, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	s.StopBackground()
	defer closeTestStore(t, s)
	tables, err := s.newNativeTables(strings.Repeat("a", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tables.close()
	defer s.fs.RemoveAll(tables.directory)
	value := make([]byte, 1024)
	// Reuse input memory while streaming 128 MiB through real SST writers.
	for i := range value {
		value[i] = byte(i*31 + i/7)
	}
	runtime.GC()
	var before, peak runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 128*1024; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		if err := tables.add(context.Background(), Metadata{ID: 1, Generation: 1}, RelayoutKV{key, value}); err != nil {
			t.Fatal(err)
		}
		if i%8192 == 0 {
			runtime.GC()
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			if mem.HeapAlloc > peak.HeapAlloc {
				peak = mem
			}
		}
	}
	if err := tables.close(); err != nil {
		t.Fatal(err)
	}
	receipts, err := tables.receipts()
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) < 16 {
		t.Fatalf("entire range retained in %d SSTs", len(receipts))
	}
	for _, r := range receipts {
		if r.Bytes > nativeTableBytes+1<<20 {
			t.Fatalf("unbounded SST: %d bytes", r.Bytes)
		}
	}
	if peak.HeapAlloc > before.HeapAlloc+64<<20 {
		t.Fatalf("128 MiB streamed range retained %d bytes of heap", peak.HeapAlloc-before.HeapAlloc)
	}
	t.Logf("128 MiB stream: %d SSTs, peak retained Go heap delta %d bytes", len(receipts), int64(peak.HeapAlloc)-int64(before.HeapAlloc))
}

func TestNativeTablesRejectMetadataBeforeOpeningAnotherFile(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{fs: nativeTestDiskFS{vfs.Default}})
	defer closeTestStore(t, s)
	tables, err := s.newNativeTables(strings.Repeat("e", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tables.close()
	defer s.fs.RemoveAll(tables.directory)
	tables.metadataBytes = nativeRangeMetadataBytes
	if err := tables.add(context.Background(), Metadata{ID: 1, Generation: 1}, RelayoutKV{[]byte("key"), []byte("value")}); !errors.Is(err, ErrTransferLimit) {
		t.Fatal(err)
	}
	if len(tables.paths) != 0 || len(tables.writers) != 0 {
		t.Fatal("opened a file after exhausting receipt metadata")
	}
	intent := &nativeCombineIntent{Spans: make([]nativeSpan, nativeRangeMetadataBytes/256+1)}
	if err := s.ingestNativeCombineTables(context.Background(), intent, tables, false, time.Now()); !errors.Is(err, ErrTransferLimit) {
		t.Fatal("unbounded cumulative cleanup metadata", err)
	}
}

func TestNativeBucketReadersAndWritersBoundDeepHistory(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{fs: nativeTestDiskFS{vfs.Default}, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	tables, err := s.newNativeTables(strings.Repeat("b", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tables.close()
	defer s.fs.RemoveAll(tables.directory)
	for bucket := uint32(0); bucket < 100; bucket++ {
		kv := RelayoutKV{ContinuationBucketKey(bucket, []byte("payload")), []byte("value")}
		if err := tables.add(context.Background(), Metadata{ID: 1, Generation: 1, Continuation: true}, kv); err != nil {
			t.Fatal(err)
		}
		if len(tables.writers) > nativeTableWriters {
			t.Fatal("bucket writer cache grew", len(tables.writers))
		}
	}
	if err := tables.close(); err != nil {
		t.Fatal(err)
	}
	if err := s.ingestNativeFiles(context.Background(), tables.paths); err != nil {
		t.Fatal(err)
	}
	walk := &transferReader{reader: s.db, native: true, ordered: true}
	defer walk.close()
	for bucket := uint32(0); bucket < 100; bucket++ {
		prefix := appendContinuationKey(nil, continuationPrefix(1, 1), ContinuationBucketKey(bucket, nil))
		count := 0
		if err := walk.visitBucket(context.Background(), prefix, func(_, value []byte) error {
			count++
			if string(value) != "value" {
				return ErrFormat
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatal("lost history beyond cached buckets", bucket, count)
		}
		if len(walk.bucketReaders) > nativeTableWriters {
			t.Fatal("native reader cache grew", len(walk.bucketReaders))
		}
	}
}

func TestNativeIngestCancellationBetweenFileGroups(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{fs: nativeTestDiskFS{vfs.Default}, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	tables, err := s.newNativeTables(strings.Repeat("c", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tables.close()
	defer s.fs.RemoveAll(tables.directory)
	for bucket := uint32(0); bucket < 9; bucket++ {
		if err := tables.add(context.Background(), Metadata{ID: 1, Generation: 1, Continuation: true}, RelayoutKV{ContinuationBucketKey(bucket, []byte("x")), []byte("y")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tables.close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ingests := 0
	s.nativeTestHook = func(stage string) error {
		if stage == "native-ingest-batch" {
			ingests++
			cancel()
		}
		return nil
	}
	if err := s.ingestNativeFiles(ctx, tables.paths); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if ingests != 1 {
		t.Fatal("ingest continued after cancellation", ingests)
	}
	key := appendContinuationKey(nil, continuationPrefix(1, 1), ContinuationBucketKey(8, []byte("x")))
	if _, closer, err := s.db.Get(key); err == nil {
		closer.Close()
		t.Fatal("cancelled ingest imported later file")
	}
}

func TestNativeRejectsOversizedHistoriesBeforeDecode(t *testing.T) {
	r := TransferMetric{Config: MetricConfig{Name: "large", Retentions: []Retention{{Step: 1, Count: nativeMetricPoints + 1}}}}
	if err := validateNativeMemory(r); !errors.Is(err, ErrTransferLimit) {
		t.Fatal(err)
	}
	r.Config.Retentions[0].Count = 100
	// Valid classic groups must not be mistaken for sealed codec headers.
	r.Keys = []RelayoutKV{{append(append([]byte{'f'}, make([]byte, 8)...), 'c', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), EncodePointBlock(&PointBlock{})}}
	if err := validateNativeMemory(r); err != nil {
		t.Fatal(err)
	}
	r.Keys = make([]RelayoutKV, nativeMetricPoints/128+1)
	if err := validateNativeMemory(r); !errors.Is(err, ErrTransferLimit) {
		t.Fatal(err)
	}
}

func TestNativeStreamRejectsMetricBeforePayloadAllocation(t *testing.T) {
	var wire bytes.Buffer
	record := TransferMetric{Config: MetricConfig{Name: "large"}, Version: TransferVersion{Name: "large"}, Keys: []RelayoutKV{{[]byte("k"), bytes.Repeat([]byte("v"), 1<<20)}}}
	if err := writeTransferStream2(&wire, 1, func(int) (TransferMetric, error) { return record, nil }); err != nil {
		t.Fatal(err)
	}
	stream, err := NewTransferStream(&wire)
	if err != nil {
		t.Fatal(err)
	}
	stream.metricLimit = 1024
	if _, err := stream.Next(); !errors.Is(err, ErrTransferLimit) {
		t.Fatal(err)
	}
	if len(stream.arena.slab) > 64<<10 {
		t.Fatal("allocated rejected payload", len(stream.arena.slab))
	}
}

type nativeUnreadable struct{}

func (nativeUnreadable) Read([]byte) (int, error) { panic("read before admission") }
func TestNativeCancellationWhileWaitingForAdmissionOrReceipt(t *testing.T) {
	var s Store
	release, err := s.admitNative(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.NativeImport(ctx, NativeRange{}, nativeUnreadable{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	unlock, err := s.lockNativeRange(context.Background(), "range")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if _, err := s.lockNativeRange(ctx, "range"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestNativeOldOversizedRangeIsRejectedWithoutReading(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{fs: nativeTestDiskFS{vfs.Default}, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	names := make([]string, 10000)
	for i := range names {
		names[i] = fmt.Sprintf("old.%05d", i)
	}
	request := NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: int64(len(names)), Names: names, Combine: true}
	if _, err := s.NativeImport(context.Background(), request, nativeUnreadable{}); !errors.Is(err, ErrTransferLimit) {
		t.Fatal(err)
	}
	if _, ready, err := s.ResumeNativeCombine(context.Background(), request); err != nil || ready {
		t.Fatal("unknown range is not a ready resume", ready, err)
	}
}

func TestNativeLegacyReadyRangeUsesMetadataBudget(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{fs: nativeTestDiskFS{vfs.Default}, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	names := make([]string, 100000)
	for i := range names {
		names[i] = fmt.Sprintf("legacy.%06d", i)
	}
	request := NativeRange{Operation: strings.Repeat("4", 64), Plan: strings.Repeat("5", 64), Stream: strings.Repeat("6", 64), Source: "donor", End: int64(len(names)), Names: names, Candidates: make([]byte, (len(names)+7)/8), Combine: true}
	id, hash, err := request.Identity()
	if err != nil {
		t.Fatal(err)
	}
	// A legacy ready range with no selected records needs only ordinal
	// metadata. It must remain resumable without a donor or new range identity.
	intent := nativeCombineIntent{Request: request, Ready: true, Receipt: NativeReceipt{Identity: id, NamesSHA256: hash, Count: len(names), Copied: make([]byte, (len(names)+7)/8), Digest: strings.Repeat("7", 64)}}
	if err := s.saveNativeCombineIntent(intent); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, peak runtime.MemStats
	runtime.ReadMemStats(&before)
	s.nativeTestHook = func(string) error {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		if mem.HeapAlloc > peak.HeapAlloc {
			peak = mem
		}
		return nil
	}
	got, ready, err := s.ResumeNativeCombine(context.Background(), request)
	if err != nil || !ready || got.Identity != id || got.DeferredCount != len(names) {
		t.Fatal("legacy identity did not resume", ready, got.Count, err)
	}
	if peak.HeapAlloc > before.HeapAlloc+128<<20 {
		t.Fatal("legacy ordinal metadata exceeded fixed heap budget", peak.HeapAlloc-before.HeapAlloc)
	}
	// Legacy intent values are checked before JSON decoding, even when the
	// new-request name count is valid. A huge old value must fail explicitly.
	legacyIDs := []string{id, strings.Repeat("a", 64), strings.Repeat("b", 64)}
	oversized := bytes.Repeat([]byte(" "), nativeRangeMetadataBytes+1)
	for _, legacyID := range legacyIDs {
		if err := s.db.Set(nativeCombineIntentKey(legacyID), oversized, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.readNativeCombineIntent(id); !errors.Is(err, ErrTransferLimit) {
		t.Fatal("decoded oversized legacy metadata", err)
	}
	runtime.GC()
	runtime.ReadMemStats(&before)
	errorsBefore := s.nativeCounters.errors.Load()
	if err := s.recoverNativeCombines(); err != nil {
		t.Fatal("oversized legacy state prevented startup", err)
	}
	runtime.ReadMemStats(&peak)
	if peak.HeapAlloc > before.HeapAlloc+128<<20 {
		t.Fatal("startup exceeded fixed metadata heap budget", peak.HeapAlloc-before.HeapAlloc)
	}
	if s.nativeCounters.errors.Load()-errorsBefore != uint64(len(legacyIDs)) {
		t.Fatal("startup did not report every oversized legacy intent")
	}
	for _, legacyID := range legacyIDs {
		value, closer, err := s.db.Get(nativeCombineIntentKey(legacyID))
		if err != nil {
			t.Fatal("startup discarded legacy evidence", err)
		}
		size := len(value)
		closer.Close()
		if size != nativeRangeMetadataBytes+1 {
			t.Fatal("startup rewrote oversized legacy intent")
		}
	}
}

// Keep real SST data on disk; the test does not require a production 10% free reserve.
type nativeTestDiskFS struct{ vfs.FS }

func (nativeTestDiskFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	return vfs.DiskUsage{}, vfs.ErrUnsupported
}

func TestNativeRetirementCommitsBoundedDeletesAndResumes(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{fs: nativeTestDiskFS{vfs.Default}, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	old := createTestMetric(t, s, "retired")
	other := createTestMetric(t, s, "live")
	var last []byte
	for batch := 0; batch < 100; batch++ {
		b := s.db.NewBatch()
		for i := 0; i < 1000; i++ {
			key := append(chunkPrefix(old.ID, old.Generation), make([]byte, 100)...)
			binary.BigEndian.PutUint32(key[len(key)-4:], uint32(batch*1000+i))
			if err := b.Set(key, []byte("history"), nil); err != nil {
				t.Fatal(err)
			}
			last = key
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}
	interrupted := errors.New("interrupt after first retirement commit")
	batches := 0
	s.nativeTestHook = func(stage string) error {
		if stage == "combine-retire-batch" {
			batches++
			return interrupted
		}
		return nil
	}
	if err := s.retireNativeCombine(context.Background(), old); !errors.Is(err, interrupted) {
		t.Fatal(err)
	}
	if _, closer, err := s.db.Get(last); err != nil {
		t.Fatal("retired whole history before first bounded commit", err)
	} else {
		closer.Close()
	}
	s.nativeTestHook = func(stage string) error {
		if stage == "combine-retire-batch" {
			batches++
		}
		return nil
	}
	if err := s.retireNativeCombine(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	if batches < 10 {
		t.Fatal("retirement retained a history-sized WAL batch", batches)
	}
	if _, closer, err := s.db.Get(last); err == nil {
		closer.Close()
		t.Fatal("resume left old history")
	}
	if _, err := s.Metadata(context.Background(), other.Name); err != nil {
		t.Fatal("retirement damaged live catalog", err)
	}
}
