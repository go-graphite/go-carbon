package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestNativeAbandonOversizedIntentThenFreshPlan(t *testing.T) {
	ctx := context.Background()
	s, _ := openTestStore(t, "/target", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	request := combineTestRange(make([]string, 100000))
	for i := range request.Names {
		request.Names[i] = fmt.Sprintf("legacy.metric.%06d", i)
	}
	id, hash, err := request.Identity()
	if err != nil {
		t.Fatal(err)
	}
	intent := nativeCombineIntent{Request: request, Receipt: NativeReceipt{Identity: id, NamesSHA256: hash, Count: len(request.Names), Copied: make([]byte, (len(request.Names)+7)/8), Digest: strings.Repeat("0", 64)}, Ready: true,
		Raw: make([]nativeCombineMetadata, len(request.Names)), Positions: make([]int, len(request.Names)), Origins: make([]nativeCombineOrigin, len(request.Names))}
	for i := range intent.Positions {
		intent.Positions[i] = i
	}
	if err := validateNativeCombineIntent(id, intent); err != nil {
		t.Fatal(err)
	}
	value, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	if len(value) <= nativeRangeMetadataBytes {
		t.Fatal("fixture not oversized", len(value))
	}
	if err := s.db.Set(nativeCombineIntentKey(id), value, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	intent = nativeCombineIntent{}
	runtime.GC()
	runtime.GC() // Drop the JSON encoder pool before measuring decoder scratch.
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak uint64
	s.nativeTestHook = func(stage string) error {
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		peak = max(peak, mem.HeapAlloc)
		return nil
	}
	out, err := s.AbandonNativeCombine(ctx, id)
	if err != nil || out.State != "abandoned" || out.IntentBytes <= nativeRangeMetadataBytes {
		t.Fatal(out, err)
	}
	if peak > before.HeapAlloc+uint64(out.IntentBytes)+8<<20 {
		t.Fatalf("unbounded retained heap beyond the encoded intent: %d", peak-before.HeapAlloc)
	}
	t.Logf("%d-byte legacy intent, retained heap growth %d bytes", out.IntentBytes, int64(peak)-int64(before.HeapAlloc))
	s.nativeTestHook = nil
	if _, _, err := s.ResumeNativeCombine(ctx, request); !errors.Is(err, ErrConflict) {
		t.Fatal("old identity reusable", err)
	}
	if got, err := s.AbandonNativeCombine(ctx, id); err != nil || got != out {
		t.Fatal("retry", got, err)
	}
	if _, c, err := s.db.Get(nativeCombineIntentKey(id)); !errors.Is(err, pebble.ErrNotFound) {
		if c != nil {
			_ = c.Close()
		}
		t.Fatal("intent retained", err)
	}
	// Real consumer: re-copy the same names in new admitted ranges, keeping
	// target-wins semantics. Abandoning never turns into a final copy receipt.
	fresh := combineTestRange(request.Names[:2])
	fresh.Operation = strings.Repeat("4", 64)
	donor, _ := openTestStore(t, "/donor", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, donor)
	for _, name := range fresh.Names {
		createTestMetric(t, donor, name)
	}
	pin, err := donor.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	var wire bytes.Buffer
	if err := pin.Write(ctx, fresh.Names, &wire); err != nil {
		t.Fatal(err)
	}
	if r, err := s.NativeImport(ctx, fresh, &wire); err != nil || r.CopiedCount != len(fresh.Names) {
		t.Fatal("fresh plan", r, err)
	}
	// Every old name can subsequently be considered in bounded new ranges.
	// The donor has only the first two names; absent names are acknowledged as
	// deferred rather than being silently excluded from the new plan.
	for first := 2; first < len(request.Names); first += NativeMaxRangeNames {
		next := combineTestRange(request.Names[first:min(first+NativeMaxRangeNames, len(request.Names))])
		next.Operation = fresh.Operation
		next.Start = int64(first)
		next.End = next.Start + int64(len(next.Names))
		wire.Reset()
		if err := pin.Write(ctx, next.Names, &wire); err != nil {
			t.Fatal(err)
		}
		if r, err := s.NativeImport(ctx, next, &wire); err != nil || r.DeferredCount != len(next.Names) {
			t.Fatal("remaining fresh range", first, r, err)
		}
	}

}

func TestNativeAbandonCrashPreservesPublishedName(t *testing.T) {
	for _, stage := range []string{"abandon-fenced", "abandon-delete-batch", "abandon-cleaned"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			fs := vfs.NewStrictMem()
			opts := Options{fs: fs, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour}
			s, _ := openTestStore(t, "/target", opts)
			donor, now := openTestStore(t, "/donor", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			defer closeTestStore(t, donor)
			names := []string{"abandon.published", "abandon.hidden"}
			for _, name := range names {
				createTestMetric(t, donor, name)
				createTestMetric(t, s, name)
				if err := s.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 42}}); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := donor.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			var wire bytes.Buffer
			if err := pin.Write(ctx, names, &wire); err != nil {
				t.Fatal(err)
			}
			request := combineTestRange(names)
			id, _, _ := request.Identity()
			s.nativeTestHook = func(got string) error {
				if got == "combine-name-published" {
					return errors.New("interrupt")
				}
				return nil
			}
			if _, err := s.NativeImport(ctx, request, &wire); err == nil {
				t.Fatal("not interrupted")
			}
			s.nativeTestHook = nil
			before, err := s.Snapshot(ctx, names[0])
			if err != nil {
				t.Fatal(err)
			}
			published, err := s.Metadata(ctx, names[0])
			if err != nil {
				t.Fatal(err)
			}
			// Make enough hidden keys to require several bounded WAL batches.
			intent, err := s.readNativeCombineIntent(id)
			if err != nil {
				t.Fatal(err)
			}
			hidden := chunkPrefix(10000000, 1)
			b := s.db.NewBatch()
			for i := 0; i < 60000; i++ {
				key := binary.BigEndian.AppendUint32(bytes.Clone(hidden), uint32(i))
				if err := b.Set(key, []byte("v"), nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.Commit(pebble.Sync); err != nil {
				t.Fatal(err)
			}
			_ = b.Close()
			intent.Spans = append(intent.Spans, nativeSpan{hidden, prefixEnd(hidden)})
			if err := s.saveNativeCombineIntent(intent); err != nil {
				t.Fatal(err)
			}
			s.nativeTestHook = func(got string) error {
				if got == stage {
					fs.SetIgnoreSyncs(true)
					return errors.New("crash")
				}
				return nil
			}
			if _, err := s.AbandonNativeCombine(ctx, id); err == nil {
				t.Fatal("not interrupted")
			}
			closeTestStore(t, s)
			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)
			s, _ = openTestStore(t, "/target", opts)
			defer closeTestStore(t, s)
			if _, _, err := s.ResumeNativeCombine(ctx, request); !errors.Is(err, ErrConflict) {
				t.Fatal("cleanup resumed publication", err)
			}
			out, err := s.AbandonNativeCombine(ctx, id)
			if err != nil || out.State != "abandoned" || out.DeletedKeys < 60000 || out.PublishedNames != 1 {
				t.Fatal(out, err)
			}
			after, err := s.Snapshot(ctx, names[0])
			if err != nil {
				t.Fatal(err)
			}
			afterMeta, _ := s.Metadata(ctx, names[0])
			if afterMeta.ID != published.ID || !bytes.Equal(mustJSON(t, before), mustJSON(t, after)) {
				t.Fatal("published history changed")
			}
			if _, c, err := s.db.Get(binary.BigEndian.AppendUint32(bytes.Clone(hidden), 0)); !errors.Is(err, pebble.ErrNotFound) {
				if c != nil {
					_ = c.Close()
				}
				t.Fatal("hidden row retained", err)
			}
		})
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestNativeAbandonCompletedReceiptAndInvalidIntent(t *testing.T) {
	s, _ := openTestStore(t, "/target", Options{fs: vfs.NewMem(), ExactBlocks: true})
	defer closeTestStore(t, s)
	id := strings.Repeat("c", 64)
	saved := []byte(`{"Identity":"` + id + `","preserve":"exact bytes"}`)
	if err := s.db.Set(nativeReceiptKey(id), saved, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	out, err := s.AbandonNativeCombine(context.Background(), id)
	if err != nil || out.State != "completed" {
		t.Fatal(out, err)
	}
	value, c, err := s.db.Get(nativeReceiptKey(id))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, saved) {
		t.Fatal("receipt changed")
	}
	_ = c.Close()
	bad := strings.Repeat("d", 64)
	if err := s.db.Set(nativeCombineIntentKey(bad), []byte(`{"Receipt":{"Identity":"wrong"},"Spans":[]}`), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AbandonNativeCombine(context.Background(), bad); !errors.Is(err, ErrFormat) {
		t.Fatal(err)
	}
	if err := s.nativeCombineNotAbandoned(bad); err != nil {
		t.Fatal("invalid request fenced", err)
	}
	for _, payload := range []string{`{"Receipt":{"Identity":"` + bad + `"},"Raw":[` + strings.Repeat("[", 66) + `0` + strings.Repeat("]", 66) + `]}`, `{"Receipt":{"Identity":"` + bad + `"},"Raw":"` + strings.Repeat("x", (1<<20)+1) + `"}`} {
		if err := s.walkNativeAbandon(context.Background(), bad, []byte(payload), nil); err == nil {
			t.Fatal("unbounded JSON accepted")
		}
	}
}

func TestNativeAbandonFormatFenceSurvivesCodecUpgrade(t *testing.T) {
	fs := vfs.NewMem()
	s, _ := openTestStore(t, "/target", Options{fs: fs, ExactBlocks: true})
	if err := s.fenceNativeAbandon(); err != nil {
		t.Fatal(err)
	}
	if format, err := checkFormatMarker(fs, "/target"); err != nil || format != 4 {
		t.Fatal(format, err)
	}
	if err := replaceFormatMarker(fs, "/target", formatMarkerReferences); err != nil {
		t.Fatal(err)
	}
	f, err := fs.Open("/target/CHUNKSTORE")
	if err != nil {
		t.Fatal(err)
	}
	var buf [128]byte
	n, err := f.Read(buf[:])
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if string(buf[:n]) != nativeAbandonFormatPrefix+formatMarkerReferences {
		t.Fatal("lost old-reader fence", string(buf[:n]))
	}
	if format, err := checkFormatMarker(fs, "/target"); err != nil || format != 12 {
		t.Fatal(format, err)
	}
	closeTestStore(t, s)
}

func TestNativeAbandonCancellationStopsBoundedDeleteBatch(t *testing.T) {
	s, _ := openTestStore(t, "/target", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	request := combineTestRange([]string{"cancel.name"})
	id, hash, _ := request.Identity()
	prefix := chunkPrefix(12345, 1)
	intent := nativeCombineIntent{Request: request, Receipt: NativeReceipt{Identity: id, NamesSHA256: hash, Count: 1}, Spans: []nativeSpan{{prefix, prefixEnd(prefix)}}}
	if err := s.saveNativeCombineIntent(intent); err != nil {
		t.Fatal(err)
	}
	b := s.db.NewBatch()
	for i := 0; i < 100000; i++ {
		if err := b.Set(binary.BigEndian.AppendUint32(bytes.Clone(prefix), uint32(i)), nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batches := 0
	s.nativeTestHook = func(stage string) error {
		if stage == "abandon-delete-batch" {
			batches++
			cancel()
		}
		return nil
	}
	out, err := s.AbandonNativeCombine(ctx, id)
	if !errors.Is(err, context.Canceled) || batches != 1 || out.DeletedKeys == 0 || out.DeletedKeys > 50000 {
		t.Fatal("unbounded cancellation", out, batches, err)
	}
	s.nativeTestHook = nil
	if out, err := s.AbandonNativeCombine(context.Background(), id); err != nil || out.State != "abandoned" || out.DeletedKeys != 100000 {
		t.Fatal("retry", out, err)
	}
}
