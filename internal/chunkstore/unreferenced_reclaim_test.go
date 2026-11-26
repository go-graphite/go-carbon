package chunkstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func dumpStoreKeys(t *testing.T, s *Store) map[string]string {
	t.Helper()
	it, err := s.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	keys := make(map[string]string)
	for valid := it.First(); valid; valid = it.Next() {
		keys[string(it.Key())] = string(it.Value())
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		t.Fatal(err)
	}
	return keys
}

// writeGenerationKeys adds keys in every per-ID key space for m: chunks,
// continuation keys of the current and an older generation, time buckets 0
// and 3 (beside the codec's 7 and 9), a transfer receipt and a mutation
// history.
func writeGenerationKeys(t *testing.T, s *Store, m Metadata) {
	t.Helper()
	ctx := context.Background()
	b := s.db.NewIndexedBatch()
	older := m
	older.Generation++
	for _, g := range []Metadata{m, older} {
		io := newContinuationIO(ctx, b, b, g)
		for _, k := range [][]byte{[]byte("fstate"), ContinuationBucketKey(0, []byte("f1")), ContinuationBucketKey(3, []byte("f2"))} {
			if err := io.Set(k, []byte(fmt.Sprintf("%s/%d/%d", k, g.ID, g.Generation))); err != nil {
				t.Fatal(err)
			}
		}
		var c chunk
		c.set(0, Point{Timestamp: 100_000, Value: float64(g.ID)})
		for _, kv := range []struct{ k, v []byte }{
			{chunkKey(g, 0, 3), s.encodeChunk(&c)},
			{transferReceiptKey(g), []byte("receipt")},
			{mutationKey(g), []byte("history")},
		} {
			if err := b.Set(kv.k, kv.v, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
}

// The differential check: after the sweep every key outside the orphaned
// IDs' own keys is byte-identical, every orphaned key is gone, IDs at or above
// the fence and IDs referenced only outside the catalog are untouched, and a
// dry run writes nothing.
func TestReclaimUnreferencedIDsDifferential(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fs := vfs.NewMem()
	opts := Options{fs: fs, ExactBlocks: true, Continuation: bucketCodec{}, TimeBuckets: true, SyncInterval: time.Hour}
	s, _ := openTestStore(t, "/store", opts)
	defer func() {
		if s != nil {
			closeTestStore(t, s)
		}
	}()

	// metric creates name with keys in every per-ID space and returns the
	// keys that belong to its ID (the catalog record and sequence excluded).
	metric := func(name string) (Metadata, map[string]bool) {
		before := dumpStoreKeys(t, s)
		m := createTestMetric(t, s, name)
		writeGenerationKeys(t, s, m)
		owned := make(map[string]bool)
		for k := range dumpStoreKeys(t, s) {
			if _, ok := before[k]; !ok && !strings.HasPrefix(k, "m/") && !strings.HasPrefix(k, "z/") {
				owned[k] = true
			}
		}
		if len(owned) < 12 {
			t.Fatalf("%s owns only %d keys", name, len(owned))
		}
		return m, owned
	}
	// orphan drops the catalog record alone, as a replacement that forgot the
	// old generation did.
	orphan := func(m Metadata) {
		if err := s.db.Delete(catalogKey(m.Name), pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	setJSON := func(key string, v any) {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.db.Set([]byte(key), data, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}

	orphaned := make(map[string]bool)
	var live []Metadata
	add := func(m Metadata, owned map[string]bool, isOrphan bool) {
		if isOrphan {
			orphan(m)
			maps.Copy(orphaned, owned)
		} else {
			live = append(live, m)
		}
	}
	// Runs: two orphans together, one between live metrics, and a replaced
	// name whose old ID stays behind.
	for i, kind := range []bool{false, true, true, false, true, false} {
		m, owned := metric(fmt.Sprintf("a.%d", i))
		add(m, owned, kind)
	}
	replacedOld, owned := metric("replaced")
	add(replacedOld, owned, true)
	replacedNew, _ := metric("replaced")
	if replacedNew.ID == replacedOld.ID {
		t.Fatal("replacement reused the ID")
	}
	live = append(live, replacedNew)

	// One ID referenced only outside the catalog for each reference kind.
	refs := map[string]func(Metadata){
		"native-intent": func(m Metadata) { setJSON("NATIVE/intent/n1", nativeIntent{First: m.ID, Count: 1}) },
		"native-receipt": func(m Metadata) {
			setJSON("NATIVE/receipt/n2", NativeReceipt{Identity: "n2", Mappings: []NativeMapping{{TargetID: m.ID, TargetGeneration: m.Generation}}})
		},
		"combine-intent": func(m Metadata) {
			setJSON("COMBINE/intent/c1", nativeCombineIntent{Receipt: NativeReceipt{Identity: "c1", FirstID: m.ID, ReservedIDs: 1}})
		},
		"combine-origin": func(m Metadata) {
			setJSON("COMBINE/intent/c2", nativeCombineIntent{Receipt: NativeReceipt{Identity: "c2"}, Origins: []nativeCombineOrigin{{ID: m.ID, Generation: m.Generation}}})
		},
		"combine-piece-meta": func(m Metadata) {
			setJSON(string(nativeCombinePieceKey("c3", 0)), nativeCombinePiece{Meta: nativeCombineMetadata(m)})
		},
		"combine-piece-old": func(m Metadata) {
			setJSON(string(nativeCombinePieceKey("c3", 1)), nativeCombinePiece{Position: 1, Old: nativeCombineMetadata(m)})
		},
		"transfer-delete": func(m Metadata) {
			if err := s.db.Set(transferDeleteKey(m), []byte("staged"), pebble.Sync); err != nil {
				t.Fatal(err)
			}
		},
		"file-cleanup": func(m Metadata) {
			if err := s.db.Set(fileCleanupKey(m.ID), []byte(m.Name), pebble.Sync); err != nil {
				t.Fatal(err)
			}
		},
	}
	for kind, reference := range refs {
		m, _ := metric("ref." + kind)
		orphan(m)
		reference(m)
	}
	// An orphan between referenced IDs, then the fence.
	m, owned := metric("b.orphan")
	add(m, owned, true)
	fenced, _ := metric("fenced")
	orphan(fenced)
	for i := range 3 {
		m, _ := metric(fmt.Sprintf("tail.%d", i))
		live = append(live, m)
	}
	watermark, err := s.CatalogHighWatermark(ctx)
	if err != nil {
		t.Fatal(err)
	}
	opt := UnreferencedOptions{Below: fenced.ID, Margin: watermark - fenced.ID, CountKeys: true}

	tooHigh := opt
	tooHigh.Below++
	if _, err := s.ReclaimUnreferencedIDs(ctx, tooHigh); err == nil || !strings.Contains(err.Error(), "watermark") {
		t.Fatalf("fence above watermark minus margin: %v", err)
	}

	before := dumpStoreKeys(t, s)
	dry, err := s.ReclaimUnreferencedIDs(ctx, opt)
	if err != nil {
		t.Fatal(err)
	}
	if after := dumpStoreKeys(t, s); !maps.Equal(before, after) {
		t.Fatal("dry run changed the store")
	}
	if dry.Keys != int64(len(orphaned)) || dry.RangeDeletes != 0 {
		t.Fatalf("dry run counted %d keys and %d deletes, want %d and 0: %+v", dry.Keys, dry.RangeDeletes, len(orphaned), dry)
	}
	for _, kind := range []string{"catalog", "native-intent", "native-receipt", "combine-intent", "combine-piece", "transfer-delete", "file-cleanup"} {
		if dry.References[kind] == 0 {
			t.Fatalf("no %s reference counted: %v", kind, dry.References)
		}
	}
	// a.1-a.2, a.4, replaced and b.orphan form four runs; spaces c, u, r, a,
	// proof/ranges and buckets 0, 3, 7 and 9 each hold every run.
	if dry.Buckets != 4 || len(dry.Spaces) != 9 || dry.Ranges != 4*9 {
		t.Fatalf("unexpected layout: %+v", dry)
	}

	applied := opt
	applied.Apply = true
	got, err := s.ReclaimUnreferencedIDs(ctx, applied)
	if err != nil {
		t.Fatal(err)
	}
	if got.RangeDeletes != dry.Ranges || got.Keys != dry.Keys {
		t.Fatalf("apply wrote %d deletes over %d keys, dry run %d over %d", got.RangeDeletes, got.Keys, dry.Ranges, dry.Keys)
	}
	check := func(stage string) {
		t.Helper()
		after := dumpStoreKeys(t, s)
		for k, v := range before {
			if orphaned[k] {
				if _, ok := after[k]; ok {
					t.Fatalf("%s: orphaned key %q survived", stage, k)
				}
			} else if after[k] != v {
				t.Fatalf("%s: key %q changed", stage, k)
			}
		}
		if len(after) != len(before)-len(orphaned) {
			t.Fatalf("%s: %d keys, want %d", stage, len(after), len(before)-len(orphaned))
		}
		for _, m := range live {
			got, err := s.Metadata(ctx, m.Name)
			if err != nil || got.ID != m.ID {
				t.Fatalf("%s: live %s reads %+v, %v", stage, m.Name, got, err)
			}
		}
	}
	check("apply")
	again, err := s.ReclaimUnreferencedIDs(ctx, opt)
	if err != nil || again.Ranges != 0 {
		t.Fatalf("second dry run: %+v, %v", again, err)
	}

	// The synthetic native records would fail recovery; a read-only open
	// recovers nothing and proves the range deletions are durable.
	closeTestStore(t, s)
	s = nil
	readOnly := opts
	readOnly.ReadOnly = true
	s, _ = openTestStore(t, "/store", readOnly)
	check("reopen")
}

func TestReclaimUnreferencedIDsMinRunAndReadOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fs := vfs.NewMem()
	opts := Options{fs: fs, ExactBlocks: true, Continuation: bucketCodec{}, TimeBuckets: true, SyncInterval: time.Hour}
	s, _ := openTestStore(t, "/store", opts)
	var ids []Metadata
	for i := range 8 {
		ids = append(ids, createTestMetric(t, s, fmt.Sprintf("m.%d", i)))
	}
	// One single-ID run (m.1) and one two-ID run (m.3-m.4).
	for _, i := range []int{1, 3, 4} {
		if err := s.db.Delete(catalogKey(ids[i].Name), pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	closeTestStore(t, s)
	readOnly := opts
	readOnly.ReadOnly = true
	s, _ = openTestStore(t, "/store", readOnly)
	defer closeTestStore(t, s)
	opt := UnreferencedOptions{Below: ids[7].ID, MinRunIDs: 2}
	if _, err := s.ReclaimUnreferencedIDs(ctx, UnreferencedOptions{Below: opt.Below, Apply: true}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("apply on a read-only store: %v", err)
	}
	got, err := s.ReclaimUnreferencedIDs(ctx, opt)
	if err != nil {
		t.Fatal(err)
	}
	if got.Runs != 2 || got.RunIDs != 3 || got.ShortRuns != 1 || got.ShortRunIDs != 1 || got.Samples[0] != [2]uint64{ids[3].ID, ids[5].ID} {
		t.Fatalf("unexpected runs: %+v", got)
	}
	// bucketCodec writes u, r, and buckets 7 and 9 for each metric.
	if got.Ranges != 4 {
		t.Fatalf("ranges %d, want 4: %+v", got.Ranges, got)
	}
}

func TestIDBitsRuns(t *testing.T) {
	t.Parallel()
	b := make(idBits, 4)
	for _, id := range []uint64{3, 64, 65, 200} {
		b.set(id)
	}
	for _, c := range []struct {
		fn            string
		id, arg, want uint64
	}{
		{"nextSet", 0, 256, 3}, {"nextSet", 4, 256, 64}, {"nextSet", 66, 150, 150}, {"nextSet", 66, 256, 200},
		{"nextClear", 64, 256, 66}, {"nextClear", 3, 256, 4}, {"nextClear", 0, 256, 0},
		{"prevSet", 63, 1, 4}, {"prevSet", 66, 1, 66}, {"prevSet", 199, 1, 66}, {"prevSet", 2, 1, 1}, {"prevSet", 150, 100, 100},
	} {
		var got uint64
		switch c.fn {
		case "nextSet":
			got = b.nextSet(c.id, c.arg)
		case "nextClear":
			got = b.nextClear(c.id, c.arg)
		case "prevSet":
			got = b.prevSet(c.id, c.arg)
		}
		if got != c.want {
			t.Errorf("%s(%d, %d) = %d, want %d", c.fn, c.id, c.arg, got, c.want)
		}
	}
}
