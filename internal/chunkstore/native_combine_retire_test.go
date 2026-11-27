package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// countIDKeys counts every u/t/r/a/c key of a numeric ID, in any generation.
func countIDKeys(t *testing.T, s *Store, id uint64) int {
	t.Helper()
	it, err := s.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	n := 0
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) >= 22 && key[0] == continuationBucketKeyPrefix {
			key = key[5:]
		}
		if len(key) >= 17 && strings.IndexByte("cura", key[0]) >= 0 && binary.BigEndian.Uint64(key[1:9]) == id {
			n++
		}
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return n
}

type retireFixture struct {
	s       *Store
	opts    Options
	fs      *vfs.MemFS
	request NativeRange
	id      string
	names   []string
	old     map[string]Metadata
	before  map[string][]byte
	padding int // synthetic old-generation keys per published name
}

// newRetireFixture interrupts a combine after publishing all but the last
// name, which keeps its original ID. Each published name's old
// generation gets extra keys so its retirement needs several WAL batches.
func newRetireFixture(t *testing.T, names []string) *retireFixture {
	t.Helper()
	publish := len(names) - 1
	ctx := context.Background()
	f := &retireFixture{fs: vfs.NewStrictMem(), names: names, old: map[string]Metadata{}, before: map[string][]byte{}, padding: 60000}
	f.opts = Options{fs: f.fs, ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour}
	var now int
	f.s, now = openTestStore(t, "/target", f.opts)
	donor, _ := openTestStore(t, "/donor", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, donor)
	for _, name := range names {
		createTestMetric(t, donor, name)
		if err := donor.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 120), Value: 7}}); err != nil {
			t.Fatal(err)
		}
		createTestMetric(t, f.s, name)
		if err := f.s.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 42}}); err != nil {
			t.Fatal(err)
		}
		m, err := f.s.Metadata(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		f.old[name] = m
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
	f.request = combineTestRange(names)
	f.id, _, _ = f.request.Identity()
	published := 0
	f.s.nativeTestHook = func(got string) error {
		if got == "combine-name-published" {
			if published++; published == publish {
				return errors.New("interrupt")
			}
		}
		return nil
	}
	if _, err := f.s.NativeImport(ctx, f.request, &wire); err == nil {
		t.Fatal("not interrupted")
	}
	f.s.nativeTestHook = nil
	for i, name := range names {
		m, err := f.s.Metadata(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if (m.ID != f.old[name].ID) != (i < publish) {
			t.Fatal("unexpected publication", name, m.ID, f.old[name].ID)
		}
		if f.old[name].Continuation {
			t.Fatal("fixture expects the chunk layout")
		}
		snap, err := f.s.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		f.before[name] = mustJSON(t, snap)
		if i < publish {
			// Unreadable extra rows of the replaced generation: only
			// retirement removes them.
			b := f.s.db.NewBatch()
			prefix := chunkPrefixArchive(f.old[name].ID, f.old[name].Generation, 0xfff0)
			for k := 0; k < f.padding; k++ {
				if err := b.Set(binary.BigEndian.AppendUint32(bytes.Clone(prefix), uint32(k)), bytes.Repeat([]byte("v"), 16), nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.Commit(pebble.Sync); err != nil {
				t.Fatal(err)
			}
			_ = b.Close()
		}
	}
	return f
}

func (f *retireFixture) reopen(t *testing.T) {
	t.Helper()
	closeTestStore(t, f.s)
	f.fs.ResetToSyncedState()
	f.fs.SetIgnoreSyncs(false)
	f.s, _ = openTestStore(t, "/target", f.opts)
}

// verify checks that no published name's old generation keeps a key, that
// unpublished names keep theirs, and that every name reads back unchanged.
func (f *retireFixture) verify(t *testing.T) {
	t.Helper()
	publish := len(f.names) - 1
	ctx := context.Background()
	for i, name := range f.names {
		n := countIDKeys(t, f.s, f.old[name].ID)
		if i < publish && n != 0 {
			t.Fatalf("%s: %d keys of replaced ID %d left unreferenced", name, n, f.old[name].ID)
		}
		if i >= publish && n == 0 {
			t.Fatalf("%s: live ID %d lost its keys", name, f.old[name].ID)
		}
		snap, err := f.s.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustJSON(t, snap); !bytes.Equal(got, f.before[name]) {
			t.Fatalf("%s changed:\n%s\n%s", name, f.before[name], got)
		}
	}
}

func TestNativeAbandonRetiresPublishedOldGenerations(t *testing.T) {
	names := []string{"retire.a", "retire.b", "retire.c"}
	const publish = 2
	f := newRetireFixture(t, names)
	defer func() { closeTestStore(t, f.s) }()
	for i := 0; i < publish; i++ {
		if n := countIDKeys(t, f.s, f.old[names[i]].ID); n < f.padding {
			t.Fatal("fixture has no old generation", n)
		}
	}
	batches := 0
	f.s.nativeTestHook = func(got string) error {
		if got == "combine-retire-batch" {
			batches++
		}
		return nil
	}
	out, err := f.s.AbandonNativeCombine(context.Background(), f.id)
	if err != nil || out.State != "abandoned" || out.PublishedNames != publish {
		t.Fatal(out, err)
	}
	if batches < 2*publish {
		t.Fatal("old generations not retired in bounded batches", batches)
	}
	f.s.nativeTestHook = nil
	f.verify(t)
	f.reopen(t)
	f.verify(t)
}

func TestNativeAbandonInterruptedRetireResumes(t *testing.T) {
	for _, stage := range []string{"combine-retire-batch", "combine-name-retired", "combine-retire-pieces", "abandon-retired", "abandon-delete-batch"} {
		t.Run(stage, func(t *testing.T) {
			names := []string{"resume.a", "resume.b", "resume.c"}
			const publish = 2
			f := newRetireFixture(t, names)
			defer func() { closeTestStore(t, f.s) }()
			hits := 0
			f.s.nativeTestHook = func(got string) error {
				// Crash part way: mid retirement, or at the first marker or
				// record-deletion batch.
				if got == stage {
					if hits++; hits == 2 || !strings.HasPrefix(stage, "combine-retire-batch") && stage != "combine-name-retired" {
						f.fs.SetIgnoreSyncs(true)
						return errors.New("crash")
					}
				}
				return nil
			}
			if _, err := f.s.AbandonNativeCombine(context.Background(), f.id); err == nil {
				t.Fatal("not interrupted", hits)
			}
			f.reopen(t)
			// The piece records still name every unretired old generation.
			if _, _, err := f.s.ResumeNativeCombine(context.Background(), f.request); !errors.Is(err, ErrConflict) {
				t.Fatal("cleanup resumed publication", err)
			}
			out, err := f.s.AbandonNativeCombine(context.Background(), f.id)
			if err != nil || out.State != "abandoned" || out.PublishedNames != publish {
				t.Fatal(out, err)
			}
			f.verify(t)
		})
	}
}

// padNativeCombineIntent grows a stored intent past the decode budget with a
// field neither the decoder nor the abandon walk interprets.
func padNativeCombineIntent(t *testing.T, s *Store, id string) {
	t.Helper()
	value, closer, err := s.db.Get(nativeCombineIntentKey(id))
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	b.Write(bytes.TrimSuffix(bytes.TrimSpace(value), []byte("}")))
	_ = closer.Close()
	b.WriteString(`,"Pad":[`)
	item := fmt.Sprintf("%q", strings.Repeat("x", 1000))
	for i := 0; b.Len() <= nativeRangeMetadataBytes; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(item)
	}
	b.WriteString("]}")
	if !json.Valid(b.Bytes()) {
		t.Fatal("padded intent is not JSON")
	}
	if err := s.db.Set(nativeCombineIntentKey(id), b.Bytes(), pebble.Sync); err != nil {
		t.Fatal(err)
	}
}

func TestNativeRetainedOversizedIntentRetiresPublished(t *testing.T) {
	names := []string{"retained.a", "retained.b", "retained.c"}
	const publish = 2
	t.Run("batches", func(t *testing.T) {
		f := newRetireFixture(t, names)
		defer func() { closeTestStore(t, f.s) }()
		padNativeCombineIntent(t, f.s, f.id)
		batches, marks := 0, 0
		f.s.nativeTestHook = func(got string) error {
			switch got {
			case "combine-retire-batch":
				batches++
			case "combine-retire-pieces":
				marks++
			}
			return nil
		}
		// The same call Open makes; run directly to observe its batches.
		if err := f.s.recoverNativeCombines(); err != nil {
			t.Fatal(err)
		}
		if batches < 2*publish || marks != 1 {
			t.Fatal("not retired in bounded batches", batches, marks)
		}
		f.verify(t)
		// Marked pieces are skipped on the next start; the intent is retained.
		batches = 0
		if err := f.s.recoverNativeCombines(); err != nil || batches != 0 {
			t.Fatal("retired again", batches, err)
		}
		if _, c, err := f.s.db.Get(nativeCombineIntentKey(f.id)); err != nil {
			t.Fatal("intent not retained", err)
		} else {
			_ = c.Close()
		}
		f.s.nativeTestHook = nil
		out, err := f.s.AbandonNativeCombine(context.Background(), f.id)
		if err != nil || out.State != "abandoned" || out.PublishedNames != publish {
			t.Fatal(out, err)
		}
		f.verify(t)
	})
	t.Run("crash-then-open", func(t *testing.T) {
		f := newRetireFixture(t, names)
		defer func() { closeTestStore(t, f.s) }()
		padNativeCombineIntent(t, f.s, f.id)
		hits := 0
		f.s.nativeTestHook = func(got string) error {
			if got == "combine-retire-batch" {
				if hits++; hits == 2 {
					f.fs.SetIgnoreSyncs(true)
					return errors.New("crash")
				}
			}
			return nil
		}
		if err := f.s.recoverNativeCombines(); err != nil {
			t.Fatal(err) // Logged and retained; Open is not refused.
		}
		if hits != 2 {
			t.Fatal("not interrupted", hits)
		}
		// Open replays the retirement from the unmarked piece records.
		f.reopen(t)
		f.verify(t)
		if _, c, err := f.s.db.Get(nativeCombineIntentKey(f.id)); err != nil {
			t.Fatal("intent not retained", err)
		} else {
			_ = c.Close()
		}
	})
}
