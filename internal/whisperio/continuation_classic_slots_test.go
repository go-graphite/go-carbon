package whisperio

import (
	"bytes"
	"context"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Preserve the previous slot policy while using the same codec and store.
type noClassicSlotCodec struct{ *ContinuationCodec }

func (c noClassicSlotCodec) StateSlot(key []byte) []byte {
	plain := key
	if len(plain) > 5 && plain[0] == 'T' {
		plain = plain[5:]
	}
	if len(plain) > 9 && plain[9] == 'p' {
		return nil
	}
	return c.ContinuationCodec.StateSlot(key)
}

// The history fixture's tiny cache admits metadata only. Give both sides
// identical spare capacity for optional point groups, as a warmed store has.
func classicSlotFixture(t testing.TB, slots bool) *tailReadFixture {
	t.Helper()
	f := newTailReadFixture(t, false)
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.options.MetadataCacheSize = 128 << 20
	if !slots {
		f.options.Continuation = noClassicSlotCodec{f.options.Continuation.(*ContinuationCodec)}
	}
	var err error
	f.db, err = openTestStore(f.dir, f.options)
	if err != nil {
		t.Fatal(err)
	}
	f.db.StopBackground()
	return f
}

func TestClassicTailSlotsStoreParity(t *testing.T) {
	plain, cached := classicSlotFixture(t, false), classicSlotFixture(t, true)
	// Imported classic archives, group boundaries, deletes and reopen must
	// retain identical stored bytes and bit-for-bit fetch results.
	reads := tailReadStoreParity(t, plain, cached)
	if reads[1] >= reads[0] {
		t.Fatalf("classic slots did not reduce LSM reads: %v", reads)
	}
	t.Logf("64 updates: LSM reads without classic slots=%d, with slots=%d", reads[0], reads[1])
	// Replace a warmed classic generation with its earlier imported image.
	// Its cached groups must not outlive the delete, re-import or late write.
	ctx := context.Background()
	image, err := plain.db.ExportTransfer(ctx, plain.names[:1])
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []*tailReadFixture{plain, cached} {
		f.update(t, 64, 564)
		if err := f.db.Delete(ctx, f.names[0]); err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.ImportTransfer(ctx, image); err != nil {
			t.Fatal(err)
		}
		if err := f.db.UpdateMany(ctx, f.names[0], []store.Point{{Timestamp: f.now.Unix() - 1800, Value: 123.5}}); err != nil {
			t.Fatal(err)
		}
		checkCachedReads(t, f.db, f.names[:1], f.now, "classic re-import and late correction")
	}
}

func TestClassicTailSlotIdentity(t *testing.T) {
	c := &ContinuationCodec{}
	base := continuationBodyKey(1, 'p', 0, 0, 0)
	tail := continuationBodyKey(1, 'p', 0, 0, 1)
	next := continuationBodyKey(1, 'p', 0, 0, 2)
	if bytes.Equal(c.StateSlot(base), c.StateSlot(tail)) {
		t.Fatal("current group displaced the base interval")
	}
	if !bytes.Equal(c.StateSlot(tail), c.StateSlot(next)) {
		t.Fatal("current groups must share one bounded slot")
	}
	for _, other := range [][]byte{
		continuationBodyKey(2, 'p', 0, 0, 1),
		continuationBodyKey(1, 'p', 1, 0, 1),
		continuationBodyKey(1, 'c', 0, 0, 1),
	} {
		if bytes.Equal(c.StateSlot(tail), c.StateSlot(other)) {
			t.Fatal("different file, archive or encoding shares a slot")
		}
	}
	if c.StateSlot(base[:16]) != nil || c.StateKey(base) {
		t.Fatal("classic groups are optional slots, not mandatory metadata")
	}
	if slot := c.StateSlot(tail); len(slot) == 0 || slot[0] != 0 {
		t.Fatal("slot overlaps persistent key space")
	}
}

func BenchmarkClassicTailSlots(b *testing.B) {
	for _, slots := range []bool{false, true} {
		name := "without"
		if slots {
			name = "with"
		}
		b.Run(name, func(b *testing.B) {
			f := classicSlotFixture(b, slots)
			for i := range 32 {
				f.update(b, i, 564)
			}
			count := store.CountStoreReadsForTest()
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				f.update(b, i+32, 564)
			}
			b.StopTimer()
			b.ReportMetric(float64(count())/float64(b.N), "LSM-reads/update")
		})
	}
}
