package whisperio

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func classicProbeStore(tb testing.TB) (*store.Store, []string, int) {
	tb.Helper()
	m, kv, now := tailClassicSeed(tb, store.Last, 0)
	db, err := openTestStore(tb.TempDir(), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, Now: func() time.Time { return time.Unix(now, 0) }, CacheSize: 16 << 20, MemTableSize: 1 << 20, L0CompactionThreshold: 100, L0StopWritesThreshold: 200})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	db.StopBackground()
	names := make([]string, 16)
	for i := range names {
		m.Name = fmt.Sprintf("classic.probe.%02d", i)
		names[i] = m.Name
		if _, err := db.ImportTransfer(context.Background(), []store.TransferMetric{classicTransfer(kv, store.Snapshot{Metadata: m})}); err != nil {
			tb.Fatal(err)
		}
		if i%2 == 1 {
			if err := db.Flush(); err != nil {
				tb.Fatal(err)
			}
		}
	}
	return db, names, int(now)
}

func TestClassicFetchProbeVerdicts(t *testing.T) {
	db, names, now := classicProbeStore(t)
	ctx := context.Background()
	read := func(shared bool) [][32]byte {
		restore := db.SetFetchSharedIteratorForTest(shared)
		defer restore()
		hashes := make([][32]byte, len(names))
		for i, name := range names {
			s, err := db.Fetch(ctx, name, now-8*86400, now)
			if err != nil {
				t.Fatal(err)
			}
			hashes[i] = shaSeries(s, now-8*86400, now)
		}
		return hashes
	}
	before := read(false)
	if got := read(true); !reflect.DeepEqual(before, got) {
		t.Fatal("shared iterator changed exact digests")
	}
	// A correction must produce the same mismatch set with either lookup path.
	if err := db.UpdateMany(ctx, names[5], []store.Point{{Timestamp: int64(now - 600), Value: 12345}}); err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, shared := range []bool{false, true} {
		got := read(shared)
		var mismatch []string
		for i := range got {
			if got[i] != before[i] {
				mismatch = append(mismatch, names[i])
			}
		}
		if !shared {
			want = mismatch
		} else if !reflect.DeepEqual(want, mismatch) {
			t.Fatalf("mismatch sets changed: %v vs %v", want, mismatch)
		}
	}
	if !reflect.DeepEqual(want, []string{names[5]}) {
		t.Fatalf("correction not detected: %v", want)
	}
}

func BenchmarkClassicFetchProbes(b *testing.B) {
	db, names, now := classicProbeStore(b)
	for _, shared := range []bool{false, true} {
		b.Run(fmt.Sprintf("shared=%v", shared), func(b *testing.B) {
			restore := db.SetFetchSharedIteratorForTest(shared)
			defer restore()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := db.Fetch(context.Background(), names[0], now-8*86400, now); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
