package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func TestContinuationPrefetchedNamespaceMatchesPointLookups(t *testing.T) {
	seed := mapContinuationIO{}
	create := func(fs *continuationFiles, name string) {
		t.Helper()
		f, err := fs.OpenFile(filepath.Join(fs.dir, name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		layout := whisper.StorageFileLayout{HeaderSize: 16, Size: 1552, Archives: []whisper.StorageArchiveLayout{{Offset: 16, Step: 1, Count: 128}}}
		if err := f.(*continuationFile).ConfigureStorage(layout); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	initial := newContinuationFiles(seed, false)
	create(initial, "main.wsp")
	create(initial, "other.wsp")
	if err := initial.flush(); err != nil {
		t.Fatal(err)
	}
	// An unopened alias must retain its physical data after the loaded name
	// disappears. Looking only at opened handles would incorrectly delete it.
	seed["n/alias.wsp"] = bytes.Clone(seed["n/main.wsp"])
	clone := func() mapContinuationIO {
		kv := mapContinuationIO{}
		for k, v := range seed {
			kv[k] = bytes.Clone(v)
		}
		return kv
	}
	left, right := clone(), clone()
	point, prefetched := newContinuationFiles(left, false), newContinuationFiles(right, false)
	if err := prefetched.prefetchNames(); err != nil {
		t.Fatal(err)
	}
	held := make(map[*continuationFiles]whisper.StorageFile)
	operations := []func(*continuationFiles) error{
		func(fs *continuationFiles) error {
			f, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
			held[fs] = f
			return err
		},
		func(fs *continuationFiles) error { return fs.Remove(fs.main()) },
		func(fs *continuationFiles) error { _, err := fs.Stat(filepath.Join(fs.dir, "alias.wsp")); return err },
		func(fs *continuationFiles) error { create(fs, "main.wsp"); return nil },
		func(fs *continuationFiles) error { return fs.Rename(fs.main(), filepath.Join(fs.dir, "other.wsp")) },
		func(fs *continuationFiles) error { return fs.Remove(filepath.Join(fs.dir, "alias.wsp")) },
		func(fs *continuationFiles) error { create(fs, "main.wsp"); return nil },
		func(fs *continuationFiles) error { return fs.Remove(filepath.Join(fs.dir, "other.wsp")) },
		func(fs *continuationFiles) error {
			var header [16]byte
			if _, err := held[fs].ReadAt(header[:], 0); err != nil {
				return err
			}
			return held[fs].Close()
		},
	}
	for i, op := range operations {
		for _, fs := range []*continuationFiles{point, prefetched} {
			if err := op(fs); err != nil {
				t.Fatalf("operation %d: %v", i, err)
			}
			if _, err := fs.Stat(filepath.Join(fs.dir, "missing.wsp")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if err := fs.flush(); err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(left, right) {
			t.Fatalf("operation %d changed encoded state", i)
		}
	}
	for key := range left {
		if bytes.HasPrefix([]byte(key), continuationFilePrefix(1)) {
			t.Fatal("unlinked original was retained after its final handle closed")
		}
	}
	// No prefetched state survives a new operation.
	for _, kv := range []mapContinuationIO{left, right} {
		fs := newContinuationFiles(kv, true)
		if _, err := fs.Stat(fs.main()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestContinuationNamespacePrefetchBoundAndInvalidPointer(t *testing.T) {
	for _, count := range []int{64, 65} {
		kv := mapContinuationIO{}
		for i := 0; i < count; i++ {
			kv[fmt.Sprintf("n/f%03d", i)] = binary.BigEndian.AppendUint64(nil, uint64(i+1))
		}
		fs := newContinuationFiles(kv, false)
		if err := fs.prefetchNames(); err != nil {
			t.Fatal(err)
		}
		if (fs.names == nil) != (count > 64) {
			t.Fatalf("namespace count %d: fallback=%v", count, fs.names == nil)
		}
	}
	fs := newContinuationFiles(mapContinuationIO{"n/bad": {1}}, false)
	if err := fs.prefetchNames(); err == nil || fs.names != nil {
		t.Fatalf("accepted invalid pointer: %v", err)
	}
	fs = newContinuationFiles(mapContinuationIO{"n/" + strings.Repeat("x", 256): binary.BigEndian.AppendUint64(nil, 1)}, false)
	if err := fs.prefetchNames(); err != nil || fs.names != nil {
		t.Fatalf("oversized namespace key retained: %v", err)
	}
}

type namespaceErrorIO struct {
	store.ContinuationIO
	fail error
}

func (kv *namespaceErrorIO) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	return kv.ContinuationIO.Visit(prefix, func(key, value []byte) error {
		if err := visit(key, value); err != nil {
			return err
		}
		return kv.fail
	})
}

func TestContinuationNamespaceReadErrorDoesNotCachePartialState(t *testing.T) {
	failure := errors.New("injected namespace read failure")
	values := mapContinuationIO{"n/main.wsp": binary.BigEndian.AppendUint64(nil, 1)}
	kv := &namespaceErrorIO{ContinuationIO: values, fail: failure}
	fs := newContinuationFiles(kv, false)
	if err := fs.prefetchNames(); !errors.Is(err, failure) || fs.names != nil {
		t.Fatalf("partial state survived namespace error: %v", err)
	}
	kv.fail = nil
	values["n/main.wsp"] = binary.BigEndian.AppendUint64(nil, 2)
	if err := fs.prefetchNames(); err != nil || fs.names["main.wsp"] != 2 {
		t.Fatalf("retry used failed namespace: %v", err)
	}
}

type namespaceCancelIO struct {
	store.ContinuationIO
	ctx    context.Context
	cancel context.CancelFunc
}

func (kv namespaceCancelIO) Get(key []byte) ([]byte, error) {
	if err := kv.ctx.Err(); err != nil {
		return nil, err
	}
	value, err := kv.ContinuationIO.Get(key)
	if err == nil && len(key) == 10 && key[0] == 'f' && key[9] == 'h' {
		kv.cancel()
	}
	return value, err
}

func (kv namespaceCancelIO) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	if err := kv.ctx.Err(); err != nil {
		return err
	}
	return kv.ContinuationIO.Visit(prefix, visit)
}

func TestContinuationPrefetchRetainsLateCancellation(t *testing.T) {
	values := mapContinuationIO{}
	codec := &ContinuationCodec{}
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "cancel", Retentions: []store.Retention{{Step: 1, Count: 600}}, AggregationMethod: store.Average, XFilesFactor: .5}}
	clock := func() time.Time { return time.Unix(1792432631, 0) }
	if err := codec.Create(context.Background(), values, m, clock); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv := namespaceCancelIO{ContinuationIO: values, ctx: ctx, cancel: cancel}
	if err := codec.Update(ctx, kv, m, nil, 0, clock, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached namespace hid cancellation after manifest read: %v", err)
	}
}

type namespaceReopenCodec struct {
	ContinuationCodec
	prefetch            bool
	gets, visits, opens int64
}

type namespaceReopenIO struct {
	store.ContinuationIO
	stats *namespaceReopenCodec
}

func (kv namespaceReopenIO) Get(key []byte) ([]byte, error) {
	kv.stats.gets++
	return kv.ContinuationIO.Get(key)
}

func (kv namespaceReopenIO) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	kv.stats.visits++
	return kv.ContinuationIO.Visit(prefix, visit)
}

// Use the real indexed-batch backend while isolating virtual reopen and the
// namespace walk at flush from compression and WAL write costs.
func (c *namespaceReopenCodec) Update(ctx context.Context, kv store.ContinuationIO, _ store.Metadata, _ []store.Point, _ int, clock func() time.Time, _ *store.ContinuationMaintenance) error {
	c.opens++
	kv = namespaceReopenIO{ContinuationIO: kv, stats: c}
	fs := newContinuationFiles(kv, false)
	if c.prefetch {
		if err := fs.prefetchNames(); err != nil {
			return err
		}
	}
	w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
	if err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return fs.flush()
}

func BenchmarkContinuationNamespaceReopen(b *testing.B) {
	for _, workload := range []struct {
		name  string
		count int
		cache int64
	}{
		{"warm", 1024, 16 << 20},
		{"pressure", 4096, 1 << 20},
	} {
		b.Run(workload.name, func(b *testing.B) { benchmarkNamespaceReopen(b, workload.count, workload.cache) })
	}
}

func benchmarkNamespaceReopen(b *testing.B, count int, cache int64) {
	ctx := context.Background()
	codec := &namespaceReopenCodec{}
	db, err := store.Open(b.TempDir(), store.Options{ExactBlocks: true, Continuation: codec, CacheSize: cache, MemTableSize: 4 << 20, SyncInterval: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	names := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("namespace.benchmark.%d", i)
		if _, err := db.Create(ctx, store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 1, Count: 172800}, {Step: 60, Count: 40320}, {Step: 3600, Count: 17520}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.Compact(); err != nil {
		b.Fatal(err)
	}
	for _, prefetch := range []bool{false, true} {
		b.Run(fmt.Sprintf("prefetch=%t", prefetch), func(b *testing.B) {
			codec.prefetch = prefetch
			codec.gets, codec.visits, codec.opens = 0, 0, 0
			before := db.Stats()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.UpdateMany(ctx, names[i%count], nil); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if codec.opens != int64(b.N) {
				b.Fatalf("reopen was bypassed: %d/%d", codec.opens, b.N)
			}
			b.ReportMetric(float64(codec.gets)/float64(b.N), "KV-gets/op")
			b.ReportMetric(float64(codec.visits)/float64(b.N), "KV-visits/op")
			after := db.Stats()
			b.ReportMetric(float64(after.CacheMisses-before.CacheMisses)/float64(b.N), "cache-misses/op")
		})
	}
}
