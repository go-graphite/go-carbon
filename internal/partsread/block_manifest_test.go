package partsread

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func manifestFixture(t *testing.T) (string, []Sample) {
	t.Helper()
	want := []Sample{{Time: 60, Bits: 0x8000000000000000, Sequence: 1}, {Time: 660, Bits: 0x7ff8000000000042, Sequence: 2}, {Time: 60, Bits: 0xfff0000000000000, Sequence: ^uint64(0)}}
	path, err := PublishBlockPart(t.TempDir(), "generation-1", 1, []BlockData{{SlotBlock: SlotBlock{ID: 7, Archive: Archive{60, 10}, Slots: SlotRange{1, 2}, Key: []byte("pg/x/vshard/3/full/name/archive/60/block/1")}, Samples: want}})
	if err != nil {
		t.Fatal(err)
	}
	return path, want
}

func scanManifest(s Source) ([]Sample, error) {
	var got []Sample
	err := s.Scan(context.Background(), 7, Archive{60, 10}, 0, 120, func(p Sample) error { got = append(got, p); return nil })
	return got, err
}

func TestBlockManifestDurableAndPinned(t *testing.T) {
	path, want := manifestFixture(t)
	source, err := OpenBlockPart(path)
	if err != nil {
		t.Fatal(err)
	}
	if source.Generation != 1 || source.RetainedBytes() <= 0 {
		t.Fatal("missing metadata")
	}
	got, err := scanManifest(source)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("roundtrip %v %v", got, err)
	}
	// Recovery uses the durable manifest, rather than a rebuilt database index.
	source.Close()
	source, err = OpenBlockPart(path)
	if err != nil {
		t.Fatal(err)
	}
	var pub Publication
	pub.Publish(View{Generation: 1, Parts: []Source{source}, RetainedBytes: source.RetainedBytes(), Close: source.Close})
	h, err := pub.Pin(NewBudget(1<<20), 4096)
	if err != nil {
		t.Fatal(err)
	}
	pub.Publish(View{Generation: 2})
	// Retirement removes names, but the old pinned generation owns open inodes.
	if err = os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	got, err = scanManifest(h.v.view.Parts[0])
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("retired pinned data %v %v", got, err)
	}
	pub.Close()
	got, err = scanManifest(source)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("publication close invalidated active view %v", err)
	}
	h.Close()
	if _, err = scanManifest(source); err == nil {
		t.Fatal("last handle did not close file")
	}
	source.Close()
}

func TestBlockManifestCorruptionBeforeEmit(t *testing.T) {
	path, _ := manifestFixture(t)
	source, err := OpenBlockPart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	file, err := os.OpenFile(filepath.Join(path, "blocks"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, 8); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	emitted := false
	err = source.Scan(context.Background(), 7, Archive{60, 10}, 0, 120, func(Sample) error { emitted = true; return nil })
	if err == nil || emitted {
		t.Fatal("corrupt block emitted samples", err)
	}
}

func TestBlockManifestValidation(t *testing.T) {
	for _, tc := range []string{"version", "file", "negative", "length", "truncated", "duplicate", "missing", "symlink"} {
		t.Run(tc, func(t *testing.T) {
			path, _ := manifestFixture(t)
			data, err := os.ReadFile(filepath.Join(path, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			var m BlockManifest
			if err = json.Unmarshal(data, &m); err != nil {
				t.Fatal(err)
			}
			switch tc {
			case "version":
				m.Version++
			case "file":
				m.Blocks[0].Ref.File = "../blocks"
			case "negative":
				m.Blocks[0].Ref.Offset = -1
			case "length":
				m.Blocks[0].Ref.Length = MaxBlockBytes + 1
			case "truncated":
				m.Blocks[0].Ref.Offset = 72
			case "duplicate":
				m.Blocks = append(m.Blocks, m.Blocks[0])
			case "missing":
				m.Blocks[0].Ref.File = "absent"
			case "symlink":
				if err = os.Symlink(filepath.Join(path, "blocks"), filepath.Join(path, "link")); err != nil {
					t.Fatal(err)
				}
				m.Blocks[0].Ref.File = "link"
			}
			data, err = json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(path, "manifest.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			if source, err := OpenBlockPart(path); err == nil {
				source.Close()
				t.Fatal("invalid manifest accepted")
			}
		})
	}
}

func TestBlockManifestCancelAndSourceError(t *testing.T) {
	path, _ := manifestFixture(t)
	s, err := OpenBlockPart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = s.Scan(ctx, 7, Archive{60, 10}, 0, 120, func(Sample) error { t.Fatal("emitted after cancel"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	sentinel := errors.New("stop")
	if err = s.Scan(context.Background(), 7, Archive{60, 10}, 0, 120, func(Sample) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if _, err = PublishBlockPart(filepath.Dir(path), filepath.Base(path), 2, nil); !errors.Is(err, os.ErrExist) {
		t.Fatal("reused published name", err)
	}
	if _, err = PublishBlockPart(filepath.Dir(path), "../escape", 2, nil); err == nil {
		t.Fatal("path traversal")
	}
}

func TestArchiveBlockBytes(t *testing.T) {
	if ArchiveBlockBytes(Archive{1, 86400}) <= ArchiveBlockBytes(Archive{60, 43200}) {
		t.Fatal("archive-aware byte limits lost")
	}
}

func TestBlockManifestPortableReadHandles(t *testing.T) {
	path, want := manifestFixture(t)
	source, err := OpenBlockPartWithIDs(path, func(key []byte) (uint64, error) {
		if string(key) != "pg/x/vshard/3/full/name/archive/60/block/1" {
			t.Fatal("incomplete portable identity")
		}
		return 42, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	var got []Sample
	err = source.Scan(context.Background(), 42, Archive{60, 10}, 0, 120, func(p Sample) error { got = append(got, p); return nil })
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("remapped handle %v %v", got, err)
	}
	if old, err := scanManifest(source); err != nil || len(old) != 0 {
		t.Fatal("stale handle retained", err)
	}
}

func TestBlockManifestArchiveByteLimit(t *testing.T) {
	a := Archive{60, 43200}
	samples := make([]Sample, ArchiveBlockBytes(a)/sampleBytes+1)
	_, err := PublishBlockPart(t.TempDir(), "oversized", 1, []BlockData{{SlotBlock: SlotBlock{ID: 7, Archive: a, Slots: SlotRange{0, 1}, Key: []byte("full-identity")}, Samples: samples}})
	if err == nil {
		t.Fatal("archive byte limit ignored")
	}
}

// A cached block is an immutable verified copy. Reopening a generation must
// still reject on-disk corruption, before any corrupt sample reaches a reader.
func TestBlockManifestVerifiedCacheAndReopen(t *testing.T) {
	path, want := manifestFixture(t)
	s, err := OpenBlockPart(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := scanManifest(s)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, err)
	}
	for _, b := range s.refs {
		if b.decoded.Load() == nil {
			t.Fatal("hot block was not cached")
		}
	}
	f, err := os.OpenFile(filepath.Join(path, "blocks"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte{0xff}, 8); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	got, err = scanManifest(s)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("pinned bytes changed", got, err)
	}
	s.Close()
	if _, err = scanManifest(s); !errors.Is(err, ErrClosed) {
		t.Fatal("closed cached source readable", err)
	}
	s, err = OpenBlockPart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got, err = scanManifest(s); err == nil || len(got) != 0 {
		t.Fatal("reopen missed corruption", got, err)
	}
}
func TestBlockManifestCacheBudgetAndConcurrent(t *testing.T) {
	a := Archive{1, 86400}
	var blocks []BlockData
	for i := 0; i < 20; i++ {
		var samples []Sample
		for j := 0; j < 512; j++ {
			samples = append(samples, Sample{Time: int64(i*512 + j), Bits: uint64(i*512 + j), Sequence: 1})
		}
		blocks = append(blocks, BlockData{SlotBlock: SlotBlock{ID: 7, Archive: a, Slots: SlotRange{int64(i * 512), int64((i + 1) * 512)}, Key: []byte(fmt.Sprintf("complete-block-%d", i))}, Samples: samples})
	}
	path, err := PublishBlockPart(t.TempDir(), "budgeted", 1, blocks)
	if err != nil {
		t.Fatal(err)
	}
	s, err := OpenBlockPartWithCache(path, nil, MaxBlockBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	full, err := OpenBlockPart(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := full.Scan(context.Background(), 7, a, 0, 20*512, func(Sample) error { return nil }); err != nil {
		t.Fatal(err)
	}
	for _, b := range full.refs {
		if b.decoded.Load() == nil {
			t.Fatal("default cache did not cover sealed working set")
		}
	}
	full.Close()
	charged := s.RetainedBytes()
	var wg sync.WaitGroup
	failures := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := 0
			err := s.Scan(context.Background(), 7, a, 0, 20*512, func(p Sample) error {
				if p.Bits != uint64(p.Time) {
					return errors.New("changed bits")
				}
				n++
				return nil
			})
			if err == nil && n != 20*512 {
				err = fmt.Errorf("count %d", n)
			}
			if err != nil {
				failures <- err
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	var cached int64
	for _, b := range s.refs {
		if p := b.decoded.Load(); p != nil {
			cached += int64(cap(*p) * sampleBytes)
		}
	}
	if cached == 0 || cached > MaxBlockBytes || cached+s.cacheRemaining.Load() != MaxBlockBytes {
		t.Fatal("unbounded cache", cached, s.cacheRemaining.Load())
	}
	if s.RetainedBytes() != charged {
		t.Fatal("cache was not reserved before publication")
	}
}

func TestBlockManifestCacheCRCFailureRefund(t *testing.T) {
	path, want := manifestFixture(t)
	original, err := os.ReadFile(filepath.Join(path, "blocks"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := OpenBlockPart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	reserved := s.cacheRemaining.Load()
	broken := append([]byte(nil), original...)
	broken[8] ^= 0xff
	if err = os.WriteFile(filepath.Join(path, "blocks"), broken, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := scanManifest(s); err == nil || len(got) != 0 {
		t.Fatal("corruption emitted", got, err)
	}
	if s.cacheRemaining.Load() != reserved {
		t.Fatal("failed fill leaked capacity")
	}
	for _, b := range s.refs {
		if b.decoded.Load() != nil {
			t.Fatal("failed fill published")
		}
	}
	if err = os.WriteFile(filepath.Join(path, "blocks"), original, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := scanManifest(s)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("retry did not verify", got, err)
	}
}

// A cancellation during a block is observed before Scan returns, and no next
// block is decoded. The number of delivered samples is bounded by one block.
func TestBlockManifestCancelAtBlockBoundary(t *testing.T) {
	path, _ := manifestFixture(t)
	s, err := OpenBlockPart(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	err = s.Scan(ctx, 7, Archive{60, 10}, 0, 120, func(Sample) error { n++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || n == 0 || n > MaxBlockBytes/sampleBytes {
		t.Fatal(n, err)
	}
}

func TestBlockManifestCacheDisabledAndInvalid(t *testing.T) {
	path, want := manifestFixture(t)
	if s, err := OpenBlockPartWithCache(path, nil, -1); err == nil {
		s.Close()
		t.Fatal("negative budget accepted")
	}
	s, err := OpenBlockPartWithCache(path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := scanManifest(s)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, err)
	}
	for _, b := range s.refs {
		if b.decoded.Load() != nil {
			t.Fatal("disabled cache filled")
		}
	}
	f, err := os.OpenFile(filepath.Join(path, "blocks"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte{0xff}, 8)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if got, err = scanManifest(s); err == nil || len(got) != 0 {
		t.Fatal("uncached read missed corruption", got, err)
	}
}

// Source adapters can return after their last yield without polling context.
// Render still observes cancellation at the final source boundary.
type cancelingFinalSource struct{ cancel context.CancelFunc }

func (s cancelingFinalSource) HasArchive(context.Context, uint64, int64) (bool, error) {
	return true, nil
}
func (s cancelingFinalSource) Scan(_ context.Context, _ uint64, _ Archive, _, _ int64, yield func(Sample) error) error {
	s.cancel()
	return yield(Sample{Time: 1, Bits: 42, Sequence: 1})
}
func TestRenderCancellationAtFinalSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var p Publication
	p.Publish(View{Catalog: testCatalog([]Archive{{1, 100}}), Parts: []Source{cancelingFinalSource{cancel}}})
	defer p.Close()
	h, err := p.Pin(NewBudget(1024), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if _, err = h.Render(ctx, "sample", 0, 10, 10); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestBlockManifestRangeFilterDifferential(t *testing.T) {
	for _, step := range []int64{1, 7, 60} {
		a := Archive{Step: step, Points: 10}
		var samples []Sample
		for i := int64(-30); i < 30; i++ {
			timestamp := i * step
			if i%5 == 0 {
				timestamp++
			} // Preserve generic Scan's handling of unaligned data.
			bits := []uint64{0x8000000000000000, 0x7ff8000000000042, 1, 0x7ff0000000000000, 0xfff0000000000000}[int(i+30)%5]
			samples = append(samples, Sample{Time: timestamp, Bits: bits, Sequence: uint64(i + 31)})
		}
		block := SlotBlock{ID: 7, Archive: a, Slots: SlotRange{0, a.Points}, Key: []byte("complete/portable/range-block")}
		path, err := PublishBlockPart(t.TempDir(), "ranges", 1, []BlockData{{SlotBlock: block, Samples: samples}})
		if err != nil {
			t.Fatal(err)
		}
		reference, err := NewSlotIndex([]SlotBlock{block}, func(_ context.Context, _ []byte, _ []SlotRange, yield func(Sample) error) error {
			for _, p := range samples {
				if err := yield(p); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, budget := range []int64{0, 1, DefaultBlockCacheBytes} {
			s, err := OpenBlockPartWithCache(path, nil, budget)
			if err != nil {
				t.Fatal(err)
			}
			for from := int64(-12); from < 12; from++ {
				for _, width := range []int64{0, 1, 3, 9, 10, 14} {
					var got, want []Sample
					lo, hi := from*step, (from+width)*step
					if err := reference.Scan(context.Background(), 7, a, lo, hi, func(p Sample) error { want = append(want, p); return nil }); err != nil {
						t.Fatal(err)
					}
					if err := s.Scan(context.Background(), 7, a, lo, hi, func(p Sample) error { got = append(got, p); return nil }); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("step%d cache%d [%d,%d) got%v want%v", step, budget, lo, hi, got, want)
					}
				}
			}
			s.Close()
		}
	}
}

func BenchmarkBlockManifestShortScan(b *testing.B) {
	a := Archive{Step: 1, Points: 86400}
	var blocks []BlockData
	for n := 0; n < 64; n++ {
		var samples []Sample
		for i := 0; i < 512; i++ {
			samples = append(samples, Sample{Time: int64(100 + i%60), Bits: uint64(i), Sequence: uint64(i + 1)})
		}
		blocks = append(blocks, BlockData{SlotBlock: SlotBlock{ID: 7, Archive: a, Slots: SlotRange{0, a.Points}, Key: []byte(fmt.Sprintf("block-%d", n))}, Samples: samples})
	}
	path, err := PublishBlockPart(b.TempDir(), "bench", 1, blocks)
	if err != nil {
		b.Fatal(err)
	}
	s, err := OpenBlockPart(path)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	yield := func(Sample) error { return nil }
	if err = s.Scan(context.Background(), 7, a, 99, 161, yield); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = s.Scan(context.Background(), 7, a, 99, 161, yield); err != nil {
			b.Fatal(err)
		}
	}
}
