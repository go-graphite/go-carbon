package partsread

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
