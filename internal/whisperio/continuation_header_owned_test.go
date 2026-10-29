package whisperio

import (
	"bytes"
	"os"
	"reflect"
	"testing"

	whisper "github.com/go-graphite/go-whisper"
)

func ownedHeaderFixture(compressed bool, archives int) *continuationFileData {
	d := &continuationFileData{pages: make(map[uint32]bool)}
	d.layout = whisper.StorageFileLayout{Compressed: compressed, HeaderSize: 4096, Size: 4096}
	for i := 0; i < archives; i++ {
		a := whisper.StorageArchiveLayout{Offset: d.layout.Size, Step: 60, Count: 1024}
		if compressed {
			a.BlockSize, a.BlockCount = 65536, 1
			d.layout.Size += 65536
		} else {
			d.layout.Size += a.Count * 12
		}
		d.layout.Archives = append(d.layout.Archives, a)
	}
	d.header = bytes.Repeat([]byte{0x35}, d.layout.HeaderSize)
	return d
}

func TestContinuationOwnedHeaderRetentionBound(t *testing.T) {
	for _, tc := range []struct {
		name       string
		compressed bool
		archives   int
		extra      int
		reuse      bool
	}{
		{"compressed", true, 1, 0, true},
		{"compressed-capacity-slack", true, 1, 128, true},
		{"compressed-large-layout", true, 64, 0, false},
		{"compressed-large-buffer", true, 1, 8192, false},
		{"classic", false, 1, 0, false},
		{"classic-small-header-many-pages", false, 1, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := ownedHeaderFixture(tc.compressed, tc.archives)
			if tc.name == "classic-small-header-many-pages" {
				want.layout.HeaderSize = 16
				want.layout.Archives[0].Offset = 16
				want.layout.Archives[0].Count = 400000
				want.layout.Size = 16 + 400000*12
				want.header = want.header[:16]
				for i := uint32(0); i < 1000; i++ {
					want.pages[i] = true
				}
			}
			encoded, err := encodeContinuationManifest(want)
			if err != nil {
				t.Fatal(err)
			}
			owned := make([]byte, len(encoded), len(encoded)+tc.extra)
			copy(owned, encoded)
			got, err := decodeContinuationManifest(owned)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatal("manifest round trip changed layout, pages, or header")
			}
			reused := &got.header[0] == &owned[len(owned)-len(got.header)]
			if reused != tc.reuse {
				t.Fatalf("retention policy: reused=%t, want %t", reused, tc.reuse)
			}
			if reused && cap(owned)-len(got.header) > 256 {
				t.Fatal("header retains more than 256 extra bytes")
			}
			if !reused {
				// A large layout/buffer must be releasable independently.
				owned[len(owned)-1] ^= 0xff
				if !bytes.Equal(got.header, want.header) {
					t.Fatal("fallback header still aliases the oversized manifest")
				}
			}
			if gotBytes, err := encodeContinuationManifest(got); err != nil || !bytes.Equal(gotBytes, encoded) {
				t.Fatalf("encoded manifest differs from reference: %v", err)
			}
		})
	}
}

func TestContinuationOwnedHeaderOperationIsolation(t *testing.T) {
	for _, backend := range []string{"base", "overlay-base", "overlay-value"} {
		t.Run(backend, func(t *testing.T) {
			base := mapContinuationIO{}
			initial := newContinuationFiles(base, false)
			f, err := initial.OpenFile(initial.main(), os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			fixture := ownedHeaderFixture(true, 1)
			if err := f.(*continuationFile).ConfigureStorage(fixture.layout); err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteAt(fixture.header, 0); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if err := initial.flush(); err != nil {
				t.Fatal(err)
			}
			kv := initial.kv
			if backend != "base" {
				o := newContinuationOverlay(base)
				if backend == "overlay-value" {
					for k, v := range base {
						if err := o.Set([]byte(k), v); err != nil {
							t.Fatal(err)
						}
					}
				}
				kv = o
			}
			writer := newContinuationFiles(kv, false)
			snapshot := newContinuationFiles(kv, true)
			old, err := snapshot.load("main.wsp")
			if err != nil {
				t.Fatal(err)
			}
			f, err = writer.OpenFile(writer.main(), os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			change := []byte{0x13, 0x42, 0x57}
			at := fixture.layout.HeaderSize - len(change)
			if _, err := f.WriteAt(change, int64(at)); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			check := func(fs *continuationFiles, want []byte) {
				t.Helper()
				got, err := fs.load("main.wsp")
				if err != nil || !bytes.Equal(got.header, want) {
					t.Fatalf("header isolation/persistence: %v", err)
				}
			}
			// Aborting this operation now would leave persisted state untouched.
			check(newContinuationFiles(kv, true), fixture.header)
			if err := writer.flush(); err != nil {
				t.Fatal(err)
			}
			want := bytes.Clone(fixture.header)
			copy(want[at:], change)
			check(newContinuationFiles(kv, true), want)
			if !bytes.Equal(old.header, fixture.header) {
				t.Fatal("a later flush mutated an earlier operation's header")
			}
			if backend != "base" {
				check(newContinuationFiles(base, true), fixture.header)
			}
		})
	}
}
