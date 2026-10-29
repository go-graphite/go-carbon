package whisperio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

type nameCountingKV struct {
	mapContinuationIO
	gets int
	fail error
}

func (k *nameCountingKV) Get(key []byte) ([]byte, error) {
	k.gets++
	if k.fail != nil {
		return nil, k.fail
	}
	return k.mapContinuationIO.Get(key)
}

func TestContinuationAbsentNamesFollowNamespaceMutations(t *testing.T) {
	kv := &nameCountingKV{mapContinuationIO: mapContinuationIO{}}
	fs := newContinuationFiles(kv, false)
	a, b := fs.main(), filepath.Join(fs.dir, "renamed.wsp")
	for i := 0; i < 8; i++ {
		if _, err := fs.Stat(a); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	if kv.gets != 1 {
		t.Fatalf("same missing name read %d times", kv.gets)
	}
	f, err := fs.OpenFile(a, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
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
	if _, err := fs.Stat(a); err != nil {
		t.Fatalf("cached absence hid create: %v", err)
	}
	if _, err := fs.Stat(b); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := fs.Rename(a, b); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat(b); err != nil {
		t.Fatalf("cached absence hid rename: %v", err)
	}
	if _, err := fs.Stat(a); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old name remained after rename: %v", err)
	}
	if err := fs.Remove(b); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat(b); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed name remained: %v", err)
	}
	f, err = fs.OpenFile(b, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("cached absence hid recreate: %v", err)
	}
	if err := f.(*continuationFile).ConfigureStorage(layout); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := fs.flush(); err != nil {
		t.Fatal(err)
	}
	fresh := newContinuationFiles(kv, true)
	// Names are basenames, so this is a new operation over the committed state.
	if _, err := fresh.Stat(filepath.Join(fresh.dir, filepath.Base(b))); err != nil {
		t.Fatalf("fresh operation did not see persisted rename/recreate: %v", err)
	}
}

func TestContinuationNameCacheDoesNotLatchReadErrors(t *testing.T) {
	kv := &nameCountingKV{mapContinuationIO: mapContinuationIO{}, fail: errors.New("injected storage read failure")}
	fs := newContinuationFiles(kv, true)
	if _, err := fs.Stat(fs.main()); !errors.Is(err, kv.fail) {
		t.Fatal(err)
	}
	kv.fail = nil
	if _, err := fs.Stat(fs.main()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if kv.gets != 2 {
		t.Fatal("a storage failure was cached as absence")
	}
	// Absence is local to this operation and does not affect a future snapshot.
	if _, err := newContinuationFiles(kv, true).load("main.wsp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if kv.gets != 3 {
		t.Fatal("absent-name cache escaped its operation")
	}
}

func BenchmarkContinuationAbsentSidecarProbes(b *testing.B) {
	for _, uncached := range []bool{true, false} {
		name := "cached"
		if uncached {
			name = "old-point-probes"
		}
		b.Run(name, func(b *testing.B) {
			kv := &nameCountingKV{mapContinuationIO: mapContinuationIO{}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fs := newContinuationFiles(kv, false)
				for j := 0; j < 4; j++ {
					if uncached {
						delete(fs.files, "main.ooo.wsp")
					}
					if _, err := fs.load("main.ooo.wsp"); !errors.Is(err, os.ErrNotExist) && !errors.Is(err, store.ErrNotFound) {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(kv.gets)/float64(b.N), "LSM-probes/op")
		})
	}
}
