package chunkstore

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func markerIdentity(legacy, stable string) func(vfs.FS, string) (incrementalIdentity, error) {
	return func(vfs.FS, string) (incrementalIdentity, error) {
		return incrementalIdentity{legacy, stable}, nil
	}
}

func TestIncrementalMarkerStableIdentity(t *testing.T) {
	fs := vfs.NewMem()
	if err := fs.MkdirAll("/legacy", 0750); err != nil {
		t.Fatal(err)
	}
	opts := Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy", sourceIdentity: markerIdentity("259005:42", "birth:42:1790000000:000000001")}
	s, err := Open("/store", opts)
	if err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	for _, tc := range []struct {
		name, legacy, stable string
		accept               bool
	}{
		{"renumbered device", "259001:42", "birth:42:1790000000:000000001", true},
		{"replaced directory", "259005:43", "birth:43:1790000000:000000001", false},
		{"reused inode", "259005:42", "birth:42:1790000000:000000002", false},
		{"different filesystem with same inode", "259009:42", "birth:42:1790000001:000000001", false},
		{"birth time unavailable", "259005:42", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts.sourceIdentity = markerIdentity(tc.legacy, tc.stable)
			reopened, err := Open("/store", opts)
			if reopened != nil {
				closeTestStore(t, reopened)
			}
			if tc.accept && err != nil || !tc.accept && !errors.Is(err, ErrFormat) {
				t.Fatalf("accept=%v: %v", tc.accept, err)
			}
		})
	}
}

func TestIncrementalMarkerLegacyUpgrade(t *testing.T) {
	fs := vfs.NewStrictMem()
	s := openFileMigrationStore(t, fs)
	closeTestStore(t, s)
	old := incrementalMarkerLegacy + "/legacy\n259005:42"
	if err := writeStoreMarker(fs, "/store", incrementalMarker, old); err != nil {
		t.Fatal(err)
	}
	opts := Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy", sourceIdentity: markerIdentity("259005:42", "birth:42:1790000000:000000001")}
	s, err := Open("/store", opts)
	if err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	fs.ResetToSyncedState()
	got, err := readStoreMarker(fs, "/store", incrementalMarker)
	if want := strings.TrimSuffix(incrementalMarkerContents("/legacy", "birth:42:1790000000:000000001"), "\n"); err != nil || got != want {
		t.Fatalf("upgraded marker after crash: %q %v", got, err)
	}
	opts.sourceIdentity = markerIdentity("259001:42", "birth:42:1790000000:000000001")
	s, err = Open("/store", opts)
	if err != nil {
		t.Fatal("upgraded marker did not survive renumbering:", err)
	}
	closeTestStore(t, s)
}

func TestIncrementalMarkerLegacyMismatch(t *testing.T) {
	fs := vfs.NewMem()
	s := openFileMigrationStore(t, fs)
	closeTestStore(t, s)
	old := incrementalMarkerLegacy + "/legacy\n259005:42"
	if err := writeStoreMarker(fs, "/store", incrementalMarker, old); err != nil {
		t.Fatal(err)
	}
	opts := Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy", sourceIdentity: markerIdentity("259001:42", "birth:42:1790000000:000000001")}
	if s, err := Open("/store", opts); !errors.Is(err, ErrFormat) {
		if s != nil {
			closeTestStore(t, s)
		}
		t.Fatalf("accepted old mismatching device: %v", err)
	} else {
		for _, text := range []string{"/store/INCREMENTAL-FILE-MIGRATION", "259005:42", "259001:42", "verify", "filesystem", "directory"} {
			if !strings.Contains(err.Error(), text) {
				t.Errorf("missing %q: %v", text, err)
			}
		}
	}
	got, err := readStoreMarker(fs, "/store", incrementalMarker)
	if err != nil || got != old {
		t.Fatalf("mismatch changed marker: %q %v", got, err)
	}
}

func TestIncrementalMarkerCheckpoint(t *testing.T) {
	for _, stable := range []string{"virtual", "birth:42:1790000000:000000001", ""} {
		t.Run(stable, func(t *testing.T) {
			fs := vfs.NewMem()
			if err := fs.MkdirAll("/legacy", 0750); err != nil {
				t.Fatal(err)
			}
			opts := Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy", sourceIdentity: markerIdentity("1:42", stable)}
			s, err := Open("/store", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestStore(t, s)
			if err := s.Checkpoint("/copy"); err != nil {
				t.Fatal(err)
			}
			before, err := readStoreMarker(fs, s.dir, incrementalMarker)
			if err != nil {
				t.Fatal(err)
			}
			opts.sourceIdentity = markerIdentity("2:42", stable)
			copy, err := Open("/copy", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestStore(t, copy)
			after, err := readStoreMarker(fs, copy.dir, incrementalMarker)
			if err != nil || before != after {
				t.Fatalf("checkpoint binding: %q != %q: %v", before, after, err)
			}
		})
	}
}

func TestIncrementalMarkerNativeReadOnlySource(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "original")
	if err := os.Mkdir(source, 0500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(source, 0700)
	identity, err := readIncrementalIdentity(vfs.Default, source)
	if err != nil {
		t.Fatal(err)
	}
	if identity.stable == "" {
		t.Skip("filesystem has no birth time")
	}
	opts := Options{ExactBlocks: true, IncrementalSource: source}
	s, err := Open(filepath.Join(root, "store"), opts)
	if err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	entries, err := os.ReadDir(source)
	if err != nil || len(entries) != 0 {
		t.Fatalf("birth identity wrote to source: %v %v", entries, err)
	}
	// A legacy binding must also upgrade without writing to this source.
	old := incrementalMarkerLegacy + source + "\n" + identity.legacy
	if err := writeStoreMarker(vfs.Default, filepath.Join(root, "store"), incrementalMarker, old); err != nil {
		t.Fatal(err)
	}
	s, err = Open(filepath.Join(root, "store"), opts)
	if err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
}

func TestIncrementalMarkerNoMigrationDoesNotReadIdentity(t *testing.T) {
	s, err := Open("/store", Options{fs: vfs.NewMem(), sourceIdentity: func(vfs.FS, string) (incrementalIdentity, error) { panic("nonincremental source stat") }})
	if err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
}

type incrementalReadOnlyFS struct {
	vfs.FS
	readonly atomic.Bool
}

func (fs *incrementalReadOnlyFS) Create(name string) (vfs.File, error) {
	if strings.HasPrefix(name, "/legacy/") && fs.readonly.Load() {
		return nil, os.ErrPermission
	}
	return fs.FS.Create(name)
}

func TestIncrementalMarkerReadOnlyToken(t *testing.T) {
	fs := &incrementalReadOnlyFS{FS: vfs.NewMem()}
	if err := fs.MkdirAll("/legacy", 0750); err != nil {
		t.Fatal(err)
	}
	opts := Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy", sourceIdentity: markerIdentity("1:42", "")}
	fs.readonly.Store(true)
	if s, err := Open("/store", opts); err == nil || !strings.Contains(err.Error(), "writable source") || !strings.Contains(err.Error(), "/legacy/"+incrementalSourceToken) {
		if s != nil {
			closeTestStore(t, s)
		}
		t.Fatalf("missing first-open permission error: %v", err)
	}
	fs.readonly.Store(false)
	s, err := Open("/store", opts)
	if err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	fs.readonly.Store(true)
	s, err = Open("/store", opts)
	if err != nil {
		t.Fatal("read-only token reopen:", err)
	}
	closeTestStore(t, s)
}

func BenchmarkIncrementalIdentity(b *testing.B) {
	source := b.TempDir()
	b.Run("birth", func(b *testing.B) {
		for b.Loop() {
			identity, err := readIncrementalIdentity(vfs.Default, source)
			if err != nil {
				b.Fatal(err)
			}
			if identity.stable == "" {
				b.Skip("filesystem has no birth time")
			}
		}
	})
	if _, err := createIncrementalToken(vfs.Default, source); err != nil {
		b.Fatal(err)
	}
	b.Run("token read", func(b *testing.B) {
		for b.Loop() {
			if _, err := readIncrementalToken(vfs.Default, source); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("token creation", func(b *testing.B) {
		for b.Loop() {
			if err := os.Remove(filepath.Join(source, incrementalSourceToken)); err != nil {
				b.Fatal(err)
			}
			if _, err := createIncrementalToken(vfs.Default, source); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestIncrementalMarkerTokenFallback(t *testing.T) {
	fs := vfs.NewStrictMem()
	if err := fs.MkdirAll("/legacy", 0750); err != nil {
		t.Fatal(err)
	}
	if err := syncStoreDirectory(fs, "/"); err != nil {
		t.Fatal(err)
	}
	s := openFileMigrationStore(t, fs)
	closeTestStore(t, s)
	if err := writeStoreMarker(fs, "/store", incrementalMarker, incrementalMarkerLegacy+"/legacy\n1:42"); err != nil {
		t.Fatal(err)
	}
	opts := Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy", sourceIdentity: markerIdentity("1:42", "")}
	s, err := Open("/store", opts)
	if err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	fs.ResetToSyncedState()
	bound, err := readStoreMarker(fs, "/store", incrementalMarker)
	if err != nil || !strings.Contains(bound, "\ntoken:") {
		t.Fatalf("missing token binding: %q %v", bound, err)
	}
	// A later kernel gaining birth-time support must still use the token.
	opts.sourceIdentity = markerIdentity("2:42", "birth:42:1790000000:000000001")
	s, err = Open("/store", opts)
	if err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	if err := fs.RemoveAll("/legacy"); err != nil {
		t.Fatal(err)
	}
	if err := fs.MkdirAll("/legacy", 0750); err != nil {
		t.Fatal(err)
	}
	if s, err := Open("/store", opts); !errors.Is(err, ErrFormat) {
		if s != nil {
			closeTestStore(t, s)
		}
		t.Fatalf("accepted replacement without original token: %v", err)
	}
	if _, err := fs.Stat(fs.PathJoin("/legacy", incrementalSourceToken)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recreated a missing token: %v", err)
	}
	if _, err := createIncrementalToken(fs, "/legacy"); err != nil {
		t.Fatal(err)
	}
	if _, err := checkIncrementalMarker(fs, "/store", "/legacy", opts.sourceIdentity); !errors.Is(err, ErrFormat) {
		t.Fatalf("accepted different filesystem token: %v", err)
	}
}

func TestIncrementalMarkerConcurrentTokenCreation(t *testing.T) {
	fs := vfs.NewMem()
	if err := fs.MkdirAll("/legacy", 0750); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	values := make(chan string, 8)
	for range cap(values) {
		group.Go(func() {
			value, err := createIncrementalToken(fs, "/legacy")
			if err != nil {
				t.Error(err)
				return
			}
			values <- value
		})
	}
	group.Wait()
	close(values)
	var first string
	for value := range values {
		if first != "" && first != value {
			t.Errorf("concurrent source bindings differ: %q != %q", first, value)
		}
		first = value
	}
	names, err := fs.List("/legacy")
	if err != nil || len(names) != 1 || names[0] != incrementalSourceToken {
		t.Fatalf("token scratch remains: %v %v", names, err)
	}
}

func TestIncrementalMarkerExistingTokenIsSynced(t *testing.T) {
	fs := vfs.NewStrictMem()
	if err := fs.MkdirAll("/legacy", 0750); err != nil {
		t.Fatal(err)
	}
	if err := syncStoreDirectory(fs, "/"); err != nil {
		t.Fatal(err)
	}
	f, err := fs.Create("/legacy/" + incrementalSourceToken)
	if err != nil {
		t.Fatal(err)
	}
	const token = "0123456789abcdef0123456789abcdef\n"
	_, writeErr := f.Write([]byte(token))
	if err := errors.Join(writeErr, f.Close()); err != nil {
		t.Fatal(err)
	}
	if _, err := createIncrementalToken(fs, "/legacy"); err != nil {
		t.Fatal(err)
	}
	fs.ResetToSyncedState()
	got, err := readIncrementalToken(fs, "/legacy")
	if err != nil || got != "token:"+strings.TrimSpace(token) {
		t.Fatalf("existing token was not made durable: %q %v", got, err)
	}
}

type incrementalFailureFS struct {
	vfs.FS
	stage         string
	fail, renamed atomic.Bool
}

func (fs *incrementalFailureFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err == nil && strings.HasSuffix(name, "/"+incrementalMarker+".tmp") && fs.stage == "file sync" {
		return syncFailFile{File: f, fail: &fs.fail}, nil
	}
	return f, err
}

func (fs *incrementalFailureFS) Rename(oldname, newname string) error {
	if strings.HasSuffix(newname, "/"+incrementalMarker) {
		if fs.fail.Load() && fs.stage == "rename" {
			return errors.New("injected marker rename failure")
		}
		if err := fs.FS.Rename(oldname, newname); err != nil {
			return err
		}
		fs.renamed.Store(true)
		return nil
	}
	return fs.FS.Rename(oldname, newname)
}

func (fs *incrementalFailureFS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.FS.OpenDir(name)
	if err == nil && fs.stage == "directory sync" {
		return incrementalDirectory{f, fs}, nil
	}
	return f, err
}

type incrementalDirectory struct {
	vfs.File
	fs *incrementalFailureFS
}

func (d incrementalDirectory) Sync() error {
	if d.fs.fail.Load() && d.fs.renamed.Load() {
		return errors.New("injected marker directory sync failure")
	}
	return d.File.Sync()
}

func TestIncrementalMarkerUpgradeFailures(t *testing.T) {
	for _, stage := range []string{"file sync", "rename", "directory sync", "lock"} {
		t.Run(stage, func(t *testing.T) {
			strict := vfs.NewStrictMem()
			fs := &incrementalFailureFS{FS: strict, stage: stage}
			s := openFileMigrationStore(t, fs)
			closeTestStore(t, s)
			old := incrementalMarkerLegacy + "/legacy\nvirtual"
			if err := writeStoreMarker(fs, "/store", incrementalMarker, old); err != nil {
				t.Fatal(err)
			}
			fs.renamed.Store(false)
			fs.fail.Store(true)
			var release func() error
			if stage == "lock" {
				lock, err := fs.Lock("/store/LOCK")
				if err != nil {
					t.Fatal(err)
				}
				release = lock.Close
			}
			if s, err := Open("/store", Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy"}); err == nil {
				closeTestStore(t, s)
				t.Fatal("upgrade failure allowed open")
			}
			got, err := readStoreMarker(fs, "/store", incrementalMarker)
			if stage != "directory sync" && (err != nil || got != old) {
				t.Fatalf("failed upgrade changed binding: %q %v", got, err)
			}
			if release != nil {
				if err := release(); err != nil {
					t.Fatal(err)
				}
			}
			fs.fail.Store(false)
			s, err = Open("/store", Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy"})
			if err != nil {
				t.Fatal("retry interrupted upgrade:", err)
			}
			closeTestStore(t, s)
			strict.ResetToSyncedState()
			if _, err := checkIncrementalMarker(fs, "/store", "/legacy", readIncrementalIdentity); err != nil {
				t.Fatal("retry did not make marker durable:", err)
			}

		})
	}
}

func TestIncrementalMarkerRechecksLegacyUnderLock(t *testing.T) {
	fs := vfs.NewMem()
	s := openFileMigrationStore(t, fs)
	closeTestStore(t, s)
	old := incrementalMarkerLegacy + "/legacy\n1:42"
	if err := writeStoreMarker(fs, "/store", incrementalMarker, old); err != nil {
		t.Fatal(err)
	}
	calls := 0
	identify := func(vfs.FS, string) (incrementalIdentity, error) {
		calls++
		if calls == 1 {
			return incrementalIdentity{"1:42", "birth:42:1:000000001"}, nil
		}
		return incrementalIdentity{"1:43", "birth:43:1:000000002"}, nil
	}
	if s, err := Open("/store", Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy", sourceIdentity: identify}); !errors.Is(err, ErrFormat) {
		if s != nil {
			closeTestStore(t, s)
		}
		t.Fatalf("upgraded a replaced legacy source: %v", err)
	}
	got, err := readStoreMarker(fs, "/store", incrementalMarker)
	if err != nil || got != old {
		t.Fatalf("changed legacy marker: %q %v", got, err)
	}
}

func TestCompletedIncrementalMarkerNeverChecksSource(t *testing.T) {
	fs := vfs.NewMem()
	if err := fs.MkdirAll("/legacy", 0750); err != nil {
		t.Fatal(err)
	}
	opts := Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy", sourceIdentity: markerIdentity("dev:inode", "birth:one")}
	db, err := Open("/store", opts)
	if err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, db)
	if err = fs.RemoveAll("/legacy"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	opts.IncrementalSourceComplete = true
	opts.sourceIdentity = func(vfs.FS, string) (incrementalIdentity, error) {
		calls++
		return incrementalIdentity{}, errors.New("source unavailable")
	}
	db, err = Open("/store", opts)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("completed store checked source identity")
	}
	if db.IncrementalSource() != "/legacy" {
		t.Fatal("lost ownership encoding")
	}
	closeTestStore(t, db)
	opts.IncrementalSource = "/different"
	if db, err = Open("/store", opts); !errors.Is(err, ErrFormat) {
		if db != nil {
			closeTestStore(t, db)
		}
		t.Fatalf("accepted changed source binding: %v", err)
	}
	opts.IncrementalSource = "/legacy"
	opts.IncrementalSourceComplete = false
	if db, err = Open("/store", opts); err == nil {
		closeTestStore(t, db)
		t.Fatal("incomplete migration ignored unavailable source")
	}
}
