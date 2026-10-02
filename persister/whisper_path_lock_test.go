package persister

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

func createPathLockTestWhisper(t *testing.T, path string) {
	t.Helper()

	db, err := whisper.Create(path, whisper.MustParseRetentionDefs("1s:1h"), whisper.Average, 0.5)
	if err != nil {
		t.Fatalf("create whisper file: %s", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close created whisper file: %s", err)
	}
}

func flockOptions() *whisper.Options {
	return &whisper.Options{FLock: true}
}

func TestPathLockCreatesAndSerializesConcurrentOpens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metric.wsp")
	createPathLockTestWhisper(t, path)

	const openers = 16
	start := make(chan struct{})
	errs := make(chan error, openers)
	var wg sync.WaitGroup
	wg.Add(openers)
	for range openers {
		go func() {
			defer wg.Done()
			<-start
			db, err := whisper.OpenWithOptions(path, flockOptions())
			if err == nil {
				err = db.Close()
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("concurrent flocked open: %s", err)
		}
	}
	if _, err := os.Stat(path + ".lock"); err != nil {
		t.Fatalf("persistent path lock was not created: %s", err)
	}
}

func TestPathLockSurvivesDataRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metric.wsp")
	createPathLockTestWhisper(t, path)

	holder, err := whisper.OpenWithOptions(path, flockOptions())
	if err != nil {
		t.Fatalf("open lock holder: %s", err)
	}
	t.Cleanup(func() { _ = holder.Close() })

	replacement := filepath.Join(dir, "replacement.wsp")
	createPathLockTestWhisper(t, replacement)
	if err := os.Rename(replacement, path); err != nil {
		t.Fatalf("replace data file while path lock is held: %s", err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		db, err := whisper.OpenWithOptions(path, flockOptions())
		if err == nil {
			err = db.Close()
		}
		done <- err
	}()
	<-started

	select {
	case err := <-done:
		t.Fatalf("open after rename completed before the old path lock was released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := holder.Close(); err != nil {
		t.Fatalf("release path lock: %s", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("open replacement after releasing path lock: %s", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("open replacement did not resume after releasing path lock")
	}
}

func BenchmarkPathLockOpenExisting(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "metric.wsp.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		b.Fatalf("create lock file: %s", err)
	}
	if err := file.Close(); err != nil {
		b.Fatalf("close lock file: %s", err)
	}

	for _, flags := range []struct {
		name  string
		flags int
	}{
		{name: "read-write", flags: os.O_RDWR},
		{name: "create-read-write", flags: os.O_CREATE | os.O_RDWR},
	} {
		b.Run(flags.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				f, err := os.OpenFile(path, flags.flags, 0o666)
				if err != nil {
					b.Fatal(err)
				}
				if err := f.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
