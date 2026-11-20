package whisperpath

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	whisper "github.com/go-graphite/go-whisper"
)

func TestCreateAtomicIncompleteFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metric.wsp")
	failure := errors.New("interrupted allocation")
	_, err := CreateAtomic(path, func(tmp string) (*os.File, error) {
		// This is the observed crash boundary: metadata plus one 16 KiB
		// allocation chunk. Direct creation exposes this unusable file.
		if err := os.WriteFile(tmp, make([]byte, 16424), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("incomplete file was published: %v", err)
		}
		return nil, failure
	}, os.Open)
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed creation left visible files: %v %v", entries, err)
	}
	// Replay can create the same name successfully after the interruption.
	for _, compressed := range []bool{false, true} {
		name := path
		if compressed {
			name = filepath.Join(dir, "compressed.wsp")
		}
		options := &whisper.Options{Compressed: compressed, FLock: true, OutOfOrder: compressed}
		if err := os.WriteFile(name+".ooo", []byte("old incarnation"), 0600); err != nil {
			t.Fatal(err)
		}
		w, err := CreateAtomic(name, func(tmp string) (*whisper.Whisper, error) {
			retention := whisper.NewRetention(60, 120)
			return whisper.CreateWithOptions(tmp, whisper.Retentions{&retention}, whisper.Average, .5, options)
		}, func(final string) (*whisper.Whisper, error) { return whisper.OpenWithOptions(final, options) }, name+".ooo")
		if err != nil {
			t.Fatal(err)
		}
		if w.File().Name() != name {
			t.Fatalf("handle retained temporary path %q", w.File().Name())
		}
		if _, err := os.Stat(name + ".ooo"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("old sidecar survived creation: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCreateAtomicPreservesPublishedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metric.wsp")
	if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".ooo", []byte("existing correction"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := CreateAtomic(path, os.Create, os.Open, path+".ooo")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "existing" {
		t.Fatalf("replaced published file: %q %v", got, err)
	}
	got, err = os.ReadFile(path + ".ooo")
	if err != nil || string(got) != "existing correction" {
		t.Fatalf("removed a published file's sidecar: %q %v", got, err)
	}
}
