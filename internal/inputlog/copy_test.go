package inputlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCopyPinRetainsReplayHistoryAndReleases(t *testing.T) {
	source := newFakeSource()
	source.setLow(0, true)
	dir := t.TempDir()
	log := openStarted(t, dir, Options{SegmentSize: 1024}, source)
	defer log.Close()
	if _, err := log.CaptureCopy(); err == nil {
		t.Fatal("capture without retention accepted")
	}
	source.add("before", 1, 1)
	first, err := log.PinCopy()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.PinCopy(); err == nil {
		t.Fatal("second pin accepted")
	}
	want := recorded{}
	want.add("before", 1, 1)
	for cycle := range 12 {
		for metric := range 100 {
			name := fmt.Sprintf("live.%d", metric)
			source.add(name, float64(cycle), int64(cycle+2))
			want.add(name, float64(cycle), int64(cycle+2))
		}
		if err := log.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	final, err := log.CaptureCopy()
	if err != nil {
		t.Fatal(err)
	}
	if final.From != first.From || len(final.Files) < 3 {
		t.Fatalf("retention did not span segments: %+v", final)
	}
	target := t.TempDir()
	for _, file := range final.Files {
		src, err := os.Open(filepath.Join(dir, file.Name))
		if err != nil {
			t.Fatal(err)
		}
		dst, err := os.Create(filepath.Join(target, file.Name))
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.CopyN(dst, src, file.Size)
		if err != nil {
			t.Fatal(err)
		}
		_ = src.Close()
		_ = dst.Close()
	}
	if err := ValidateCopyFiles(target, final.Files); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(target, final.Files[0].Name)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var original [1]byte
	if _, err := f.ReadAt(original[:], segmentHeaderSize); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{original[0] ^ 1}, segmentHeaderSize); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCopyFiles(target, final.Files); err == nil {
		t.Fatal("captured corrupt frame header accepted")
	}
	if _, err := f.WriteAt(original[:], segmentHeaderSize); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := log.ReleaseCopy(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := log.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if files(t, dir, logSuffix) > 1 {
		t.Fatal("retention was not released")
	}
	cloned, got, stats := reopen(t, target, Options{SegmentSize: 1024}, newFakeSource())
	defer cloned.Close()
	if stats.Corrupt != 0 {
		t.Fatal(stats)
	}
	equalRecorded(t, got, want)
}

func TestCopyPinEmptyLog(t *testing.T) {
	log := openStarted(t, t.TempDir(), Options{}, newFakeSource())
	defer log.Close()
	state, err := log.PinCopy()
	if err != nil || state.From.Segment == 0 || len(state.Files) != 0 {
		t.Fatal(state, err)
	}
	if err := log.ReleaseCopy(); err != nil {
		t.Fatal(err)
	}
}
