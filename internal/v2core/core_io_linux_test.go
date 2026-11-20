//go:build linux

package v2core

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

func TestCoreDirectSSTBytesMatchBuffered(t *testing.T) {
	var outputs [][]byte
	for _, direct := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "stage.sst")
		file, err := vfs.Default.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		owner := new(coreOwner)
		owner.fileBuffer.preallocate = direct
		owner.fileBuffer.direct = direct
		writer := sstable.NewWriter(objstorageprovider.NewFileWritable(owner.stageFile(file)), sstable.WriterOptions{Compression: sstable.SnappyCompression})
		for i := range 1000 {
			if err = writer.Set([]byte{byte(i >> 8), byte(i)}, bytes.Repeat([]byte{0x81, 0, 0xff, 0x7f}, 128)); err != nil {
				t.Fatal(err)
			}
		}
		if err = writer.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, got)
		if owner.syncCalls.Load() != 1 {
			t.Fatal("SST was not synced")
		}
	}
	if !bytes.Equal(outputs[0], outputs[1]) {
		t.Fatal("direct output changed SST bytes")
	}
}

func TestCoreDirectAlignmentAndScratchReuse(t *testing.T) {
	var scratch []byte
	for _, size := range []int{1, 4095, 4096, 4097, 17001, 3} {
		path := filepath.Join(t.TempDir(), "stage")
		file, err := vfs.Default.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		data := bytes.Repeat([]byte{byte(size)}, size)
		scratch, err = coreWriteDirect(file, data, scratch)
		if err != nil {
			t.Fatal(err)
		}
		info, err := file.Stat()
		if err != nil || info.Size() != int64(size) {
			t.Fatalf("before sync: size=%v err=%v", info, err)
		}
		if err = file.Sync(); err != nil {
			t.Fatal(err)
		}
		if err = file.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("size=%d bytes changed: %v", size, err)
		}
	}
}

type coreDirectShortFile struct{ vfs.File }

func (f coreDirectShortFile) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestCoreDirectShortWriteFailsBeforeSync(t *testing.T) {
	file, err := vfs.Default.Create(filepath.Join(t.TempDir(), "stage"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	_, err = coreWriteDirect(coreDirectShortFile{file}, []byte{1}, nil)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("got %v", err)
	}
}
