package main

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

type coreSyncRecorder struct {
	vfs.File
	writes, durable int
	fail            error
}

func TestCorePreallocatedSSTBytesMatchBuffered(t *testing.T) {
	var outputs [][]byte
	for _, preallocate := range []bool{false, true} {
		fs := vfs.NewMem()
		file, err := fs.Create("stage.sst")
		if err != nil {
			t.Fatal(err)
		}
		owner := new(coreOwner)
		owner.fileBuffer.preallocate = preallocate
		writer := sstable.NewWriter(objstorageprovider.NewFileWritable(owner.stageFile(file)), sstable.WriterOptions{Compression: sstable.SnappyCompression})
		for i := range 1000 {
			key := []byte{byte(i >> 8), byte(i)}
			if err = writer.Set(key, bytes.Repeat([]byte{0x81, 0, 0xff, 0x7f}, 128)); err != nil {
				t.Fatal(err)
			}
		}
		if err = writer.Close(); err != nil {
			t.Fatal(err)
		}
		r, err := fs.Open("stage.sst")
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, got)
		if owner.syncCalls.Load() != 1 {
			t.Fatal("SST was not synced")
		}
	}
	if !bytes.Equal(outputs[0], outputs[1]) {
		t.Fatal("preallocation changed SST bytes")
	}
}

type corePreallocationRecorder struct {
	vfs.File
	size                              int64
	preallocateErr, writeErr, syncErr error
	short                             bool
	writes, syncs                     int
}

func (f *corePreallocationRecorder) Preallocate(offset, length int64) error {
	if offset != 0 {
		return errors.New("unexpected offset")
	}
	f.size = length
	return f.preallocateErr
}
func (f *corePreallocationRecorder) Write(p []byte) (int, error) {
	f.writes++
	if f.short {
		return len(p) - 1, nil
	}
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.File.Write(p)
}
func (f *corePreallocationRecorder) Sync() error { f.syncs++; return f.syncErr }

func TestCorePreallocationErrorsPreventDurableFinish(t *testing.T) {
	failure := errors.New("injected IO failure")
	for _, stage := range []string{"preallocate", "write", "short-write", "sync", "success"} {
		t.Run(stage, func(t *testing.T) {
			file, err := vfs.NewMem().Create("stage.sst")
			if err != nil {
				t.Fatal(err)
			}
			recorder := &corePreallocationRecorder{File: file}
			want := failure
			switch stage {
			case "preallocate":
				recorder.preallocateErr = failure
			case "write":
				recorder.writeErr = failure
			case "short-write":
				recorder.short = true
				want = io.ErrShortWrite
			case "sync":
				recorder.syncErr = failure
			case "success":
				want = nil
			}
			owner := new(coreOwner)
			owner.fileBuffer.preallocate = true
			writer := objstorageprovider.NewFileWritable(owner.stageFile(recorder))
			payload := bytes.Repeat([]byte{0x81}, 17000)
			if err = writer.Write(payload); err != nil {
				t.Fatal(err)
			}
			err = writer.Finish()
			if !errors.Is(err, want) || recorder.size != int64(len(payload)) {
				t.Fatalf("err=%v size=%d", err, recorder.size)
			}
			if stage == "success" && (recorder.writes != 1 || recorder.syncs != 1) {
				t.Fatal("success did not write and sync exactly once")
			}
			if stage != "success" && stage != "sync" && recorder.syncs != 0 {
				t.Fatal("failed output was synced")
			}
			if recorder.writes > 1 {
				t.Fatal("failed output was retried on Close")
			}
		})
	}
}

func (f *coreSyncRecorder) Write(p []byte) (int, error) { f.writes++; return f.File.Write(p) }
func (f *coreSyncRecorder) Sync() error                 { f.durable++; return f.fail }

func TestCoreSSTBufferKeepsBytesAndDurableFinish(t *testing.T) {
	for _, fail := range []error{nil, errors.New("sync failed")} {
		fs := vfs.NewMem()
		file, err := fs.Create("stage.sst")
		if err != nil {
			t.Fatal(err)
		}
		recorder := &coreSyncRecorder{File: file, fail: fail}
		owner := new(coreOwner)
		writer := objstorageprovider.NewFileWritable(owner.stageFile(recorder))
		block := bytes.Repeat([]byte{0xa5}, 4096)
		for range 1000 {
			if err = writer.Write(block); err != nil {
				t.Fatal(err)
			}
		}
		err = writer.Finish()
		if !errors.Is(err, fail) || recorder.durable != 1 || owner.syncCalls.Load() != 1 {
			t.Fatalf("Finish err=%v durable=%d measured=%d", err, recorder.durable, owner.syncCalls.Load())
		}
		if recorder.writes > 4 {
			t.Fatalf("uncoalesced writes: %d", recorder.writes)
		}
		r, err := fs.Open("stage.sst")
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil || !bytes.Equal(got, bytes.Repeat(block, 1000)) {
			t.Fatalf("buffer changed bytes: %v", err)
		}
	}
}
