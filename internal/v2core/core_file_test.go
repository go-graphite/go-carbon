package v2core

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/vfs"
)

type coreSyncRecorder struct {
	vfs.File
	writes, durable int
	fail            error
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
