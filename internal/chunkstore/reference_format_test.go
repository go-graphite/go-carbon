package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

type referenceTestCodec struct{ testColdCodec }

func (referenceTestCodec) NativeReferenceReader() bool { return true }

func TestReferenceFormatFenceAndReaderRejection(t *testing.T) {
	fs := vfs.NewMem()
	opts := Options{fs: fs, ExactBlocks: true, Continuation: referenceTestCodec{}, TimeBuckets: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
	s, _ := openTestStore(t, "/db", opts)
	if s.TransferFormat() != 7 || s.AcceptTransferFormat() != 7 {
		t.Fatal("capability upgraded stored data", s.TransferFormat(), s.AcceptTransferFormat())
	}
	if err := s.fenceReferences(); err != nil {
		t.Fatal(err)
	}
	if s.TransferFormat() != 12 {
		t.Fatal("reference fence missing")
	}
	if err := s.DowngradeColdPack(context.Background()); !errors.Is(err, ErrColdPackIncomplete) {
		t.Fatal("cold downgrade lowered reference fence", err)
	}
	if err := s.DowngradeFloatCodec(context.Background()); !errors.Is(err, ErrColdPackIncomplete) {
		t.Fatal("float downgrade lowered reference fence", err)
	}
	closeTestStore(t, s)
	opts.Continuation = testColdCodec{}
	if bad, err := Open("/db", opts); !errors.Is(err, ErrFormat) {
		if bad != nil {
			bad.Close()
		}
		t.Fatal("codec without reference reader opened v12", err)
	}
	opts.Continuation = referenceTestCodec{}
	s, err := Open("/db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, s)
	if s.TransferFormat() != 12 {
		t.Fatal("reference fence lost on reopen")
	}
}

func TestReferenceFenceSyncFailures(t *testing.T) {
	for _, stage := range []string{"file sync", "rename", "directory sync"} {
		t.Run(stage, func(t *testing.T) {
			fs := &upgradeFailureFS{FS: vfs.NewMem(), stage: stage}
			s, _ := openTestStore(t, "/db", Options{fs: fs, ExactBlocks: true, Continuation: referenceTestCodec{}, TimeBuckets: true})
			defer closeTestStore(t, s)
			s.StopBackground()
			fs.fail.Store(true)
			if err := s.fenceReferences(); err == nil {
				t.Fatal("failed fence accepted")
			}
			if s.referenceFormat.Load() {
				t.Fatal("unsynced reference fence published")
			}
			fs.fail.Store(false)
			if err := s.fenceReferences(); err != nil {
				t.Fatal(err)
			}
			if format, err := checkFormatMarker(fs, s.dir); err != nil || format != 12 {
				t.Fatal(format, err)
			}
		})
	}
}
