package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func TestFloatFormatFenceAndFailure(t *testing.T) {
	fs := vfs.NewMem()
	s, _ := openTestStore(t, "/store", Options{fs: fs, ExactBlocks: true, TimeBuckets: true, Continuation: testColdCodec{}})
	defer closeTestStore(t, s)
	s.stopColdPack()
	if err := s.ConfigureColdPack(ColdPackOptions{Mode: "exact", FloatCodec: true}); err != nil {
		t.Fatal(err)
	}
	if s.TransferFormat() != 9 {
		t.Fatal("flag eagerly fenced")
	}
	if err := s.fenceFloatCodec(); err != nil {
		t.Fatal(err)
	}
	if format, err := checkFormatMarker(fs, s.dir); err != nil || format != 11 {
		t.Fatalf("format=%d %v", format, err)
	}
	if err := s.ConfigureColdPack(ColdPackOptions{Mode: "float-downgrade"}); err != nil {
		t.Fatal(err)
	}
	if err := s.DowngradeFloatCodec(context.Background()); err == nil {
		t.Fatal("partial sweep lowered fence")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.DowngradeFloatCodec(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestFloatDowngradeAuditOrphans(t *testing.T) {
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true, Continuation: testColdCodec{}})
	defer closeTestStore(t, s)
	s.stopColdPack()
	if err := s.ConfigureColdPack(ColdPackOptions{Mode: "float-downgrade"}); err != nil {
		t.Fatal(err)
	}
	if err := s.fenceFloatCodec(); err != nil {
		t.Fatal(err)
	}
	// A staged generation is deliberately outside the live catalog/descriptors.
	suffix := make([]byte, 20)
	suffix[0] = 'f'
	suffix[9] = 'c'
	key := appendContinuationKey(nil, continuationPrefix(123, 1), ContinuationBucketKey(23, suffix))
	v := make([]byte, sealedHeader)
	v[0] = 4
	if err := s.db.Set(key, v, nil); err != nil {
		t.Fatal(err)
	}
	s.coldPack.cleanUnpack = true
	if err := s.DowngradeFloatCodec(context.Background()); err == nil {
		t.Fatal("orphan did not retain fence")
	}
	if s.TransferFormat() != 11 {
		t.Fatal("audit failure lowered marker")
	}
}

func TestFloatFenceSyncFailures(t *testing.T) {
	for _, stage := range []string{"file sync", "rename", "directory sync"} {
		t.Run(stage, func(t *testing.T) {
			fs := &upgradeFailureFS{FS: vfs.NewMem(), stage: stage}
			s, _ := openTestStore(t, "/store", Options{fs: fs, ExactBlocks: true, TimeBuckets: true, Continuation: testColdCodec{}})
			defer closeTestStore(t, s)
			s.stopColdPack()
			if err := s.ConfigureColdPack(ColdPackOptions{Mode: "exact", FloatCodec: true}); err != nil {
				t.Fatal(err)
			}
			fs.fail.Store(true)
			if err := s.fenceFloatCodec(); err == nil {
				t.Fatal("failed fence accepted")
			}
			if s.coldPack.floatFormat.Load() {
				t.Fatal("unsynced fence published")
			}
			fs.fail.Store(false)
			if err := s.fenceFloatCodec(); err != nil {
				t.Fatal(err)
			}
			if format, err := checkFormatMarker(fs, s.dir); err != nil || format != 11 {
				t.Fatalf("retry format=%d %v", format, err)
			}
		})
	}
}
