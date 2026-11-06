package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestNativeCombineDefersCacheOnlyTargetAcrossCrash(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-prepare", true: "at-publish"}[late], func(t *testing.T) {
			ctx := context.Background()
			opts := Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour}
			opts.fs = vfs.NewMem()
			donor, now := openTestStore(t, "/donor", opts)
			defer closeTestStore(t, donor)
			opts.fs = vfs.NewMem()
			reference, _ := openTestStore(t, "/reference", opts)
			defer closeTestStore(t, reference)
			fs := vfs.NewStrictMem()
			opts.fs = fs
			target, _ := openTestStore(t, "/target", opts)
			names := []string{"pending.cache-only", "pending.present", "pending.absent"}
			for _, name := range names {
				createTestMetric(t, donor, name)
				if err := donor.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 1}}); err != nil {
					t.Fatal(err)
				}
			}
			for _, s := range []*Store{target, reference} {
				createTestMetric(t, s, names[1])
				if err := s.UpdateMany(ctx, names[1], []Point{{Timestamp: int64(now - 60), Value: 42}}); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := donor.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			var wire bytes.Buffer
			if err := pin.Write(ctx, names, &wire); err != nil {
				t.Fatal(err)
			}
			var cacheOnly atomic.Bool
			cacheOnly.Store(!late)
			target.SetTransferPending(func(name string) bool {
				return name == names[1] || name == names[0] && cacheOnly.Load()
			})
			target.nativeTestHook = func(stage string) error {
				if stage == "combine-before-publish" {
					cacheOnly.Store(true)
				}
				if stage == "combine-name-published" {
					if err := target.syncWAL(); err != nil {
						return err
					}
					fs.SetIgnoreSyncs(true)
					return errors.New("crash after durable cache-only decision")
				}
				return nil
			}
			request := combineTestRange(names)
			if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); err == nil {
				t.Fatal("crash not injected")
			}
			closeTestStore(t, target)
			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)
			target, _ = openTestStore(t, "/target", opts)
			defer closeTestStore(t, target)
			// Even after the cache fence clears, the durable decision belongs
			// to the same journal. Resume must not silently copy this name.
			receipt, ready, err := target.ResumeNativeCombine(ctx, request)
			if err != nil || !ready {
				t.Fatal("source-free resume", ready, err)
			}
			if receipt.CopiedCount != 2 || receipt.DeferredCount != 1 || receipt.IsCopied(0) || !receipt.IsCopied(1) || !receipt.IsCopied(2) || len(receipt.Mappings) != 2 {
				t.Fatal("cache-only target was published or present target was gated", receipt)
			}
			if _, err := target.Metadata(ctx, names[0]); !errors.Is(err, ErrNotFound) {
				t.Fatal("cache-only history became visible", err)
			}
			if _, err := target.NativeImport(ctx, request, nil); err != nil {
				t.Fatal("lost acknowledgement", err)
			}
			for _, s := range []*Store{target, reference} {
				createTestMetric(t, s, names[0])
				if err := s.UpdateMany(ctx, names[0], []Point{{Timestamp: int64(now - 60), Value: 99}}); err != nil {
					t.Fatal(err)
				}
			}
			records, err := donor.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reference.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
			if _, err := target.ImportTransfer(ctx, records[:1]); err != nil {
				t.Fatal(err)
			}
			for _, name := range names {
				want, err := reference.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := target.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				if err := EqualSnapshots(want, got); err != nil {
					t.Fatal(name, err)
				}
			}
		})
	}
}
