package chunkstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func sourceFiles(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	out := make(map[string][32]byte)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == "LOCK" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[entry.Name()] = sha256.Sum256(data)
	}
	return out
}

func TestTransferSourceDifferentialAndReplay(t *testing.T) {
	for _, mode := range []struct {
		name                  string
		continuation, buckets bool
	}{{"exact", false, false}, {"v6", true, false}, {"v7", true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			donor := transferStore(t, mode.continuation, mode.buckets)
			var names []string
			for i := range 129 {
				name := fmt.Sprintf("checkpoint.oracle.%03d", i)
				names = append(names, name)
				putTransferPoints(t, donor, name, []store.Point{{Timestamp: 999810, Value: math.Copysign(0, -1)}, {Timestamp: 999880, Value: float64(i)}, {Timestamp: 999890, Value: math.Inf(1)}})
			}
			checkpoint := filepath.Join(t.TempDir(), "checkpoint")
			if err := donor.Checkpoint(checkpoint); err != nil {
				t.Fatal(err)
			}
			before := sourceFiles(t, checkpoint)
			var codec store.ContinuationCodec
			if mode.continuation {
				codec = &whisperio.ContinuationCodec{}
			}
			source, err := store.OpenTransferSource(checkpoint, store.TransferSourceOptions{Continuation: codec, CacheBytes: 1 << 20, Now: func() time.Time { return time.Unix(1000000, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			if other, err := store.OpenTransferSource(checkpoint, store.TransferSourceOptions{Continuation: codec}); err == nil {
				other.Close()
				t.Fatal("second checkpoint opener admitted")
			}
			want, err := donor.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			got, err := source.ExportTransferLimit(ctx, names, 160<<20)
			if err != nil {
				t.Fatal(err)
			}
			var a, b bytes.Buffer
			if err := store.WriteTransfer2(&a, want); err != nil {
				t.Fatal(err)
			}
			if err := store.WriteTransfer2(&b, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(a.Bytes(), b.Bytes()) {
				t.Fatal("checkpoint export changed raw records")
			}
			for _, overlap := range []bool{false, true} {
				baseline := transferStore(t, mode.continuation, mode.buckets)
				bulk := transferStore(t, mode.continuation, mode.buckets)
				if overlap {
					for i, name := range names {
						if i%2 == 0 {
							for _, s := range []*store.Store{baseline, bulk} {
								putTransferPoints(t, s, name, []store.Point{{Timestamp: 999880, Value: 42}, {Timestamp: 999910, Value: 7}})
							}
						}
					}
				}
				for _, record := range want {
					if _, err := baseline.ImportTransfer(ctx, []store.TransferMetric{record}); err != nil {
						t.Fatal(err)
					}
				}
				proofs, err := bulk.ImportTransfer(ctx, got)
				if err != nil {
					t.Fatal(err)
				}
				if err := bulk.VerifyTransfer(ctx, proofs); err != nil {
					t.Fatal(err)
				}
				for _, name := range names {
					x, err := baseline.Snapshot(ctx, name)
					if err != nil {
						t.Fatal(err)
					}
					y, err := bulk.Snapshot(ctx, name)
					if err != nil {
						t.Fatal(err)
					}
					if err := store.EqualSnapshots(x, y); err != nil {
						t.Fatal(name, err)
					}
				}
				// Journal resume verifies receipts without replaying an import.
				// Foreground writes between verification attempts must survive.
				for _, s := range []*store.Store{baseline, bulk} {
					putTransferPoints(t, s, names[0], []store.Point{{Timestamp: 999920, Value: 99}})
				}
				if err := bulk.VerifyTransfer(ctx, proofs); err != nil {
					t.Fatal(err)
				}
				for _, name := range names {
					x, _ := baseline.Snapshot(ctx, name)
					y, _ := bulk.Snapshot(ctx, name)
					if err := store.EqualSnapshots(x, y); err != nil {
						t.Fatal("replay", name, err)
					}
				}
			}
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			after := sourceFiles(t, checkpoint)
			if fmt.Sprint(before) != fmt.Sprint(after) {
				t.Fatal("read-only checkpoint files changed")
			}
		})
	}
}
