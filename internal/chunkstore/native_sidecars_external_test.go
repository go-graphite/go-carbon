package chunkstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Emulate a legacy writer whose physical sidecar predates summary metadata.
type unindexedSidecarCodec struct{ *whisperio.ContinuationCodec }

func (c unindexedSidecarCodec) Update(ctx context.Context, kv store.ContinuationIO, m store.Metadata, input []store.Point, retention int, clock func() time.Time, maintenance *store.ContinuationMaintenance) error {
	if err := c.ContinuationCodec.Update(ctx, kv, m, input, retention, clock, maintenance); err != nil {
		return err
	}
	var obsolete [][]byte
	if err := kv.Visit([]byte("f"), func(k, _ []byte) error {
		if len(k) >= 10 && strings.ContainsRune("qrs", rune(k[9])) {
			obsolete = append(obsolete, bytes.Clone(k))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, k := range obsolete {
		if err := kv.Delete(k); err != nil {
			return err
		}
	}
	return kv.Delete(store.SidecarSummaryKey)
}

func TestNativeMissingSidecarSummary(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		for _, combine := range []bool{false, true} {
			t.Run(fmt.Sprintf("buckets=%v/combine=%v", buckets, combine), func(t *testing.T) {
				ctx := context.Background()
				open := func(codec store.ContinuationCodec) *store.Store {
					s, err := store.Open("/db", store.Options{ExactBlocks: true, TimeBuckets: buckets, Continuation: codec, CacheSize: 1 << 20, MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(1000000, 0) }}.WithFS(vfs.NewMem()))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := s.Close(); err != nil {
							t.Error(err)
						}
					})
					return s
				}
				source := open(unindexedSidecarCodec{&whisperio.ContinuationCodec{}})
				codec := &whisperio.ContinuationCodec{}
				target, reference := open(codec), open(codec)
				name := "native.legacy.sidecar"
				var points []store.Point
				for ts := int64(995000); ts < 999900; ts += 10 {
					points = append(points, store.Point{Timestamp: ts, Value: float64(ts%91) / 3})
				}
				putTransferPoints(t, source, name, points)
				putTransferPoints(t, source, name, []store.Point{{Timestamp: 996000, Value: math.Copysign(0, -1)}, {Timestamp: 996010, Value: math.Inf(-1)}, {Timestamp: 996020, Value: math.Float64frombits(0x7ff8000000000042)}})
				if source.Sidecars().Count != 1 {
					t.Fatal("fixture has no physical sidecar")
				}
				records, err := source.ExportTransfer(ctx, []string{name})
				if err != nil {
					t.Fatal(err)
				}
				for _, kv := range records[0].Keys {
					if bytes.Equal(kv.Key, store.SidecarSummaryKey) {
						t.Fatal("fixture retained its summary")
					}
				}
				if _, err := reference.ImportTransfer(ctx, records); err != nil {
					t.Fatal("classic transfer", err)
				}
				// The writable classic path still persists its recovered summary.
				persisted, err := reference.ExportTransfer(ctx, []string{name})
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, kv := range persisted[0].Keys {
					if bytes.Equal(kv.Key, store.SidecarSummaryKey) {
						summary, err := store.DecodeSidecarSummary(kv.Value)
						if err != nil || !summary.Present || summary.Points == 0 {
							t.Fatal("bad persisted summary", summary, err)
						}
						found = true
					}
				}
				if !found {
					t.Fatal("writable transfer did not persist summary")
				}
				pin, err := source.PinNativeSource()
				if err != nil {
					t.Fatal(err)
				}
				defer pin.Close()
				var wire bytes.Buffer
				if err := pin.Write(ctx, []string{name}, &wire); err != nil {
					t.Fatal("read-only snapshot export", err)
				}
				request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: []string{name}, Combine: combine}
				if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); err != nil {
					t.Fatal("native import", err)
				}
				want, err := reference.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := target.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.EqualSnapshots(want, got); err != nil {
					t.Fatal("point bits differ from classic", err)
				}
				if target.Sidecars() != reference.Sidecars() {
					t.Fatal("sidecar inventory differs from classic", target.Sidecars(), reference.Sidecars())
				}
				after, err := source.ExportTransfer(ctx, []string{name})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(records[0].Keys, after[0].Keys) {
					t.Fatal("native copy changed donor keys")
				}
			})
		}
	}
}

type blockedSidecarReader struct {
	*whisperio.ContinuationCodec
	blocked bool
}

var errSidecarReadBlocked = errors.New("injected sidecar read failure")

func (c *blockedSidecarReader) ReadSidecar(kv store.ContinuationIO) (store.SidecarSummary, error) {
	if c.blocked {
		return store.SidecarSummary{}, errSidecarReadBlocked
	}
	return c.ContinuationCodec.ReadSidecar(kv)
}

func TestNativeReadyCombineMissingSidecarSummary(t *testing.T) {
	ctx := context.Background()
	source := nativeMemoryStore(t, true, true)
	name := "native.ready.raw.sidecar"
	var points []store.Point
	for ts := int64(995000); ts < 999900; ts += 10 {
		points = append(points, store.Point{Timestamp: ts, Value: float64(ts%91) / 3})
	}
	putTransferPoints(t, source, name, points)
	putTransferPoints(t, source, name, []store.Point{{Timestamp: 996000, Value: 42.25}})
	records, err := source.ExportTransfer(ctx, []string{name})
	if err != nil {
		t.Fatal(err)
	}
	// An older staged raw record may lack the summary despite a Ready inventory.
	keys := records[0].Keys[:0]
	for _, kv := range records[0].Keys {
		if !bytes.Equal(kv.Key, store.SidecarSummaryKey) {
			keys = append(keys, kv)
		}
	}
	records[0].Keys = keys
	var wire bytes.Buffer
	if err := store.WriteTransfer2(&wire, records); err != nil {
		t.Fatal(err)
	}
	codec := &blockedSidecarReader{ContinuationCodec: &whisperio.ContinuationCodec{}, blocked: true}
	fs := vfs.NewMem()
	options := store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: codec, CacheSize: 1 << 20, MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(1000000, 0) }}.WithFS(fs)
	target, err := store.Open("/target", options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if target != nil {
			target.Close()
		}
	}()
	request := store.NativeRange{Operation: strings.Repeat("4", 64), Plan: strings.Repeat("5", 64), Stream: strings.Repeat("6", 64), Source: "donor", End: 1, Names: []string{name}, Combine: true}
	if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); !errors.Is(err, errSidecarReadBlocked) || !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), string(store.SidecarSummaryKey)) {
		t.Fatal("failure lacks metric/key context", err)
	}
	if target.NativeStats().Errors != 1 {
		t.Fatal("failure not counted", target.NativeStats())
	}
	if _, err := target.Metadata(ctx, name); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("staged raw name entered catalog", err)
	}
	scan, err := target.ScanSidecars(ctx, "", 10, true)
	if err != nil || scan.Scanned != 0 || !scan.Inventory.Ready {
		t.Fatal("catalog scan unexpectedly reached staged raw", scan, err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	target = nil
	codec.blocked = false
	target, err = store.Open("/target", options)
	if err != nil {
		t.Fatal(err)
	}
	receipt, ready, err := target.ResumeNativeCombine(ctx, request)
	if err != nil || !ready || receipt.CopiedCount != 1 {
		t.Fatal("source-free Ready resume failed", receipt, ready, err)
	}
	reference := nativeMemoryStore(t, true, true)
	if _, err := reference.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	want, err := reference.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	got, err := target.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal("resumed point bits differ", err)
	}
}
