package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

// Mixed ranges must persist the same healthy points as the old single-name
// reference, while conflicts retain their original generation and exact bytes.
func TestNativeWindowConflictBulkDifferentialResume(t *testing.T) {
	ctx := context.Background()
	open := func(path string) *Store {
		s, _ := openTestStore(t, path, Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
		t.Cleanup(func() { closeTestStore(t, s) })
		return s
	}
	source, target, oracle := open("/source"), open("/target"), open("/oracle")
	names := []string{"a.healthy", "b.conflict", "c.healthy"}
	for _, name := range names {
		r := Retention{Step: 10, Count: 10}
		cfg := MetricConfig{Name: name, Retentions: []Retention{r}, AggregationMethod: Sum}
		seed := func(s *Store, points ...Point) {
			_, err := s.CreateFromSnapshot(ctx, Snapshot{Metadata: Metadata{MetricConfig: cfg}, Archives: []Archive{{Retention: r, Points: points}}})
			if err != nil {
				t.Fatal(err)
			}
		}
		seed(source, Point{Timestamp: 1200, Value: math.Copysign(0, -1)}, Point{Timestamp: 1210, Value: math.SmallestNonzeroFloat64})
		if name == names[1] {
			seed(target, Point{Timestamp: 1300, Value: math.Float64frombits(0x7ff8000000000042)})
		} else {
			seed(target, Point{Timestamp: 1210, Value: math.Inf(-1)})
			seed(oracle, Point{Timestamp: 1210, Value: math.Inf(-1)})
		}
	}
	before, err := target.ExportTransfer(ctx, []string{names[1]})
	if err != nil {
		t.Fatal(err)
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	window := &NativeWindow{From: 1200, Until: 1220}
	var wire bytes.Buffer
	if err := pin.WriteWindow(ctx, names, &wire, window); err != nil {
		t.Fatal(err)
	}
	request := combineTestRange(names)
	request.Window = window
	crashed := false
	target.nativeTestHook = func(stage string) error {
		if stage == "combine-name-published" && !crashed {
			crashed = true
			return errors.New("lost acknowledgement")
		}
		return nil
	}
	if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); err == nil {
		t.Fatal("crash hook did not run")
	}
	target.nativeTestHook = nil
	for range 2 {
		receipt, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes()))
		if err != nil || receipt.CopiedCount != 2 || receipt.DeferredCount != 1 || len(receipt.Skipped) != 1 {
			t.Fatal(receipt, err)
		}
		skip := receipt.Skipped[0]
		if skip.Name != names[1] || skip.Ordinal != 1 || !strings.Contains(skip.Reason, "window combine conflict") {
			t.Fatal(skip)
		}
		if err := receipt.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	after, err := target.ExportTransfer(ctx, []string{names[1]})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("conflict changed raw target record")
	}
	for _, name := range []string{names[0], names[2]} {
		src, err := source.ExportTransfer(ctx, []string{name})
		if err != nil {
			t.Fatal(err)
		}
		dst, err := oracle.ExportTransfer(ctx, []string{name})
		if err != nil {
			t.Fatal(err)
		}
		result, _, err := oracle.nativeCombineWindowRecord(ctx, src[0], dst[0], *window)
		if err != nil {
			t.Fatal(err)
		}
		got, err := target.ExportTransfer(ctx, []string{name})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result.Keys, got[0].Keys) {
			t.Fatal("healthy persisted bytes differ from single-name reference", name)
		}
	}
	recovered, err := target.NativeReceipt(ctx, request)
	if err != nil || len(recovered.Skipped) != 1 || recovered.Skipped[0].Name != names[1] {
		t.Fatal(recovered, err)
	}
}
