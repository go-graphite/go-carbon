package chunkstore_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Exercise the real continuation combiner as well as the ordinary exact-block
// path. The large fixture is physical history, not just a retention declaration.
func TestNativeContinuationTwentyMillionPointsBoundedHeap(t *testing.T) {
	const count = 20_000_000
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(2*count, 0) }
	open := func() *store.Store {
		s, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, Now: clock, CacheSize: 1 << 20, MemTableSize: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		s.StopBackground()
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		return s
	}
	source, target := open(), open()
	cfg := store.MetricConfig{Name: "large.continuation", AggregationMethod: store.Sum, Retentions: []store.Retention{{Step: 1, Count: count}}}
	if err := seedLargeContinuation(source, cfg, clock); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err := target.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: count, Value: 42}}); err != nil {
		t.Fatal(err)
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak atomic.Uint64
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				runtime.GC()
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				for old := peak.Load(); ms.HeapAlloc > old; old = peak.Load() {
					if peak.CompareAndSwap(old, ms.HeapAlloc) {
						break
					}
				}
			}
		}
	}()
	input, output := io.Pipe()
	exported := make(chan error, 1)
	go func() { err := pin.Write(ctx, []string{cfg.Name}, output); output.CloseWithError(err); exported <- err }()
	request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: []string{cfg.Name}, Combine: true}
	receipt, err := target.NativeImport(ctx, request, input)
	input.CloseWithError(err)
	exportErr := <-exported
	close(stop)
	<-stopped
	if err != nil || exportErr != nil {
		t.Fatal(err, exportErr)
	}
	if receipt.CopiedCount != 1 || len(receipt.Skipped) != 0 {
		t.Fatal(receipt)
	}
	retained := int64(peak.Load()) - int64(before.HeapAlloc)
	if retained > 1<<30 {
		t.Fatalf("20M continuation points retained %d bytes (1 GiB worker budget)", retained)
	}
	t.Logf("%d continuation points: peak retained heap delta %d bytes", count, retained)
	// Snapshot and Fetch remain the independent read oracles. Check every bit in
	// the snapshot, then query bounded windows at both ends and in the middle.
	snap, err := target.Snapshot(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Archives) != 1 || len(snap.Archives[0].Points) != count {
		t.Fatal("missing physical history")
	}
	for i, p := range snap.Archives[0].Points {
		want := math.Float64bits(1)
		if i == 0 {
			want = math.Float64bits(42)
		}
		if p.Timestamp != int64(count+i) || math.Float64bits(p.Value) != want {
			t.Fatalf("point %d changed: %+v", i, p)
		}
	}
	for _, from := range []int{count, count + count/2, 2*count - 256} {
		series, err := target.Fetch(ctx, cfg.Name, from, from+128)
		if err != nil || series == nil {
			t.Fatal("Fetch", err)
		}
		for i, v := range series.Values {
			ts := series.FromTime + i*series.Step
			if ts < count || ts >= 2*count {
				continue
			}
			want := math.Float64bits(1)
			if ts == count {
				want = math.Float64bits(42)
			}
			if math.Float64bits(v) != want {
				t.Fatalf("Fetch point %d changed", ts)
			}
		}
	}
}

type largeContinuationMemory map[string][]byte

func (m largeContinuationMemory) Get(k []byte) ([]byte, error) {
	v, ok := m[string(k)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}
func (m largeContinuationMemory) Set(k, v []byte) error {
	m[string(k)] = append([]byte(nil), v...)
	return nil
}
func (m largeContinuationMemory) Delete(k []byte) error { delete(m, string(k)); return nil }
func (m largeContinuationMemory) DeletePrefix(prefix []byte) error {
	for k := range m {
		if strings.HasPrefix(k, string(prefix)) {
			delete(m, k)
		}
	}
	return nil
}
func (m largeContinuationMemory) Visit(prefix []byte, fn func([]byte, []byte) error) error {
	for k, v := range m {
		if strings.HasPrefix(k, string(prefix)) {
			if err := fn([]byte(k), v); err != nil {
				return err
			}
		}
	}
	return nil
}

func seedLargeContinuation(s *store.Store, cfg store.MetricConfig, clock func() time.Time) error {
	const count = 20_000_000
	groups := make([]store.RelayoutKV, count/128)
	for index := range groups {
		var block store.PointBlock
		for i := range 128 {
			block.Present[i/64] |= 1 << uint(i%64)
			block.Points[i] = store.Point{Timestamp: int64(count + index*128 + i), Value: 1}
		}
		key := binary.BigEndian.AppendUint16(nil, 0)
		key = binary.BigEndian.AppendUint32(key, uint32(index))
		groups[index] = store.RelayoutKV{Key: key, Value: store.EncodePointBlock(&block)}
	}
	kv := largeContinuationMemory{}
	codec := &whisperio.ContinuationCodec{}
	ok, err := codec.RestoreNativeClassicBlocks(context.Background(), kv, store.Metadata{MetricConfig: cfg}, groups, clock)
	if err != nil {
		return err
	}
	if !ok {
		return store.ErrFormat
	}
	record := store.TransferMetric{Config: cfg, Version: store.TransferVersion{Name: cfg.Name, ID: 1, Generation: 1, Revision: 1}, Continuation: true}
	for k, v := range kv {
		record.Keys = append(record.Keys, store.RelayoutKV{Key: []byte(k), Value: v})
	}
	slices.SortFunc(record.Keys, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	_, err = s.ImportTransfer(context.Background(), []store.TransferMetric{record})
	return err
}
