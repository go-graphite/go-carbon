package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

type gatedNativeExportWriter struct {
	bytes.Buffer
	once    sync.Once
	entered chan<- struct{}
	release <-chan struct{}
	ctx     context.Context
}

func (w *gatedNativeExportWriter) Write(p []byte) (int, error) {
	var err error
	w.once.Do(func() {
		w.entered <- struct{}{}
		select {
		case <-w.release:
		case <-w.ctx.Done():
			err = w.ctx.Err()
		}
	})
	if err != nil {
		return 0, err
	}
	return w.Buffer.Write(p)
}

func TestNativeExportConcurrentBytesAndAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	source, now := openTestStore(t, "/export", Options{fs: vfs.NewMem(), ExactBlocks: true, NativeExportConcurrency: 2, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, source)
	createTestMetric(t, source, "native.export")
	if err := source.UpdateMany(ctx, "native.export", []Point{{Timestamp: int64(now - 60), Value: math.Copysign(0, -1)}, {Timestamp: int64(now - 30), Value: math.SmallestNonzeroFloat64}}); err != nil {
		t.Fatal(err)
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	var oracle bytes.Buffer
	if err := pin.Write(ctx, []string{"native.export"}, &oracle); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	outputs := make([]*gatedNativeExportWriter, 2)
	done := make(chan error, 2)
	for i := range outputs {
		outputs[i] = &gatedNativeExportWriter{entered: entered, release: release, ctx: ctx}
		go func(i int) { done <- pin.Write(ctx, []string{"native.export"}, outputs[i]) }(i)
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("exports serialized", ctx.Err())
		}
	}
	queued, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	var untouched bytes.Buffer
	if err := pin.Write(queued, []string{"native.export"}, &untouched); !errors.Is(err, context.DeadlineExceeded) || untouched.Len() != 0 {
		t.Fatal("queued export escaped admission", err, untouched.Len())
	}
	once.Do(func() { close(release) })
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	for _, out := range outputs {
		if !bytes.Equal(out.Bytes(), oracle.Bytes()) {
			t.Fatal("concurrent export changed bytes")
		}
	}
}

func TestNativeExportBudgetOptions(t *testing.T) {
	for _, n := range []int{-1, 0, 1, 2, 8, 9} {
		s, err := Open(t.TempDir(), Options{NativeExportConcurrency: n, CacheSize: 1 << 20, MemTableSize: 1 << 20})
		if n < 0 || n > 8 {
			if err == nil {
				s.Close()
				t.Fatal("invalid export admission accepted", n)
			}
			continue
		}
		if err != nil {
			t.Fatal(n, err)
		}
		want := max(1, n)
		s.initNativeContext()
		if s.NativeExportConcurrency() != want || cap(s.nativeExportAdmission) != want || cap(s.nativeAdmission) != 1 {
			s.Close()
			t.Fatal("wrong independent admission", n)
		}
		s.Close()
	}
}
