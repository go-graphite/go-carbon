package recovery

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestWriterProducesLegacyAndIndexedRecovery(t *testing.T) {
	dir, root := t.TempDir(), t.TempDir()
	builder := NewBuilder(func(name string) bool { return name == "known" })
	dump, err := NewWriter(filepath.Join(dir, "cache.1.2.bin"), 0, 1<<20, builder)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := NewWriter(filepath.Join(dir, "input.1.2.bin"), 1, 4096, builder)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := wal.WritePoints(points.OnePoint(fmt.Sprintf("metric%d", worker), float64(i+1000), int64(i))); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	for worker := 0; worker < 4; worker++ {
		for i := 0; i < 100; i++ {
			if err := dump.WritePoints(points.OnePoint(fmt.Sprintf("metric%d", worker), float64(i), int64(i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	wg.Wait()
	cacheFile, err := dump.Close()
	if err != nil {
		t.Fatal(err)
	}
	walFile, err := wal.Close()
	if err != nil {
		t.Fatal(err)
	}
	indexFile, err := WriteIndex(dir, builder)
	if err != nil {
		t.Fatal(err)
	}
	if err = Publish(dir, root, cacheFile, walFile, indexFile, "test-read-index"); err != nil {
		t.Fatal(err)
	}
	bundle, err := OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	if bundle.Points() != 800 || bundle.Metrics() != 4 {
		t.Fatal("bundle totals")
	}
	for worker := 0; worker < 4; worker++ {
		name := fmt.Sprintf("metric%d", worker)
		slot, ok, err := bundle.Find(name)
		if err != nil || !ok {
			t.Fatal(err)
		}
		p, err := bundle.Read(slot)
		if err != nil {
			t.Fatal(err)
		}
		for i, point := range p.Data {
			want := float64(i)
			if i >= 100 {
				want = float64(i - 100 + 1000)
			}
			if point.Value != want || point.Timestamp != int64(i%100) {
				t.Fatal("writer replay ordering", i, point)
			}
		}
	}
	var legacy uint64
	for _, f := range []File{cacheFile, walFile} {
		path := filepath.Join(dir, f.Name)
		want, err := Describe(path)
		if err != nil || !reflect.DeepEqual(want, f) {
			t.Fatal("streaming digest differs", want, err)
		}
		if err = points.ReadFromFile(path, func(p *points.Points) { legacy += uint64(len(p.Data)) }); err != nil {
			t.Fatal(err)
		}
	}
	if legacy != 800 {
		t.Fatal("legacy point count", legacy)
	}
	if err = wal.WritePoints(points.OnePoint("after-close", 1, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatal("write after close", err)
	}
}

// Optional catalogue work must never delay the authoritative legacy files.
func TestCheckpointClassificationAfterDurableSources(t *testing.T) {
	dir := t.TempDir()
	closed := false
	calls := 0
	builder := NewBuilder(func(string) bool {
		calls++
		if !closed {
			t.Fatal("catalogue lookup before durable dump completion")
		}
		return false
	})
	p := points.OnePoint("new.metric", 42, 100)
	writer, err := NewWriter(filepath.Join(dir, "cache.1.2.bin"), 0, 4096, builder)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.WritePoints(p); err != nil {
		t.Fatal(err)
	}
	file, err := writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	closed = true
	data, err := os.ReadFile(filepath.Join(dir, file.Name))
	if err != nil {
		t.Fatal(err)
	}
	var got []*points.Points
	if err = points.ReadBinary(bytes.NewReader(data), func(p *points.Points) { got = append(got, p) }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], p) {
		t.Fatalf("legacy source differs: %v", got)
	}
	if _, err = WriteIndex(dir, builder); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("catalogue calls: %d", calls)
	}
}

type failAfter struct{ left int }

func (f *failAfter) Write(p []byte) (int, error) {
	if f.left -= len(p); f.left < 0 {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

func TestPipelinePreservesOrderAndReportsFailure(t *testing.T) {
	var file, digest bytes.Buffer
	p := newPipeline(&file, 7, &digest)
	var want []byte
	for i := 0; i < 1000; i++ {
		chunk := []byte(fmt.Sprintf("%d,", i))
		want = append(want, chunk...)
		if _, err := p.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Flush(); err != nil || !bytes.Equal(file.Bytes(), want) || !bytes.Equal(digest.Bytes(), want) {
		t.Fatal("pipeline reordered or lost bytes", err)
	}

	p = newPipeline(&failAfter{left: 64}, 16, io.Discard)
	var err error
	for i := 0; i < 100 && err == nil; i++ {
		_, err = p.Write(make([]byte, 16))
	}
	if !errors.Is(errors.Join(err, p.Flush()), io.ErrClosedPipe) {
		t.Fatal("write failure not reported")
	}
}

// Concurrent segments must produce the exact file and index a serial writer
// would, so older binaries and the legacy restore path read it unchanged.
func TestWriteSegmentsMatchesSerial(t *testing.T) {
	dir := t.TempDir()
	const segments = 7
	metrics := func(seg int, emit func(*points.Points) error) error {
		for i := seg; i < 30000; i += segments {
			p := &points.Points{Metric: fmt.Sprintf("seg%d.m%d", seg, i)}
			for j := 0; j <= i%5; j++ {
				p.Data = append(p.Data, points.Point{Value: float64(i*10 + j), Timestamp: int64(1000 + j)})
			}
			if err := emit(p); err != nil {
				return err
			}
		}
		return nil
	}
	write := func(name string, parallel bool) ([]byte, []byte) {
		b := NewConcurrentBuilder(nil, 4)
		w, err := NewWriter(filepath.Join(dir, name), 0, 1<<20, b)
		if err != nil {
			t.Fatal(err)
		}
		if parallel {
			err = w.WriteSegments(segments, metrics)
		} else {
			for seg := 0; seg < segments && err == nil; seg++ {
				err = metrics(seg, w.WritePoints)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Close(); err != nil {
			t.Fatal(err)
		}
		var index bytes.Buffer
		if err = b.Write(&index); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return data, index.Bytes()
	}
	sd, si := write("serial.bin", false)
	pd, pi := write("parallel.bin", true)
	if !bytes.Equal(sd, pd) || len(sd) == 0 {
		t.Fatal("segmented dump bytes differ")
	}
	if !bytes.Equal(si, pi) {
		t.Fatal("segmented dump index differs")
	}
}
