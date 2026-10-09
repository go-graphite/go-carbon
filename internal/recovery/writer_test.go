package recovery

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

type failAt struct{ limit int64 }

func (f failAt) WriteAt(p []byte, off int64) (int, error) {
	if off+int64(len(p)) > f.limit {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

// Out-of-order parallel writes and per-chunk hashing must still produce the
// exact bytes and the chunk digests a serial hash gives.
func TestSinkMatchesSerialDigest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := newSink(f, 4096)
	var want []byte
	released := 0
	rng := rand.New(rand.NewSource(1))
	for len(want) < 3*checksumChunkSize+12345 {
		data := make([]byte, 1+rng.Intn(3<<20))
		rng.Read(data)
		want = append(want, data...)
		if rng.Intn(2) == 0 {
			if _, err = s.Write(data); err != nil {
				t.Fatal(err)
			}
		} else if err = s.Submit(data, func([]byte) { released++ }); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("sink bytes differ", err)
	}
	d := newFileDigester()
	_, _ = d.Write(want)
	serial := d.descriptor("s.bin", int64(len(want)))
	desc := s.descriptor("s.bin", int64(len(want)))
	if !reflect.DeepEqual(desc.Chunks, serial.Chunks) || desc.ChunkSize != serial.ChunkSize || desc.SHA256 != ([32]byte{}) {
		t.Fatal("sink chunk digests differ")
	}
	if !verifyFileChecksum(got, desc) {
		t.Fatal("sink descriptor does not verify")
	}
	if released == 0 {
		t.Fatal("submitted buffers not released")
	}
}

func TestSinkReportsWriteFailure(t *testing.T) {
	s := newSink(failAt{limit: 64 << 10}, 16<<10)
	var err error
	for i := 0; i < 100 && err == nil; i++ {
		_, err = s.Write(make([]byte, 16<<10))
	}
	if !errors.Is(errors.Join(err, s.Flush()), io.ErrClosedPipe) {
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
			err = w.WriteSegments(segments, 3, func(seg int, out *Segment) error { return metrics(seg, out.WritePoints) })
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
	// Ids are assigned concurrently, so colliding slots may be ordered
	// differently; every lookup must still answer identically.
	sx, err := Open(si, sd, nil)
	if err != nil {
		t.Fatal(err)
	}
	px, err := Open(pi, pd, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sx.Metrics() != px.Metrics() || sx.Points() != px.Points() {
		t.Fatal("segmented dump aggregates differ")
	}
	for seg := 0; seg < segments; seg++ {
		for i := seg; i < 30000; i += segments {
			name := fmt.Sprintf("seg%d.m%d", seg, i)
			ss, sok, _ := sx.Find(name)
			ps, pok, _ := px.Find(name)
			if !sok || !pok {
				t.Fatal("metric missing", name)
			}
			a, _ := sx.Read(ss)
			b, _ := px.Read(ps)
			if !reflect.DeepEqual(a, b) {
				t.Fatal("history differs", name)
			}
		}
	}
}

// A failing segment stops the bounded workers without deadlock; no worker may
// run more than the window ahead of the ordered append.
func TestWriteSegmentsFailureAndWindow(t *testing.T) {
	w, err := NewWriter(filepath.Join(t.TempDir(), "d.bin"), 0, 1<<20, NewBuilder(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var started, maxAhead atomic.Int64
	var appended atomic.Int64
	boom := errors.New("boom")
	done := make(chan error, 1)
	go func() {
		done <- w.WriteSegments(5000, 4, func(i int, out *Segment) error {
			started.Add(1)
			if ahead := int64(i) - appended.Load(); ahead > maxAhead.Load() {
				maxAhead.Store(ahead)
			}
			if i == 3000 {
				return boom
			}
			appended.Store(int64(i)) // segments finish roughly in order here
			return out.WritePoints(points.OnePoint(fmt.Sprintf("m%d", i), 1, 1))
		})
	}()
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("WriteSegments deadlocked after a failure")
	}
	if !errors.Is(err, boom) {
		t.Fatal("error not returned", err)
	}
	if started.Load() > 3000+2*4+4 {
		t.Fatal("workers kept running after failure", started.Load())
	}
	if maxAhead.Load() > 2*4+4 {
		t.Fatal("workers ran beyond the window", maxAhead.Load())
	}
}
