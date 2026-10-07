package carbonserver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand"
	"testing"
)

func TestSnapshotMetadataRoundTripAndUsage(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, count := range []int{0, 1, 255, 256, 257, 2053} {
		var data bytes.Buffer
		writer := snapshotMetadataWriter{w: &data}
		values := make([][4]int64, count)
		for row := range values {
			values[row] = [4]int64{math.MinInt64 + int64(row), math.MaxInt64 - int64(row), rng.Int63(), 1700000000}
			if row%2 == 0 {
				values[row][2] = -values[row][2]
			}
			if err := writer.append(values[row]); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.finish(); err != nil {
			t.Fatal(err)
		}
		reader, err := openSnapshotMetadata(data.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		for row, want := range values {
			got, err := reader.get(uint64(row))
			if err != nil || got != want {
				t.Fatalf("row %d: got %v/%v, want %v", row, got, err, want)
			}
		}
		for i := 0; i < 1000; i++ {
			start, end := rng.Intn(count+1), rng.Intn(count+1)
			if start > end {
				start, end = end, start
			}
			var want [3]int64
			for _, value := range values[start:end] {
				for column := range want {
					want[column] += value[column]
				}
			}
			got, err := reader.usage(uint64(start), uint64(end))
			if err != nil || got != want {
				t.Fatalf("range [%d,%d): got %v/%v, want %v", start, end, got, err, want)
			}
		}
		if _, err := reader.get(uint64(count)); err == nil {
			t.Fatal("out-of-range row accepted")
		}
		if _, err := reader.usage(0, uint64(count+1)); err == nil {
			t.Fatal("out-of-range usage accepted")
		}
	}
}

func TestSnapshotMetadataRejectsTruncatedAndInvalidExtents(t *testing.T) {
	var data bytes.Buffer
	w := snapshotMetadataWriter{w: &data}
	for i := 0; i < 257; i++ {
		if err := w.append([4]int64{int64(i), 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.finish(); err != nil {
		t.Fatal(err)
	}
	valid := data.Bytes()
	for end := 0; end < len(valid); end++ {
		if _, err := openSnapshotMetadata(valid[:end]); err == nil {
			t.Fatalf("accepted truncation at %d", end)
		}
	}
	for _, mutate := range []func([]byte){
		func(b []byte) { b[8] = 9 }, // width beyond uint64
		func(b []byte) { binary.LittleEndian.PutUint64(b[len(b)-32:], math.MaxUint64) },
		func(b []byte) { binary.LittleEndian.PutUint64(b[len(b)-24:], math.MaxUint64) },
		func(b []byte) { binary.LittleEndian.PutUint64(b[len(b)-16:], 128) },
		func(b []byte) { b[len(b)-1] ^= 1 },
	} {
		corrupt := bytes.Clone(valid)
		mutate(corrupt)
		if _, err := openSnapshotMetadata(corrupt); err == nil {
			t.Fatal("accepted invalid metadata")
		}
	}
}

type snapshotFailWriter struct{ err error }

func (w snapshotFailWriter) Write([]byte) (int, error) { return 0, w.err }

func TestSnapshotMetadataWriteErrors(t *testing.T) {
	for _, writeErr := range []error{nil, errors.New("disk full")} {
		w := snapshotMetadataWriter{w: snapshotFailWriter{writeErr}}
		if err := w.append([4]int64{1, 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
		err := w.finish()
		want := writeErr
		if want == nil {
			want = io.ErrShortWrite
		}
		if !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
	}
}

func FuzzSnapshotMetadata(f *testing.F) {
	var data bytes.Buffer
	w := snapshotMetadataWriter{w: &data}
	if err := w.append([4]int64{1, 2, 3, 4}); err != nil {
		f.Fatal(err)
	}
	if err := w.finish(); err != nil {
		f.Fatal(err)
	}
	f.Add(data.Bytes())
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := openSnapshotMetadata(data)
		if err != nil {
			return
		}
		if m.count > 0 {
			if _, err := m.get(m.count - 1); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := m.usage(0, m.count); err != nil {
			t.Fatal(err)
		}
	})
}
