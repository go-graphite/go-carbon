package recovery

import (
	"crypto/sha256"
	"io"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/blevesearch/mmap-go"
)

func TestChunkChecksumsMatchWholeFileAndRejectCorruption(t *testing.T) {
	data := make([]byte, 2*checksumChunkSize+73)
	for i := range data {
		data[i] = byte(i*131 + 17)
	}
	digest := newFileDigester()
	for start := 0; start < len(data); {
		end := min(len(data), start+100003)
		_, _ = digest.Write(data[start:end])
		start = end
	}
	f := digest.descriptor("cache.test.bin", int64(len(data)))
	if f.SHA256 != sha256.Sum256(data) || !verifyFileChecksum(data, f) {
		t.Fatal("whole-file oracle differs or chunk validation failed")
	}
	for i, sum := range f.Chunks {
		start := i * checksumChunkSize
		if sum != sha256.Sum256(data[start:min(start+checksumChunkSize, len(data))]) {
			t.Fatal("chunk digest differs from independent slice", i)
		}
	}
	if again := digest.descriptor(f.Name, f.Size); !reflect.DeepEqual(f, again) {
		t.Fatal("finalizing twice changed the descriptor")
	}
	for _, offset := range []int{0, checksumChunkSize - 1, checksumChunkSize, 2*checksumChunkSize - 1, 2 * checksumChunkSize, len(data) - 1} {
		data[offset] ^= 1
		if verifyFileChecksum(data, f) {
			t.Fatal("accepted corruption at chunk boundary", offset)
		}
		data[offset] ^= 1
	}
	for _, change := range []func(*File){
		func(f *File) { f.ChunkSize++ },
		func(f *File) { f.Size-- },
		func(f *File) { f.Chunks = f.Chunks[:len(f.Chunks)-1] },
		func(f *File) { f.Chunks = append(f.Chunks, [sha256.Size]byte{}) },
		func(f *File) { f.Chunks[1][0] ^= 1 },
	} {
		bad := f
		bad.Chunks = slices.Clone(f.Chunks)
		change(&bad)
		if verifyFileChecksum(data, bad) {
			t.Fatal("accepted invalid descriptor", bad.ChunkSize, bad.Size, len(bad.Chunks))
		}
	}
	// Parallel writers omit the whole-file digest; the chunks cover every byte.
	noWhole := f
	noWhole.SHA256 = [sha256.Size]byte{}
	if !verifyFileChecksum(data, noWhole) {
		t.Fatal("rejected chunked descriptor without a whole-file digest")
	}
	legacy := f
	legacy.Chunks, legacy.ChunkSize = nil, 0
	if !verifyFileChecksum(data, legacy) {
		t.Fatal("legacy whole-file checksum rejected")
	}
}

func TestChunkChecksumsEmptyAndExactBoundaries(t *testing.T) {
	data := make([]byte, 2*checksumChunkSize)
	for _, size := range []int{0, 1, checksumChunkSize - 1, checksumChunkSize, checksumChunkSize + 1, len(data)} {
		digest := newFileDigester()
		_, _ = digest.Write(data[:size])
		f := digest.descriptor("cache.test.bin", int64(size))
		if !verifyFileChecksum(data[:size], f) || f.SHA256 != sha256.Sum256(data[:size]) {
			t.Fatal("boundary checksum differs", size)
		}
	}
}

func TestCapturedCheckpointChecksum(t *testing.T) {
	path := os.Getenv("GO_CARBON_CHECKSUM_FILE")
	if path == "" {
		t.Skip("set an immutable captured file for checksum comparison")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	digest := newFileDigester()
	size, err := io.Copy(digest, file)
	if err != nil {
		t.Fatal(err)
	}
	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Unmap()
	parallel := digest.descriptor("captured", size)
	serial := parallel
	serial.Chunks, serial.ChunkSize = nil, 0
	for i := 0; i < 3; i++ {
		start := time.Now()
		if !verifyFileChecksum(data, serial) {
			t.Fatal("serial oracle rejected the fixture")
		}
		serialTime := time.Since(start)
		start = time.Now()
		if !verifyFileChecksum(data, parallel) {
			t.Fatal("parallel checksums rejected the same fixture")
		}
		t.Logf("bytes=%d serial_seconds=%.6f chunk_seconds=%.6f", size, serialTime.Seconds(), time.Since(start).Seconds())
	}
}
