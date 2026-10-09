package recovery

import (
	"crypto/sha256"
	"hash"
	"io"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
)

const checksumChunkSize = 16 << 20

// fileDigester hashes the bytes supplied to the durable writer. Keep the whole
// digest for older readers and independent fixed-size digests for parallel reads.
type fileDigester struct {
	whole, chunk hash.Hash
	chunkBytes   int
	chunks       [][sha256.Size]byte
}

func newFileDigester() *fileDigester {
	return &fileDigester{whole: sha256.New(), chunk: sha256.New()}
}

func (d *fileDigester) Write(p []byte) (int, error) {
	_, _ = d.whole.Write(p)
	return d.writeChunks(p)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// wholeWriter and chunkWriter split the two digests so separate pipeline stages
// can compute them concurrently; each must see every byte in file order.
func (d *fileDigester) wholeWriter() io.Writer { return d.whole }
func (d *fileDigester) chunkWriter() io.Writer { return writerFunc(d.writeChunks) }

func (d *fileDigester) writeChunks(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		take := min(len(p), checksumChunkSize-d.chunkBytes)
		_, _ = d.chunk.Write(p[:take])
		d.chunkBytes += take
		p = p[take:]
		if d.chunkBytes == checksumChunkSize {
			var sum [sha256.Size]byte
			d.chunk.Sum(sum[:0])
			d.chunks = append(d.chunks, sum)
			d.chunk.Reset()
			d.chunkBytes = 0
		}
	}
	return n, nil
}

func (d *fileDigester) descriptor(name string, size int64) File {
	f := File{Name: name, Size: size, Chunks: slices.Clone(d.chunks)}
	d.whole.Sum(f.SHA256[:0])
	if d.chunkBytes > 0 {
		var sum [sha256.Size]byte
		d.chunk.Sum(sum[:0])
		f.Chunks = append(f.Chunks, sum)
	}
	if len(f.Chunks) > 0 {
		f.ChunkSize = checksumChunkSize
	}
	return f
}

func validChecksumShape(f File) bool {
	if len(f.Chunks) == 0 {
		return f.ChunkSize == 0
	}
	// Multi-chunk files need no whole-file digest: the chunks cover every
	// byte. Writers older than this rule always set one.
	return f.Size > 0 && f.ChunkSize == checksumChunkSize && int64(len(f.Chunks)) == (f.Size-1)/checksumChunkSize+1 &&
		(len(f.Chunks) != 1 || f.Chunks[0] == f.SHA256)
}

func verifyFileChecksum(data []byte, f File) bool {
	if int64(len(data)) != f.Size || !validChecksumShape(f) {
		return false
	}
	if len(f.Chunks) <= 1 {
		return sha256.Sum256(data) == f.SHA256
	}
	// Startup waits on this: use enough workers that hashing, not one core per
	// file, bounds it. Callers verify independent files concurrently.
	workers := min(16, runtime.GOMAXPROCS(0), len(f.Chunks))
	var bad atomic.Bool
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Go(func() {
			for i := worker; i < len(f.Chunks) && !bad.Load(); i += workers {
				start := i * checksumChunkSize
				end := start + min(checksumChunkSize, len(data)-start)
				if sha256.Sum256(data[start:end]) != f.Chunks[i] {
					bad.Store(true)
				}
			}
		})
	}
	// Every worker must finish before the caller can unmap a rejected file.
	wg.Wait()
	return !bad.Load()
}
