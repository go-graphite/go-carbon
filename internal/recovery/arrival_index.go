package recovery

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/blevesearch/mmap-go"
	"github.com/cespare/xxhash/v2"
)

// arrivalIndex owns no name strings: slots hold offsets into the immutable
// companion mapping, which Bundle keeps alive just like its point sources.
// Building it checks every record before the bundle can be published.
type arrivalIndex struct {
	data  mmap.MMap
	slots []atomic.Uint64 // record offset, zero means empty
}

func arrivalAt(data []byte) (name []byte, at int64, size int, err error) {
	n, k := binary.Uvarint(data)
	if k <= 0 || n == 0 || n > 1<<20 || n > uint64(len(data)-k) {
		return nil, 0, 0, errors.New("invalid arrival name length")
	}
	name = data[k : k+int(n)]
	pos := k + int(n)
	at, k = binary.Varint(data[pos:])
	if k <= 0 || len(data)-pos-k < 4 {
		return nil, 0, 0, io.ErrUnexpectedEOF
	}
	return name, at, pos + k + 4, nil
}

// Use the canonical encoding for the checksum, as ReadArrivals does, including
// when an older writer supplied nonminimal varints.
func arrivalChecksum(name []byte, at int64, buf *[binary.MaxVarintLen64]byte) uint32 {
	n := binary.PutUvarint(buf[:], uint64(len(name)))
	sum := crc32.Update(0, castagnoli, buf[:n])
	sum = crc32.Update(sum, castagnoli, name)
	n = binary.PutVarint(buf[:], at)
	return crc32.Update(sum, castagnoli, buf[:n])
}

func openArrivalIndex(path string) (_ *arrivalIndex, err error) {
	f, err := os.Open(ArrivalPath(path))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 8 {
		return nil, io.ErrUnexpectedEOF
	}
	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = data.Unmap()
		}
	}()
	if string(data[:8]) != "GCAGE001" {
		return nil, errors.New("unsupported arrival companion")
	}
	// Find complete record ranges without copying or hashing their names. Each
	// worker then owns consecutive bytes, keeping the file access sequential.
	bounds := []int{8}
	count := 0
	for pos := 8; pos < len(data); {
		_, _, size, e := arrivalAt(data[pos:])
		if e != nil {
			return nil, e
		}
		pos += size
		count++
		if pos-bounds[len(bounds)-1] >= 1<<20 {
			bounds = append(bounds, pos)
		}
	}
	if bounds[len(bounds)-1] != len(data) {
		bounds = append(bounds, len(data))
	}
	// At most 3/4 full even if every record names a different metric.
	n := 2
	for count > n/4*3 {
		n *= 2
	}
	t := &arrivalIndex{data: data, slots: make([]atomic.Uint64, n)}
	workers := min(32, runtime.GOMAXPROCS(0), len(bounds)-1)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			var checksum [binary.MaxVarintLen64]byte
			for r := w; r < len(bounds)-1; r += workers {
				for pos := bounds[r]; pos < bounds[r+1]; {
					name, at, size, e := arrivalAt(data[pos:bounds[r+1]])
					if e != nil {
						errs[w] = e
						return
					}
					if binary.LittleEndian.Uint32(data[pos+size-4:]) != arrivalChecksum(name, at, &checksum) {
						errs[w] = errors.New("arrival companion checksum mismatch")
						return
					}
					t.insert(uint64(pos), name, at)
					pos += size
				}
			}
		})
	}
	wg.Wait() // no worker may retain data when an error unmaps it
	if err = errors.Join(errs...); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *arrivalIndex) insert(pos uint64, name []byte, at int64) {
	mask := uint64(len(t.slots) - 1)
	for slot := xxhash.Sum64(name) & mask; ; slot = (slot + 1) & mask {
		dst := &t.slots[slot]
		for {
			old := dst.Load()
			if old == 0 {
				if dst.CompareAndSwap(0, pos) {
					return
				}
				continue
			}
			key, earlier, _, _ := arrivalAt(t.data[old:])
			if !bytes.Equal(key, name) {
				break
			}
			if earlier == 0 || at != 0 && at >= earlier || dst.CompareAndSwap(old, pos) {
				return
			}
		}
	}
}

func (t *arrivalIndex) first(name string) int64 {
	if t == nil {
		return 0
	}
	mask := uint64(len(t.slots) - 1)
	for slot := xxhash.Sum64String(name) & mask; ; slot = (slot + 1) & mask {
		pos := t.slots[slot].Load()
		if pos == 0 {
			return 0
		}
		key, at, _, _ := arrivalAt(t.data[pos:])
		if string(key) == name {
			return at
		}
	}
}
