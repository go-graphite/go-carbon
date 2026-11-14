package recovery

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// ArrivalPath is an optional versioned companion to an immutable dump. Older
// binaries ignore it and retain their ordinary point recovery format.
func ArrivalPath(path string) string {
	return filepath.Join(filepath.Dir(path), ".arrival-v1."+filepath.Base(path))
}

type arrivalRecord struct {
	name string
	at   int64
}

func appendArrival(dst []byte, name string, at int64) []byte {
	start := len(dst)
	dst = binary.AppendUvarint(dst, uint64(len(name)))
	dst = append(dst, name...)
	dst = binary.AppendVarint(dst, at)
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], castagnoli))
}

// ReadArrivals validates the complete optional companion before returning it.
// Missing legacy metadata leaves the existing recovery path intact.
func ReadArrivals(path string) (map[string]int64, error) {
	f, err := os.Open(ArrivalPath(path))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	magic := make([]byte, 8)
	if _, err = io.ReadFull(r, magic); err != nil {
		return nil, err
	}
	if string(magic) != "GCAGE001" {
		return nil, errors.New("unsupported arrival companion")
	}
	out := make(map[string]int64)
	for {
		n, e := binary.ReadUvarint(r)
		if errors.Is(e, io.EOF) {
			return out, nil
		}
		if e != nil {
			return nil, e
		}
		if n == 0 || n > 1<<20 {
			return nil, errors.New("invalid arrival name length")
		}
		name := make([]byte, n)
		if _, e = io.ReadFull(r, name); e != nil {
			return nil, e
		}
		at, e := binary.ReadVarint(r)
		if e != nil {
			return nil, e
		}
		var sum [4]byte
		if _, e = io.ReadFull(r, sum[:]); e != nil {
			return nil, e
		}
		rec := appendArrival(nil, string(name), at)
		if binary.LittleEndian.Uint32(sum[:]) != binary.LittleEndian.Uint32(rec[len(rec)-4:]) {
			return nil, errors.New("arrival companion checksum mismatch")
		}
		key := string(name)
		if old, ok := out[key]; !ok || at == 0 || old != 0 && at < old {
			out[key] = at
		}
	}
}

func (w *Writer) writeArrivals(records []arrivalRecord) error {
	if len(records) == 0 {
		return nil
	}
	if w.arrivals == nil {
		f, err := os.OpenFile(ArrivalPath(w.file.Name()), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		w.arrivals = f
		w.arrivalBuffer = bufio.NewWriterSize(f, 64<<10)
		if _, err = w.arrivalBuffer.WriteString("GCAGE001"); err != nil {
			return err
		}
	}
	var buf []byte
	for _, r := range records {
		buf = appendArrival(buf[:0], r.name, r.at)
		if _, err := w.arrivalBuffer.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func (g *Segment) NoteArrival(name string, at int64) {
	g.s.arrivals = append(g.s.arrivals, arrivalRecord{name, at})
}
func (b *Bundle) FirstArrival(name string) int64 { return b.arrivals[name] }
