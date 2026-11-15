package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

type extent struct{ virtual, physical, size int64 }
type selectedReader struct {
	source  io.ReaderAt
	extents []extent
	size    int64
}

func (r *selectedReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("negative read offset")
	}
	for len(p) > 0 {
		if off >= r.size {
			return n, io.EOF
		}
		i := sort.Search(len(r.extents), func(i int) bool { return r.extents[i].virtual+r.extents[i].size > off })
		x := r.extents[i]
		take := min(int64(len(p)), x.virtual+x.size-off)
		got, e := r.source.ReadAt(p[:take], x.physical+off-x.virtual)
		n += got
		off += int64(got)
		p = p[got:]
		if e != nil {
			return n, e
		}
		if got == 0 {
			return n, io.ErrNoProgress
		}
	}
	return n, nil
}
func selectPartition(f *os.File, frontier int64, part int) (io.ReaderAt, int64, int64, error) {
	if part < 0 {
		return f, frontier, 0, nil
	}
	if part > 15 {
		return nil, 0, 0, fmt.Errorf("partition must be 0-15")
	}
	r := &selectedReader{source: f, extents: []extent{{0, 0, 64}}, size: 64}
	var count int64
	for off := int64(64); off < frontier; {
		var h [52]byte
		if _, e := f.ReadAt(h[:], off); e != nil {
			return nil, 0, 0, e
		}
		size := 52 + int64(binary.LittleEndian.Uint32(h[16:]))
		if int(binary.LittleEndian.Uint16(h[6:])) == part {
			r.extents = append(r.extents, extent{r.size, off, size})
			r.size += size
			count += int64(binary.LittleEndian.Uint32(h[24:]))
		}
		off += size
	}
	return r, r.size, count, nil
}
