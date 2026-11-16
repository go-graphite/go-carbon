package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const pageSize = 4096

type catalogName struct {
	Name    string
	Sources uint64
	Target  bool
}
type catalogPage struct {
	Last    string
	FirstID uint64
	Data    []byte
}
type dictionary struct {
	pages []catalogPage
	cache map[int][]catalogName
	fifo  []int
	enc   *zstd.Encoder
	dec   *zstd.Decoder
	count uint64
}

func newDictionary() (*dictionary, error) {
	e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(128<<20))
	if err != nil {
		e.Close()
		return nil, err
	}
	return &dictionary{cache: map[int][]catalogName{}, enc: e, dec: d}, nil
}
func (d *dictionary) close() { d.enc.Close(); d.dec.Close() }
func encodeNames(names []catalogName) []byte {
	var b []byte
	prev := ""
	for _, n := range names {
		common := 0
		for common < len(prev) && common < len(n.Name) && prev[common] == n.Name[common] {
			common++
		}
		b = binary.AppendUvarint(b, uint64(common))
		b = binary.AppendUvarint(b, uint64(len(n.Name)-common))
		b = append(b, n.Name[common:]...)
		b = binary.AppendUvarint(b, n.Sources)
		if n.Target {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
		prev = n.Name
	}
	return b
}
func decodeNames(b []byte) ([]catalogName, error) {
	var out []catalogName
	prev := ""
	for len(b) > 0 {
		common, k := binary.Uvarint(b)
		if k <= 0 {
			return nil, fmt.Errorf("invalid prefix")
		}
		b = b[k:]
		suffix, k := binary.Uvarint(b)
		if k <= 0 {
			return nil, fmt.Errorf("invalid suffix")
		}
		b = b[k:]
		if common > uint64(len(prev)) || suffix > uint64(len(b)) {
			return nil, fmt.Errorf("invalid name bounds")
		}
		name := prev[:int(common)] + string(b[:int(suffix)])
		b = b[int(suffix):]
		sources, k := binary.Uvarint(b)
		if k <= 0 || len(b) <= k {
			return nil, fmt.Errorf("invalid flags")
		}
		b = b[k:]
		if b[0] > 1 {
			return nil, fmt.Errorf("invalid target")
		}
		out = append(out, catalogName{name, sources, b[0] == 1})
		b = b[1:]
		prev = name
	}
	return out, nil
}
func (d *dictionary) add(names []catalogName) catalogPage {
	p := catalogPage{names[len(names)-1].Name, d.count + 1, d.enc.EncodeAll(encodeNames(names), nil)}
	d.pages = append(d.pages, p)
	d.count += uint64(len(names))
	return p
}
func (d *dictionary) names(i int) ([]catalogName, error) {
	if n, ok := d.cache[i]; ok {
		return n, nil
	}
	b, err := d.dec.DecodeAll(d.pages[i].Data, nil)
	if err != nil {
		return nil, err
	}
	n, err := decodeNames(b)
	if err != nil {
		return nil, err
	}
	if len(d.fifo) == 256 {
		delete(d.cache, d.fifo[0])
		d.fifo = d.fifo[1:]
	}
	d.fifo = append(d.fifo, i)
	d.cache[i] = n
	return n, nil
}
func (d *dictionary) lookup(name string) (uint64, bool, error) {
	i := sort.Search(len(d.pages), func(i int) bool { return d.pages[i].Last >= name })
	if i == len(d.pages) {
		return 0, false, nil
	}
	names, err := d.names(i)
	if err != nil {
		return 0, false, err
	}
	j := sort.Search(len(names), func(j int) bool { return names[j].Name >= name })
	if j == len(names) || names[j].Name != name {
		return 0, false, nil
	}
	return d.pages[i].FirstID + uint64(j), true, nil
}
func parseCatalog(s string) (catalogName, error) {
	v := strings.Split(s, "\t")
	if len(v) != 3 {
		return catalogName{}, fmt.Errorf("invalid catalog row")
	}
	mask, err := strconv.ParseUint(v[1], 16, 64)
	if err != nil {
		return catalogName{}, err
	}
	if v[2] != "0" && v[2] != "1" {
		return catalogName{}, fmt.Errorf("invalid target flag")
	}
	return catalogName{v[0], mask, v[2] == "1"}, nil
}

// An object tag avoids collisions with literal legacy strings. Unknown names
// remain intact, including residuals for names outside the captured catalog.
func (d *dictionary) normalize(v any) (any, error) {
	switch x := v.(type) {
	case string:
		id, ok, err := d.lookup(x)
		if err != nil {
			return nil, err
		}
		if ok {
			return map[string]any{"$catalog_id": id}, nil
		}
		return x, nil
	case []any:
		for i, y := range x {
			n, err := d.normalize(y)
			if err != nil {
				return nil, err
			}
			x[i] = n
		}
	case map[string]any:
		for k, y := range x {
			n, err := d.normalize(y)
			if err != nil {
				return nil, err
			}
			x[k] = n
		}
	}
	return v, nil
}

// restore is an import oracle: replacing known strings by IDs must preserve the
// complete canonical JSON value, including unknown names and large integers.
func (d *dictionary) restore(v any) (any, error) {
	switch x := v.(type) {
	case []any:
		for i, y := range x {
			n, e := d.restore(y)
			if e != nil {
				return nil, e
			}
			x[i] = n
		}
	case map[string]any:
		if id, ok := x["$catalog_id"].(uint64); ok && len(x) == 1 {
			i := sort.Search(len(d.pages), func(i int) bool { return d.pages[i].FirstID > id }) - 1
			if i < 0 {
				return nil, fmt.Errorf("ID outside dictionary")
			}
			names, e := d.names(i)
			if e != nil {
				return nil, e
			}
			off := id - d.pages[i].FirstID
			if off >= uint64(len(names)) {
				return nil, fmt.Errorf("ID outside page")
			}
			return names[off].Name, nil
		}
		for k, y := range x {
			n, e := d.restore(y)
			if e != nil {
				return nil, e
			}
			x[k] = n
		}
	}
	return v, nil
}
func (d *dictionary) canonical(v any) ([]byte, error) {
	before, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	v, e = d.normalize(v)
	if e != nil {
		return nil, e
	}
	normalized, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	v, e = d.restore(v)
	if e != nil {
		return nil, e
	}
	after, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	if !bytes.Equal(before, after) {
		return nil, fmt.Errorf("legacy round-trip mismatch")
	}
	return normalized, nil
}
