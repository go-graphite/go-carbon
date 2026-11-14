package sealedinventory

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

var inventoryDecoders = sync.Pool{New: func() any {
	z, e := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(1<<20), zstd.WithDecoderConcurrency(1))
	if e != nil {
		panic(e)
	}
	return z
}}

type pacedFile struct {
	*os.File
	r *runner
}

func (f *pacedFile) ReadAt(p []byte, off int64) (int, error) {
	total := 0
	for len(p) > 0 {
		if err := f.r.ctx.Err(); err != nil {
			return total, err
		}
		size := min(len(p), 1<<20)
		if f.r.limiter != nil {
			if err := f.r.limiter.WaitN(f.r.ctx, size); err != nil {
				return total, err
			}
		}
		n, err := f.File.ReadAt(p[:size], off)
		f.r.readBytes.Add(int64(n))
		total += n
		off += int64(n)
		p = p[n:]
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
	return total, nil
}
func (r *runner) open(path string) (*sstable.Reader, *pacedFile, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, err
	}
	f := &pacedFile{File: os.NewFile(uintptr(fd), path), r: r}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{Filters: map[string]sstable.FilterPolicy{"rocksdb.BuiltinBloomFilter": bloom.FilterPolicy(10)}}, sstable.Mergers{"go-carbon.chunks.exact.v1": nil, "go-carbon.chunks.v1": nil})
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return reader, f, nil
}

// Read only the entry framing to establish exact block membership; Pebble's
// iterator supplies keys/values and verifies checksums. Never estimate boundaries
// from target block size or index separators (which may be shortened).
func blockEntries(f *pacedFile, b sstable.BlockHandle) (int, error) {
	if b.Length > 256<<20 {
		return 0, errors.New("data block exceeds 256 MiB bound")
	}
	raw := make([]byte, int(b.Length)+5)
	if _, err := f.ReadAt(raw, int64(b.Offset)); err != nil {
		return 0, err
	}
	body := raw[:b.Length]
	var err error
	switch raw[b.Length] {
	case 0:
	case 1:
		decodedLen, e := snappy.DecodedLen(body)
		if e != nil {
			return 0, e
		}
		if decodedLen > 256<<20 {
			return 0, errors.New("expanded Snappy block exceeds 256 MiB bound")
		}
		body, err = snappy.Decode(nil, body)
	case 7:
		size, n := binary.Uvarint(body)
		if n <= 0 || size > 256<<20 {
			return 0, errors.New("bad Zstd block size")
		}
		var z *zstd.Decoder
		z, err = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(256<<20), zstd.WithDecoderConcurrency(1))
		if err == nil {
			body, err = z.DecodeAll(body[n:], nil)
			z.Close()
		}
	default:
		return 0, fmt.Errorf("unsupported block compression %d", raw[b.Length])
	}
	if err != nil {
		return 0, err
	}
	if len(body) < 4 {
		return 0, errors.New("short block")
	}
	restarts := uint64(binary.LittleEndian.Uint32(body[len(body)-4:]))
	if restarts > (uint64(len(body))-4)/4 {
		return 0, errors.New("bad restart array")
	}
	end := len(body) - 4 - int(restarts)*4
	pos, count, previous := 0, 0, uint64(0)
	for pos < end {
		shared, n := binary.Uvarint(body[pos:end])
		if n <= 0 || shared > previous {
			return 0, errors.New("bad shared key")
		}
		pos += n
		unshared, n := binary.Uvarint(body[pos:end])
		if n <= 0 {
			return 0, errors.New("bad unshared key")
		}
		pos += n
		value, n := binary.Uvarint(body[pos:end])
		if n <= 0 {
			return 0, errors.New("bad value length")
		}
		pos += n
		if unshared > uint64(end-pos) || value > uint64(end-pos)-unshared {
			return 0, errors.New("entry beyond block")
		}
		previous = shared + unshared
		pos += int(unshared + value)
		count++
	}
	return count, nil
}
func classify(key, value []byte) (string, uint64, error) {
	suffix := key
	if len(key) >= 22 && key[0] == 't' && key[5] == 'u' {
		suffix = key[22:]
	} else if len(key) >= 17 && key[0] == 'u' {
		suffix = key[17:]
	}
	// v10 payloads retain the sealed v3 body, with ordinary group references.
	if len(suffix) == 37 && suffix[0] == 'f' && suffix[9] == 'c' && suffix[20] == 0 && len(value) > 1 && value[0] == 4 {
		class, n, err := classify(key[:len(key)-17], value[1:])
		return strings.Replace(class, "sealed/", "sealed/prefix/", 1), n, err
	}
	if len(suffix) == 20 && suffix[0] == 'f' && suffix[9] == 'c' && len(value) == 21 && value[0] == 5 {
		return "sealed/prefix-reference", 0, nil
	}
	if len(suffix) == 20 && suffix[0] == 'f' && suffix[9] == 'c' && len(value) > 1 && value[0] == 4 && value[1] == 3 {
		class, n, err := classify(key, value[1:])
		return strings.Replace(class, "sealed/", "sealed/prefix/", 1), n, err
	}
	if len(suffix) == 16 && suffix[0] == 'f' && suffix[9] == 'b' {
		return "descriptor", 0, nil
	}
	if len(suffix) == 20 && suffix[0] == 'f' && (suffix[9] == 'c' || suffix[9] == 'p') {
		if len(value) >= 16 && value[0] == 4 {
			_, _, count, err := chunkstore.SealedTimeBounds(value)
			if err != nil {
				return "", 0, err
			}
			body, err := sealedBody(value)
			if err != nil {
				return "", 0, err
			}
			if value[1]&4 == 0 {
				for i := 1; i < count; i++ {
					_, n := binary.Uvarint(body)
					if n <= 0 {
						return "", 0, errors.New("bad float timestamps")
					}
					body = body[n:]
				}
			}
			if len(body) == 0 {
				return "", 0, errors.New("missing float mode")
			}
			kind := "unknown"
			switch body[0] {
			case 1:
				kind = "float-alp"
			case 2:
				kind = "float-dictionary"
			case 3:
				kind = "float-chimp128"
			}
			if value[1]&8 != 0 {
				kind += "/zstd"
			} else {
				kind += "/raw"
			}
			return "sealed/" + kind, uint64(count), nil
		}
		if len(value) >= 16 && value[0] == 3 {
			n := uint64(binary.LittleEndian.Uint16(value[2:4]))
			if n < 1 || n > chunkstore.MaxSealedPoints {
				return "sealed/unknown", 0, nil
			}
			flags := value[1]
			kind := "float-planes"
			switch {
			case flags & ^byte(31) != 0:
				kind = "unknown"
			case flags&16 != 0:
				body, err := sealedBody(value)
				if err != nil {
					return "", 0, err
				}
				if flags&4 == 0 {
					for i := uint64(1); i < n; i++ {
						_, size := binary.Uvarint(body)
						if size <= 0 {
							return "", 0, errors.New("bad sealed timestamps")
						}
						body = body[size:]
					}
				}
				exponent, size := binary.Varint(body)
				if size <= 0 || size >= len(body) {
					return "", 0, errors.New("bad decimal mode")
				}
				switch body[size] {
				case 0:
					kind = "constant"
				case 1:
					kind = "linear"
				case 2:
					kind = "decimal-delta"
				case 3:
					kind = "decimal-dod"
				default:
					kind = "unknown"
				}
				if exponent == 0 && (kind == "decimal-delta" || kind == "decimal-dod") {
					kind = strings.Replace(kind, "decimal", "integer", 1)
				}
			case flags&1 != 0:
				kind = "integer-delta"
				if flags&2 != 0 {
					kind = "integer-dod"
				}
			}
			if flags&8 != 0 {
				kind += "/zstd"
			} else {
				kind += "/raw"
			}
			return "sealed/" + kind, n, nil
		}
		if len(value) > 0 && value[0] == 2 {
			var block chunkstore.PointBlock
			if err := chunkstore.DecodePointBlock(value, &block); err != nil {
				return "", 0, err
			}
			n := uint64(bits.OnesCount64(block.Present[0]) + bits.OnesCount64(block.Present[1]))
			return "ordinary", n, nil
		}
		return "group/unknown", 0, nil
	}
	if bytes.HasPrefix(key, []byte("m/")) || bytes.HasPrefix(key, []byte("z/")) || len(key) == 17 && key[0] == 'r' || bytes.HasPrefix(key, []byte("s/")) || bytes.HasPrefix(key, []byte("o/")) || bytes.HasPrefix(key, []byte("R/")) || len(key) == 17 && (key[0] == 'a' || key[0] == 'd') || len(suffix) > 0 && (suffix[0] == 'n' || suffix[0] == 'f') {
		return "catalog/metadata", 0, nil
	}
	return "journal/other", 0, nil
}
func sealedBody(v []byte) ([]byte, error) {
	if v[1]&8 == 0 {
		return v[16:], nil
	}
	z := inventoryDecoders.Get().(*zstd.Decoder)
	body, err := z.DecodeAll(v[16:], nil)
	inventoryDecoders.Put(z)
	return body, err
}
func (r *runner) scan(path string) (*fileReport, error) {
	reader, f, err := r.open(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	layout, err := reader.Layout()
	if err != nil {
		return nil, err
	}
	if len(layout.ValueBlock) > 0 {
		return nil, errors.New("value-block SST unsupported; production uses in-place values")
	}
	out := &fileReport{Ordinary: map[string]*ordinaryRow{}, OrdinaryByReason: map[string]*tally{}, Name: filepath.Base(path), Bytes: uint64(stat.Size()), Classes: map[string]*tally{}, Policies: map[string]*policyReport{}, Level: r.cfg.level, LevelSource: "fallback -level (default L6)"}
	if level, ok := r.levels[out.Name]; ok {
		out.Level = level
		out.LevelSource = "levels-json"
	}
	for _, b := range layout.Index {
		out.IndexBytes += b.Length + 5
	}
	if layout.TopIndex.Length > 0 {
		out.IndexBytes += layout.TopIndex.Length + 5
	}
	if layout.Filter.Length > 0 {
		out.FilterBytes = layout.Filter.Length + 5
	}
	index := r.ordinary
	if index == nil {
		index = &ordinaryIndex{readers: []*sstable.Reader{reader}, cache: map[string]*list.Element{}, order: list.New()}
	}
	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	dels, err := reader.NewRawRangeDelIter()
	if err != nil {
		return nil, err
	}
	if dels != nil {
		defer dels.Close()
	}
	// Raw range spans are sorted, fragmented and sequence-descending. Use a
	// streaming cursor; never materialise a potentially large tombstone index.
	k, v := iter.First()
	if dels != nil {
		for span := dels.First(); span != nil; span = dels.Next() {
			out.RangeDelSpans++
			out.RangeDelKeys += uint64(len(span.Keys))
		}
		if err = dels.Error(); err != nil {
			return nil, err
		}
	}
	covered := func(_ []byte, _ uint64) bool { return false }
	if dels != nil {
		span := dels.First()
		covered = func(key []byte, seq uint64) bool {
			for span != nil && bytes.Compare(span.End, key) <= 0 {
				span = dels.Next()
			}
			return span != nil && bytes.Compare(span.Start, key) <= 0 && len(span.Keys) > 0 && span.Keys[0].SeqNum() > seq
		}
	}
	for _, b := range layout.Data {
		count, e := blockEntries(f, b.BlockHandle)
		if e != nil {
			return nil, e
		}
		weights := map[string]uint64{}
		ordinaryWeights := map[string]uint64{}
		var logical uint64
		for i := 0; i < count; i++ {
			if err = r.ctx.Err(); err != nil {
				return nil, err
			}
			if k == nil {
				return nil, errors.New("block/iterator count mismatch")
			}
			value, _, e := v.Value(nil)
			if e != nil {
				return nil, e
			}
			class, n, e := classify(k.UserKey, value)
			if e != nil {
				return nil, e
			}
			size := uint64(len(k.UserKey) + len(value))
			if class == "ordinary" && r.cfg.mode != "transcode" {
				row, e := index.row(k.UserKey)
				if e != nil {
					return nil, e
				}
				key := ordinaryRowKey(row)
				dst := out.Ordinary[key]
				if dst == nil {
					row.tally = tally{}
					dst = &row
					out.Ordinary[key] = dst
				}
				dst.Entries++
				dst.Points += n
				dst.LogicalBytes += size
				if covered(k.UserKey, k.SeqNum()) {
					dst.TombstonedEntries++
					dst.TombstonedPoints += n
				}
				ordinaryWeights[key] += size
			}
			weights[class] += size
			logical += size
			t := out.Classes[class]
			if t == nil {
				t = &tally{}
				out.Classes[class] = t
			}
			t.Entries++
			t.Points += n
			t.LogicalBytes += size
			if strings.HasPrefix(class, "sealed/") {
				switch {
				case n == 4096:
					t.Full4096++
				case n < 4096:
					t.Partial++
				default:
					t.Larger++
				}
			}
			if covered(k.UserKey, k.SeqNum()) {
				t.TombstonedEntries++
				t.TombstonedPoints += n
			}
			k, v = iter.Next()
		}
		physical := b.Length + 5
		out.DataBytes += physical
		if logical == 0 && count > 0 {
			return nil, errors.New("zero logical block weight")
		}
		if count == 0 {
			t := out.Classes["journal/other"]
			if t == nil {
				t = &tally{}
				out.Classes["journal/other"] = t
			}
			t.PhysicalBytes += float64(physical)
		}
		for key, weight := range ordinaryWeights {
			out.Ordinary[key].PhysicalBytes += float64(physical) * float64(weight) / float64(logical)
		}
		for class, weight := range weights {
			out.Classes[class].PhysicalBytes += float64(physical) * float64(weight) / float64(logical)
		}
	}
	if k != nil {
		return nil, errors.New("unaccounted point entries")
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}
	if out.DataBytes > out.Bytes {
		return nil, errors.New("invalid data size")
	}
	out.OverheadBytes = out.Bytes - out.DataBytes
	for class, t := range out.Classes {
		if strings.HasPrefix(class, "sealed/") {
			out.SealedDataBytes += t.PhysicalBytes
		}
		if strings.HasPrefix(class, "sealed/") && strings.Contains(class, "float-planes/") {
			out.FloatDataBytes += t.PhysicalBytes
		}
	}
	for _, row := range out.Ordinary {
		if out.OrdinaryByReason[row.Reason] == nil {
			out.OrdinaryByReason[row.Reason] = &tally{}
		}
		addTally(out.OrdinaryByReason[row.Reason], &row.tally)
	}
	out.FloatSharePct = 100 * out.FloatDataBytes / float64(out.Bytes)
	out.SealedSharePct = 100 * out.SealedDataBytes / float64(out.Bytes)
	if dels != nil {
		if err = dels.Error(); err != nil {
			return nil, err
		}
	}
	return out, nil
}
