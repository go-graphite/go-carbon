package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/partspack"
)

func oldArchive(v []byte) (partspack.Policy, uint32, []chunkstore.Point, error) {
	var p partspack.Policy
	if len(v) < 28 || string(v[:4]) != "GCA2" {
		return p, 0, nil, errors.New("expected GCA2 archive")
	}
	n := uint64(binary.BigEndian.Uint32(v[12:]))
	a := binary.BigEndian.Uint32(v[16:])
	count := uint64(binary.BigEndian.Uint32(v[20:]))
	codec := binary.BigEndian.Uint32(v[24:])
	h := 28 + 8*n
	if n < 1 || n > 64 || uint64(a) >= n || h > uint64(len(v)) || count > partspack.MaxPoints || codec > 1 {
		return p, 0, nil, errors.New("invalid archive bounds")
	}
	p.Method = binary.BigEndian.Uint32(v[4:])
	p.XFilesFactorBits = binary.BigEndian.Uint32(v[8:])
	p.Archives = make([]partspack.Retention, n)
	for i := range p.Archives {
		p.Archives[i] = partspack.Retention{Step: binary.BigEndian.Uint32(v[28+8*i:]), Slots: binary.BigEndian.Uint32(v[32+8*i:])}
	}
	if codec == 1 {
		points, err := chunkstore.DecodeSealedPoints(v[h:])
		if err != nil || uint64(len(points)) != count {
			return p, 0, nil, errors.New("sealed count mismatch")
		}
		return p, a, points, nil
	}
	if h+16*count != uint64(len(v)) {
		return p, 0, nil, errors.New("invalid raw length")
	}
	points := make([]chunkstore.Point, count)
	for i := range points {
		points[i] = chunkstore.Point{Timestamp: int64(binary.BigEndian.Uint64(v[h+uint64(16*i):])), Value: math.Float64frombits(binary.BigEndian.Uint64(v[h+uint64(16*i+8):]))}
	}
	return p, a, points, nil
}

// repack is an offline diagnostic, never a daemon publication or expiry protocol.
// Without repartition it preserves all keys. Repartition is explicitly opt-in.
func repack(src, dst string, zstd, repartition bool, divisor uint32) error {
	if err := os.Mkdir(dst, 0750); err != nil {
		return err
	}
	in, err := pebble.Open(src, &pebble.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := pebble.Open(filepath.Join(dst, "db"), &pebble.Options{})
	if err != nil {
		return err
	}
	defer func() {
		if out != nil {
			_ = out.Close()
		}
	}()
	it, err := in.NewIter(nil)
	if err != nil {
		return err
	}
	defer it.Close()
	var w *sstable.Writer
	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()
	var paths []string
	var prefix, policyBytes []byte
	var current string
	var points, blocks uint64
	expected := sha256.New()
	type interval struct {
		Group       partsformat.Group `json:"group"`
		VShard      uint16            `json:"vshard"`
		Partition   int64             `json:"partition"`
		Part        string            `json:"part"`
		Archive     uint32            `json:"archive"`
		Start, End  int64
		Step, Slots uint32
	}
	var intervals []interval
	intervalIndex := map[string]int{}
	closeTable := func() error {
		if w == nil {
			return nil
		}
		if err := w.Close(); err != nil {
			return err
		}
		w = nil
		paths = append(paths, current)
		return nil
	}
	rows, widths, err := loadRows(it, repartition, divisor)
	if err != nil {
		return err
	}
	for _, row := range rows {
		key, err := partsformat.Decode(row.key)
		if err != nil {
			return err
		}
		p, ai, ps, err := oldArchive(row.value)
		if err != nil {
			return err
		}
		if key.Archive != ai {
			return errors.New("archive key mismatch")
		}
		pb, err := p.MarshalBinary()
		if err != nil {
			return err
		}
		if policyBytes == nil {
			policyBytes = pb
		} else if !bytes.Equal(policyBytes, pb) {
			return errors.New("fixture has mixed policies; split by policy before repacking")
		}
		lo, err := partsformat.PartitionPrefixForGroup(key.Group, key.VShard, key.Partition)
		if err != nil {
			return err
		}
		lo = append(lo, key.Part[:]...)
		if !bytes.Equal(prefix, lo) {
			if err := closeTable(); err != nil {
				return err
			}
			prefix = bytes.Clone(lo)
			current = filepath.Join(dst, fmt.Sprintf("part-%06d.sst", len(paths)))
			f, err := vfs.Default.Create(current)
			if err != nil {
				return err
			}
			w = sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: func() sstable.Compression {
				if zstd {
					return sstable.ZstdCompression
				}
				return sstable.SnappyCompression
			}()})
		}
		value, err := partspack.EncodeBlock(ps, p.Archives[ai].Step)
		if err != nil {
			return err
		}
		decoded, err := partspack.DecodeBlock(value)
		if err != nil {
			return err
		}
		if len(ps) != len(decoded) {
			return errors.New("point count mismatch")
		}
		for i := range ps {
			if ps[i].Timestamp != decoded[i].Timestamp || math.Float64bits(ps[i].Value) != math.Float64bits(decoded[i].Value) {
				return errors.New("repacked bits differ")
			}
		}
		id := string(lo) + string(binary.BigEndian.AppendUint32(nil, ai))
		if len(ps) == 0 {
			return errors.New("empty archives require separate catalog support")
		}
		start, end := ps[0].Timestamp, ps[0].Timestamp
		for _, point := range ps {
			start = min(start, point.Timestamp)
			end = max(end, point.Timestamp)
		}
		if end > math.MaxInt64-int64(p.Archives[ai].Step) {
			return errors.New("interval end overflow")
		}
		end += int64(p.Archives[ai].Step)
		if i, ok := intervalIndex[id]; ok {
			intervals[i].Start = min(intervals[i].Start, start)
			intervals[i].End = max(intervals[i].End, end)
		} else {
			intervalIndex[id] = len(intervals)
			intervals = append(intervals, interval{key.Group, key.VShard, key.Partition, fmt.Sprintf("%x", key.Part), ai, start, end, p.Archives[ai].Step, p.Archives[ai].Slots})
		}
		writeDigest(expected, row.key, value)
		if err := w.Set(row.key, value); err != nil {
			return err
		}
		points += uint64(len(ps))
		blocks++
	}
	if err := it.Error(); err != nil {
		return err
	}
	if err := closeTable(); err != nil {
		return err
	}
	if len(paths) == 0 {
		return errors.New("empty source")
	}
	digest := sha256.Sum256(policyBytes)
	layout, err := json.Marshal(struct {
		Version         string     `json:"version"`
		ProductionReady bool       `json:"production_ready"`
		Repartition     bool       `json:"repartition"`
		Widths          []int64    `json:"width_seconds"`
		PolicyDigest    string     `json:"policy_sha256"`
		Intervals       []interval `json:"intervals"`
	}{"offline-pack-diagnostic-v1", false, repartition, widths, fmt.Sprintf("%x", digest), intervals})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dst, "layout.json"), layout, 0640); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dst, "policy"), policyBytes, 0640); err != nil {
		return err
	}
	if err := out.Ingest(paths); err != nil {
		return err
	}
	if err := out.Compact([]byte{0}, []byte{255}, true); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	out = nil
	persisted, err := pebble.Open(filepath.Join(dst, "db"), &pebble.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer persisted.Close()
	verify, err := persisted.NewIter(nil)
	if err != nil {
		return err
	}
	actual := sha256.New()
	var persistedBlocks uint64
	for ok := verify.First(); ok; ok = verify.Next() {
		if _, err := partspack.DecodeBlock(verify.Value()); err != nil {
			verify.Close()
			return err
		}
		writeDigest(actual, verify.Key(), verify.Value())
		persistedBlocks++
	}
	iterErr := verify.Error()
	closeErr := verify.Close()
	if iterErr != nil {
		return iterErr
	}
	if closeErr != nil {
		return closeErr
	}
	if persistedBlocks != blocks || !bytes.Equal(expected.Sum(nil), actual.Sum(nil)) {
		return errors.New("persisted key/value digest differs")
	}
	tables, err := persisted.SSTables()
	if err != nil {
		return err
	}
	size := uint64(len(policyBytes) + len(layout))
	for _, l := range tables {
		for _, t := range l {
			size += t.Size
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"points": points, "blocks": blocks, "sst_policy_layout_bytes": size, "bytes_per_point": float64(size) / float64(points), "bit_exact": true, "partitions_unchanged": !repartition, "width_seconds": widths})
}

type kvrow struct{ key, value []byte }

func loadRows(it *pebble.Iterator, repartition bool, divisor uint32) ([]kvrow, []int64, error) {
	var rows []kvrow
	bytesUsed := 0
	for ok := it.First(); ok; ok = it.Next() {
		bytesUsed += len(it.Key()) + len(it.Value())
		if bytesUsed > 128<<20 {
			return nil, nil, errors.New("fixture exceeds 128MiB bound")
		}
		rows = append(rows, kvrow{bytes.Clone(it.Key()), bytes.Clone(it.Value())})
	}
	if err := it.Error(); err != nil {
		return nil, nil, err
	}
	if !repartition {
		return rows, nil, nil
	}
	type archiveID struct {
		name    partsformat.ID
		archive uint32
	}
	type image struct {
		key    partsformat.Key
		policy partspack.Policy
		points []chunkstore.Point
	}
	images := map[archiveID]*image{}
	h := sha256.New()
	for _, row := range rows {
		writeDigest(h, row.key, row.value)
		key, err := partsformat.Decode(row.key)
		if err != nil {
			return nil, nil, err
		}
		p, a, ps, err := oldArchive(row.value)
		if err != nil {
			return nil, nil, err
		}
		id := archiveID{key.Metric, a}
		im := images[id]
		if im == nil {
			im = &image{key: key, policy: p}
			images[id] = im
		}
		if im.key.Group != key.Group || im.key.VShard != key.VShard {
			return nil, nil, errors.New("metric ownership changed")
		}
		firstPolicy, e := im.policy.MarshalBinary()
		if e != nil {
			return nil, nil, e
		}
		nextPolicy, e := p.MarshalBinary()
		if e != nil {
			return nil, nil, e
		}
		if !bytes.Equal(firstPolicy, nextPolicy) {
			return nil, nil, errors.New("metric policy changed")
		}
		if len(ps) == 0 {
			return nil, nil, errors.New("empty archives require separate catalog support")
		}
		im.points = append(im.points, ps...)
	}
	sourceID := h.Sum(nil)
	rows = nil
	var widths []int64
	options := partspack.PartitionOptions{BaseSeconds: 86400, MaxSeconds: 30 * 86400, TargetPoints: 4096, HorizonDivisor: divisor}
	for _, im := range images {
		a := im.key.Archive
		ar := im.policy.Archives[a]
		width, err := options.Width(ar)
		if err != nil {
			return nil, nil, err
		}
		if len(widths) == 0 {
			widths = make([]int64, len(im.policy.Archives))
			for j, ret := range im.policy.Archives {
				widths[j], err = options.Width(ret)
				if err != nil {
					return nil, nil, err
				}
			}
		}
		slices.SortFunc(im.points, func(a, b chunkstore.Point) int {
			if a.Timestamp < b.Timestamp {
				return -1
			}
			if a.Timestamp > b.Timestamp {
				return 1
			}
			return 0
		})
		for i := 1; i < len(im.points); i++ {
			if im.points[i-1].Timestamp == im.points[i].Timestamp {
				return nil, nil, errors.New("duplicate stored timestamp")
			}
		}
		for start := 0; start < len(im.points); {
			key := im.key
			key.Partition = floor(im.points[start].Timestamp, width)
			end := start + 1
			for end < len(im.points) && end-start < 4096 && floor(im.points[end].Timestamp, width) == key.Partition {
				end++
			}
			key.BlockStart = im.points[start].Timestamp
			key.Ordinal = uint32(start)
			prefix, err := partsformat.PartitionPrefixForGroup(key.Group, key.VShard, key.Partition)
			if err != nil {
				return nil, nil, err
			}
			identity := append(bytes.Clone(sourceID), prefix...)
			identity = binary.BigEndian.AppendUint32(identity, a)
			sum := sha256.Sum256(identity)
			copy(key.Part[:], sum[:16])
			k, err := partsformat.Encode(key)
			if err != nil {
				return nil, nil, err
			}
			// Only an offline diagnostic intermediate: the final writer consumes these
			// exact raw archive images through the ordinary GCA2 compatibility decoder.
			v := make([]byte, 28+8*len(im.policy.Archives)+16*(end-start))
			copy(v, "GCA2")
			binary.BigEndian.PutUint32(v[4:], im.policy.Method)
			binary.BigEndian.PutUint32(v[8:], im.policy.XFilesFactorBits)
			binary.BigEndian.PutUint32(v[12:], uint32(len(im.policy.Archives)))
			binary.BigEndian.PutUint32(v[16:], a)
			binary.BigEndian.PutUint32(v[20:], uint32(end-start))
			pos := 28
			for _, ret := range im.policy.Archives {
				binary.BigEndian.PutUint32(v[pos:], ret.Step)
				binary.BigEndian.PutUint32(v[pos+4:], ret.Slots)
				pos += 8
			}
			for _, p := range im.points[start:end] {
				binary.BigEndian.PutUint64(v[pos:], uint64(p.Timestamp))
				binary.BigEndian.PutUint64(v[pos+8:], math.Float64bits(p.Value))
				pos += 16
			}
			rows = append(rows, kvrow{k, v})
			start = end
		}
	}
	slices.SortFunc(rows, func(a, b kvrow) int { return bytes.Compare(a.key, b.key) })
	return rows, widths, nil
}
func floor(t, w int64) int64 {
	q := t / w
	if t%w < 0 {
		q--
	}
	return q
}

func writeDigest(w io.Writer, key, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(key)))
	_, _ = w.Write(size[:])
	_, _ = w.Write(key)
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = w.Write(size[:])
	_, _ = w.Write(value)
}
