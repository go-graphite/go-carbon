package main

// Experimental seal metadata and raw SST data-block repair. The transport never
// decodes metric samples. This narrow format deliberately rejects SST layouts it
// cannot copy verbatim; it does not fall back to the snapshot export iterator.
import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

type copyBlock struct {
	File        int
	Offset      uint64
	Length      uint64 // physical data block including its five-byte trailer
	SHA256      [32]byte
	Key         []byte // exact portable SST user key
	Canonical   []byte // same record identity, independent of physical PartID
	ValueSHA256 [32]byte
	ValueBytes  uint64
}

type copyBlockManifest struct {
	Version uint32
	Policy  [32]byte
	Prefix  []byte
	Digest  [32]byte // canonical opaque key/value record multiset, not SST layout
	Blocks  []copyBlock
}

// copySealBlocks computes metadata once while a newly sealed file is still hot.
// Each encoded v2 value must occupy its own physical data block. Writer-side
// integration can produce a richer manifest for blocks containing several keys.
// Sample bytes are hashed as opaque strings; float/timestamp codecs are absent.
func copySealBlocks(p copyPartition) (copyBlockManifest, error) {
	prefix, err := partsformat.PartitionPrefixForGroup(p.Manifest.Group, p.Manifest.VShard, p.Manifest.Partition)
	if err != nil {
		return copyBlockManifest{}, err
	}
	m := copyBlockManifest{Version: 1, Policy: p.Manifest.Policy, Prefix: prefix}
	for fi, file := range p.Files {
		f, err := os.Open(file.Path)
		if err != nil {
			return m, err
		}
		blocks, err := copySealFile(f, fi, prefix)
		if err != nil {
			return m, err
		}
		m.Blocks = append(m.Blocks, blocks...)
	}
	m.Digest = copyLogicalDigest(m)
	return m, nil
}

func copySealFile(f *os.File, fi int, prefix []byte) (_ []copyBlock, retErr error) {
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, r.Close()) }()
	layout, err := r.Layout()
	if err != nil {
		return nil, err
	}
	if layout.Format != sstable.TableFormatPebblev4 || len(layout.ValueBlock) != 0 ||
		r.Properties.NumRangeDeletions != 0 || r.Properties.NumRangeKeys() != 0 ||
		r.Properties.NumDeletions != 0 || r.Properties.NumMergeOperands != 0 {
		return nil, errors.New("raw copy requires Pebblev4 inline SET blocks")
	}
	var checksum [1]byte
	if _, err := f.ReadAt(checksum[:], int64(layout.Footer.Offset)); err != nil {
		return nil, err
	}
	if checksum[0] != byte(sstable.ChecksumTypeCRC32c) {
		return nil, errors.New("raw copy requires CRC32c block trailers")
	}
	it, err := r.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, it.Close()) }()
	var out []copyBlock
	for key, lazy := it.First(); key != nil; key, lazy = it.Next() {
		if len(out) >= len(layout.Data) || key.Trailer != 1 {
			return nil, errors.New("raw copy requires one sequence-zero SET per physical block")
		}
		value, _, err := lazy.Value(nil)
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(key.UserKey, prefix) || len(key.UserKey) < len(prefix)+16 {
			return nil, errors.New("raw block outside manifest partition")
		}
		if _, err := partsformat.Decode(key.UserKey); err != nil {
			return nil, err
		}
		canonical := bytes.Clone(key.UserKey)
		clear(canonical[len(prefix) : len(prefix)+16])
		bh := layout.Data[len(out)]
		b := copyBlock{File: fi, Offset: bh.Offset, Length: bh.Length + 5,
			Key: bytes.Clone(key.UserKey), Canonical: canonical,
			ValueSHA256: sha256.Sum256(value), ValueBytes: uint64(len(value))}
		h := sha256.New()
		if _, err = io.Copy(h, io.NewSectionReader(f, int64(b.Offset), int64(b.Length))); err != nil {
			return nil, err
		}
		copy(b.SHA256[:], h.Sum(nil))
		out = append(out, b)
	}
	if err = it.Error(); err != nil {
		return nil, err
	}
	if len(out) != len(layout.Data) {
		return nil, errors.New("raw copy requires one record per physical data block")
	}
	return out, nil
}

func copyLogicalDigest(m copyBlockManifest) [32]byte {
	blocks := append([]copyBlock(nil), m.Blocks...)
	sort.Slice(blocks, func(i, j int) bool {
		if d := bytes.Compare(blocks[i].Canonical, blocks[j].Canonical); d != 0 {
			return d < 0
		}
		if d := bytes.Compare(blocks[i].ValueSHA256[:], blocks[j].ValueSHA256[:]); d != 0 {
			return d < 0
		}
		return blocks[i].ValueBytes < blocks[j].ValueBytes
	})
	h := sha256.New()
	_, _ = h.Write([]byte("copy-logical-records-v1"))
	_, _ = h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(m.Prefix))))
	_, _ = h.Write(m.Prefix)
	_, _ = h.Write(m.Policy[:])
	for _, b := range blocks {
		_, _ = h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(b.Canonical))))
		_, _ = h.Write(b.Canonical)
		_, _ = h.Write(binary.BigEndian.AppendUint64(nil, b.ValueBytes))
		_, _ = h.Write(b.ValueSHA256[:])
	}
	var d [32]byte
	copy(d[:], h.Sum(nil))
	return d
}

type copyHolePlan struct {
	Missing    []int
	Conflicts  int // same canonical block key, different opaque bytes
	Shared     int
	RawBytes   uint64
	ValueBytes uint64
}

// copyPlanBlocks reads persisted key/hash metadata only. Shared-different blocks
// are counted and withheld: their merge needs the normal precedence-aware
// reader/writer, never an arbitrary donor-wins overwrite. Target-only keys stay.
func copyPlanBlocks(source, target copyBlockManifest) (copyHolePlan, error) {
	var out copyHolePlan
	if source.Version != 1 || target.Version != 1 || source.Policy != target.Policy || !bytes.Equal(source.Prefix, target.Prefix) {
		return out, errors.New("incompatible block manifests")
	}
	if copyLogicalDigest(source) != source.Digest || copyLogicalDigest(target) != target.Digest {
		return out, errors.New("invalid persisted logical digest")
	}
	if source.Digest == target.Digest {
		out.Shared = len(source.Blocks)
		return out, nil
	}
	type value struct {
		hash [32]byte
		size uint64
	}
	targetKeys := make(map[string][]value, len(target.Blocks))
	for _, b := range target.Blocks {
		targetKeys[string(b.Canonical)] = append(targetKeys[string(b.Canonical)], value{b.ValueSHA256, b.ValueBytes})
	}
	seen := make(map[string]bool, len(source.Blocks))
	for i, b := range source.Blocks {
		key := string(b.Canonical)
		if seen[key] {
			return out, errors.New("ambiguous duplicate canonical donor block; requires merge")
		}
		seen[key] = true
		values := targetKeys[key]
		if len(values) == 0 {
			out.Missing = append(out.Missing, i)
			out.RawBytes += b.Length
			out.ValueBytes += b.ValueBytes
			continue
		}
		match := len(values) == 1 && values[0] == (value{b.ValueSHA256, b.ValueBytes})
		if match {
			out.Shared++
		} else {
			out.Conflicts++
		}
	}
	return out, nil
}

// copyAssembleBlocks copies selected physical data blocks verbatim, including
// compression and checksum trailers. Only small index/properties/footer blocks
// are created afresh. No key/value iterator or value codec is used here.
func copyAssembleBlocks(p copyPartition, selected []int, dir string) (_ copyPartition, raw uint64, retErr error) {
	if p.Blocks == nil {
		return copyPartition{}, 0, errors.New("missing seal block manifest")
	}
	blocks := make([]copyBlock, len(selected))
	for i, bi := range selected {
		if bi < 0 || bi >= len(p.Blocks.Blocks) {
			return copyPartition{}, 0, errors.New("invalid block index")
		}
		blocks[i] = p.Blocks.Blocks[bi]
	}
	sort.Slice(blocks, func(i, j int) bool { return bytes.Compare(blocks[i].Key, blocks[j].Key) < 0 })
	if len(blocks) == 0 {
		m := p.Manifest
		m.Parts = nil
		return copyPartition{Manifest: m, Additive: true}, 0, nil
	}
	f, err := os.CreateTemp(dir, "holes-*.sst")
	if err != nil {
		return copyPartition{}, 0, err
	}
	path := f.Name()
	defer func() {
		retErr = errors.Join(retErr, f.Close())
		if retErr != nil {
			_ = os.Remove(path)
		}
	}()
	var indexKeys, indexValues [][]byte
	var rawKeys, rawValues uint64
	inputs := make(map[int]*os.File)
	defer func() {
		for _, input := range inputs {
			retErr = errors.Join(retErr, input.Close())
		}
	}()
	buf := make([]byte, 64<<10)
	for i, b := range blocks {
		if b.File < 0 || b.File >= len(p.Files) || b.Length < 5 || b.Length > 1<<30 ||
			b.Offset > p.Files[b.File].Part.Bytes || b.Length > p.Files[b.File].Part.Bytes-b.Offset {
			return copyPartition{}, raw, errors.New("invalid raw block bounds")
		}
		if i > 0 && bytes.Compare(blocks[i-1].Key, b.Key) >= 0 {
			return copyPartition{}, raw, errors.New("overlapping raw block keys")
		}
		if err = partsformat.ValidateFileRange(p.Manifest, b.Key, b.Key); err != nil {
			return copyPartition{}, raw, err
		}
		src := inputs[b.File]
		if src == nil {
			src, err = os.Open(p.Files[b.File].Path)
			if err != nil {
				return copyPartition{}, raw, err
			}
			if copyBeforeFileRead != nil {
				if err = copyBeforeFileRead(src); err != nil {
					_ = src.Close()
					return copyPartition{}, raw, err
				}
			}
			inputs[b.File] = src
		}
		h := sha256.New()
		n, err := io.CopyBuffer(io.MultiWriter(f, h), io.NewSectionReader(src, int64(b.Offset), int64(b.Length)), buf)
		if err != nil {
			return copyPartition{}, raw, err
		}
		if uint64(n) != b.Length || !bytes.Equal(h.Sum(nil), b.SHA256[:]) {
			return copyPartition{}, raw, errors.New("raw block checksum mismatch")
		}
		key := binary.LittleEndian.AppendUint64(bytes.Clone(b.Key), 1) // seq=0, SET
		indexKeys = append(indexKeys, key)
		indexValues = append(indexValues, copyBlockHandle(raw, b.Length-5))
		raw += b.Length
		rawKeys += uint64(len(key))
		rawValues += b.ValueBytes
	}
	index, err := copyWriteMetaBlock(f, indexKeys, indexValues)
	if err != nil {
		return copyPartition{}, raw, err
	}
	props := map[string][]byte{
		"rocksdb.comparator":      []byte("leveldb.BytewiseComparator"),
		"rocksdb.merge.operator":  []byte("nullptr"),
		"rocksdb.data.size":       binary.AppendUvarint(nil, raw),
		"rocksdb.index.size":      binary.AppendUvarint(nil, index.length+5),
		"rocksdb.num.data.blocks": binary.AppendUvarint(nil, uint64(len(blocks))),
		"rocksdb.num.entries":     binary.AppendUvarint(nil, uint64(len(blocks))),
		"rocksdb.raw.key.size":    binary.AppendUvarint(nil, rawKeys),
		"rocksdb.raw.value.size":  binary.AppendUvarint(nil, rawValues),
	}
	var names []string
	for k := range props {
		names = append(names, k)
	}
	sort.Strings(names)
	var pk, pv [][]byte
	for _, k := range names {
		pk = append(pk, []byte(k))
		pv = append(pv, props[k])
	}
	properties, err := copyWriteMetaBlock(f, pk, pv)
	if err != nil {
		return copyPartition{}, raw, err
	}
	meta, err := copyWriteMetaBlock(f, [][]byte{[]byte("rocksdb.properties")}, [][]byte{copyBlockHandle(properties.offset, properties.length)})
	if err != nil {
		return copyPartition{}, raw, err
	}
	footer := make([]byte, 53)
	footer[0] = byte(sstable.ChecksumTypeCRC32c)
	handles := append(copyBlockHandle(meta.offset, meta.length), copyBlockHandle(index.offset, index.length)...)
	copy(footer[1:41], handles)
	magic, version := sstable.TableFormatPebblev4.AsTuple()
	binary.LittleEndian.PutUint32(footer[41:45], version)
	copy(footer[45:], magic)
	if _, err = f.Write(footer); err != nil {
		return copyPartition{}, raw, err
	}
	if err = f.Sync(); err != nil {
		return copyPartition{}, raw, err
	}
	desc, err := copyDescribe(path, partsformat.PartID{})
	if err != nil {
		return copyPartition{}, raw, err
	}
	copy(desc.Part.ID[:], desc.Part.SHA256[:16])
	m := p.Manifest
	m.Parts = []partsformat.Part{desc.Part}
	return copyPartition{Manifest: m, Files: []copyFile{desc}, Additive: true}, raw, nil
}

type copyMetaHandle struct{ offset, length uint64 }

func copyBlockHandle(offset, length uint64) []byte {
	return binary.AppendUvarint(binary.AppendUvarint(nil, offset), length)
}

// Index/meta blocks use restart interval one. Data blocks are never rewritten.
func copyWriteMetaBlock(f *os.File, keys, values [][]byte) (copyMetaHandle, error) {
	off, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return copyMetaHandle{}, err
	}
	var data []byte
	var restarts []uint32
	for i, k := range keys {
		restarts = append(restarts, uint32(len(data)))
		data = binary.AppendUvarint(data, 0)
		data = binary.AppendUvarint(data, uint64(len(k)))
		data = binary.AppendUvarint(data, uint64(len(values[i])))
		data = append(data, k...)
		data = append(data, values[i]...)
	}
	if len(restarts) == 0 {
		restarts = []uint32{0}
	}
	for _, r := range restarts {
		data = binary.LittleEndian.AppendUint32(data, r)
	}
	data = binary.LittleEndian.AppendUint32(data, uint32(len(restarts)))
	length := len(data)
	data = append(data, 0) // uncompressed metadata block
	crc := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	crc = (crc>>15 | crc<<17) + 0xa282ead8
	data = binary.LittleEndian.AppendUint32(data, crc)
	_, err = f.Write(data)
	return copyMetaHandle{uint64(off), uint64(length)}, err
}

// copyRepairPartition plans with seal metadata and stages only missing blocks.
// The returned SST uses additive ingestion. A nonzero conflict count MUST block
// READY until a precedence-aware merge has resolved those particular records.
func copyRepairPartition(source copyPartition, target copyBlockManifest, dir string) (copyPartition, copyHolePlan, error) {
	if source.Blocks == nil {
		return copyPartition{}, copyHolePlan{}, errors.New("missing source seal metadata")
	}
	prefix, err := partsformat.PartitionPrefixForGroup(source.Manifest.Group, source.Manifest.VShard, source.Manifest.Partition)
	if err != nil {
		return copyPartition{}, copyHolePlan{}, err
	}
	if source.Manifest.Policy != source.Blocks.Policy || !bytes.Equal(prefix, source.Blocks.Prefix) {
		return copyPartition{}, copyHolePlan{}, errors.New("seal metadata does not describe source partition")
	}
	plan, err := copyPlanBlocks(*source.Blocks, target)
	if err != nil {
		return copyPartition{}, plan, err
	}
	if len(plan.Missing) == 0 {
		m := source.Manifest
		m.Parts = nil
		return copyPartition{Manifest: m, Additive: true}, plan, nil
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return copyPartition{}, plan, err
	}
	p, raw, err := copyAssembleBlocks(source, plan.Missing, filepath.Clean(dir))
	if err == nil && raw != plan.RawBytes {
		return p, plan, fmt.Errorf("raw repair size %d != planned %d", raw, plan.RawBytes)
	}
	return p, plan, err
}
