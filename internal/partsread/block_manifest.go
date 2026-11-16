package partsread

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const MaxBlockBytes = 64 << 10
const sampleBytes = 24

// BlockRef addresses an immutable file, never a mutable database table. File is
// relative to the atomically published part directory. CRC32 is IEEE CRC-32.
type BlockRef struct {
	File   string
	Offset int64
	Length uint32
	CRC32  uint32
}

type LocatedBlock struct {
	SlotBlock
	Ref BlockRef
}

// BlockManifest is the durable directory for one sealed generation. Key carries
// the complete placement-group/vshard/series/archive/block identity; ID is the
// catalog's read handle. The owner must publish catalog and manifest coherently.
type BlockManifest struct {
	Version    uint32
	Generation uint64
	Blocks     []LocatedBlock
}

// BlockData is a writer's frozen block. Samples must include all retained slot
// versions, including occupied NaNs and out-of-query overwrite timestamps.
type BlockData struct {
	SlotBlock
	Samples []Sample
}

// ArchiveBlockBytes bounds payload per archive, independently of slot count or
// version density. Fine archives favor sequential bandwidth; coarser archives
// bound over-read for short and multi-archive queries.
func ArchiveBlockBytes(a Archive) int {
	if a.Step < 60 {
		return 12 << 10
	}
	return 4 << 10
}

func validPartName(s string) bool {
	return s != "" && s != "." && s != ".." && filepath.Base(s) == s
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

// PublishBlockPart durably writes data and manifest in a staging directory,
// then renames the complete directory and syncs its parent. name is unique and
// never reused. A failure after rename can leave a complete published part;
// recovery must reconcile by generation, rather than overwrite it. This helper
// does not publish a shard catalog/frontier: that remains the owner's envelope.
func PublishBlockPart(parent, name string, generation uint64, blocks []BlockData) (string, error) {
	if !validPartName(name) {
		return "", errors.New("invalid immutable part name")
	}
	stage, err := os.MkdirTemp(parent, ".part-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(stage)
	file, err := os.OpenFile(filepath.Join(stage, "blocks"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	defer file.Close()
	manifest := BlockManifest{Version: 1, Generation: generation}
	var offset int64
	seen := map[string]bool{}
	for _, b := range blocks {
		if len(b.Samples) == 0 || len(b.Samples) > ArchiveBlockBytes(b.Archive)/sampleBytes || seen[string(b.Key)] {
			return "", errors.New("invalid or duplicate immutable block")
		}
		seen[string(b.Key)] = true
		payload := make([]byte, 0, len(b.Samples)*sampleBytes)
		for _, p := range b.Samples {
			payload = binary.LittleEndian.AppendUint64(payload, uint64(p.Time))
			payload = binary.LittleEndian.AppendUint64(payload, p.Bits)
			payload = binary.LittleEndian.AppendUint64(payload, p.Sequence)
		}
		if _, err = file.Write(payload); err != nil {
			return "", err
		}
		manifest.Blocks = append(manifest.Blocks, LocatedBlock{SlotBlock: b.SlotBlock, Ref: BlockRef{File: "blocks", Offset: offset, Length: uint32(len(payload)), CRC32: crc32.ChecksumIEEE(payload)}})
		offset += int64(len(payload))
	}
	if _, err = NewSlotIndex(slotBlocks(manifest.Blocks), func(context.Context, []byte, []SlotRange, func(Sample) error) error { return nil }); err != nil {
		return "", err
	}
	if err = file.Sync(); err != nil {
		return "", err
	}
	if err = file.Close(); err != nil {
		return "", err
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	if len(data) > 16<<20 {
		return "", errors.New("immutable manifest exceeds limit")
	}
	mf, err := os.OpenFile(filepath.Join(stage, "manifest.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	_, writeErr := mf.Write(data)
	err = errors.Join(writeErr, mf.Sync(), mf.Close())
	if err != nil {
		return "", err
	}
	if err = syncDir(stage); err != nil {
		return "", err
	}
	dest := filepath.Join(parent, name)
	// A committed directory always contains a manifest, so Rename cannot replace
	// it. Do not permit an existing empty directory to stand in for a generation.
	if _, err = os.Lstat(dest); err == nil {
		return "", os.ErrExist
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if err = os.Rename(stage, dest); err != nil {
		return "", err
	}
	return dest, syncDir(parent)
}

func slotBlocks(blocks []LocatedBlock) []SlotBlock {
	out := make([]SlotBlock, len(blocks))
	for i := range blocks {
		out[i] = blocks[i].SlotBlock
	}
	return out
}

// BlockFileSource owns open descriptors, so unlink/compaction after publication
// cannot invalidate a pinned view. Close belongs in View.Close, never in a
// per-request defer. The owner serializes OpenBlockPart against retirement;
// once all descriptors are open, directory paths are no longer used for reads.
type BlockFileSource struct {
	*SlotIndex
	files          map[string]*os.File
	refs           map[string]*fileBlock
	closeOnce      sync.Once
	closed         atomic.Bool
	cacheRemaining atomic.Int64
	retained       int64
	Generation     uint64
}

func OpenBlockPart(path string) (*BlockFileSource, error) {
	return OpenBlockPartWithIDs(path, nil)
}

// OpenBlockPartWithIDs remaps local read handles from complete portable keys.
// Use this when the current catalog assigns different IDs after restart/copy.
// The resolver is called only while opening a generation, never during a query.
func OpenBlockPartWithIDs(path string, resolve func([]byte) (uint64, error)) (*BlockFileSource, error) {
	f, err := os.Open(filepath.Join(path, "manifest.json"))
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, (16<<20)+1))
	err = errors.Join(err, f.Close())
	if err != nil {
		return nil, err
	}
	if len(data) > 16<<20 {
		return nil, errors.New("immutable manifest exceeds limit")
	}
	var m BlockManifest
	if err = json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m.Version != 1 {
		return nil, errors.New("unsupported immutable manifest")
	}
	if resolve != nil {
		for i := range m.Blocks {
			id, e := resolve(m.Blocks[i].Key)
			if e != nil {
				return nil, e
			}
			m.Blocks[i].ID = id
		}
	}
	s := &BlockFileSource{files: map[string]*os.File{}, refs: map[string]*fileBlock{}, Generation: m.Generation}
	fail := func(err error) (*BlockFileSource, error) { s.Close(); return nil, err }
	for _, b := range m.Blocks {
		r := b.Ref
		if !validPartName(r.File) || r.Offset < 0 || r.Length == 0 || r.Length > MaxBlockBytes || r.Length%sampleBytes != 0 || r.Offset > int64(^uint64(0)>>1)-int64(r.Length) {
			return fail(errors.New("invalid immutable extent"))
		}
		if _, exists := s.refs[string(b.Key)]; exists {
			return fail(errors.New("duplicate immutable block identity"))
		}
		if s.files[r.File] == nil {
			info, e := os.Lstat(filepath.Join(path, r.File))
			if e != nil {
				return fail(e)
			}
			if !info.Mode().IsRegular() {
				return fail(errors.New("immutable extent is not a regular file"))
			}
			opened, e := os.Open(filepath.Join(path, r.File))
			if e != nil {
				return fail(e)
			}
			s.files[r.File] = opened
		}
		info, e := s.files[r.File].Stat()
		if e != nil {
			return fail(e)
		}
		if r.Offset+int64(r.Length) > info.Size() {
			return fail(io.ErrUnexpectedEOF)
		}
		s.refs[string(b.Key)] = &fileBlock{ref: r}
		s.retained += int64(2*len(b.Key) + 2*len(r.File) + 256)
	}
	s.SlotIndex, err = NewSlotIndex(slotBlocks(m.Blocks), s.readBlock)
	if err != nil {
		return fail(err)
	}
	// Reserve the entire lazy cache capacity before publication. Each source
	// admits at most 64KiB of decoded samples, shared by its pinned generation.
	var payloadBytes int64
	for _, b := range s.refs {
		payloadBytes += int64(b.ref.Length)
	}
	cacheBytes := min(payloadBytes, int64(MaxBlockBytes))
	s.cacheRemaining.Store(cacheBytes)
	s.retained += cacheBytes
	// Also reserve one maximum decode buffer for each pinned reader.
	s.retained += s.SlotIndex.RetainedBytes() + int64(len(s.files))*256 + MaxBlockBytes
	return s, nil
}

func (s *BlockFileSource) RetainedBytes() int64 { return s.retained }
func (s *BlockFileSource) Close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		for _, f := range s.files {
			_ = f.Close()
		}
	})
}

// fileBlock publishes only immutable, CRC-verified decoded samples. Its mutex
// covers a cold fill only; readers never hold it while calling a consumer.
type fileBlock struct {
	ref     BlockRef
	mu      sync.Mutex
	decoded atomic.Pointer[[]Sample]
}

// Idle payload scratch has a process-wide 4MiB bound. Active calls are already
// charged one MaxBlockBytes buffer by the pinned view's RetainedBytes.
var blockPayloads = make(chan []byte, 64)

func takeBlockPayload() []byte {
	select {
	case b := <-blockPayloads:
		return b
	default:
		return make([]byte, MaxBlockBytes)
	}
}
func releaseBlockPayload(b []byte) {
	select {
	case blockPayloads <- b[:MaxBlockBytes]:
	default:
	}
}
func decodeBlockSample(b []byte) Sample {
	return Sample{Time: int64(binary.LittleEndian.Uint64(b)), Bits: binary.LittleEndian.Uint64(b[8:]), Sequence: binary.LittleEndian.Uint64(b[16:])}
}
func (s *BlockFileSource) payload(ref BlockRef, b []byte) ([]byte, error) {
	b = b[:ref.Length]
	if _, err := s.files[ref.File].ReadAt(b, ref.Offset); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(b) != ref.CRC32 {
		return nil, fmt.Errorf("immutable block checksum mismatch: %s@%d", ref.File, ref.Offset)
	}
	return b, nil
}
func (s *BlockFileSource) cached(b *fileBlock) ([]Sample, error) {
	if p := b.decoded.Load(); p != nil {
		return *p, nil
	}
	if s.cacheRemaining.Load() < int64(b.ref.Length) {
		return nil, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if p := b.decoded.Load(); p != nil {
		return *p, nil
	}
	n := int64(b.ref.Length)
	for remaining := s.cacheRemaining.Load(); remaining >= n; remaining = s.cacheRemaining.Load() {
		if !s.cacheRemaining.CompareAndSwap(remaining, remaining-n) {
			continue
		}
		scratch := takeBlockPayload()
		payload, err := s.payload(b.ref, scratch)
		if err != nil {
			releaseBlockPayload(scratch)
			s.cacheRemaining.Add(n)
			return nil, err
		}
		samples := make([]Sample, len(payload)/sampleBytes)
		for i := range samples {
			samples[i] = decodeBlockSample(payload[i*sampleBytes:])
		}
		releaseBlockPayload(scratch)
		b.decoded.Store(&samples)
		return samples, nil
	}
	return nil, nil
}
func (s *BlockFileSource) readBlock(ctx context.Context, key []byte, _ []SlotRange, yield func(Sample) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed.Load() {
		return ErrClosed
	}
	b, ok := s.refs[string(key)]
	if !ok {
		return errors.New("immutable block missing from manifest")
	}
	samples, err := s.cached(b)
	if err != nil {
		return err
	}
	if samples != nil {
		for _, p := range samples {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := yield(p); err != nil {
				return err
			}
		}
		return nil
	}
	// Blocks outside the generation's cache budget retain per-read CRC checking.
	scratch := takeBlockPayload()
	defer releaseBlockPayload(scratch)
	payload, err := s.payload(b.ref, scratch)
	if err != nil {
		return err
	}
	for len(payload) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := yield(decodeBlockSample(payload)); err != nil {
			return err
		}
		payload = payload[sampleBytes:]
	}
	return nil
}
