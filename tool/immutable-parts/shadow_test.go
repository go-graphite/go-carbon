package main

// This opt-in harness uses the unchanged experimental core with real journal
// frames. It is RAW-ONLY: no rollups, live render comparison, history seeding or
// restart contract is implemented. Example (compile with go test -c):
//
// V2_SHADOW_LOG_DIR=/tmp/journal V2_SHADOW_OUT=/tmp/new-shadow \
// V2_SHADOW_DURATION=2m ./immutable-parts.test -test.run '^TestLiveShadow$' -test.v
//
// Source journals are opened O_RDONLY, without
// taking the daemon's lock or invoking Log.Open. Each run requires a fresh sink.
import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/tags"
)

type shadowTail struct {
	file   *os.File
	replay *inputlog.SegmentReplay
	header [64]byte
	offset int64
}

func openShadowTail(path string) (*shadowTail, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	s := &shadowTail{file: f, offset: 64}
	if _, err = f.ReadAt(s.header[:], 0); err == nil {
		s.replay, err = inputlog.NewSegmentReplay(f)
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}
func (s *shadowTail) close() { s.replay.Close(); _ = s.file.Close() }
func (s *shadowTail) next() ([]*points.Points, int64, error) {
	var header [64]byte
	if _, err := s.file.ReadAt(header[:], 0); err != nil {
		return nil, 0, err
	}
	if header != s.header {
		return nil, 0, errors.New("source recycled: refusing to skip journal data")
	}
	var h [52]byte
	if _, err := s.file.ReadAt(h[:], s.offset); err != nil {
		return nil, 0, err
	}
	if binary.LittleEndian.Uint32(h[:]) != 0x4c494347 || !bytes.Equal(h[8:16], s.header[16:24]) {
		return nil, 0, io.EOF
	}
	table := crc32.MakeTable(crc32.Castagnoli)
	if crc32.Checksum(h[:48], table) != binary.LittleEndian.Uint32(h[48:]) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	n := binary.LittleEndian.Uint32(h[16:])
	if n == 0 || n > 64<<20 {
		return nil, 0, errors.New("invalid journal frame length")
	}
	// The preallocated file may expose a header before its payload. Validate the
	// payload first, before allowing the stateful dictionary decoder to mutate.
	payload := make([]byte, n)
	if _, err := s.file.ReadAt(payload, s.offset+52); err != nil {
		return nil, 0, err
	}
	if crc32.Checksum(payload, table) != binary.LittleEndian.Uint32(h[44:]) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	end := s.offset + 52 + int64(n)
	rows, off, _, err := s.replay.Next(end)
	if err != nil {
		return nil, 0, err
	}
	if _, err = s.file.ReadAt(header[:], 0); err != nil {
		return nil, 0, err
	}
	if header != s.header {
		return nil, 0, errors.New("source recycled during decode")
	}
	s.offset = off
	return rows, int64(n) + 52, nil
}

func TestShadowTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	writer, err := inputlog.NewSegmentWriter(f, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	bits := uint64(0x7ff8000000000042)
	if _, err = writer.Append([]*points.Points{points.OnePoint("a", math.Float64frombits(bits), 123)}, nil); err != nil {
		t.Fatal(err)
	}
	tail, err := openShadowTail(path)
	if err != nil {
		t.Fatal(err)
	}
	defer tail.close()
	// A header-visible partial payload must not advance the dictionary or cursor.
	info, _ := f.Stat()
	last := make([]byte, 1)
	_, _ = f.ReadAt(last, info.Size()-1)
	if err = f.Truncate(info.Size() - 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err = tail.next(); err == nil {
		t.Fatal("accepted partial payload")
	}
	if tail.offset != 64 {
		t.Fatal("advanced partial frame")
	}
	if _, err = f.WriteAt(last, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	groups, _, err := tail.next()
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].Metric != "a" || len(groups[0].Data) != 1 || math.Float64bits(groups[0].Data[0].Value) != bits {
		t.Fatalf("raw bits changed: %+v", groups)
	}
	if _, err = writer.Append([]*points.Points{points.OnePoint("a", math.Copysign(0, -1), 124)}, nil); err != nil {
		t.Fatal(err)
	}
	groups, _, err = tail.next()
	if err != nil {
		t.Fatal(err)
	}
	if math.Float64bits(groups[0].Data[0].Value) != 1<<63 {
		t.Fatal("lost signed zero across growing frontier")
	}
	if _, err = f.WriteAt([]byte{0}, 16); err != nil {
		t.Fatal(err)
	}
	if _, _, err = tail.next(); err == nil {
		t.Fatal("accepted recycled segment")
	}
}

func TestLiveShadow(t *testing.T) {
	source := os.Getenv("V2_SHADOW_LOG_DIR")
	out := os.Getenv("V2_SHADOW_OUT")
	if source == "" || out == "" {
		t.Skip("opt-in raw-only journal shadow; set V2_SHADOW_LOG_DIR and fresh V2_SHADOW_OUT")
	}
	duration := 2 * time.Minute
	if v := os.Getenv("V2_SHADOW_DURATION"); v != "" {
		var err error
		duration, err = time.ParseDuration(v)
		if err != nil || duration <= 0 {
			t.Fatal("invalid duration")
		}
	}
	shards := 8
	if v := os.Getenv("V2_SHADOW_SHARDS"); v != "" {
		var err error
		shards, err = strconv.Atoi(v)
		if err != nil || shards < 1 || shards > 64 {
			t.Fatal("invalid shards")
		}
	}
	if err := os.Mkdir(out, 0750); err != nil {
		t.Fatal("fresh output required:", err)
	}
	paths, err := filepath.Glob(filepath.Join(source, "*.log"))
	if err != nil || len(paths) == 0 {
		t.Fatal("no source journal")
	}
	// Start at the newest segment's dictionary prefix, never at an arbitrary
	// byte offset. Older segments/history are deliberately outside this run.
	path := paths[len(paths)-1]
	tail, err := openShadowTail(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { tail.close() }()
	cfg := coreConfig{AppendParts: true, JournalIdentity: fmt.Sprintf("shadow:%x", tail.header), Shards: shards, Producers: 1, MaxRows: 262144, PartitionSeconds: 3600}
	engine, err := openCore(filepath.Join(out, "parts"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if engine != nil {
			if err := engine.close(); err != nil {
				t.Error(err)
			}
		}
	}()
	start := time.Now()
	deadline := start.Add(duration)
	sampleAt := start
	var partialSince time.Time
	names := map[string]uint64{}
	nameFile, err := os.Create(filepath.Join(out, "names.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer nameFile.Close()
	nameBuffer := bufio.NewWriterSize(nameFile, 256<<10)
	nameEncoder := json.NewEncoder(nameBuffer)
	var sequence, sourceBytes uint64
	var newest int64
	batch := make([]row, 0, 32768)
	flush := func(seal bool) error {
		if len(batch) == 0 {
			return nil
		}
		err := engine.submit(0, batch, sequence, seal)
		batch = batch[:0]
		return err
	}
	baseline := procStats()
	enc := json.NewEncoder(os.Stdout)
	emit := func(stage string) {
		sample := coreSample(engine, stage, start, sequence)
		sample["raw_only"] = true
		sample["render_comparisons"] = 0
		sample["baseline_proc"] = baseline
		sample["canonical_names"] = len(names)
		sample["source_path"] = path
		sample["source_offset"] = tail.offset
		sample["source_bytes_decoded"] = sourceBytes
		var behind int64
		if current, e := tail.file.Stat(); e == nil {
			if end, e := sealedFrontier(tail.file, current.Size()); e == nil {
				behind += max(int64(0), end-tail.offset)
			} else {
				sample["header_lag_error"] = e.Error()
			}
		}
		paths, _ := filepath.Glob(filepath.Join(source, "*.log"))
		for _, pending := range paths {
			if pending <= path {
				continue
			}
			f, e := os.Open(pending)
			if e != nil {
				continue
			}
			info, e := f.Stat()
			if e == nil {
				end, e := sealedFrontier(f, info.Size())
				if e == nil {
					behind += end - 64
				} else {
					sample["header_lag_error"] = e.Error()
				}
			}
			_ = f.Close()
		}
		sample["header_lag_bytes"] = behind
		sample["header_lag_is_synced_frontier"] = false
		sample["newest_event_time"] = newest
		if newest != 0 {
			sample["event_age_seconds"] = time.Now().Unix() - newest
		}
		sample["event_age_is_ingest_lag"] = false // source timestamps may already be late
		if err := enc.Encode(sample); err != nil {
			t.Error(err)
		}
	}
	emit("shadow_start")
	for time.Now().Before(deadline) {
		if time.Now().After(sampleAt) {
			emit("shadow_sample")
			sampleAt = time.Now().Add(10 * time.Second)
			if procStats()["write_bytes"]-baseline["write_bytes"] > 90e9 {
				t.Fatal("90GB physical write safety budget reached")
			}
		}
		groups, n, err := tail.next()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				if partialSince.IsZero() {
					partialSince = time.Now()
				}
				if time.Since(partialSince) > 5*time.Second {
					t.Fatal("partial/corrupt source frame stalled for 5s")
				}
			}
			// Switch only after the current segment reaches its valid end and the
			// immediate successor exists. Gaps and recycling fail closed.
			if errors.Is(err, io.EOF) {
				seq := binary.LittleEndian.Uint64(tail.header[8:])
				nextPath := filepath.Join(source, fmt.Sprintf("%016x.log", seq+1))
				if _, e := os.Stat(nextPath); e == nil {
					next, e := openShadowTail(nextPath)
					if e != nil {
						t.Fatal(e)
					}
					if binary.LittleEndian.Uint64(next.header[8:]) != seq+1 {
						next.close()
						t.Fatal("segment sequence gap")
					}
					tail.close()
					tail = next
					path = nextPath
					continue
				} else if !errors.Is(e, os.ErrNotExist) {
					t.Fatal(e)
				} else {
					available, e := filepath.Glob(filepath.Join(source, "*.log"))
					if e != nil {
						t.Fatal(e)
					}
					for _, later := range available {
						if later > nextPath {
							t.Fatal("source segment gap before", later)
						}
					}
				}
			}
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		partialSince = time.Time{}
		sourceBytes += uint64(n)
		for _, group := range groups {
			name, e := tags.Normalize(group.Metric)
			if e != nil {
				t.Fatal(e)
			}
			id, ok := names[name]
			if !ok {
				id = uint64(len(names))
				names[name] = id
				if e = nameEncoder.Encode(struct {
					ID   uint64
					Name string
				}{id, name}); e != nil {
					t.Fatal(e)
				}
			}
			for _, point := range group.Data {
				sequence++
				newest = max(newest, point.Timestamp)
				batch = append(batch, row{id, point.Timestamp, math.Float64bits(point.Value), sequence})
				if len(batch) == cap(batch) {
					if e = flush(false); e != nil {
						t.Fatal(e)
					}
				}
			}
		}
	}
	if err = flush(true); err != nil {
		t.Fatal(err)
	}
	if sequence > 0 {
		if err = engine.submit(0, nil, sequence, true); err != nil {
			t.Fatal(err)
		}
	}
	if err = nameBuffer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = nameFile.Sync(); err != nil {
		t.Fatal(err)
	}
	frontier := engine.globalFrontier()
	if len(frontier) != 1 || frontier[0] != sequence {
		t.Fatalf("frontier %v != submitted %d", frontier, sequence)
	}
	var committed uint64
	for _, owner := range engine.owners {
		committed += owner.committed.Load()
	}
	if committed != sequence {
		t.Fatalf("committed %d != submitted %d", committed, sequence)
	}
	emit("shadow_sealed")
	if err = engine.close(); err != nil {
		engine = nil
		t.Fatal(err)
	}
	engine = nil
	if err = enc.Encode(map[string]any{"stage": "shadow_closed", "seconds": time.Since(start).Seconds(), "source_points": sequence, "raw_only": true, "proc": procStats(), "baseline_proc": baseline}); err != nil {
		t.Fatal(err)
	}
}

func TestShadowEngineInput(t *testing.T) {
	source := t.TempDir()
	path := filepath.Join(source, "0000000000000001.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	writer, err := inputlog.NewSegmentWriter(f, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	want := []uint64{0x7ff8000000000042, 1 << 63}
	record := points.OnePoint("fixture", math.Float64frombits(want[0]), 123)
	record.Add(math.Float64frombits(want[1]), 124)
	if _, err = writer.Append([]*points.Points{record}, nil); err != nil {
		t.Fatal(err)
	}
	tail, err := openShadowTail(path)
	if err != nil {
		t.Fatal(err)
	}
	header := tail.header
	tail.close()
	out := filepath.Join(t.TempDir(), "sink")
	t.Setenv("V2_SHADOW_LOG_DIR", source)
	t.Setenv("V2_SHADOW_OUT", out)
	t.Setenv("V2_SHADOW_DURATION", "25ms")
	t.Setenv("V2_SHADOW_SHARDS", "2")
	TestLiveShadow(t)
	cfg := coreConfig{AppendParts: true, JournalIdentity: fmt.Sprintf("shadow:%x", header), Shards: 2, Producers: 1, MaxRows: 262144, PartitionSeconds: 3600}
	engine, err := openCore(filepath.Join(out, "parts"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.close()
	owner := engine.owners[coreShard(0, 2)]
	snap := owner.db.NewSnapshot()
	defer snap.Close()
	rows, err := coreRead(snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows", len(rows))
	}
	for i, r := range rows {
		if r.ID != 0 || r.Time != int64(123+i) || r.Bits != want[i] || r.Seq != uint64(i+1) {
			t.Fatalf("row %d: %+v", i, r)
		}
	}
}
