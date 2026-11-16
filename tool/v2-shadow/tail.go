package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
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
		table := crc32.MakeTable(crc32.Castagnoli)
		if string(s.header[:8]) != "GCINLOG1" || crc32.Checksum(s.header[:32], table) != binary.LittleEndian.Uint32(s.header[32:]) {
			err = io.ErrUnexpectedEOF
		} else if seq, e := strconv.ParseUint(strings.TrimSuffix(filepath.Base(path), ".log"), 16, 64); e == nil && seq != binary.LittleEndian.Uint64(s.header[8:]) {
			err = io.ErrUnexpectedEOF
		} else {
			s.replay, err = inputlog.NewSegmentReplay(f)
		}
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// A newly visible preallocated path can precede its new segment header.
func readyShadowTail(path string) (*shadowTail, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		s, err := openShadowTail(path)
		if err == nil {
			return s, nil
		}
		if (!errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF)) || time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(25 * time.Millisecond)
	}
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
