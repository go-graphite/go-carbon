package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type segment struct {
	Path                  string
	Created               int64
	Bytes, Points, Frames int64
	f                     *os.File
	header                [64]byte
}

func scan(path string) (s segment, err error) {
	s.Path = path
	s.f, err = os.Open(path)
	if err != nil {
		return
	}
	h := make([]byte, 64)
	_, err = s.f.ReadAt(h, 0)
	if err != nil {
		return
	}
	tab := crc32.MakeTable(crc32.Castagnoli)
	if string(h[:8]) != "GCINLOG1" || binary.LittleEndian.Uint32(h[32:]) != crc32.Checksum(h[:32], tab) {
		err = fmt.Errorf("segment header: %s", path)
		return
	}
	copy(s.header[:], h)
	salt := binary.LittleEndian.Uint64(h[16:])
	s.Created = int64(binary.LittleEndian.Uint64(h[24:]))
	s.Bytes = 64
	b := make([]byte, 52)
	for {
		_, e := s.f.ReadAt(b, s.Bytes)
		if e == io.EOF {
			break
		}
		if e != nil {
			err = e
			return
		}
		if binary.LittleEndian.Uint32(b) != 0x4c494347 || binary.LittleEndian.Uint64(b[8:]) != salt {
			break
		}
		if binary.LittleEndian.Uint32(b[48:]) != crc32.Checksum(b[:48], tab) {
			err = fmt.Errorf("frame CRC: %s:%d", path, s.Bytes)
			return
		}
		s.Bytes += 52 + int64(binary.LittleEndian.Uint32(b[16:]))
		s.Points += int64(binary.LittleEndian.Uint32(b[24:]))
		s.Frames++
	}
	return
}
