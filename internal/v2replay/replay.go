// Package v2replay reads bounded producer journals into the caller's normal
// write path. The caller owns payload validation and core cursor filtering.
package v2replay

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const MaxRecord = 8 << 20

// Scan validates every complete record, including committed prefixes (predictor
// state crosses records). Only the newest segment may have a torn EOF; complete
// invalid records fail closed. Truncation is synced before a new segment opens.
// The record buffer is reused: apply must not retain it after returning.
func Scan(path string, newest bool, apply func([]byte) error) (int64, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var offset int64
	var buf []byte
	for {
		var h [4]byte
		n, err := io.ReadFull(r, h[:])
		if errors.Is(err, io.EOF) && n == 0 {
			return offset, nil
		}
		if err != nil {
			if newest && (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) {
				return offset, errors.Join(f.Truncate(offset), f.Sync())
			}
			return offset, fmt.Errorf("journal header at %d: %w", offset, err)
		}
		size := binary.LittleEndian.Uint32(h[:])
		if size == 0 || size > MaxRecord {
			return offset, fmt.Errorf("invalid journal length at %d", offset)
		}
		if cap(buf) < int(size) {
			buf = make([]byte, int(size))
		}
		b := buf[:int(size)]
		if _, err = io.ReadFull(r, b); err != nil {
			if newest && (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) {
				return offset, errors.Join(f.Truncate(offset), f.Sync())
			}
			return offset, fmt.Errorf("journal payload at %d: %w", offset, err)
		}
		if err = apply(b); err != nil {
			return offset, fmt.Errorf("journal record at %d: %w", offset, err)
		}
		offset += int64(4 + size)
	}
}
