// Package rawlog is an experimental journal of unchanged stream wire frames.
// It is deliberately not wired into the daemon's recovery or retention logic.
package rawlog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const magic = "GCRAW001"
const headerSize = 40
const (
	openRecord byte = iota
	frameRecord
	snapshotRecord
	readyRecord
)
const maxRecord = 8<<20 + 20

var table = crc32.MakeTable(crc32.Castagnoli)
var ErrCorrupt = errors.New("raw log corrupt or unsupported")

type record struct {
	kind      byte
	seq, conn uint64
	arrival   int64
	data      []byte
}

func (r record) write(w io.Writer) error {
	var h [headerSize]byte
	h[0] = r.kind
	binary.BigEndian.PutUint32(h[4:8], uint32(len(r.data)))
	binary.BigEndian.PutUint64(h[8:16], r.seq)
	binary.BigEndian.PutUint64(h[16:24], r.conn)
	binary.BigEndian.PutUint64(h[24:32], uint64(r.arrival))
	sum := crc32.Update(0, table, h[:32])
	sum = crc32.Update(sum, table, r.data)
	binary.BigEndian.PutUint32(h[32:36], sum)
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.Write(r.data)
	return err
}

func readRecord(rd io.Reader) (record, error) {
	var h [headerSize]byte
	if _, err := io.ReadFull(rd, h[:]); err != nil {
		return record{}, err
	}
	n := binary.BigEndian.Uint32(h[4:8])
	if h[0] > readyRecord || h[1]|h[2]|h[3]|h[36]|h[37]|h[38]|h[39] != 0 || n > maxRecord {
		return record{}, ErrCorrupt
	}
	r := record{kind: h[0], seq: binary.BigEndian.Uint64(h[8:16]), conn: binary.BigEndian.Uint64(h[16:24]), arrival: int64(binary.BigEndian.Uint64(h[24:32])), data: make([]byte, n)}
	if _, err := io.ReadFull(rd, r.data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return record{}, err
	}
	sum := crc32.Update(0, table, h[:32])
	sum = crc32.Update(sum, table, r.data)
	if sum != binary.BigEndian.Uint32(h[32:36]) {
		return record{}, ErrCorrupt
	}
	return r, nil
}

func checkMagic(r io.Reader) error {
	var b [len(magic)]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	if string(b[:]) != magic {
		return ErrCorrupt
	}
	return nil
}
