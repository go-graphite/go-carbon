package partlog

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
)

// A sidecar is published only after the immutable segment is synced. It binds
// validated bounds to the journal identity and the exact file incarnation.
// Missing, stale, unsupported or damaged sidecars fall back to full scanning.
// Checksums are still checked before every replay callback: storage corruption
// which does not update filesystem metadata is detected when data is consumed.
type segmentStamp struct {
	Device, Inode                                uint64
	Size, Modified, ChangedSeconds, ChangedNanos int64
}

type segmentIndex struct {
	Version     int
	Identity    string
	Stream      int
	First, Last uint64
	Stamp       segmentStamp
}

func (s *stream) loadIndex(path string) (segment, bool) {
	data, err := os.ReadFile(path + ".index")
	if err != nil || len(data) < 4 || len(data) > 4096 || crc32.Checksum(data[4:], checksum) != binary.LittleEndian.Uint32(data) {
		return segment{}, false
	}
	var idx segmentIndex
	if json.Unmarshal(data[4:], &idx) != nil || idx.Version != 1 || idx.Identity != s.log.identity || idx.Stream != s.id || idx.First == 0 || idx.Last < idx.First || idx.Stamp.Size < headerSize {
		return segment{}, false
	}
	f, err := os.Open(path)
	if err != nil {
		return segment{}, false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return segment{}, false
	}
	stamp, ok := fileStamp(info)
	if !ok || stamp != idx.Stamp {
		return segment{}, false
	}
	var h [headerSize]byte
	if _, err = io.ReadFull(f, h[:]); err != nil {
		return segment{}, false
	}
	id, _ := hex.DecodeString(idx.Identity)
	if binary.LittleEndian.Uint32(h[:]) != magic || binary.LittleEndian.Uint64(h[8:]) != idx.First || !bytes.Equal(h[16:], id) {
		return segment{}, false
	}
	return segment{path, idx.First, idx.Last, stamp.Size}, true
}

func (s *stream) saveIndex(seg segment) error {
	if seg.last < seg.first {
		return nil
	}
	info, err := os.Stat(seg.path)
	if err != nil {
		return err
	}
	stamp, ok := fileStamp(info)
	if !ok {
		return nil
	}
	if stamp.Size != seg.size {
		return io.ErrUnexpectedEOF
	}
	idx := segmentIndex{1, s.log.identity, s.id, seg.first, seg.last, stamp}
	body, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	data := binary.LittleEndian.AppendUint32(nil, crc32.Checksum(body, checksum))
	data = append(data, body...)
	path := seg.path + ".index"
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if n, e := f.Write(data); e != nil {
		err = e
	} else if n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	s.hook("before-index-rename")
	if err = os.Rename(path+".tmp", path); err != nil {
		return err
	}
	if err = syncDir(s.log.dir); err != nil {
		return err
	}
	s.hook("after-index-rename")
	return nil
}
