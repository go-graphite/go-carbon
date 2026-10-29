package chunkstore

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
)

// V2 keeps stored key/value bytes unchanged and frames metadata without a JSON
// marshal/unmarshal and duplicate name allocation for every sparse metric.
func TransferWireSize2(records []TransferMetric) int64 {
	n := int64(44)
	for _, r := range records {
		n += int64(56 + len(r.Config.Name) + 8*len(r.Config.Retentions) + len(r.PointDigest) + 4*len(r.Counts))
		for _, kv := range r.Keys {
			n += int64(8 + len(kv.Key) + len(kv.Value))
		}
	}
	return n
}
func WriteTransfer2(w io.Writer, records []TransferMetric) error {
	for _, record := range records {
		if record.Version.Name != record.Config.Name {
			return ErrFormat
		}
	}
	h := sha256.New()
	out := io.MultiWriter(w, h)
	var data [8]byte
	var err error
	write := func(v []byte) {
		if err == nil {
			_, err = out.Write(v)
		}
	}
	u32 := func(n uint32) { binary.BigEndian.PutUint32(data[:4], n); write(data[:4]) }
	u64 := func(n uint64) { binary.BigEndian.PutUint64(data[:], n); write(data[:]) }
	blob := func(v []byte) { u32(uint32(len(v))); write(v) }
	write([]byte("BKTFR002"))
	u32(uint32(len(records)))
	for _, r := range records {
		blob([]byte(r.Config.Name))
		u64(r.Version.ID)
		u64(r.Version.Generation)
		u64(r.Version.Revision)
		flags := uint32(0)
		if r.Continuation {
			flags = 1
		}
		u32(flags)
		u32(uint32(r.Config.AggregationMethod))
		u32(math.Float32bits(r.Config.XFilesFactor))
		u32(uint32(len(r.Config.Retentions)))
		for _, retention := range r.Config.Retentions {
			u32(uint32(retention.Step))
			u32(uint32(retention.Count))
		}
		blob([]byte(r.PointDigest))
		u32(uint32(len(r.Counts)))
		for _, count := range r.Counts {
			u32(uint32(count))
		}
		u32(uint32(len(r.Keys)))
		for _, kv := range r.Keys {
			blob(kv.Key)
			blob(kv.Value)
		}
	}
	if err != nil {
		return err
	}
	_, err = w.Write(h.Sum(nil))
	return err
}

type transferDecoder struct {
	data []byte
	err  error
}

func (d *transferDecoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || n > len(d.data) {
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	v := d.data[:n:n]
	d.data = d.data[n:]
	return v
}
func (d *transferDecoder) u32() uint32 {
	v := d.take(4)
	if v == nil {
		return 0
	}
	return binary.BigEndian.Uint32(v)
}
func (d *transferDecoder) u64() uint64 {
	v := d.take(8)
	if v == nil {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}
func (d *transferDecoder) blob(limit uint32) []byte {
	n := d.u32()
	if n > limit || uint64(n) > uint64(len(d.data)) {
		d.err = ErrTransferLimit
		return nil
	}
	return d.take(int(n))
}
func (d *transferDecoder) count(width uint32) int {
	n := d.u32()
	if uint64(n)*uint64(width) > uint64(len(d.data)) {
		d.err = ErrTransferLimit
		return 0
	}
	return int(n)
}
func readTransfer2(data []byte, count uint32) ([]TransferMetric, error) {
	d := transferDecoder{data: data}
	records := make([]TransferMetric, count)
	for i := range records {
		r := &records[i]
		r.Config.Name = string(d.blob(4096))
		r.Version.Name = r.Config.Name
		r.Version.ID = d.u64()
		r.Version.Generation = d.u64()
		r.Version.Revision = d.u64()
		flags := d.u32()
		if flags > 1 {
			return nil, ErrFormat
		}
		r.Continuation = flags == 1
		r.Config.AggregationMethod = AggregationMethod(d.u32())
		r.Config.XFilesFactor = math.Float32frombits(d.u32())
		n := d.count(8)
		if n > 0 {
			r.Config.Retentions = make([]Retention, n)
		}
		for j := range r.Config.Retentions {
			r.Config.Retentions[j] = Retention{Step: int(d.u32()), Count: int(d.u32())}
		}
		r.PointDigest = string(d.blob(256))
		n = d.count(4)
		if n > 0 {
			r.Counts = make([]int, n)
		}
		for j := range r.Counts {
			r.Counts[j] = int(d.u32())
		}
		n = d.count(8)
		if n > 0 {
			r.Keys = make([]RelayoutKV, n)
		}
		for j := range r.Keys {
			r.Keys[j] = RelayoutKV{d.blob(4096), d.blob(16 << 20)}
		}
		if d.err != nil {
			return nil, d.err
		}
	}
	if len(d.data) != 0 {
		return nil, errors.New("trailing transfer bytes")
	}
	return records, nil
}
