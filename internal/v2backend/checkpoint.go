package v2backend

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// V2C2 keeps the owner frontier small. Metric and scope records are replacements
// staged in the SAME atomic ingest as this header and its data. No delta chain
// grows with uptime, and checkpointing never reads an existing key.
func encodeCheckpoint(c checkpoint) []byte {
	b := append(make([]byte, 0, 32+len(c.Journal)), "V2C2"...)
	b = binary.BigEndian.AppendUint64(b, c.Cursor)
	b = binary.BigEndian.AppendUint64(b, c.Points)
	b = binary.BigEndian.AppendUint64(b, c.Order)
	return appendString(b, c.Journal)
}
func checkpointPrefix(id int, kind byte) []byte {
	b := binary.BigEndian.AppendUint32([]byte{255, 'V', '2', 2}, uint32(id))
	return append(b, kind)
}
func appendString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}
func encodeMetric(m *metricState) []byte {
	b := appendString(make([]byte, 0, 72+len(m.Name)+len(m.Group)+len(m.Policy.AggregationMethod)+16*len(m.Policy.Archives)), string(m.Name))
	b = appendString(b, string(m.Group))
	b = appendString(b, m.Policy.AggregationMethod)
	b = binary.BigEndian.AppendUint32(b, math.Float32bits(m.Policy.XFilesFactor))
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Policy.Archives)))
	for _, a := range m.Policy.Archives {
		b = binary.BigEndian.AppendUint64(b, uint64(a.Step))
		b = binary.BigEndian.AppendUint64(b, uint64(a.Points))
	}
	b = binary.BigEndian.AppendUint64(b, m.Frontier.Order)
	b = binary.BigEndian.AppendUint64(b, uint64(m.Frontier.MaxTime))
	return append(b, m.Frontier.Policy[:]...)
}

type checkpointDecoder struct {
	b   []byte
	err error
}

func (d *checkpointDecoder) take(n int) []byte {
	if n < 0 || n > len(d.b) {
		d.err = errors.New("truncated binary checkpoint")
		return make([]byte, max(n, 0))
	}
	b := d.b[:n]
	d.b = d.b[n:]
	return b
}
func (d *checkpointDecoder) u64() uint64 { return binary.BigEndian.Uint64(d.take(8)) }
func (d *checkpointDecoder) u32() uint32 { return binary.BigEndian.Uint32(d.take(4)) }
func (d *checkpointDecoder) str() string {
	n := uint64(d.u32())
	if n > uint64(len(d.b)) {
		d.err = errors.New("invalid checkpoint string")
		return ""
	}
	return string(d.take(int(n)))
}
func (d *checkpointDecoder) end() error {
	if d.err != nil {
		return d.err
	}
	if len(d.b) != 0 {
		return errors.New("trailing binary checkpoint bytes")
	}
	return nil
}
func decodeMetric(b []byte) (*metricState, error) {
	d := checkpointDecoder{b: b}
	m := &metricState{Name: []byte(d.str()), Group: []byte(d.str())}
	m.Policy.AggregationMethod = d.str()
	m.Policy.XFilesFactor = math.Float32frombits(d.u32())
	n := uint64(d.u32())
	if n > uint64(len(d.b))/16 {
		return nil, errors.New("invalid checkpoint archive count")
	}
	m.Policy.Archives = make([]daemonstore.Archive, int(n))
	for i := range m.Policy.Archives {
		step, points := d.u64(), d.u64()
		if step > math.MaxInt || points > math.MaxInt {
			return nil, errors.New("checkpoint archive overflow")
		}
		m.Policy.Archives[i] = daemonstore.Archive{Step: int(step), Points: int(points)}
	}
	m.Frontier.Order = d.u64()
	m.Frontier.MaxTime = int64(d.u64())
	copy(m.Frontier.Policy[:], d.take(32))
	return m, d.end()
}
func (o *owner) readCheckpoint(data []byte) error {
	if !bytes.HasPrefix(data, []byte("V2C2")) {
		return json.Unmarshal(data, &o.meta)
	}
	d := checkpointDecoder{b: data[4:]}
	o.meta = checkpoint{Version: 2, Cursor: d.u64(), Points: d.u64(), Order: d.u64(), Journal: d.str(), Revisions: make(map[string]uint64)}
	if err := d.end(); err != nil {
		return err
	}
	for _, kind := range []byte{'M', 'R'} {
		p := checkpointPrefix(o.id, kind)
		it, err := o.db.NewIter(&pebble.IterOptions{LowerBound: p, UpperBound: partsformat.PrefixEnd(p)})
		if err != nil {
			return err
		}
		for it.First(); it.Valid(); it.Next() {
			name := string(it.Key()[len(p):])
			if kind == 'M' {
				m, e := decodeMetric(it.Value())
				if e != nil {
					err = e
					break
				}
				if string(m.Name) != name {
					err = errors.New("checkpoint metric key mismatch")
					break
				}
				o.meta.Metrics = append(o.meta.Metrics, m)
			} else {
				if len(it.Value()) != 8 {
					err = errors.New("invalid checkpoint scope revision")
					break
				}
				o.meta.Revisions[name] = binary.BigEndian.Uint64(it.Value())
			}
		}
		err = errors.Join(err, it.Error(), it.Close())
		if err != nil {
			return err
		}
	}
	return nil
}
