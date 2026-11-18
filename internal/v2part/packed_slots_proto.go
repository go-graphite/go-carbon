//go:build packedslots

package v2part

// Bench-only prototype (build tag packedslots): fold a metric's envelope into
// its last dense run, so each (name, archive) run is the only KV a name pays
// per part, like one block header per series per part. Not a published format.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

var packedMagic = []byte{'G', 'C', 'P', 1}

// SetPlannedMetadataPacked writes metric's envelope as the trailer of its last
// pending run: value = magic | uvarint(len(envelope)) | envelope | dense run.
// A metric without a pending run keeps the separate envelope KV.
func (w *DenseWriter) SetPlannedMetadataPacked(vshard uint16, metric ID, value []byte) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	if len(w.records) == 0 || w.key.Metric != metric {
		return w.SetPlannedMetadata(vshard, metric, value)
	}
	_, err := w.plannedKey(vshard, metric, MetadataArchive, 0)
	if err == nil {
		b := append(w.encoded[:0], packedMagic...)
		b = binary.AppendUvarint(b, uint64(len(value)))
		b = append(b, value...)
		b, err = appendSlots(b, w.records)
		w.encoded = b
		if err == nil {
			err = w.writer.setDecoded(w.first, w.key, b)
		}
		w.records = w.records[:0]
	}
	w.err = err
	return err
}

// SplitPacked returns a packed value's envelope and dense run. A plain dense
// run returns ok=false and the value as the run.
func SplitPacked(v []byte) (envelope, run []byte, ok bool, err error) {
	if !bytes.HasPrefix(v, packedMagic) {
		return nil, v, false, nil
	}
	n, k := binary.Uvarint(v[4:])
	if k <= 0 || uint64(len(v)-4-k) < n {
		return nil, nil, false, errors.New("invalid packed run")
	}
	p := v[4+k:]
	return p[:n], p[n:], true, nil
}
