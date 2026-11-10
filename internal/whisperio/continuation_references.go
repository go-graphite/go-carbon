package whisperio

import (
	"encoding/binary"
	"errors"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A reference maps a contiguous part of an immutable encoded group into a
// classic 128-slot group. Ordinary classic writes replace the complete group;
// even an empty replacement is authoritative over the older references.
type pointReference struct {
	ID                         uint32
	Source, Destination, Count uint16
	Sealed                     bool
}

func (*ContinuationCodec) NativeReferenceReader() bool { return true }

func encodePointReferences(refs []pointReference) []byte {
	data := []byte{1}
	for _, r := range refs {
		data = binary.BigEndian.AppendUint32(data, r.ID)
		data = binary.BigEndian.AppendUint16(data, r.Source)
		data = binary.BigEndian.AppendUint16(data, r.Destination)
		data = binary.BigEndian.AppendUint16(data, r.Count)
		if r.Sealed {
			data = append(data, 1)
		} else {
			data = append(data, 0)
		}
	}
	return data
}

func decodePointReferences(data []byte) ([]pointReference, error) {
	if len(data) < 1 || data[0] != 1 || (len(data)-1)%11 != 0 || len(data) > 1+128*11 {
		return nil, store.ErrFormat
	}
	var refs []pointReference
	var occupied [2]uint64
	for data = data[1:]; len(data) > 0; data = data[11:] {
		r := pointReference{ID: binary.BigEndian.Uint32(data), Source: binary.BigEndian.Uint16(data[4:]), Destination: binary.BigEndian.Uint16(data[6:]), Count: binary.BigEndian.Uint16(data[8:]), Sealed: data[10] == 1}
		if r.ID == 0 || r.Count == 0 || int(r.Destination)+int(r.Count) > 128 || int(r.Source)+int(r.Count) > store.MaxSealedPoints || !r.Sealed && int(r.Source)+int(r.Count) > 128 || data[10] > 1 {
			return nil, store.ErrFormat
		}
		for i := int(r.Destination); i < int(r.Destination+r.Count); i++ {
			mask := uint64(1) << uint(i%64)
			if occupied[i/64]&mask != 0 {
				return nil, store.ErrFormat
			}
			occupied[i/64] |= mask
		}
		refs = append(refs, r)
	}
	return refs, nil
}

func (f *continuationFile) referencePoints(archive int, r pointReference) ([]store.Point, error) {
	key := continuationBodyKey(f.data.id, 'r', archive, 0, int(r.ID))
	cacheKey := string(key)
	if r.Sealed {
		cacheKey += "s"
	}
	if points, ok := f.fs.referencedPoints[cacheKey]; ok {
		return points, nil
	}
	data, err := f.fs.kv.Get(key)
	if err != nil {
		return nil, err
	}
	var points []store.Point
	if r.Sealed {
		points, err = store.DecodeSealedPoints(data)
	} else {
		var group store.PointBlock
		err = store.DecodePointBlock(data, &group)
		if err == nil {
			points = make([]store.Point, 128)
			for i, p := range group.Points {
				if group.Present[i/64]&(1<<uint(i%64)) != 0 {
					points[i] = p
				}
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if f.fs.referencedPoints == nil {
		f.fs.referencedPoints = make(map[string][]store.Point)
	}
	f.fs.referencedPoints[cacheKey] = points
	return points, nil
}

func (f *continuationFile) referencedClassicGroup(archive, index int, mutable bool) (*store.PointBlock, error) {
	key := continuationBodyKey(f.data.id, 'p', archive, 0, index)
	buffer, _ := f.fs.kv.(*continuationPointBuffer)
	if buffer != nil {
		if block := buffer.point(key, mutable); block != nil {
			return block, nil
		}
	}
	group, err := f.readReferencedClassicGroup(archive, index, key)
	if err == nil && buffer != nil {
		buffer.remember(key, group)
	}
	return group, err
}

func (f *continuationFile) readReferencedClassicGroup(archive, index int, key []byte) (*store.PointBlock, error) {
	data, err := f.fs.kv.Get(key)
	group := new(store.PointBlock)
	if err == nil {
		return group, store.DecodePointBlock(data, group)
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	data, err = f.fs.kv.Get(continuationBodyKey(f.data.id, 'a', archive, 0, index))
	if errors.Is(err, store.ErrNotFound) {
		return group, nil
	}
	if err != nil {
		return nil, err
	}
	refs, err := decodePointReferences(data)
	if err != nil {
		return nil, err
	}
	for _, r := range refs {
		points, err := f.referencePoints(archive, r)
		if err != nil {
			return nil, err
		}
		if int(r.Source)+int(r.Count) > len(points) {
			return nil, store.ErrFormat
		}
		for i := 0; i < int(r.Count); i++ {
			slot := int(r.Destination) + i
			p := points[int(r.Source)+i]
			if p.Timestamp <= 0 {
				return nil, store.ErrFormat
			}
			group.Present[slot/64] |= 1 << uint(slot%64)
			group.Points[slot] = p
		}
	}
	return group, nil
}

// A complete classic group replacement retires its immutable mapping in the
// same writer batch. The payload disappears after its final mapping does.
func (f *continuationFile) retireReferences(key []byte) error {
	if len(key) != 20 || key[9] != 'p' {
		return store.ErrFormat
	}
	alias := append([]byte(nil), key...)
	alias[9] = 'a'
	data, err := f.fs.probe(alias)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	refs, err := decodePointReferences(data)
	if err != nil {
		return err
	}
	seen := make(map[uint32]bool, len(refs))
	for _, ref := range refs {
		if seen[ref.ID] {
			continue
		}
		seen[ref.ID] = true
		countKey := continuationBodyKey(f.data.id, 'R', int(binary.BigEndian.Uint16(key[10:12])), 0, int(ref.ID))
		value, err := f.fs.kv.Get(countKey)
		if err != nil {
			return err
		}
		if len(value) != 8 {
			return store.ErrFormat
		}
		count := binary.BigEndian.Uint64(value)
		if count == 0 {
			return store.ErrFormat
		}
		if count == 1 {
			if err := f.fs.kv.Delete(countKey); err != nil {
				return err
			}
			payload := append([]byte(nil), countKey...)
			payload[9] = 'r'
			if err := f.fs.kv.Delete(payload); err != nil {
				return err
			}
		} else {
			if err := f.fs.kv.Set(countKey, binary.BigEndian.AppendUint64(nil, count-1)); err != nil {
				return err
			}
		}
	}
	return f.fs.kv.Delete(alias)
}
