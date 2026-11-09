package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
)

// The ordinary exact-block representation is already indexed by archive and
// circular chunk. Disjoint keys retain their value bytes. For matching keys we
// combine at most 128 physical slots, with no whole-archive maps or sort.
// Exceptional timestamp/NaN normalization uses the reference archive path.
func (s *Store) nativeCombineChunks(ctx context.Context, source, target TransferMetric) (TransferMetric, NativeCombineClass, bool, error) {
	result := TransferMetric{Config: source.Config, Version: source.Version}
	class := NativeCombineClass{Archives: len(source.Config.Retentions)}
	intersected := make([]bool, class.Archives)
	var a, b chunk
	for i, j := 0, 0; i < len(source.Keys) || j < len(target.Keys); {
		if err := ctx.Err(); err != nil {
			return result, class, false, err
		}
		order := 0
		switch {
		case i == len(source.Keys):
			order = 1
		case j == len(target.Keys):
			order = -1
		default:
			order = bytes.Compare(source.Keys[i].Key, target.Keys[j].Key)
		}
		var kv RelayoutKV
		if order <= 0 {
			kv = source.Keys[i]
		} else {
			kv = target.Keys[j]
		}
		if len(kv.Key) != 6 {
			return result, class, false, ErrFormat
		}
		archive := int(binary.BigEndian.Uint16(kv.Key))
		index := int(binary.BigEndian.Uint32(kv.Key[2:]))
		if archive >= len(source.Config.Retentions) {
			return result, class, false, ErrFormat
		}
		r := source.Config.Retentions[archive]
		decode := func(value []byte, c *chunk) (bool, error) {
			if err := decodeChunk(value, c); err != nil {
				return false, err
			}
			for slot, p := range c.Points {
				if !c.has(slot) {
					continue
				}
				if math.IsNaN(p.Value) || p.Timestamp <= 0 || p.Timestamp%int64(r.Step) != 0 || p.Timestamp/int64(r.Step)%int64(r.Count) != int64(index*chunkSlots+slot) {
					return false, nil
				}
			}
			return true, nil
		}
		if order <= 0 {
			if canonical, err := decode(source.Keys[i].Value, &a); !canonical || err != nil {
				return result, class, false, err
			}
		}
		if order >= 0 {
			if canonical, err := decode(target.Keys[j].Value, &b); !canonical || err != nil {
				return result, class, false, err
			}
		}
		if order == 0 {
			if a.Present[0]&b.Present[0] != 0 || a.Present[1]&b.Present[1] != 0 {
				intersected[archive] = true
			}
			for slot, p := range b.Points {
				if b.has(slot) && (!a.has(slot) || p.Timestamp >= a.Points[slot].Timestamp) {
					a.set(slot, p)
				}
			}
			a.Deltas = 0
			kv.Value = s.encodeChunk(&a)
			class.CombinedBlocks++
			class.CombinedBytes += uint64(len(kv.Value))
		} else {
			class.CopiedBlocks++
			class.CopiedBytes += uint64(len(kv.Value))
		}
		result.Keys = append(result.Keys, kv)
		if order <= 0 {
			i++
		}
		if order >= 0 {
			j++
		}
	}
	for _, yes := range intersected {
		if yes {
			class.IntersectingArchives++
		}
	}
	return result, class, true, nil
}
