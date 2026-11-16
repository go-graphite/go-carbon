package v2core

import "errors"

// coreApply stores no name, portable key, policy or per-apply point allocation.
// Offsets refer to SealInput.Rows before the planner returns. The engine may
// sort/reuse those rows afterward; planners must not retain borrowed storage.
type coreApply struct {
	ID, Order     uint64
	Clock         int64
	Offset, Count int
	Producer      int
	Cursor        uint64
}

// AppendNumeric copies a flat buffer and its original apply boundaries once.
// Each producer calls serially. Applies must cover rows exactly, in input order;
// every row in an apply has its ID. Row.Seq preserves order inside that apply.
// The caller owns durable global ID allocation and a journal containing the
// same boundaries, clocks, order and IEEE value bits. This API does no lookup.
func (e *coreEngine) AppendNumeric(producer int, rows []Row, applies []Apply, cursor uint64, seal bool) error {
	if producer < 0 || producer >= e.cfg.Producers || cursor == 0 {
		return errors.New("invalid numeric producer/cursor")
	}
	offset := 0
	for _, a := range applies {
		if a.ID == 0 || a.Order == 0 || a.Offset != offset || a.Count <= 0 || a.Count > len(rows)-offset {
			return errors.New("invalid numeric apply span")
		}
		for _, r := range rows[offset : offset+a.Count] {
			if r.ID != a.ID {
				return errors.New("mixed numeric apply identity")
			}
		}
		offset += a.Count
	}
	if offset != len(rows) {
		return errors.New("numeric applies do not cover rows")
	}
	return e.submitPacket(producer, rows, nil, applies, cursor, seal)
}

func routeCoreApplies(applies []coreApply, shards, producer int, cursor uint64) [][]coreApply {
	if len(applies) == 0 {
		return nil
	}
	out := make([][]coreApply, shards)
	offsets := make([]int, shards)
	for _, a := range applies {
		s := coreShard(a.ID, shards)
		a.Offset, a.Producer, a.Cursor = offsets[s], producer, cursor
		offsets[s] += a.Count
		out[s] = append(out[s], a)
	}
	return out
}
