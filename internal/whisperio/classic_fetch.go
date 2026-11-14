package whisperio

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

var errClassicUnalignedBase = errors.New("unaligned classic base interval")

// visitClassicWindow follows Whisper's circular slot addressing on the same
// immutable view. Reading typed groups avoids serializing twelve bytes per slot
// only to decode them again, and visits no missing values.
func visitClassicWindow(ctx context.Context, f *continuationFile, ai, from, until int, visit func(int, float64)) error {
	a := f.data.layout.Archives[ai]
	var raw [4]byte
	if _, err := f.ReadAt(raw[:], int64(a.Offset)); err != nil {
		// Preserve Whisper's base-interval error contract, including Fetch's
		// existing translation of cancellation panics.
		panic(fmt.Sprintf("Failed to read baseInterval: %s", err))
	}
	base := int(binary.BigEndian.Uint32(raw[:]))
	if base == 0 {
		return nil
	}
	if base%a.Step != 0 {
		return errClassicUnalignedBase
	}
	slot := ((from-base)/a.Step%a.Count + a.Count) % a.Count
	for ts := from; ts < until; {
		if err := ctx.Err(); err != nil {
			return err
		}
		group, err := f.classicGroup(ai, a, slot/128, false)
		if err != nil {
			return err
		}
		n := min(128-slot%128, a.Count-slot, (until-ts)/a.Step)
		for i := 0; i < n; i++ {
			index := slot%128 + i
			if group.Present[index/64]&(uint64(1)<<uint(index%64)) != 0 {
				p := group.Points[index]
				if int(uint32(p.Timestamp)) == ts+i*a.Step {
					visit(ts+i*a.Step, p.Value)
				}
			} else if ts+i*a.Step == 0 {
				// A missing physical record is twelve zero bytes. The
				// reference accepts that record when querying epoch zero.
				visit(0, 0)
			}
		}
		ts += n * a.Step
		slot = (slot + n) % a.Count
	}
	return nil
}

func fetchClassic(ctx context.Context, w *whisper.Whisper, m store.Metadata, now, from, until int) (*store.Series, error) {
	if from > until {
		return nil, fmt.Errorf("invalid time interval: from time '%d' is after until time '%d'", from, until)
	}
	oldest := now - w.MaxRetention()
	if from > now || until < oldest {
		return nil, nil
	}
	from, until = max(from, oldest), min(until, now)
	f := w.StorageFile().(*continuationFile)
	ai := len(f.data.layout.Archives) - 1
	for i, a := range f.data.layout.Archives {
		if a.Step*a.Count >= now-from {
			ai = i
			break
		}
	}
	a := f.data.layout.Archives[ai]
	// Whisper.Interval uses floor division, including negative query bounds.
	align := func(t int) int { return t - (t%a.Step+a.Step)%a.Step + a.Step }
	lo, hi := align(from), align(until)
	if lo == hi {
		// The reference expands an aligned empty query only if slot zero's
		// base interval is nonzero. Leave this rare case to that implementation.
		series, err := w.Fetch(from, until)
		if err != nil || series == nil {
			return nil, err
		}
		return &store.Series{Metadata: m, FromTime: series.FromTime(), UntilTime: series.UntilTime(), Step: series.Step(), Values: series.Values()}, nil
	}
	values := make([]float64, (hi-lo)/a.Step)
	for i := range values {
		values[i] = math.NaN()
	}
	if err := visitClassicWindow(ctx, f, ai, lo, hi, func(ts int, value float64) { values[(ts-lo)/a.Step] = value }); err != nil {
		if !errors.Is(err, errClassicUnalignedBase) {
			return nil, err
		}
		series, err := w.Fetch(from, until)
		if err != nil || series == nil {
			return nil, err
		}
		return &store.Series{Metadata: m, FromTime: series.FromTime(), UntilTime: series.UntilTime(), Step: series.Step(), Values: series.Values()}, nil
	}
	return &store.Series{Metadata: m, FromTime: lo, UntilTime: hi, Step: a.Step, Values: values}, nil
}
