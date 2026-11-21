// Package v2read provides immutable publications for experimental part readers.
// Writers transfer frozen resources to Publish. No reader locks a writer mutex.
package v2read

import (
	"context"
	"errors"
)

var (
	ErrAdmission = errors.New("snapshot read admission exceeded")
	ErrNotFound  = errors.New("metric not found")
	ErrConflict  = errors.New("equal sequence has conflicting sample bits")
)

type Archive struct{ Step, Points int64 }
type Metric struct {
	BaselineOrder uint64

	ID                uint64
	Archives          []Archive
	AggregationMethod string
	XFilesFactor      float32
}

type Sample struct {
	Time           int64
	Bits, Sequence uint64
}

// Source owns a frozen head, part set or sealed archive block set. Scan streams
// all versions occupying the physical ring slots addressed by [from, until),
// including newer timestamps outside that range. Rollup replacements must have
// a new Sequence. Stored NaNs are occupied slots. Never access a mutable head.
// HasArchive reports whether any slot exists, even outside the query range;
// Whisper uses this distinction for aligned zero-width queries.
type Source interface {
	Scan(context.Context, uint64, Archive, int64, int64, func(Sample) error) error
	HasArchive(context.Context, uint64, int64) (bool, error)
}
