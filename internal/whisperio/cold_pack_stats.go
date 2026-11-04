package whisperio

import "sync/atomic"

type thawReason uint8

const (
	thawAppend thawReason = iota
	thawPatch
	thawRewrite
	thawUnpack
)

var coldPackThawCounters struct {
	blocks [4]atomic.Uint64
	points atomic.Uint64
	bytes  atomic.Uint64
}

// ThawStats counts completed codec thaws in this process. A later failure of
// the surrounding transaction can discard the rewritten groups. Bytes count
// logical group keys and values, excluding descriptors and batch framing.
type ThawStats struct {
	Appends, Patches, Rewrites, Unpacks uint64
	Points, Bytes                       uint64
}

// ColdPackThaws snapshots work counters; ordinary writes never touch them.
func ColdPackThaws() ThawStats {
	c := &coldPackThawCounters
	return ThawStats{Appends: c.blocks[thawAppend].Load(), Patches: c.blocks[thawPatch].Load(),
		Rewrites: c.blocks[thawRewrite].Load(), Unpacks: c.blocks[thawUnpack].Load(),
		Points: c.points.Load(), Bytes: c.bytes.Load()}
}
