// Package namefp identifies a metric name without its bytes. Owners key their
// name → local ID tables by a Fingerprint, so the write path never reads names.
package namefp

import "hash/maphash"

// Fingerprint is 96 bits: a per-process seeded maphash64 plus the high 32 bits
// of the FNV-1a64 that routed the name. P(any collision) ≈ n²/2^97, 4e-13 at
// 263M names. It is never persisted: the seed changes on every start.
type Fingerprint struct {
	Hash uint64 // maphash64 with the process seed; well mixed, use as table index
	FNV  uint32 // routing FNV-1a64 >> 32
}

var seed = maphash.MakeSeed()

// Of completes a name's fingerprint from its routing FNV-1a64 hash.
func Of(name string, fnv uint64) Fingerprint {
	return Fingerprint{Hash: maphash.String(seed, name), FNV: uint32(fnv >> 32)}
}
