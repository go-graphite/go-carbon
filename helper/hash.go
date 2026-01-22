package helper

import (
	"github.com/zeebo/xxh3"
)

// HashString is a wrapper for a fast 64-bit non-cryptographic hash function,
// typically used for sharding, distribution, and other non-security-critical purposes.
func HashString(s string) uint64 {
	return xxh3.HashString(s)
}
