//go:build !linux

package mixedread

import "testing"

func latencyEvictOwned(t *testing.T, _ string, _ ...string) {
	t.Helper()
	t.Skip("Linux FADV_DONTNEED required")
}
