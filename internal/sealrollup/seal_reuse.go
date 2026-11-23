package sealrollup

import (
	"unsafe"
)

// RetainedBytes reports private scratch high-water storage, excluding the
// Workspace value itself. Readers can discard oversized workspaces after use.
func (w *Workspace) RetainedBytes() int64 {
	return int64(cap(w.positions))*4 + int64(cap(w.offsets))*int64(unsafe.Sizeof(int(0))) +
		int64(cap(w.updates)+cap(w.versions))*int64(unsafe.Sizeof(Record{})) +
		int64(cap(w.points)+cap(w.current)+cap(w.neighbors))*int64(unsafe.Sizeof(Sample{})) + int64(cap(w.changed))*8
}
