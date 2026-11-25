package cache

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/go-graphite/go-carbon/points"
)

// Run unchanged against the parent sources too: byte limits close to an item
// boundary must not change just because stream lookup bookkeeping changed.
func TestStreamAdmissionByteBudgetReplay(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) == 8 {
		it := newItem("metric", 0)
		if got := it.memoryBytes(); got != 208+int64(len(it.Metric)) {
			t.Fatalf("item admission charge changed: %d", got)
		}
		p := points.OnePoint("metric", 1, 1)
		want := 208 + int64(unsafe.Sizeof(*p)) + int64(len(p.Metric)) + int64(cap(p.Data))*16
		if got := flightMemory(p); got != want {
			t.Fatalf("in-flight admission charge changed: %d, want %d", got, want)
		}
	}
	var transcript []byte
	for _, extra := range []uint64{1, 255, 2048, 32768} {
		c := New()
		c.SetMaxSize(0)
		c.SetMaxBytes(uint64(c.EstimatedBytes()) + extra)
		for i := range 512 {
			name := []byte(fmt.Sprintf("stream.byte-budget.%03d", i%37))
			slot := c.ResolveStreamName(name)
			c.AddStreamPoint(name, float64(i), int64(i), &slot)
		}
		transcript = binary.LittleEndian.AppendUint64(transcript, uint64(c.Size()))
		transcript = binary.LittleEndian.AppendUint64(transcript, uint64(c.EstimatedBytes()))
		transcript = binary.LittleEndian.AppendUint32(transcript, atomic.LoadUint32(&c.stat.overflowCnt))
		for i := range 37 {
			if p, ok := c.Pop(fmt.Sprintf("stream.byte-budget.%03d", i)); ok {
				transcript = append(transcript, p.AppendBinary(nil)...)
			}
		}
	}
	if path := os.Getenv("STREAM_BYTE_REPLAY_OUTPUT"); path != "" {
		if err := os.WriteFile(path, transcript, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
