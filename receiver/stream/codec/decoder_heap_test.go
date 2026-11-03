package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"
)

// These frames use the documented raw wire format directly: cold state holds
// 65,536 definitions without predictors; each warm row has a constant value.
func decoderHeapFrames(t testing.TB) (state, warm []byte) {
	t.Helper()
	const count = 65_536
	raw := binary.AppendUvarint(nil, 0)
	raw = binary.AppendUvarint(raw, count)
	for i := range count {
		name := fmt.Sprintf("service.namespace.metric.%08d", i)
		raw = binary.AppendUvarint(raw, uint64(len(name)))
		raw = append(raw, name...)
		raw = append(raw, 0)
	}
	var err error
	state, err = appendFrame(nil, raw, StateFrame, 0, nil, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	raw = binary.AppendUvarint(nil, count)
	raw = binary.AppendUvarint(raw, 0)
	raw = binary.AppendUvarint(raw, count)
	raw = binary.AppendUvarint(raw, count)
	for i := range count {
		if i == 0 {
			raw = append(raw, 0)
		} else {
			raw = append(raw, 1)
		}
	}
	raw = append(raw, make([]byte, 2*count)...)
	warm, err = appendFrame(nil, raw, DataFrame, count, nil, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	return
}

// Run with -benchtime=1x to measure one retained dictionary after a forced GC.
func BenchmarkDecoderDictionaryHeap(b *testing.B) {
	state, _ := decoderHeapFrames(b)
	var heap, budget float64
	b.ResetTimer()
	for range b.N {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		d, err := NewDecoder(Options{})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := d.Decode(state, nil); err != nil {
			b.Fatal(err)
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		heap += float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / 65_536
		budget += float64(d.RetainedBytes()) / 65_536
		d.Close()
		runtime.KeepAlive(d)
	}
	b.StopTimer()
	b.ReportMetric(heap/float64(b.N), "heap-B/name")
	b.ReportMetric(budget/float64(b.N), "budget-B/name")
	runtime.KeepAlive(state)
}

func BenchmarkDecoderPredictors(b *testing.B) {
	state, warm := decoderHeapFrames(b)
	d := decoder(b, Options{})
	if _, err := d.Decode(state, nil); err != nil {
		b.Fatal(err)
	}
	if _, err := d.Decode(warm, benchmarkVisit); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := d.Decode(warm, benchmarkVisit); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*65_536), "ns/point")
}

// Compare snapshots byte-for-byte against hand-built state frames, including
// valid zero predictors and unset predictors across dictionary growth and resets.
func TestDecoderPredictorValiditySnapshot(t *testing.T) {
	d := decoder(t, Options{})
	reset, err := appendFrame(nil, nil, ResetFrame, 0, nil, DefaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{130, 5, 257, 63, 64, 65} {
		if _, err := d.Decode(reset, nil); err != nil {
			t.Fatal(err)
		}
		raw := binary.AppendUvarint(nil, 0)
		raw = binary.AppendUvarint(raw, uint64(count))
		for i := range count {
			name := fmt.Sprintf("metric.%d", i)
			raw = binary.AppendUvarint(raw, uint64(len(name)))
			raw = append(raw, name...)
			if i%3 == 0 {
				raw = append(raw, 1)
				raw = binary.BigEndian.AppendUint64(raw, 0)
				raw = binary.BigEndian.AppendUint64(raw, uint64(i)<<48)
			} else {
				raw = append(raw, 0)
			}
		}
		state, err := appendFrame(nil, raw, StateFrame, 0, nil, DefaultMaxFrameSize)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Decode(state, nil); err != nil {
			t.Fatal(err)
		}
		got, err := d.Snapshot(nil)
		if err != nil {
			t.Fatal(err)
		}
		want := append(append([]byte(nil), reset...), state...)
		if !bytes.Equal(got, want) {
			t.Fatalf("predictor validity changed at %d names", count)
		}
	}
}
