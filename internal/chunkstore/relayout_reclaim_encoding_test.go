package chunkstore

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func BenchmarkRelayoutReclaimEncoding(b *testing.B) {
	spans := map[byte]*relayoutSpan{'u': {start: append(continuationPrefix(1, 1), spaceGroupKey(0)...), end: append(continuationPrefix(4096, 1), spaceGroupKey(127)...)}}
	b.ReportAllocs()
	for b.Loop() {
		_ = encodeRelayoutReclaim(65<<20, spans)
	}
}

func TestRelayoutReclaimEncodingMatchesRelease(t *testing.T) {
	for _, size := range []int{0, 1, 127, 128, 16384} {
		for mask := 0; mask < 4; mask++ {
			spans := make(map[byte]*relayoutSpan)
			for i, kind := range []byte{'u', 't'} {
				if mask&(1<<i) != 0 {
					spans[kind] = &relayoutSpan{start: append([]byte{kind}, bytes.Repeat([]byte{'a'}, size)...), end: append([]byte{kind}, bytes.Repeat([]byte{'z'}, size)...)}
				}
			}
			// Release encoder, kept as the wire oracle rather than deriving
			// expectations through the decoder under test.
			want := binary.BigEndian.AppendUint64([]byte{1}, 65<<20)
			for _, kind := range []byte{'u', 't'} {
				var lo, hi []byte
				if span := spans[kind]; span != nil {
					lo, hi = span.start, span.end
				}
				for _, key := range [][]byte{lo, hi} {
					want = binary.AppendUvarint(want, uint64(len(key)))
					want = append(want, key...)
				}
			}
			if got := encodeRelayoutReclaim(65<<20, spans); !bytes.Equal(got, want) {
				t.Fatalf("journal bytes changed: size=%d mask=%d", size, mask)
			}
		}
	}
}
