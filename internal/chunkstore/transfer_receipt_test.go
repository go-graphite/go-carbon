package chunkstore

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
)

func TestTransferReceiptEncodingMatchesMarshal(t *testing.T) {
	var encoder transferProofEncoder
	for _, name := range []string{"metric", "quote\"\\\n", "<>&\u2028\u2029", "invalid\xff", strings.Repeat("long", 1024)} {
		for _, counts := range [][]int{nil, {}, {0, -1, 17}} {
			p := TransferProof{KeyCount: -1, Sampled: true, Merged: true, Source: TransferVersion{name, ^uint64(0), 2, 7}, Target: TransferVersion{name, 27, 1, 99}, Counts: counts, Digest: "digest"}
			data, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			got, err := encoder.digest(&p)
			if err != nil || got != sha256.Sum256(data) {
				t.Fatal("durable receipt changed", name, counts, err)
			}
		}
	}
}

func BenchmarkTransferReceiptEncoding(b *testing.B) {
	p := TransferProof{KeyCount: 16, Source: TransferVersion{"synthetic.metric.000000001", 123456, 1, 37}, Target: TransferVersion{"synthetic.metric.000000001", 777777, 1, 1}, Digest: strings.Repeat("a", 64)}
	b.Run("marshal", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			data, err := json.Marshal(p)
			if err != nil {
				b.Fatal(err)
			}
			_ = sha256.Sum256(data)
		}
	})
	b.Run("reuse", func(b *testing.B) {
		b.ReportAllocs()
		var encoder transferProofEncoder
		for b.Loop() {
			if _, err := encoder.digest(&p); err != nil {
				b.Fatal(err)
			}
		}
	})
}
