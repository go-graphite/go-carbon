package phasecpu

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// A busy one-second window at 100Hz on 100 CPU cores.
func BenchmarkStageProfileDecode(b *testing.B) {
	packed := fixture([2]string{"go_carbon_phase", "persister"}, [2]string{"persister_stage", "commit"})
	z, err := gzip.NewReader(bytes.NewReader(packed))
	if err != nil {
		b.Fatal(err)
	}
	raw, err := io.ReadAll(z)
	if err != nil {
		b.Fatal(err)
	}
	_ = z.Close()
	var sample []byte
	if err := fields(raw, func(n protowire.Number, t protowire.Type, v []byte, _ uint64) {
		if n == 2 {
			sample = msg(2, v)
		}
	}); err != nil {
		b.Fatal(err)
	}
	for range 9999 {
		raw = append(raw, sample...)
	}
	packed = zipped(raw)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var stages [len(PersisterStageNames)]uint64
		if _, err := decodeStages(packed, &stages); err != nil {
			b.Fatal(err)
		}
	}
}
