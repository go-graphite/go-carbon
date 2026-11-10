package whisperio

import (
	"encoding/binary"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// NativeCombineMemory charges output groups, owner maps, fragment vectors and
// decoded irregular payloads before CombineNative allocates them. The output
// charge includes a possible 128-slot owner table and both selection and writer
// maps/vectors for every ring group. Dense groups keep implicit timestamps;
// unknown groups are charged for per-point owners.
func (*ContinuationCodec) NativeCombineMemory(a, b store.TransferMetric) (int64, error) {
	var memory int64
	for _, r := range a.Config.Retentions {
		memory += (int64(r.Count) + 127) / 128 * 4096
	}
	var scratch store.SealedPointBuffer
	for _, record := range []store.TransferMetric{a, b} {
		for _, kv := range record.Keys {
			key := kv.Key
			if len(key) > 5 && key[0] == 'T' {
				key = key[5:]
			}
			if len(key) != 20 || key[0] != 'f' {
				continue
			}
			ai := int(binary.BigEndian.Uint16(key[10:12]))
			if ai >= len(record.Config.Retentions) {
				return 0, store.ErrFormat
			}
			step := record.Config.Retentions[ai].Step
			switch key[9] {
			case 'p':
				_, _, _, known := store.CanonicalPointRun(kv.Value, step)
				if known {
					memory += 1024
				} else {
					memory += 128 * 512
				}
			case 'c', 'r':
				if len(kv.Value) == 0 {
					return 0, store.ErrFormat
				}
				if kv.Value[0] < 3 {
					memory += 128 * 512
					continue
				}
				_, _, _, known := scratch.CanonicalRun(kv.Value, step)
				if known {
					memory += 1024
				} else {
					// Unknown sealed encodings can decode up to one bounded group.
					memory += 8192 * 512
				}
			case 'a':
				memory += 128 * 512 // references may decode their payloads
			}
		}
	}
	return memory, nil
}
