package downsize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Keep the original physical headers, descriptors and encoded values in proof
// stores. The importer may derive the sidecar summary; it is not file layout.
func evalRecordLayoutDigest(record store.TransferMetric) string {
	h := sha256.New()
	var length [8]byte
	for _, kv := range record.Keys {
		if bytes.Equal(kv.Key, store.SidecarSummaryKey) {
			continue
		}
		for _, field := range [][]byte{kv.Key, kv.Value} {
			binary.BigEndian.PutUint64(length[:], uint64(len(field)))
			h.Write(length[:])
			h.Write(field)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func verifyEvalLayouts(ctx context.Context, source *store.Store, names []string, expected map[string]string) (int, error) {
	verified := 0
	for _, name := range names {
		want, ok := expected[name]
		if !ok {
			continue
		}
		records, err := source.ExportTransfer(ctx, []string{name})
		if err != nil {
			return verified, err
		}
		if len(records) != 1 || evalRecordLayoutDigest(records[0]) != want {
			return verified, fmt.Errorf("native donor layout changed after capture for %s", name)
		}
		verified++
	}
	return verified, nil
}
