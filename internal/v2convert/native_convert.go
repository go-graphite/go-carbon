package v2convert

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
)

// NativeReceipt binds a new baseline's exact selected names, immutable source
// inventory and complete output files. InputBytes is the ENTIRE checkpoint's
// file bytes, not an attributed selected-name denominator for density claims.
type NativeReceipt struct {
	Receipt
	Baseline NativeBaseline
	Now      int64
	Names    []string
}

// ConvertNativeCheckpoint materializes a bounded coherent checkpoint selection,
// writes it directly through CoreWriter, verifies every logical key/value, and
// atomically publishes the part plus authority receipt. Output is staging data;
// this operation neither installs a baseline nor enables live catch-up.
func ConvertNativeCheckpoint(ctx context.Context, source NativeSnapshotReader, plan NativeCheckpointPlan, output string, maxBytes, workers int) (receipt NativeReceipt, err error) {
	if output == "" || len(plan.Names) == 0 || plan.Version != 1 || plan.Now <= 0 {
		return receipt, errors.New("native conversion requires an output and nonempty checkpoint selection")
	}
	digest, err := plan.InventoryDigest()
	if err != nil || digest != plan.Baseline.Snapshot {
		return receipt, errors.New("native checkpoint inventory authority mismatch")
	}
	plan.Names = slices.Clone(plan.Names)
	slices.Sort(plan.Names)
	identity, err := json.Marshal(struct {
		Baseline NativeBaseline
		Now      int64
		Names    []string
	}{plan.Baseline, plan.Now, plan.Names})
	if err != nil {
		return receipt, err
	}
	id := sha256.Sum256(append([]byte("native-baseline-conversion-v1\x00"), identity...))
	final := filepath.Join(output, hex.EncodeToString(id[:]))
	if data, e := os.ReadFile(filepath.Join(final, "receipt.json")); e == nil {
		if err := json.Unmarshal(data, &receipt); err != nil {
			return receipt, err
		}
		if receipt.Version != 1 || receipt.Adapter != "native-baseline-v1" || receipt.SourceSHA != hex.EncodeToString(plan.Baseline.Snapshot[:]) || receipt.Baseline != plan.Baseline || receipt.Now != plan.Now || !slices.Equal(receipt.Names, plan.Names) || receipt.Writer != (CoreWriter{Source: plan.Baseline.Source}).Format() {
			return receipt, errors.New("native receipt identity mismatch")
		}
		return receipt, Verify(ctx, final, receipt.Receipt)
	} else if !errors.Is(e, os.ErrNotExist) {
		return receipt, e
	}
	rows, err := MaterializeNativeWorkers(ctx, source, plan.Names, plan.Baseline, maxBytes, workers)
	if err != nil {
		return receipt, err
	}
	if err := os.MkdirAll(output, 0700); err != nil {
		return receipt, err
	}
	stage, err := os.MkdirTemp(output, ".native-building-")
	if err != nil {
		return receipt, err
	}
	defer os.RemoveAll(stage)
	writer := CoreWriter{Source: plan.Baseline.Source}
	paths, err := writer.Write(ctx, stage, rows)
	if err != nil {
		return receipt, err
	}
	receipt = NativeReceipt{Receipt: Receipt{Version: 1, Source: "native-checkpoint", SourceSHA: hex.EncodeToString(plan.Baseline.Snapshot[:]), Adapter: "native-baseline-v1", Writer: writer.Format(), Entries: uint64(len(rows))}, Baseline: plan.Baseline, Now: plan.Now, Names: plan.Names}
	for _, file := range plan.Files {
		receipt.InputBytes += file.Bytes
	}
	h := sha256.New()
	for _, row := range rows {
		hashEntry(h, Entry{Key: row.Key, Value: row.Value})
	}
	receipt.LogicalDigest = hex.EncodeToString(h.Sum(nil))
	seen := make([]bool, len(rows))
	count := 0
	for _, path := range paths {
		if err := writer.Read(ctx, filepath.Join(stage, path), func(row Record) error {
			i, found := slices.BinarySearchFunc(rows, row, func(a, b Record) int { return bytes.Compare(a.Key, b.Key) })
			if !found || seen[i] || !sameValue(row.Key, rows[i].Value, row.Value) {
				return errors.New("native output readback mismatch")
			}
			seen[i] = true
			count++
			return nil
		}); err != nil {
			return receipt, err
		}
		sum, size, err := FileSum(filepath.Join(stage, path))
		if err != nil {
			return receipt, err
		}
		receipt.Files = append(receipt.Files, File{Name: path, SHA256: sum, Bytes: size})
	}
	if count != len(rows) {
		return receipt, errors.New("native output has missing records")
	}
	data, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return receipt, err
	}
	f, err := os.OpenFile(filepath.Join(stage, "receipt.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return receipt, err
	}
	_, err = f.Write(data)
	if err = errors.Join(err, f.Sync(), f.Close()); err != nil {
		return receipt, err
	}
	if err := syncDir(stage); err != nil {
		return receipt, err
	}
	if err := os.Rename(stage, final); err != nil {
		return receipt, err
	}
	return receipt, syncDir(output)
}
