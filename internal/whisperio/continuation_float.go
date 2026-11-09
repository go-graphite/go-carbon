package whisperio

import (
	"bytes"
	"context"
	"fmt"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Preserve each sealed group's native key, boundary, timestamps and IEEE words.
// The caller plans one descriptor in its snapshot and publishes atomically with
// the existing revision fence. Downgrade may grow values and must never use a
// size fallback that leaves a new-format group behind.
func repackSealedFloats(ctx context.Context, kv store.ContinuationIO, prefix []byte, step int, r store.ColdPackRequest) (store.RepackStats, error) {
	var stats store.RepackStats
	var changed []store.RelayoutKV
	var scratch store.SealedPointBuffer
	err := kv.Visit(prefix, func(k, v []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		points, err := scratch.Decode(v)
		if err != nil {
			return fmt.Errorf("sealed float rewrite: %w", err)
		}
		eligible := (store.IsLegacySealedFloat(v) || r.FloatExhaustive && len(v) > 0 && v[0] == 4) && points[len(points)-1].Timestamp < r.Cutoff.Unix()
		if r.Mode == "float-downgrade" {
			eligible = v[0] == 4
		}
		if !eligible {
			return nil
		}
		encode := store.EncodeSealedPointsFloat
		if r.FloatExhaustive {
			encode = store.EncodeSealedPointsFloatMin
		}
		if r.Mode == "float-downgrade" {
			encode = store.EncodeSealedPoints
		}
		value, err := encode(points, step)
		if err != nil {
			return err
		}
		if r.Mode != "float-downgrade" && (value[0] != 4 || len(value) >= len(v)) {
			return nil
		}
		changed = append(changed, store.RelayoutKV{Key: bytes.Clone(k), Value: value})
		stats.Points += uint64(len(points))
		stats.InputBytes += uint64(len(k) + len(v))
		stats.OutputBytes += uint64(len(k) + len(value))
		return nil
	})
	if err != nil {
		return stats, err
	}
	for _, kvp := range changed {
		if err := kv.Set(kvp.Key, kvp.Value); err != nil {
			return stats, err
		}
	}
	if len(changed) > 0 {
		stats.Blocks = 1
		stats.RepackedBlocks = 1
		stats.RepackedPoints = stats.Points
		stats.RepackedInputBytes = stats.InputBytes
		stats.RepackedOutputBytes = stats.OutputBytes
	}
	return stats, nil
}
