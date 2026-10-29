package whisperio

import (
	"context"
	"errors"
	"math"
	"os"
)

// SnapshotWorkingBytes conservatively reserves decoded/verified snapshots,
// encoded chunks and temporary capture extents from immutable retention sizes.
// It reads no history. Schema migration must be disabled during conversion.
func SnapshotWorkingBytes(ctx context.Context, path string) (bytes int64, err error) {
	w, err := openCapture(ctx, path)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, w.Close()) }()
	for _, retention := range w.Retentions() {
		count := int64(retention.NumberOfPoints())
		if count > (math.MaxInt64-bytes)/128 {
			return 0, errors.New("conversion working-set estimate overflows")
		}
		bytes += count * 128
	}
	for _, file := range []string{path, w.OutOfOrderPath()} {
		if file == "" {
			continue
		}
		info, err := os.Stat(file)
		if err != nil {
			return 0, err
		}
		if info.Size() > math.MaxInt64-bytes {
			return 0, errors.New("conversion capture estimate overflows")
		}
		bytes += info.Size()
	}
	return max(1, bytes), nil
}
