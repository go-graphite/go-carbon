package whisperio

import (
	"context"
	"errors"
	"os"

	whisper "github.com/go-graphite/go-whisper"
)

// SameCapture proves that source still contains the retained capture's bytes.
// It locks the original main/sidecar pair as Capture does. False means unknown
// or changed and requires a normal capture and full snapshot verification.
// The caller must separately prove the database still matches this capture.
func SameCapture(ctx context.Context, source, capture string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	w, err := openCapture(ctx, source)
	if err != nil {
		return false, err
	}
	defer w.Close()
	other, err := os.Open(capture)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	same, err := sameFileExtents(w.File(), other)
	err = errors.Join(err, other.Close())
	if err != nil || !same {
		return false, err
	}
	left, le := os.Open(whisper.OutOfOrderSidecarPath(source))
	right, re := os.Open(whisper.OutOfOrderSidecarPath(capture))
	if left != nil {
		defer left.Close()
	}
	if right != nil {
		defer right.Close()
	}
	if errors.Is(le, os.ErrNotExist) && errors.Is(re, os.ErrNotExist) {
		return true, nil
	}
	if le != nil && !errors.Is(le, os.ErrNotExist) {
		return false, le
	}
	if re != nil && !errors.Is(re, os.ErrNotExist) {
		return false, re
	}
	if le != nil || re != nil {
		return false, nil
	}
	return sameFileExtents(left, right)
}
