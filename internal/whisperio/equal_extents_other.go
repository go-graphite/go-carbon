//go:build !linux || (!amd64 && !arm64)

package whisperio

import "os"

func sameFileExtents(_, _ *os.File) (bool, error) { return false, nil }
