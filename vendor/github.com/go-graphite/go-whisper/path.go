package whisper

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
)

const (
	maxFilenameLength = 255
	lockSuffix        = ".lock"
)

// auxiliaryPath appends suffix when the resulting filename fits. For a main
// filename near NAME_MAX, use a stable hash in the same directory instead.
// Existing auxiliary filenames therefore stay unchanged wherever they already
// work, while their identity remains stable across file rewrites.
func auxiliaryPath(path, suffix string) string {
	filename := filepath.Base(path)
	if len(filename)+len(suffix) <= maxFilenameLength {
		return path + suffix
	}

	digest := sha256.Sum256([]byte(filename))
	return filepath.Join(filepath.Dir(path), fmt.Sprintf(".%x%s", digest, suffix))
}
