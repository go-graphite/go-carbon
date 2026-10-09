package helper

import "strings"

// The persister drops metrics whose whisper file path exceeds these limits
// instead of creating the file.
const (
	MaxWhisperPathLength     = 4095
	MaxWhisperFilenameLength = 255
)

// WhisperPathFits reports whether a whisper file can be created at path.
func WhisperPathFits(path string) bool {
	if len(path) > MaxWhisperPathLength {
		return false
	}
	for path != "" {
		name := path
		if i := strings.IndexByte(path, '/'); i >= 0 {
			name, path = path[:i], path[i+1:]
		} else {
			path = ""
		}
		if len(name) > MaxWhisperFilenameLength {
			return false
		}
	}
	return true
}
