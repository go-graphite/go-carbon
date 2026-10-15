//go:build !linux

package whisper

func newArchiveExtentReader(source file, archiveEnd int64, readBytes int) *archiveExtentReader {
	return nil
}
