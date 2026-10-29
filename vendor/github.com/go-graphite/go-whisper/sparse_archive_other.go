//go:build !linux

package whisper

func newArchiveExtentReader(source file, archiveEnd int64, readBytes int) *archiveExtentReader {
	if source, ok := source.(StorageExtentReader); ok {
		return &archiveExtentReader{nextExtent: source.NextStorageExtent}
	}
	return nil
}
