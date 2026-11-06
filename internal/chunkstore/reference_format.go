package chunkstore

import "fmt"

// Reader capability is explicit: merely configuring an arbitrary continuation
// codec must not advertise support for the new manifest and group references.
type ContinuationReferenceReader interface{ NativeReferenceReader() bool }

func (s *Store) supportsReferences() bool {
	c, ok := s.continuation.(ContinuationReferenceReader)
	return ok && c.NativeReferenceReader()
}

func referenceManifest(kv RelayoutKV) bool {
	return len(kv.Key) == 10 && kv.Key[0] == 'f' && kv.Key[9] == 'h' && len(kv.Value) >= 2 && kv.Value[0] >= 3 && kv.Value[0] <= 4
}

func (s *Store) fenceReferences() error {
	if !s.supportsReferences() {
		return fmt.Errorf("%w: codec cannot read referenced archives", ErrFormat)
	}
	c := &s.coldPack
	c.formatMu.Lock()
	defer c.formatMu.Unlock()
	if s.referenceFormat.Load() {
		return nil
	}
	if err := replaceFormatMarker(s.fs, s.dir, formatMarkerReferences); err != nil {
		return fmt.Errorf("fence referenced archives: %w", err)
	}
	c.format.Store(true)
	c.floatFormat.Store(true)
	s.referenceFormat.Store(true)
	return nil
}
