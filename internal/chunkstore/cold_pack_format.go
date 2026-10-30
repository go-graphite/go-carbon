package chunkstore

import (
	"context"
	"errors"
	"fmt"
)

// TransferFormat is the minimum reader version required for a physical copy.
// The fence deliberately covers an entire v9 store, including ordinary metrics.
func (s *Store) TransferFormat() int {
	if s.coldPack.format.Load() {
		return 9
	}
	return 7
}
func sealedTransferKey(kv RelayoutKV) bool {
	return len(kv.Key) == 16 && kv.Key[0] == 'f' && kv.Key[9] == 'b' && len(kv.Value) == 12
}

// DowngradeColdPack is for an exclusively opened offline store after a complete
// unpack sweep. It refuses skips/conflicts/failures; restart and retry unpacking
// after an interruption. Rounded decimal values retain their stored precision.
func (s *Store) DowngradeColdPack(ctx context.Context) error {
	s.coldPack.run.Lock()
	defer s.coldPack.run.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	c := &s.coldPack
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.options.Mode != "unpack" || !c.cleanUnpack || c.stats.Cursor != "" {
		return errors.New("downgrade requires a complete clean unpack sweep")
	}
	if err := s.db.Flush(); err != nil {
		return err
	}
	if err := replaceFormatMarker(s.fs, s.dir, formatMarkerTimeBuckets); err != nil {
		return fmt.Errorf("downgrade marker: %w", err)
	}
	c.format.Store(false)
	c.options.Mode = "off"
	c.stats.Mode = "off"
	return nil
}
