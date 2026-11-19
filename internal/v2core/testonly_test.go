package v2core

import (
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/cockroachdb/pebble"
)

// Test-only conveniences over the production entry points.

func openCore(root string, cfg coreConfig) (*coreEngine, error) {
	return openCoreWithHooks(root, cfg, coreHooks{})
}

func coreKey(r row, partition int64, part uint64, ordinal uint32) []byte {
	b := make([]byte, 41)
	coreKeyInto(b, r, partition, part, ordinal)
	return b
}

// coreExpire atomically drops a complete logical partition range of a
// stopped owner and persists its floor. Physical bytes are reclaimed by
// Pebble compaction, not immediately unlinked.
func coreExpire(db *pebble.DB, before int64) error {
	b, c, err := db.Get(coreMetaKey)
	if err != nil {
		return err
	}
	var meta coreFrontier
	err = json.Unmarshal(b, &meta)
	_ = c.Close()
	if err != nil {
		return err
	}
	if before < meta.ExpiredBefore {
		return errors.New("retention floor cannot move backwards")
	}
	meta.ExpiredBefore = before
	batch := db.NewBatch()
	defer batch.Close()
	for _, prefix := range []byte{1, 2} {
		upper := make([]byte, 9)
		upper[0] = prefix
		binary.BigEndian.PutUint64(upper[1:], uint64(before)^(1<<63))
		if err = batch.DeleteRange([]byte{prefix}, upper, nil); err != nil {
			return err
		}
	}
	b, err = json.Marshal(meta)
	if err != nil {
		return err
	}
	if err = batch.Set(coreMetaKey, b, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}
