package chunkstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

const coldPackCheckpointInterval = 30 * time.Second

var coldPackCheckpointKey = []byte("z/cold-pack-cursor")

// The catalog queue is deliberately omitted: Cursor is the last completed
// metric in the contiguous completed prefix when BlockCursor is zero, otherwise
// the oldest unfinished metric. Out-of-order completions are replayed on restart.
// Seeking after Cursor reconstructs the queue without skipping unvisited names.
// PassFailed must survive restarts so a partial unpack cannot authorize downgrade.
type coldPackCheckpoint struct {
	Version        int
	Format, Mode   string
	WriteWindow    time.Duration
	Cursor         string
	BlockCursor    uint64
	Passes         uint64
	MetricsVisited uint64
	PassFailed     bool
}

// loadColdPackCheckpoint is called once by ConfigureColdPack under run, before
// the worker starts. marker is the format observed before any policy upgrade.
func (s *Store) loadColdPackCheckpoint(marker string) error {
	c := &s.coldPack
	value, closer, err := s.db.Get(coldPackCheckpointKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read cold pack checkpoint: %w", err)
	}
	defer closer.Close()
	c.checkpointPresent = true
	var saved coldPackCheckpoint
	if len(value) < 4 || crc32.ChecksumIEEE(value[:len(value)-4]) != binary.LittleEndian.Uint32(value[len(value)-4:]) {
		err = errors.New("checksum mismatch")
	} else if err = json.Unmarshal(value[:len(value)-4], &saved); err == nil {
		if saved.Version != 1 {
			err = fmt.Errorf("unknown version %d", saved.Version)
		} else if saved.BlockCursor != 0 && saved.Cursor == "" {
			err = errors.New("block cursor without metric")
		}
	}
	if err != nil {
		zapwriter.Logger("storage").Warn("ignoring cold pack checkpoint", zap.Error(err))
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if saved.Format != marker || saved.Mode != c.options.Mode || saved.WriteWindow != c.options.WriteWindow {
		return nil
	}
	c.stats.Cursor, c.stats.BlockCursor = saved.Cursor, saved.BlockCursor
	c.stats.Passes, c.stats.MetricsVisited = saved.Passes, saved.MetricsVisited
	c.stats.PersistedMetricsVisited = saved.MetricsVisited
	c.passFailed = saved.PassFailed
	c.checkpoint, c.checkpointAt = saved, time.Now()
	if saved.Mode != "off" && (saved.Cursor != "" || saved.Passes != 0) {
		c.stats.Resumed++
		zapwriter.Logger("storage").Info("cold pack resumed", zap.String("mode", saved.Mode), zap.String("metric", saved.Cursor), zap.Uint64("block_cursor", saved.BlockCursor), zap.Uint64("passes", saved.Passes), zap.Uint64("metrics_visited", saved.MetricsVisited))
	}
	return nil
}

// saveColdPackCheckpoint requires run. No metric stripe or stats lock is held
// during the sync, and no ingestion call uses this path. Pebble orders this
// synced record after all completed range commits even with periodic WAL sync.
// A store which has never enabled maintenance needs no checkpoint writes.
func (s *Store) saveColdPackCheckpoint(force bool) error {
	if s.db == nil {
		return nil
	}
	c := &s.coldPack
	c.mu.Lock()
	saved := coldPackCheckpoint{
		Version: 1, Mode: c.options.Mode, WriteWindow: c.options.WriteWindow,
		Cursor: c.stats.Cursor, BlockCursor: c.stats.BlockCursor,
		Passes: c.stats.Passes, MetricsVisited: c.stats.MetricsVisited, PassFailed: c.passFailed,
	}
	c.mu.Unlock()
	if saved.Mode == "" || saved.Mode == "off" && !c.checkpointPresent {
		return nil
	}
	if !force && time.Since(c.checkpointAt) < coldPackCheckpointInterval {
		return nil
	}
	marker, err := readStoreMarker(s.fs, s.dir, "CHUNKSTORE")
	if err != nil {
		return fmt.Errorf("cold pack checkpoint marker: %w", err)
	}
	saved.Format = marker
	if saved == c.checkpoint {
		return nil
	}
	value, err := json.Marshal(saved)
	if err != nil {
		return err
	}
	value = binary.LittleEndian.AppendUint32(value, crc32.ChecksumIEEE(value))
	if err := s.db.Set(coldPackCheckpointKey, value, pebble.Sync); err != nil {
		return fmt.Errorf("save cold pack checkpoint: %w", err)
	}
	c.checkpoint, c.checkpointAt, c.checkpointPresent = saved, time.Now(), true
	c.mu.Lock()
	c.stats.Checkpoints++
	c.stats.PersistedMetricsVisited = saved.MetricsVisited
	c.mu.Unlock()
	return nil
}

func (s *Store) checkpointColdPackOnStop() error {
	c := &s.coldPack
	c.run.Lock()
	defer c.run.Unlock()
	if err := s.saveColdPackCheckpoint(true); err != nil {
		stats := s.ColdPackStats()
		zapwriter.Logger("storage").Error("cold pack checkpoint on stop", zap.String("metric", stats.Cursor), zap.Uint64("block_cursor", stats.BlockCursor), zap.Error(err))
		c.mu.Lock()
		c.passFailed = true
		c.stats.Errors++
		c.stats.Error = err.Error()
		c.mu.Unlock()
		return err
	}
	return nil
}
