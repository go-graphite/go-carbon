package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
)

// TransferFormat is the minimum reader version required for a physical copy.
// The fence deliberately covers an entire v9 store, including ordinary metrics.
func (s *Store) TransferFormat() int {
	if s.referenceFormat.Load() {
		return 12
	}
	if s.coldPack.floatFormat.Load() {
		return 11
	}
	if s.coldPack.format.Load() {
		return 9
	}
	return 7
}

// AcceptTransferFormat advertises reader capability without upgrading the
// on-disk marker. Receiving a new group persists the fence before ingesting it.
func (s *Store) AcceptTransferFormat() int {
	c := &s.coldPack
	c.mu.Lock()
	enabled := c.options.FloatCodec
	c.mu.Unlock()
	if s.supportsReferences() && (s.referenceFormat.Load() || c.floatFormat.Load() || enabled) {
		return 12
	}
	if enabled {
		return 11
	}
	return s.TransferFormat()
}

func (s *Store) acceptsFloatCodec() bool {
	if s.coldPack.floatFormat.Load() {
		return true
	}
	s.coldPack.mu.Lock()
	enabled := s.coldPack.options.FloatCodec
	s.coldPack.mu.Unlock()
	return enabled
}
func sealedTransferKey(kv RelayoutKV) bool {
	return len(kv.Key) == 16 && kv.Key[0] == 'f' && kv.Key[9] == 'b' && len(kv.Value) == 12
}

// ErrColdPackIncomplete means another clean downgrade sweep is required.
var ErrColdPackIncomplete = errors.New("downgrade requires a complete clean sweep")

// DowngradeColdPack is for an exclusively opened offline store after a complete
// unpack sweep. It refuses skips/conflicts/failures; restart and retry unpacking
// after an interruption. Rounded decimal values retain their stored precision.
func (s *Store) DowngradeColdPack(ctx context.Context) error {
	if s.referenceFormat.Load() {
		return fmt.Errorf("referenced history requires the v12 reader: %w", ErrColdPackIncomplete)
	}
	ctx, end, ok := s.explicitMaintenance.begin(ctx)
	if !ok {
		return context.Canceled
	}
	defer end()
	s.coldPack.run.Lock()
	defer s.coldPack.run.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	c := &s.coldPack
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.options.Mode != "unpack" || !c.cleanUnpack || c.stats.Cursor != "" {
		return ErrColdPackIncomplete
	}
	if err := s.Flush(); err != nil {
		return err
	}
	if err := replaceFormatMarker(s.fs, s.dir, formatMarkerTimeBuckets); err != nil {
		return fmt.Errorf("downgrade marker: %w", err)
	}
	c.format.Store(false)
	c.floatFormat.Store(false)
	c.options.Mode = "off"
	c.stats.Mode = "off"
	return nil
}

// newFloatGroup matches a bucketed continuation point-group key, never metadata
// or authoritative Whisper pages whose first byte might happen to be four.
func newFloatGroup(k, v []byte) bool {
	return len(k) == 25 && isContinuationBucketSuffix(k) && k[5] == 'f' && k[14] == 'c' && len(v) >= sealedHeader && v[0] == 4
}

func (s *Store) fenceFloatCodec() error {
	c := &s.coldPack
	c.formatMu.Lock()
	defer c.formatMu.Unlock()
	if c.floatFormat.Load() {
		return nil
	}
	if err := replaceFormatMarker(s.fs, s.dir, formatMarkerFloatCodec); err != nil {
		return fmt.Errorf("fence float codec: %w", err)
	}
	c.format.Store(true)
	c.floatFormat.Store(true)
	return nil
}

// DowngradeFloatCodec lowers the v11 fence after a complete clean offline sweep
// has rewritten every v4 group to legacy byte planes. Ordinary v9 sealing stays.
func (s *Store) DowngradeFloatCodec(ctx context.Context) error {
	if s.referenceFormat.Load() {
		return fmt.Errorf("referenced history requires the v12 reader: %w", ErrColdPackIncomplete)
	}
	ctx, end, ok := s.explicitMaintenance.begin(ctx)
	if !ok {
		return context.Canceled
	}
	defer end()
	c := &s.coldPack
	c.run.Lock()
	defer c.run.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.options.Mode != "float-downgrade" || c.options.FloatCodec || !c.cleanUnpack || c.stats.Cursor != "" {
		return fmt.Errorf("float downgrade requires a complete clean float-downgrade sweep: %w", ErrColdPackIncomplete)
	}
	// A full visible-key audit also covers staged/unowned metrics and orphan
	// groups that a descriptor sweep cannot reach. Never lower the reader fence
	// with any such group left behind. It is read-only and paced at the sweep's
	// byte rate; the store must remain exclusively opened offline throughout.
	if err := s.verifyFloatDowngrade(ctx, c.options.BytesPerSecond); err != nil {
		return err
	}
	if err := s.Flush(); err != nil {
		return err
	}
	if c.floatFormat.Load() {
		if err := replaceFormatMarker(s.fs, s.dir, formatMarkerColdPack); err != nil {
			return fmt.Errorf("downgrade float marker: %w", err)
		}
		c.floatFormat.Store(false)
	}
	c.options.Mode = "off"
	c.stats.Mode = "off"
	return nil
}

func (s *Store) verifyFloatDowngrade(ctx context.Context, rate int64) error {
	it, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return err
	}
	defer it.Close()
	started := time.Now()
	var bytes int64
	for valid := it.First(); valid; valid = it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		k, v := it.Key(), it.Value()
		bytes += int64(len(k) + len(v))
		var suffix []byte
		if len(k) == 42 && k[0] == continuationBucketKeyPrefix && k[5] == 'u' {
			suffix = k[22:]
		} else if len(k) == 37 && k[0] == 'u' {
			suffix = k[17:]
		}
		if len(suffix) == 20 && suffix[0] == 'f' && suffix[9] == 'c' && len(v) >= sealedHeader && v[0] == 4 {
			return errors.New("float downgrade audit found an unrevised group; reader fence retained")
		}
		if bytes >= 1<<20 {
			delay := time.Duration(float64(bytes)/float64(rate)*float64(time.Second)) - time.Since(started)
			if delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
			bytes = 0
			started = time.Now()
		}
	}
	return it.Error()
}
