package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

const DefaultRelayoutReclaimBytes = 32 << 30

// R is a separate maintenance namespace before the metric keyspaces. Putting
// this journal after u (e.g. beside the z cursor) would widen flushed tables
// across every remaining old metric, defeating bounded span compaction.
var relayoutReclaimPrefix = []byte("R/relayout/reclaim/")

type relayoutReclaimState struct {
	// Only relayout workers use gate. Writers join admitted groups, then reclaim
	// a stable journal while stopping new ingests. Foreground writers never wait.
	gate                sync.RWMutex
	initialized         atomic.Bool
	limit               atomic.Uint64
	mu                  sync.Mutex
	pending, reserved   uint64
	reclaims, reclaimed uint64
}

type relayoutReclaimGroup struct {
	s                *Store
	key              []byte
	spans            map[byte]*relayoutSpan
	bytes, remaining uint64
	finished         bool
}

func (s *Store) SetRelayoutReclaimBytes(n uint64) error {
	if n == 0 {
		return errors.New("relayout reclaim bytes must be positive")
	}
	s.relayout.reclaim.limit.Store(n)
	s.relayout.guard.wake()
	return nil
}

func (m *relayoutReclaimState) stats(out *RelayoutStats) {
	out.ReclaimBytes = m.limit.Load()
	m.mu.Lock()
	defer m.mu.Unlock()
	out.UnreclaimedBytes = m.pending
	out.Reclaims, out.ReclaimedBytes = m.reclaims, m.reclaimed
}

// capacity samples the available headroom each time. Unsupported in-memory
// filesystems have no space cap; a missing real sample admits no new bytes.
func (s *Store) relayoutReclaimCapacity() (configured, space uint64) {
	configured = s.relayout.reclaim.limit.Load()
	if configured == 0 {
		configured = DefaultRelayoutReclaimBytes
	}
	g := &s.relayout.guard
	g.mu.Lock()
	reserve := g.minFree
	g.mu.Unlock()
	usage, err := s.fs.GetDiskUsage(s.dir)
	if errors.Is(err, vfs.ErrUnsupported) {
		return configured, math.MaxUint64
	}
	if err != nil || usage.AvailBytes <= reserve {
		return configured, 0
	}
	return configured, (usage.AvailBytes - reserve) / 2
}

func (s *Store) initRelayoutReclaim(ctx context.Context) error {
	m := &s.relayout.reclaim
	if m.initialized.Load() {
		return nil
	}
	m.gate.Lock()
	defer m.gate.Unlock()
	if m.initialized.Load() {
		return nil
	}
	_, _, n, err := s.readRelayoutReclaims(ctx)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.pending = n
	m.mu.Unlock()
	m.initialized.Store(true)
	return nil
}

func (s *Store) beginRelayoutGroup(ctx context.Context, items []relayoutItem) (*relayoutReclaimGroup, error) {
	if err := s.initRelayoutReclaim(ctx); err != nil {
		return nil, err
	}
	var n uint64
	for _, item := range items {
		if item.plan.Bytes < 0 || uint64(item.plan.Bytes) > math.MaxUint64-n {
			return nil, errors.New("relayout group byte count overflow")
		}
		n += uint64(item.plan.Bytes)
	}
	m := &s.relayout.reclaim
	for {
		if err := s.waitRelayoutSize(ctx, n); err != nil {
			return nil, err
		}
		m.gate.RLock()
		if err := ctx.Err(); err != nil {
			m.gate.RUnlock()
			return nil, err
		}
		if reason, _ := s.relayout.guard.observePolicy(s.relayoutPressure, n); reason != "" {
			m.gate.RUnlock()
			continue
		}
		configured, space := s.relayoutReclaimCapacity()
		m.mu.Lock()
		// Reserve the future copies before applying the half-headroom cap.
		future := m.reserved + n
		allowance := future/2 + future%2
		if allowance > space {
			space = 0
		} else {
			space -= allowance
		}
		cap := min(configured, space)
		total := m.pending + m.reserved
		// An indivisible metric/group may exceed a deliberately tiny configured
		// threshold, but must fit the filesystem cap and owns the batch alone.
		fits := n <= space && (total == 0 || n <= cap && total <= cap-n)
		if fits {
			m.reserved += n
		}
		m.mu.Unlock()
		if !fits {
			m.gate.RUnlock()
			if err := s.reclaimRelayout(ctx, true, 0); err != nil {
				return nil, err
			}
			continue
		}
		key := binary.BigEndian.AppendUint64(append([]byte(nil), relayoutReclaimPrefix...), items[0].m.ID)
		g := &relayoutReclaimGroup{s: s, key: key, spans: make(map[byte]*relayoutSpan), remaining: n}
		value, closer, err := s.db.Get(key)
		if err == nil {
			g.bytes, g.spans, err = decodeRelayoutReclaim(value)
			_ = closer.Close()
		}
		if errors.Is(err, pebble.ErrNotFound) {
			err = nil
		}
		if err != nil {
			g.finish()
			return nil, err
		}
		return g, nil
	}
}

func (g *relayoutReclaimGroup) committed(n uint64) {
	m := &g.s.relayout.reclaim
	m.mu.Lock()
	m.reserved -= n
	m.pending += n
	m.mu.Unlock()
	g.bytes += n
	g.remaining -= n
}
func (g *relayoutReclaimGroup) finish() {
	if g.finished {
		return
	}
	g.finished = true
	m := &g.s.relayout.reclaim
	m.mu.Lock()
	m.reserved -= g.remaining
	m.mu.Unlock()
	m.gate.RUnlock()
}

// force is used for admission pressure, startup catch-up and completion.
// needed accounts for a planned group before its copies are written.
func (s *Store) reclaimRelayout(ctx context.Context, force bool, needed uint64) error {
	if err := s.initRelayoutReclaim(ctx); err != nil {
		return err
	}
	m := &s.relayout.reclaim
	m.mu.Lock()
	epoch := m.reclaims
	m.mu.Unlock()
	due := func() bool {
		configured, space := s.relayoutReclaimCapacity()
		cap := min(configured, space)
		m.mu.Lock()
		defer m.mu.Unlock()
		total := m.pending + m.reserved
		return m.pending > 0 && (total >= cap || needed > cap-total)
	}
	if !force && !due() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.gate.Lock()
	defer m.gate.Unlock()
	m.mu.Lock()
	stale := m.reclaims != epoch
	m.mu.Unlock()
	if stale {
		return nil
	}
	// Other workers may have reclaimed while this caller waited.
	keys, spans, n, err := s.readRelayoutReclaims(ctx)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.pending = n
	m.mu.Unlock()
	if len(keys) == 0 {
		return nil
	}
	if !force && !due() {
		return nil
	}
	for _, kind := range []byte{'u', 't'} {
		if span := spans[kind]; span != nil {
			// An operator can pause new reclaim IO with either pressure limit,
			// including between the old-key and discarded-copy spans.
			if err := s.relayout.guard.waitPolicy(ctx, s.relayoutPressure, nil, 0); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			compactErr := s.db.Compact(span.start, append(append([]byte(nil), span.end...), 0), false)
			if err := ctx.Err(); err != nil {
				return err
			}
			if compactErr != nil {
				return fmt.Errorf("reclaim relayout batch: %w", compactErr)
			}
		}
	}
	// Keep every obligation until all spans finish. Repeating a completed span
	// after a crash is safe; deleting this record early could leak the others.
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, key := range keys {
		if err := batch.Delete(key, nil); err != nil {
			return err
		}
	}
	if err := s.commit(batch); err != nil {
		return err
	}
	m.mu.Lock()
	m.pending = 0
	m.reclaims++
	m.reclaimed += n
	m.mu.Unlock()
	return nil
}

func (s *Store) recoverRelayoutReclaim(ctx context.Context) error {
	// Re-read even on an in-process retry, including an ambiguous WAL sync error.
	m := &s.relayout.reclaim
	m.gate.Lock()
	_, _, n, err := s.readRelayoutReclaims(ctx)
	if err == nil {
		m.mu.Lock()
		m.pending = n
		m.mu.Unlock()
		m.initialized.Store(true)
	}
	m.gate.Unlock()
	if err != nil {
		return err
	}
	return s.reclaimRelayout(ctx, true, 0)
}

func (s *Store) readRelayoutReclaims(ctx context.Context) ([][]byte, map[byte]*relayoutSpan, uint64, error) {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: relayoutReclaimPrefix, UpperBound: prefixEnd(relayoutReclaimPrefix)})
	if err != nil {
		return nil, nil, 0, err
	}
	defer it.Close()
	var keys [][]byte
	spans := make(map[byte]*relayoutSpan)
	var total uint64
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, 0, err
		}
		if len(it.Key()) != len(relayoutReclaimPrefix)+8 {
			return nil, nil, 0, errors.New("invalid relayout reclaim journal key")
		}
		n, part, err := decodeRelayoutReclaim(it.Value())
		if err != nil {
			return nil, nil, 0, err
		}
		if n > math.MaxUint64-total {
			return nil, nil, 0, errors.New("relayout journal byte count overflow")
		}
		total += n
		keys = append(keys, append([]byte(nil), it.Key()...))
		for _, span := range part {
			addRelayoutSpan(spans, span.start)
			addRelayoutSpan(spans, span.end)
		}
	}
	return keys, spans, total, it.Error()
}

func encodeRelayoutReclaim(n uint64, spans map[byte]*relayoutSpan) []byte {
	out := binary.BigEndian.AppendUint64([]byte{1}, n)
	for _, kind := range []byte{'u', 't'} {
		var lo, hi []byte
		if span := spans[kind]; span != nil {
			lo, hi = span.start, span.end
		}
		for _, key := range [][]byte{lo, hi} {
			out = binary.AppendUvarint(out, uint64(len(key)))
			out = append(out, key...)
		}
	}
	return out
}
func decodeRelayoutReclaim(value []byte) (uint64, map[byte]*relayoutSpan, error) {
	invalid := errors.New("invalid relayout reclaim journal")
	if len(value) < 9 || value[0] != 1 {
		return 0, nil, invalid
	}
	n := binary.BigEndian.Uint64(value[1:9])
	value = value[9:]
	spans := make(map[byte]*relayoutSpan)
	for _, kind := range []byte{'u', 't'} {
		var bounds [2][]byte
		for i := range bounds {
			size, k := binary.Uvarint(value)
			if k <= 0 || size > uint64(len(value)-k) {
				return 0, nil, invalid
			}
			value = value[k:]
			bounds[i] = append([]byte(nil), value[:int(size)]...)
			value = value[int(size):]
		}
		if len(bounds[0]) == 0 && len(bounds[1]) == 0 {
			continue
		}
		if len(bounds[0]) == 0 || len(bounds[1]) == 0 || bounds[0][0] != kind || bounds[1][0] != kind || bytes.Compare(bounds[0], bounds[1]) > 0 {
			return 0, nil, invalid
		}
		spans[kind] = &relayoutSpan{start: bounds[0], end: bounds[1]}
	}
	if len(value) != 0 {
		return 0, nil, invalid
	}
	return n, spans, nil
}
