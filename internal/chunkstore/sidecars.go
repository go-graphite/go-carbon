package chunkstore

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

var SidecarSummaryKey = []byte("ooo/summary")

// SidecarSummary counts physical sidecar records, including expired slots.
// Oldest is the oldest sample timestamp, zero for an empty sidecar.
type SidecarSummary struct {
	Present bool   `json:"present"`
	Points  uint64 `json:"points"`
	Oldest  int64  `json:"oldest"`
}

func (s SidecarSummary) Encode() []byte {
	value := make([]byte, 17)
	if s.Present {
		value[0] = 1
	}
	binary.BigEndian.PutUint64(value[1:], s.Points)
	binary.BigEndian.PutUint64(value[9:], uint64(s.Oldest))
	return value
}
func DecodeSidecarSummary(value []byte) (SidecarSummary, error) {
	if len(value) != 17 || value[0] > 1 {
		return SidecarSummary{}, errors.New("invalid sidecar summary")
	}
	return SidecarSummary{value[0] == 1, binary.BigEndian.Uint64(value[1:]), int64(binary.BigEndian.Uint64(value[9:]))}, nil
}

type ContinuationSidecars interface {
	InspectSidecar(ContinuationIO) (SidecarSummary, error)
}

type SidecarInventory struct {
	Count  uint64 `json:"count"`
	Points uint64 `json:"points"`
	Oldest int64  `json:"oldest"`
	Ready  bool   `json:"ready"`
}
type sidecarEntry struct {
	SidecarSummary
	index int
}
type sidecarHeap []*sidecarEntry

func (h sidecarHeap) Len() int { return len(h) }
func (h sidecarHeap) Less(i, j int) bool {
	if h[i].Oldest == 0 {
		return false
	}
	return h[j].Oldest == 0 || h[i].Oldest < h[j].Oldest
}
func (h sidecarHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *sidecarHeap) Push(v any)   { e := v.(*sidecarEntry); e.index = len(*h); *h = append(*h, e) }
func (h *sidecarHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return e
}

type sidecarCatalog struct {
	mu        sync.Mutex
	entries   map[string]*sidecarEntry
	oldest    sidecarHeap
	points    uint64
	ready     bool
	readyCh   chan struct{}
	scan      sync.Mutex
	scanAfter string
	scanning  bool
}

func (c *sidecarCatalog) set(name string, summary SidecarSummary) {
	if c.entries == nil {
		c.entries = make(map[string]*sidecarEntry)
	}
	old := c.entries[name]
	if old != nil {
		c.points -= old.Points
	}
	if !summary.Present {
		if old != nil {
			heap.Remove(&c.oldest, old.index)
			delete(c.entries, name)
		}
		return
	}
	c.points += summary.Points
	if old != nil {
		old.SidecarSummary = summary
		heap.Fix(&c.oldest, old.index)
	} else {
		e := &sidecarEntry{SidecarSummary: summary}
		c.entries[name] = e
		heap.Push(&c.oldest, e)
	}
}
func sidecarCatalogKey(name string) []byte { return append([]byte("o/"), name...) }

var sidecarReadyKey = []byte("z/sidecars-ready")

func (s *Store) loadSidecars() error {
	c := &s.sidecars
	c.readyCh = make(chan struct{})
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("o/"), UpperBound: []byte("o0")})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		summary, err := DecodeSidecarSummary(it.Value())
		if err != nil {
			return err
		}
		c.set(string(it.Key()[2:]), summary)
	}
	if err := it.Error(); err != nil {
		return err
	}
	if _, closer, err := s.db.Get(sidecarReadyKey); err == nil {
		c.ready = true
		closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	high, err := s.CatalogHighWatermark(context.Background())
	if !c.ready && (high == 0 || s.continuation == nil) {
		c.ready = true
		if err := s.db.Set(sidecarReadyKey, []byte{1}, pebble.Sync); err != nil {
			return err
		}
	}
	if c.ready {
		close(c.readyCh)
	}
	return err
}

// SidecarsReady closes once the inventory is complete and its marker is committed.
// Legacy stores need a full applied scan before callers can rely on its count.
func (s *Store) SidecarsReady() <-chan struct{} { return s.sidecars.readyCh }

// Sidecars is O(1), with no catalog or point reads on a scrape. Legacy stores
// report Ready=false until an admin scan has indexed their existing sidecars.
func (s *Store) Sidecars() SidecarInventory {
	c := &s.sidecars
	c.mu.Lock()
	defer c.mu.Unlock()
	result := SidecarInventory{Count: uint64(len(c.entries)), Points: c.points, Ready: c.ready}
	if len(c.oldest) > 0 {
		result.Oldest = c.oldest[0].Oldest
	}
	return result
}

// commitSidecars publishes the inventory record in the metric's own batch.
// Its caller holds that metric's mutation lock. Failed commits change no gauges.
func (s *Store) commitSidecars(batch *pebble.Batch, name string, summary *SidecarSummary, commit func(*pebble.Batch) error) error {
	if summary == nil {
		return commit(batch)
	}
	return s.commitSidecarRecords(batch, []SidecarRecord{{Name: name, SidecarSummary: *summary}}, commit)
}

// commitSidecarRecords keeps grouped updates and their inventory atomic. The
// caller holds each metric's mutation lock and supplies its final summary.
func (s *Store) commitSidecarRecords(batch *pebble.Batch, records []SidecarRecord, commit func(*pebble.Batch) error) error {
	if len(records) == 0 {
		return commit(batch)
	}
	c := &s.sidecars
	c.mu.Lock()
	changed := false
	for _, record := range records {
		if !record.Present && c.entries[record.Name] == nil {
			continue
		}
		changed = true
		var err error
		if record.Present {
			err = batch.Set(sidecarCatalogKey(record.Name), record.Encode(), nil)
		} else {
			err = batch.Delete(sidecarCatalogKey(record.Name), nil)
		}
		if err != nil {
			c.mu.Unlock()
			return err
		}
	}
	if !changed {
		c.mu.Unlock()
		return commit(batch)
	}
	defer c.mu.Unlock()
	if err := commit(batch); err != nil {
		return err
	}
	for _, record := range records {
		c.set(record.Name, record.SidecarSummary)
	}
	return nil
}

// SidecarPage bounds a one-off catalog inspection. After is exclusive and can
// be resumed after interruption. A scan must start at the beginning before it
// can establish Ready; callers set that marker only after the final page.
type SidecarRecord struct {
	Name string `json:"name"`
	SidecarSummary
}
type SidecarPage struct {
	After     string           `json:"after"`
	Done      bool             `json:"done"`
	Scanned   int              `json:"scanned"`
	Sidecars  []SidecarRecord  `json:"sidecars"`
	Inventory SidecarInventory `json:"inventory"`
}

func (s *Store) ScanSidecars(ctx context.Context, after string, limit int, apply bool) (SidecarPage, error) {
	c := &s.sidecars
	c.scan.Lock()
	defer c.scan.Unlock()
	if apply {
		if after == "" {
			c.scanning = true
		} else if after != c.scanAfter {
			c.scanning = false
		}
	}
	result := SidecarPage{After: after}
	codec, ok := s.continuation.(ContinuationSidecars)
	if !ok {
		return result, ErrFormat
	}
	if limit < 1 || limit > 10000 {
		return result, errors.New("limit must be 1..10000")
	}
	lower := []byte("m/")
	if after != "" {
		lower = append(catalogKey(after), 0)
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: []byte("m0")})
	if err != nil {
		return result, err
	}
	var names []string
	for it.First(); it.Valid() && len(names) < limit; it.Next() {
		names = append(names, string(it.Key()[2:]))
	}
	result.Done = !it.Valid()
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return result, err
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		summary, err := s.inspectSidecar(ctx, name, codec, apply)
		if err != nil {
			return result, fmt.Errorf("inspect %s: %w", name, err)
		}
		result.Scanned++
		result.After = name
		if summary.Present {
			result.Sidecars = append(result.Sidecars, SidecarRecord{name, summary})
		}
	}
	if apply {
		c.scanAfter = result.After
		if result.Done && c.scanning {
			if err := s.markSidecarsReady(); err != nil {
				return result, err
			}
			c.scanning = false
		}
	}
	result.Inventory = s.Sidecars()
	return result, nil
}
func (s *Store) inspectSidecar(ctx context.Context, name string, codec ContinuationSidecars, apply bool) (SidecarSummary, error) {
	unlock := s.lockMetricMutation(name)
	defer unlock()
	m, err := s.metadataLocked(name)
	if errors.Is(err, ErrNotFound) {
		return SidecarSummary{}, nil
	}
	if err != nil {
		return SidecarSummary{}, err
	}
	if !m.Continuation {
		return SidecarSummary{}, nil
	}
	b := s.db.NewIndexedBatch()
	defer b.Close()
	io := s.newContinuationIO(ctx, b, b, m)
	defer io.Close()
	summary, err := codec.InspectSidecar(io)
	if err != nil {
		return summary, err
	}
	if err := io.Close(); err != nil {
		return summary, err
	}
	if !apply {
		return summary, nil
	}
	if io.changed {
		// Summary repair can add a key absent from a prepared expiration.
		// Advance its fence while preserving the metric's activity time.
		if m.Revision == math.MaxUint64 {
			return summary, errors.New("metric revision exhausted")
		}
		m.Revision++
		if err := b.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
			return summary, err
		}
	}
	return summary, s.commitSidecars(b, name, &summary, s.commit)
}

// markSidecarsReady is called only by a completed scan from the start. Writes
// throughout that scan update the same catalog under their metric locks.
func (s *Store) markSidecarsReady() error {
	c := &s.sidecars
	c.mu.Lock()
	defer c.mu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(sidecarReadyKey, []byte{1}, nil); err != nil {
		return err
	}
	if err := s.commit(b); err != nil {
		return err
	}
	if !c.ready {
		c.ready = true
		close(c.readyCh)
	}
	return nil
}

func (s *Store) CheckNoSidecars() error {
	inventory := s.Sidecars()
	if !inventory.Ready || inventory.Count != 0 {
		return fmt.Errorf("conversion sidecar check: ready=%v count=%d points=%d oldest=%d", inventory.Ready, inventory.Count, inventory.Points, inventory.Oldest)
	}
	return nil
}

type ContinuationSidecarFolder interface {
	FoldSidecar(context.Context, ContinuationIO, func() time.Time) (string, error)
}
type SidecarFoldPage struct {
	After     string           `json:"after"`
	Done      bool             `json:"done"`
	Count     int              `json:"count"`
	Folded    int              `json:"folded"`
	Results   map[string]int   `json:"results"`
	Inventory SidecarInventory `json:"inventory"`
}

// FoldSidecars visits the sidecar catalog, so an interrupted pass resumes at
// After without re-reading already folded metrics. New names behind After are
// included by a subsequent pass. Each fold and inventory update commit together.
func (s *Store) FoldSidecars(ctx context.Context, after string, limit int, dry bool, rate float64) (SidecarFoldPage, error) {
	result := SidecarFoldPage{After: after, Results: map[string]int{}}
	if !s.Sidecars().Ready {
		return result, errors.New("sidecar inventory incomplete; run a complete scan first")
	}
	codec, ok := s.continuation.(ContinuationSidecarFolder)
	if !ok {
		return result, ErrFormat
	}
	if limit < 1 || limit > 10000 || rate < 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return result, errors.New("invalid fold limit or rate")
	}
	lower := []byte("o/")
	if after != "" {
		lower = append(sidecarCatalogKey(after), 0)
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: []byte("o0")})
	if err != nil {
		return result, err
	}
	var names []string
	for it.First(); it.Valid() && len(names) < limit; it.Next() {
		names = append(names, string(it.Key()[2:]))
	}
	result.Done = !it.Valid()
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return result, err
	}
	start := time.Now()
	for i, name := range names {
		if rate > 0 && i > 0 {
			timer := time.NewTimer(time.Until(start.Add(time.Duration(math.Min(float64(i)/rate*float64(time.Second), float64(math.MaxInt64/2))))))
			select {
			case <-ctx.Done():
				timer.Stop()
				return result, ctx.Err()
			case <-timer.C:
			}
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if !dry {
			kind, err := s.foldSidecar(ctx, name, codec)
			if err != nil {
				return result, fmt.Errorf("fold %s: %w", name, err)
			}
			result.Results[kind]++
			result.Folded++
		}
		result.Count++
		result.After = name
	}
	result.Inventory = s.Sidecars()
	return result, nil
}
func (s *Store) foldSidecar(ctx context.Context, name string, codec ContinuationSidecarFolder) (string, error) {
	unlock := s.lockMetricMutation(name)
	defer unlock()
	m, err := s.metadataLocked(name)
	if errors.Is(err, ErrNotFound) {
		return "deleted", nil
	}
	if err != nil {
		return "", err
	}
	b := s.db.NewIndexedBatch()
	defer b.Close()
	io := s.newContinuationIO(ctx, b, b, m)
	defer io.Close()
	kind, err := codec.FoldSidecar(ctx, io, s.now)
	if err != nil {
		return kind, err
	}
	if err := io.Close(); err != nil {
		return kind, err
	}
	if err := ctx.Err(); err != nil {
		return kind, err
	}
	if m.Revision == math.MaxUint64 {
		return kind, errors.New("metric revision exhausted")
	}
	m.Revision++
	if err := b.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
		return kind, err
	}
	if err := s.commitSidecars(b, name, &SidecarSummary{}, s.commit); err != nil {
		return kind, err
	}
	s.invalidateReadCache()
	return kind, nil
}
