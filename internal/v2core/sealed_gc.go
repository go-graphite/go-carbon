package v2core

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2placement"
)

const sealedPartGrace = time.Minute

var retiredPartPrefix = []byte{255, 'V', '2', 'G', 0}

type retiredPart struct {
	at       time.Time
	sequence uint64
}

// sealedPartGC owns only original archive links, never Pebble's managed SSTs.
// A retired file is unlinked once no opened publication holds it (parts) and
// no Pebble snapshot at/before its synced deletion batch can still name it.
type sealedPartGC struct {
	mergeFailure         atomic.Pointer[coreFailure]
	mergeRuns            atomic.Uint64
	mergeOverBudgetSeals atomic.Uint64
	mergeReadBytes       atomic.Uint64
	mergeWrittenBytes    atomic.Uint64
	pendingCount         atomic.Uint64
	unlinkedBytes        atomic.Uint64
	removedParts         atomic.Uint64
	fileMu               sync.Mutex // serializes seal file publication with unlink; readers never acquire it
	mu                   sync.Mutex
	inventory            *RollupInventory // immutable; captured/updated under mu
	moveEpoch            uint64           // attachment invalidates in-flight merge proposals; under mu
	db                   *pebble.DB
	dir                  string
	pending              map[v2part.Digest]retiredPart
	grace                time.Duration
	now                  func() time.Time
	stage                func(string)
}

func partBinding(prefix []byte, id v2part.Digest) []byte {
	return append(bytes.Clone(prefix), id[:]...)
}

// Open only under the owner's exclusive startup lock, before any snapshot is
// published. Restart resets the grace clock conservatively; old process reader
// references cannot survive. Unknown files and directories are never removed.
func openSealedPartGC(db *pebble.DB, dir string, stage func(string)) (*sealedPartGC, error) {
	g := &sealedPartGC{db: db, dir: filepath.Join(dir, "parts"), pending: map[v2part.Digest]retiredPart{}, grace: sealedPartGrace, now: time.Now, stage: stage}
	if db.Metrics().Snapshots.Count != 0 {
		return nil, errors.New("part GC must open before readers")
	}
	var err error
	if g.inventory, err = loadRollupInventory(db); err != nil {
		return nil, err
	}
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: retiredPartPrefix, UpperBound: v2part.PrefixEnd(retiredPartPrefix)})
	if err != nil {
		return nil, err
	}
	for it.First(); it.Valid(); it.Next() {
		if len(it.Key()) != len(retiredPartPrefix)+32 {
			_ = it.Close()
			return nil, errors.New("invalid retired part key")
		}
		var id v2part.Digest
		copy(id[:], it.Key()[len(retiredPartPrefix):])
		g.pending[id] = retiredPart{at: g.now()}
	}
	err = errors.Join(it.Error(), it.Close())
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(g.dir)
	if errors.Is(err, os.ErrNotExist) {
		g.pendingCount.Store(uint64(len(g.pending)))
		return g, nil
	}
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		name := strings.TrimSuffix(entry.Name(), ".descriptor")
		if entry.IsDir() || !strings.HasSuffix(name, ".sst") {
			continue
		}
		raw, e := hex.DecodeString(strings.TrimSuffix(name, ".sst"))
		if e != nil || len(raw) != 32 || hex.EncodeToString(raw)+".sst" != name {
			continue
		}
		var id v2part.Digest
		copy(id[:], raw)
		_, closer, e := db.Get(partBinding(sealedPartPrefix, id))
		if e == nil {
			_ = closer.Close()
			continue
		}
		if !errors.Is(e, pebble.ErrNotFound) {
			return nil, e
		}
		g.pending[id] = retiredPart{at: g.now()}
	}
	g.pendingCount.Store(uint64(len(g.pending)))
	return g, nil
}

// retire publishes descriptor removals durably BEFORE files become eligible.
// authorize must establish replacement/reset/retention/move authority for every
// descriptor. It is run for all candidates before any change is committed.
// File publication must go through the seal owner, which excludes concurrent unlink.
func (g *sealedPartGC) retire(ids []v2part.Digest, authorize func(v2part.Descriptor) error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if authorize == nil {
		return errors.New("part retirement authority required")
	}
	batch := g.db.NewBatch()
	defer batch.Close()
	selected := map[v2part.Digest]bool{}
	for _, id := range ids {
		if selected[id] {
			continue
		}
		key := partBinding(sealedPartPrefix, id)
		b, closer, err := g.db.Get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		d, err := v2part.ParseDescriptor(b)
		err = errors.Join(err, closer.Close())
		if err != nil {
			return err
		}
		if d.SHA256 != id {
			return errors.New("part retirement binding mismatch")
		}
		if err = authorize(d); err != nil {
			return err
		}
		if err = batch.Delete(key, nil); err != nil {
			return err
		}
		if err = batch.Set(partBinding(retiredPartPrefix, id), []byte{1}, nil); err != nil {
			return err
		}
		selected[id] = true
	}
	if len(selected) == 0 {
		return nil
	}
	if g.stage != nil {
		g.stage("gc-before-publish")
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	g.inventory = g.inventory.with(nil, selected)
	// SeqNum is the first operation's sequence; snapshot sequences are exclusive.
	fence := batch.SeqNum() + uint64(batch.Count()) - 1
	for id := range selected {
		g.pending[id] = retiredPart{at: g.now(), sequence: fence}
	}
	g.pendingCount.Store(uint64(len(g.pending)))
	if g.stage != nil {
		g.stage("gc-published")
	}
	return nil
}

// sweep retries interrupted unlinks. Snapshot creation after the metric sample
// is safe: retirement is already published, so it cannot discover old bindings.
func (g *sealedPartGC) sweep() (uint64, error) {
	g.fileMu.Lock()
	defer g.fileMu.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.pending) == 0 {
		return 0, nil
	}
	snapshots := g.db.Metrics().Snapshots
	var removed uint64
	completed := 0
	for id, p := range g.pending {
		if completed >= 64 {
			break
		}
		// Runtime retirements have a synced sequence fence. Startup discoveries
		// lack that fence and retain their grace for deterministic replay.
		if (p.sequence == 0 && g.now().Sub(p.at) < g.grace) || g.inventory.registry.held(id) || (snapshots.Count > 0 && p.sequence > 0 && snapshots.EarliestSeqNum <= p.sequence) {
			continue
		}
		_, closer, err := g.db.Get(partBinding(sealedPartPrefix, id))
		if err == nil {
			_ = closer.Close()
			// An unpublished startup orphan may be reused by deterministic replay.
			// Cancelling eligibility is conservative even for a caller re-publication.
			if err = g.db.Delete(partBinding(retiredPartPrefix, id), pebble.Sync); err != nil {
				return removed, err
			}
			delete(g.pending, id)
			g.pendingCount.Store(uint64(len(g.pending)))
			continue
		}
		if !errors.Is(err, pebble.ErrNotFound) {
			return removed, err
		}
		path := filepath.Join(g.dir, hex.EncodeToString(id[:])+".sst")
		for index, file := range []string{path, path + ".descriptor"} {
			st, e := os.Lstat(file)
			if errors.Is(e, os.ErrNotExist) {
				continue
			}
			if e != nil {
				return removed, e
			}
			if !st.Mode().IsRegular() {
				return removed, fmt.Errorf("GC refuses nonregular part %s", file)
			}
			if g.stage != nil {
				g.stage("gc-before-unlink")
			}
			if e = os.Remove(file); e != nil {
				return removed, e
			}
			removed += uint64(st.Size())
			g.unlinkedBytes.Add(uint64(st.Size()))
			if g.stage != nil {
				g.stage([]string{"gc-data-unlinked", "gc-descriptor-unlinked"}[index])
			}
		}
		if err = SyncPartDirectory(g.dir); err != nil {
			return removed, err
		}
		if g.stage != nil {
			g.stage("gc-directory-synced")
		}
		if err = g.db.Delete(partBinding(retiredPartPrefix, id), pebble.Sync); err != nil {
			return removed, err
		}
		delete(g.pending, id)
		g.pendingCount.Store(uint64(len(g.pending)))
		g.removedParts.Add(1)
		completed++
	}
	return removed, nil
}

// RetireSealedParts removes original archive bindings after the caller's
// authoritative replacement/reset has been durably published. It does not
// delete raw LSM rows or establish replacement coverage. Reader snapshots and
// hard-link exports keep their old bytes until their own leases are released.
func (e *coreEngine) RetireSealedParts(owner int, ids []v2part.Digest, authorize func(v2part.Descriptor) error) error {
	if owner < 0 || owner >= len(e.owners) {
		return os.ErrInvalid
	}
	if e.owners[owner].gc == nil {
		return errors.New("sealed archive GC unavailable")
	}
	err := e.owners[owner].gc.retire(ids, authorize)
	if err == nil {
		e.owners[owner].gcDirty.Store(true)
		coreWake(e.owners[owner].work)
	}
	return err
}

// ExpireSealedParts drops whole originals only when ALL covered event times are
// strictly before an already-authoritative retention cutoff. Mixed-age parts
// stay intact. Call after publishing retention/reset metadata, never from intake.
func (e *coreEngine) ExpireSealedParts(owner int, ids []v2part.Digest, before int64) error {
	return e.RetireSealedParts(owner, ids, func(d v2part.Descriptor) error {
		// Metadata-only reset parts carry deletion authority even when their
		// event-time frontier is old. Only an explicit replacement authorizer
		// may retire them after that authority is retained elsewhere.
		if d.Encoding == v2part.DenseEncoding && d.Coverage.Records == d.Coverage.Names {
			return errors.New("metadata-only part requires retained reset authority")
		}
		if d.Coverage.MaxTime >= before {
			return errors.New("part overlaps retained time")
		}
		return nil
	})
}

// RetireMovedParts requires the existing durable fence + installed-coverage
// activation contract, not a copy-start or download receipt. Only the activated
// placement group can be retired. Persist/validate the transition at the caller
// before invoking this operation; this method cannot manufacture a fence.
func (e *coreEngine) RetireMovedParts(owner int, ids []v2part.Digest, old, next v2placement.Document, activation v2placement.Activation, authority *v2placement.Authority) error {
	if authority == nil {
		return errors.New("local source retirement authority required")
	}
	if err := authority.AuthorizePartRetirement(activation); err != nil {
		return err
	}
	if err := old.ValidateTransition(next, []v2placement.Activation{activation}); err != nil {
		return err
	}
	return e.RetireSealedParts(owner, ids, func(d v2part.Descriptor) error {
		if string(d.Scope.Group) != activation.Fence.Group {
			return errors.New("part outside fenced group")
		}
		return nil
	})
}

func (e *coreEngine) runPartGC() {
	defer close(e.gcDone)
	// Read-only snapshots can outlive the retirement grace. Inventory remains
	// available to readers, but reclamation belongs only to a writable engine.
	if e.readOnly {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.gcStop:
			return
		case <-ticker.C:
			for _, o := range e.owners {
				if o.gc == nil {
					continue
				}
				// Only durable application time can authorize age retirement.
				// Wall time would delete historical source data during log replay.
				if err := o.expireSealedParts(o.retireClock()); err != nil {
					e.gcFailure.CompareAndSwap(nil, &coreFailure{err: err})
				}
				if _, err := o.gc.sweep(); err != nil {
					e.gcFailure.CompareAndSwap(nil, &coreFailure{err: err})
				}
			}
		}
	}
}

// Age retirement uses the durable ArchiveClock, never ahead of an injected
// Hooks.Now: fixtures and classic oracles replay history behind it.
func (o *coreOwner) retireClock() int64 {
	clock := o.frontier.Load().ArchiveClock
	if o.retireNow != nil {
		clock = min(clock, o.retireNow().Unix())
	}
	return clock
}

// A part whose latest record and envelope expiry precede the durable owner
// ArchiveClock holds nothing a reader may still return: drop it whole, as
// ClickHouse drops a part past its max TTL. The newest envelope of a metric
// expires no earlier than any record it can mask. Imported parts keep their
// move-catalog authority and are not retired here.
func (o *coreOwner) expireSealedParts(now int64) error {
	o.gc.mu.Lock()
	inventory := o.gc.inventory
	o.gc.mu.Unlock()
	var ids []v2part.Digest
	for _, value := range inventory.bindings {
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			return err
		}
		if d.Coverage.MaxExpiry == 0 || d.Coverage.MaxExpiry >= now || d.Source.Incarnation != o.partSource.Incarnation || d.Source.Stream != o.partSource.Stream {
			continue
		}
		ids = append(ids, d.SHA256)
	}
	if len(ids) == 0 {
		return nil
	}
	if err := o.gc.retire(ids, func(d v2part.Descriptor) error {
		if d.Coverage.MaxExpiry == 0 || d.Coverage.MaxExpiry >= now {
			return errors.New("part overlaps retained time")
		}
		return nil
	}); err != nil {
		return err
	}
	o.gcDirty.Store(true)
	coreWake(o.work)
	return nil
}

// PartGCStats reports original-link removal, not physical filesystem space:
// Pebble or exported hardlinks may still own the same inode. Counters reset
// on process restart; PendingParts includes startup orphans waiting for grace.
type PartGCStats struct {
	MergeOverBudgetSeals uint64
	LiveParts            uint64
	MergeRuns            uint64
	MergeReadBytes       uint64
	MergeWrittenBytes    uint64
	PendingParts         uint64
	RemovedParts         uint64
	UnlinkedFileBytes    uint64
	Error                string
}

func (e *coreEngine) PartGCStats() []PartGCStats {
	out := make([]PartGCStats, len(e.owners))
	for i, o := range e.owners {
		if o.gc != nil {
			o.gc.mu.Lock()
			live := len(o.gc.inventory.bindings)
			o.gc.mu.Unlock()
			out[i] = PartGCStats{MergeOverBudgetSeals: o.gc.mergeOverBudgetSeals.Load(), LiveParts: uint64(live), MergeRuns: o.gc.mergeRuns.Load(), MergeReadBytes: o.gc.mergeReadBytes.Load(), MergeWrittenBytes: o.gc.mergeWrittenBytes.Load(), PendingParts: o.gc.pendingCount.Load(), RemovedParts: o.gc.removedParts.Load(), UnlinkedFileBytes: o.gc.unlinkedBytes.Load()}
		}
		if failure := e.gcFailure.Load(); failure != nil {
			out[i].Error = failure.err.Error()
		}
	}
	return out
}
