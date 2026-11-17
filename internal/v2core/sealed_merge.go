package v2core

// Background merges rewrite only same-source local originals. Imported parts
// retain their move-catalog authority and are never reclassified by compaction.
import (
	"bytes"
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

const sealedMergeFanIn = 16

// Runtime tuning does not change the persisted routing/configuration contract.
func mergeWorkerCount(owners int) (int, error) {
	// At most one eighth of scheduler CPUs, with one worker for small stores.
	// Each owner is visited by exactly one worker; more workers cannot help it.
	limit := min(owners, max(1, runtime.GOMAXPROCS(0)/8))
	n := limit
	if s := os.Getenv("CORE_MERGE_WORKERS"); s != "" {
		var err error
		n, err = strconv.Atoi(s)
		if err != nil || n < 0 || n > limit {
			return 0, fmt.Errorf("CORE_MERGE_WORKERS must be between 0 and %d (owners and CPU budget)", limit)
		}
	}
	return n, nil
}

func (e *coreEngine) runPartMerges(workers int) {
	defer close(e.mergeDone)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Go(func() {
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			unchanged := make(map[*coreOwner]*RollupInventory)
			for {
				for i := worker; i < len(e.owners); i += workers {
					select {
					case <-e.gcStop:
						return
					default:
					}
					o := e.owners[i]
					if o.gc == nil || !o.cfg.Rollups {
						continue
					}
					o.gc.mu.Lock()
					inventory := o.gc.inventory
					o.gc.mu.Unlock()
					if unchanged[o] == inventory {
						continue
					}
					changed, err := o.mergeSealedParts(e.gcStop)
					if err != nil {
						o.gc.mergeFailure.CompareAndSwap(nil, &coreFailure{err: err})
						e.gcFailure.CompareAndSwap(nil, &coreFailure{err: err})
						return
					}
					if !changed {
						unchanged[o] = inventory
					}
				}
				select {
				case <-e.gcStop:
					return
				case <-ticker.C:
				}
			}
		})
	}
	wg.Wait()
}

// Each size tier spans a factor of sixteen and retains at most fifteen quiescent
// local parts per group. With merge service keeping up, count is O(log bytes).
func (o *coreOwner) selectMerge(snapshot *pebble.Snapshot) ([]v2part.Descriptor, error) {
	type tier struct {
		group v2part.Group
		size  int
	}
	tiers := map[tier][]v2part.Descriptor{}
	o.gc.mu.Lock()
	inventory := o.gc.inventory
	o.gc.mu.Unlock()
	for _, value := range inventory.bindings {
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			return nil, err
		}
		if d.Scope.Partition != 0 || d.Encoding != v2part.DenseEncoding || d.Scope.VShard != v2part.GroupVShard || d.Source.Incarnation != o.partSource.Incarnation || d.Source.Stream != o.partSource.Stream {
			continue
		}
		k, err := v2part.Decode(d.FirstKey)
		if err != nil {
			return nil, err
		}
		origin, err := readPartOrigin(snapshot, k)
		if err != nil {
			return nil, err
		}
		if origin.Class != LivePart {
			continue
		}
		t := tier{d.Scope.Group, (bits.Len64(d.Bytes) - 1) / 4}
		tiers[t] = append(tiers[t], d)
	}
	var chosen []v2part.Descriptor
	var best tier
	for t, parts := range tiers {
		if len(parts) >= sealedMergeFanIn && (chosen == nil || t.size < best.size || (t.size == best.size && t.group < best.group)) {
			chosen, best = parts, t
		}
	}
	slices.SortFunc(chosen, func(a, b v2part.Descriptor) int { return bytes.Compare(a.SHA256[:], b.SHA256[:]) })
	if len(chosen) > sealedMergeFanIn {
		chosen = chosen[:sealedMergeFanIn]
	}
	return chosen, nil
}

type mergeCursor struct {
	reader     *sstable.Reader
	cursor     *tableCursor
	metadata   *tableCursor
	key        v2part.Key
	encoded    []byte
	value      []byte
	record     *sealrollup.Record
	records    []sealrollup.Record
	next       int
	part       v2part.PartID
	sourcePart v2part.PartID
}

func (c *mergeCursor) advance(first bool) (bool, error) {
	if c.next < len(c.records) {
		c.record = &c.records[c.next]
		c.next++
		c.key.BlockStart = c.record.Slot
	} else {
		c.records, c.record = nil, nil
		var valid bool
		if first {
			valid = c.cursor.First()
		} else {
			valid = c.cursor.Next()
		}
		if !valid {
			return false, c.cursor.Error()
		}
		k, err := v2part.Decode(c.cursor.Key())
		if err != nil {
			return false, err
		}
		c.key = k
		c.key.Part = c.part
		c.value = c.cursor.Value()
		if k.Archive != v2part.MetadataArchive {
			c.records, err = v2part.DecodeSlots(k.Archive, k.BlockStart, c.value)
			if err != nil {
				return false, err
			}
			c.record = &c.records[0]
			c.next = 1
		}
	}
	var err error
	c.encoded, err = v2part.Encode(c.key)
	return true, err
}

type mergeHeap []*mergeCursor

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return bytes.Compare(h[i].encoded, h[j].encoded) < 0 }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(v any)        { *h = append(*h, v.(*mergeCursor)) }
func (h *mergeHeap) Pop() any          { old := *h; v := old[len(old)-1]; *h = old[:len(old)-1]; return v }

// Input iterators hold one dense run each; output holds one physical slot.
// Metadata is point-seeked once per input/name, never buffered with its history. All comparison is integer/bitwise, with newerOrigin
// as the single slot-winner rule. Coarse records are copied, never aggregated.
func mergePartFiles(ctx context.Context, dir, path string, inputs []v2part.Descriptor, stop <-chan struct{}) (_ v2part.Descriptor, err error) {
	spec := v2part.Spec{Scope: inputs[0].Scope, Source: inputs[0].Source, MinTime: inputs[0].Coverage.MinTime, MaxTime: inputs[0].Coverage.MaxTime}
	if _, err = rand.Read(spec.Scope.Part[:]); err != nil {
		return v2part.Descriptor{}, err
	}
	seen := map[struct {
		archive uint32
		digest  v2part.Digest
	}]bool{}
	for _, d := range inputs {
		spec.MinTime = min(spec.MinTime, d.Coverage.MinTime)
		spec.MaxTime = max(spec.MaxTime, d.Coverage.MaxTime)
		spec.Source.MinOrder = min(spec.Source.MinOrder, d.Source.MinOrder)
		spec.Source.MaxOrder = max(spec.Source.MaxOrder, d.Source.MaxOrder)
		for _, p := range d.Coverage.Policies {
			id := struct {
				archive uint32
				digest  v2part.Digest
			}{p.Archive, p.Digest}
			if !seen[id] {
				spec.Policies = append(spec.Policies, p)
				seen[id] = true
			}
		}
	}
	writer, err := v2part.NewDenseWriter(path, spec)
	if err != nil {
		return v2part.Descriptor{}, err
	}
	defer writer.Abort()
	var cursors []*mergeCursor
	defer func() {
		for _, c := range cursors {
			err = errors.Join(err, c.cursor.Close(), c.metadata.Close(), c.reader.Close())
		}
	}()
	var h mergeHeap
	for _, d := range inputs {
		f, e := os.Open(filepath.Join(dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst"))
		if e != nil {
			return v2part.Descriptor{}, e
		}
		readable, e := sstable.NewSimpleReadable(f)
		if e != nil {
			_ = f.Close()
			return v2part.Descriptor{}, e
		}
		reader, e := sstable.NewReader(readable, sstable.ReaderOptions{})
		if e != nil {
			return v2part.Descriptor{}, e
		}
		it, e := reader.NewIter(nil, nil)
		if e != nil {
			_ = reader.Close()
			return v2part.Descriptor{}, e
		}
		metadataIter, e := reader.NewIter(nil, nil)
		if e != nil {
			_ = it.Close()
			_ = reader.Close()
			return v2part.Descriptor{}, e
		}
		c := &mergeCursor{reader: reader, cursor: &tableCursor{it: it}, metadata: &tableCursor{it: metadataIter}, part: spec.Scope.Part, sourcePart: d.Scope.Part}
		cursors = append(cursors, c)
		valid, e := c.advance(true)
		if e != nil {
			return v2part.Descriptor{}, e
		}
		if valid {
			heap.Push(&h, c)
		}
	}
	var metric v2part.Key
	var metadata *RollupMetadata
	var policy RollupPolicy
	var winner sealrollup.Record
	var occupied bool
	var shards []uint16
	flushSlot := func() error {
		if !occupied {
			return nil
		}
		k := metric
		k.Archive = uint32(winner.Archive)
		k.BlockStart = winner.Slot
		enc, e := v2part.Encode(k)
		if e != nil {
			return e
		}
		if e = writer.SetRecordEncoded(enc, winner); e != nil {
			return e
		}
		occupied = false
		return nil
	}
	flush := func() error {
		if metric.Metric == "" {
			return nil
		}
		if e := flushSlot(); e != nil {
			return e
		}
		k := metric
		k.Archive = v2part.MetadataArchive
		k.BlockStart = 0
		enc, e := v2part.Encode(k)
		if e != nil {
			return e
		}
		if e = writer.SetEncoded(enc, AppendRollupMetadata(nil, *metadata)); e != nil {
			return e
		}
		if len(shards) == 0 || shards[len(shards)-1] != k.VShard {
			shards = append(shards, k.VShard)
		}
		return nil
	}
	loadMetadata := func() error {
		metadata = nil
		for _, input := range cursors {
			// At the first heap key for this name, only cursors positioned at
			// this name can contain it. Other inputs are already past it.
			if input.key.Metric != metric.Metric || input.key.VShard != metric.VShard {
				continue
			}
			k := metric
			k.Part = input.sourcePart
			k.Archive = v2part.MetadataArchive
			k.BlockStart = 0
			encoded, e := v2part.Encode(k)
			if e != nil {
				return e
			}
			// Keep one seek iterator per input so neighboring metadata lookups
			// reuse decoded SST blocks instead of decompressing each anew.
			lookup := input.metadata
			if lookup.SeekGE(encoded) && bytes.Equal(lookup.Key(), encoded) {
				m, decodeErr := DecodeRollupMetadata(lookup.Value())
				if decodeErr != nil {
					return decodeErr
				}
				if metadata != nil && (m.ID != metadata.ID || !bytes.Equal(m.Policy, metadata.Policy)) {
					return errors.New("merge changed metric identity/policy")
				}
				if metadata == nil || m.Frontier.Order > metadata.Frontier.Order || (m.Frontier.Order == metadata.Frontier.Order && m.Revision > metadata.Revision) {
					metadata = &m
				}
			}
			if e = lookup.Error(); e != nil {
				return e
			}
		}
		if metadata == nil {
			return errors.New("merge metric lacks metadata")
		}
		return json.Unmarshal(metadata.Policy, &policy)
	}

	var processed int
	for len(h) > 0 {
		if err = ctx.Err(); err != nil {
			return v2part.Descriptor{}, err
		}
		select {
		case <-stop:
			return v2part.Descriptor{}, context.Canceled
		default:
		}
		c := heap.Pop(&h).(*mergeCursor)
		if c.key.Metric != metric.Metric || c.key.VShard != metric.VShard {
			if err = flush(); err != nil {
				return v2part.Descriptor{}, err
			}
			metric = c.key
			metric.Archive = 0
			metric.BlockStart = 0
			metric.Ordinal = 0
			if err = loadMetadata(); err != nil {
				return v2part.Descriptor{}, err
			}
		}
		if c.record != nil {
			r := *c.record
			if r.Archive >= len(policy.Config.Archives) {
				return v2part.Descriptor{}, errors.New("merge archive outside policy")
			}
			// Reset masks precede winner selection. Age-only slots stay: their
			// exact boundary values may feed a later coarse window on recovery.
			if !metadata.Expired && r.Order > metadata.ResetOrder {
				if occupied && (winner.Archive != r.Archive || winner.Slot != r.Slot) {
					if err = flushSlot(); err != nil {
						return v2part.Descriptor{}, err
					}
				}
				newer := !occupied
				if occupied {
					newer, err = newerOrigin(partOrigin{Class: LivePart}, r, partOrigin{Class: LivePart}, winner)
					if err != nil {
						return v2part.Descriptor{}, err
					}
					if !newer && r.Sample.Time == winner.Sample.Time && r.Order == winner.Order && r.Delta != winner.Delta {
						return v2part.Descriptor{}, errors.New("conflicting merge replacement order")
					}
				}
				if newer {
					winner = r
					occupied = true
				}
			}
		}

		valid, e := c.advance(false)
		if e != nil {
			return v2part.Descriptor{}, e
		}
		if valid {
			heap.Push(&h, c)
		}
		processed++
		if processed%4096 == 0 {
			// Bounded workers plus a cooperative pause for intake.
			timer := time.NewTimer(time.Millisecond)
			select {
			case <-stop:
				timer.Stop()
				return v2part.Descriptor{}, context.Canceled
			case <-timer.C:
			}
		}
	}
	if err = flush(); err != nil {
		return v2part.Descriptor{}, err
	}
	d, err := writer.Close()
	if err != nil {
		return d, err
	}
	d.VShards = shards
	return d, d.Validate()
}

func (o *coreOwner) mergeSealedParts(stop <-chan struct{}) (bool, error) {
	// Capture the pin and inventory under the same GC fence as publications.
	o.gc.mu.Lock()
	snapshot := o.db.NewSnapshot()
	o.gc.mu.Unlock()
	defer snapshot.Close()
	inputs, err := o.selectMerge(snapshot)
	if err != nil || len(inputs) == 0 {
		return false, err
	}
	stage, err := os.MkdirTemp(o.dir, ".merge-")
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(stage)
	path := filepath.Join(stage, "output.sst")
	d, err := mergePartFiles(context.Background(), o.dir, path, inputs, stop)
	if errors.Is(err, context.Canceled) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if o.stage != nil {
		o.stage("merge-written")
	}
	o.gc.fileMu.Lock()
	defer o.gc.fileMu.Unlock()
	changed, err := o.gc.replaceMerged(inputs, path, d)
	if changed {
		o.gcDirty.Store(true)
		coreWake(o.work)
	}
	return changed, err
}

// One synced batch replaces the live set and records every retirement fence.
// Before it commits, the output is an orphan; after it commits, only output is
// live. Reader snapshots continue naming the inputs until ordinary GC permits
// their unlink. Concurrent retire/move activity invalidates the whole proposal.
func (g *sealedPartGC) replaceMerged(inputs []v2part.Descriptor, path string, d v2part.Descriptor) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, old := range inputs {
		value, closer, err := g.db.Get(partBinding(sealedPartPrefix, old.SHA256))
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		expected, err := old.MarshalBinary()
		same := bytes.Equal(value, expected)
		err = errors.Join(err, closer.Close())
		if err != nil {
			return false, err
		}
		if !same {
			return false, errors.New("merge input binding changed")
		}
	}
	added, _, err := retainSealedPart(filepath.Dir(g.dir), path, d)
	if err != nil {
		return false, err
	}
	if g.stage != nil {
		g.stage("merge-retained")
	}
	batch := g.db.NewBatch()
	defer batch.Close()
	if err = batch.Set(added.Key, added.Value, nil); err != nil {
		return false, err
	}
	removed := map[v2part.Digest]bool{}
	for _, old := range inputs {
		if err = batch.Delete(partBinding(sealedPartPrefix, old.SHA256), nil); err != nil {
			return false, err
		}
		if err = batch.Set(partBinding(retiredPartPrefix, old.SHA256), []byte{1}, nil); err != nil {
			return false, err
		}
		removed[old.SHA256] = true
	}
	if g.stage != nil {
		g.stage("merge-before-publish")
	}
	if err = batch.Commit(pebble.Sync); err != nil {
		return false, err
	}
	g.inventory = g.inventory.with([]coreKV{added}, removed)
	g.mergeRuns.Add(1)
	g.mergeWrittenBytes.Add(d.Bytes)
	for _, old := range inputs {
		g.mergeReadBytes.Add(old.Bytes)
	}
	fence := batch.SeqNum() + uint64(batch.Count()) - 1
	for id := range removed {
		g.pending[id] = retiredPart{at: g.now(), sequence: fence}
	}
	g.pendingCount.Store(uint64(len(g.pending)))
	if g.stage != nil {
		g.stage("merge-published")
	}
	return true, nil
}

// Startup runs under exclusive owner ownership, before workers or readers.
func cleanupSealedMerges(dir string) error {
	paths, err := filepath.Glob(filepath.Join(dir, ".merge-*"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err = os.RemoveAll(path); err != nil {
			return err
		}
	}
	return nil
}

// Observe the soft inventory target without waiting, scanning descriptors or
// opening snapshots on the seal path. A slow merger may accumulate more parts;
// intake must continue. LiveParts and this counter make that pressure visible.
func (o *coreOwner) observeSealedMergeBudget() error {
	if o.mergeStop == nil || o.gc == nil {
		return nil
	}
	if failure := o.gc.mergeFailure.Load(); failure != nil {
		return failure.err
	}
	o.gc.mu.Lock()
	above := len(o.gc.inventory.bindings) >= 2*sealedMergeFanIn
	o.gc.mu.Unlock()
	if above {
		o.gc.mergeOverBudgetSeals.Add(1)
	}
	return nil
}
