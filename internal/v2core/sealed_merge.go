package v2core

// Background merges rewrite only same-source local originals. Imported parts
// retain their move-catalog authority and are never reclassified by compaction.
import (
	"bytes"
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

const sealedMergeFanIn = 16

// Bound the sum of compressed inputs, rather than just each input. One owner
// has at most one merge in flight. The output may be smaller after pruning.
// The default matches ClickHouse max_bytes_to_merge_at_max_space_in_pool
// (150 GiB; VictoriaMetrics allows up to 1e12 B): monthly partitions then stay
// at O(log16 seals) parts per owner instead of a terminal tier of capped parts.
func mergeMaxBytes() (uint64, error) {
	n := uint64(150 << 30)
	if s := os.Getenv("CORE_MERGE_MAX_BYTES"); s != "" {
		var err error
		n, err = strconv.ParseUint(s, 10, 64)
		if err != nil || n == 0 {
			return 0, errors.New("CORE_MERGE_MAX_BYTES must be a positive byte count")
		}
	}
	return n, nil
}

// A big merge also needs room for its staged output. VictoriaMetrics caps one at
// free disk / 4; so do we, counting the output of merges already running. The
// configured cap stays as the writer-side hard limit.
const mergeDiskFraction = 4

type freeSpacer interface {
	Free(dir string) (uint64, error)
}

type statfsSpace struct{}

func (statfsSpace) Free(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}

var (
	diskSpace     freeSpacer    = statfsSpace{}
	mergeInflight atomic.Uint64 // staged-output upper bound of running merges, all owners
	mergeDiskSkip atomic.Uint64
)

// MergeDiskSkips counts selection rounds that found a merge but skipped it for lack of free disk.
func MergeDiskSkips() uint64 { return mergeDiskSkip.Load() }

// effectiveMergeLimit reads free space once and returns min(cap, (free-inflight)/4).
// A statfs error leaves the configured cap in force; it never fails intake.
func effectiveMergeLimit(dir string) (limit, configured uint64, err error) {
	configured, err = mergeMaxBytes()
	if err != nil {
		return 0, 0, err
	}
	free, ferr := diskSpace.Free(dir)
	if ferr != nil {
		return configured, configured, nil
	}
	free -= min(free, mergeInflight.Load())
	return min(configured, free/mergeDiskFraction), configured, nil
}

// Runtime tuning does not change the persisted routing/configuration contract.
func mergeWorkerCount(owners int) (int, error) {
	// At most half of scheduler CPUs, with one worker for small stores.
	// Each owner is visited by exactly one worker; more workers cannot help it.
	limit := min(owners, max(1, runtime.GOMAXPROCS(0)/2))
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
	if e.mergeHold != nil {
		select {
		case <-e.mergeHold:
		case <-e.gcStop:
			return
		}
	}
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

// Merge tiers are generations: a seal part is generation zero and a merge
// output is one above its inputs. A merge outputs far fewer bytes than its
// sixteen inputs (their metric envelopes collapse to one), so a size tier
// re-selected its own outputs and rewrote each record several times per tier.
// By generation, a record is rewritten at most once per tier (ClickHouse levels
// parts by merge count). Each tier retains at most fifteen quiescent local
// parts per group/partition below the cap. Terminal parts trade bounded
// rewrite size for a larger read inventory until their time partition expires.
func (o *coreOwner) selectMerge(snapshot *pebble.Snapshot) ([]v2part.Descriptor, error) {
	limit, _, err := effectiveMergeLimit(o.dir)
	if err != nil {
		return nil, err
	}
	return o.selectMergeLimit(snapshot, limit)
}

func (o *coreOwner) selectMergeLimit(snapshot *pebble.Snapshot, limit uint64) ([]v2part.Descriptor, error) {
	type tier struct {
		group      v2part.Group
		partition  int64
		generation int
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
		if d.Bytes > limit || d.Encoding != v2part.DenseEncoding || d.Scope.VShard != v2part.GroupVShard || d.Source.Incarnation != o.partSource.Incarnation || d.Source.Stream != o.partSource.Stream {
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
		t := tier{d.Scope.Group, d.Scope.Partition, mergeTier(d)}
		tiers[t] = append(tiers[t], d)
	}
	var chosen []v2part.Descriptor
	var best tier
	for t, parts := range tiers {
		slices.SortFunc(parts, func(a, b v2part.Descriptor) int {
			if a.Bytes < b.Bytes {
				return -1
			}
			if a.Bytes > b.Bytes {
				return 1
			}
			return bytes.Compare(a.SHA256[:], b.SHA256[:])
		})
		var total uint64
		if len(parts) < sealedMergeFanIn {
			continue
		}
		fits := true
		total = 0
		for _, d := range parts[:sealedMergeFanIn] {
			if d.Bytes > limit-total {
				fits = false
				break
			}
			total += d.Bytes
		}
		if fits && (chosen == nil || t.generation < best.generation || (t.generation == best.generation && (t.group < best.group || t.group == best.group && t.partition < best.partition))) {
			chosen, best = parts, t
		}
	}
	if len(chosen) > sealedMergeFanIn {
		chosen = chosen[:sealedMergeFanIn]
	}
	return chosen, nil
}

// A merge output's generation lives in the tail of its otherwise random part
// ID, so it survives restarts with no descriptor field: older binaries read
// the same canonical descriptor and treat the ID as opaque. Seal parts, and
// merge outputs written before generations, lack the tag and are generation
// zero. Eighty-eight random bits keep IDs unique.
var partGenerationTag = [4]byte{0x9e, 0x4d, 0x47, 0x31}

func partGeneration(id v2part.PartID) int {
	if [4]byte(id[11:15]) != partGenerationTag {
		return 0
	}
	return int(id[15])
}

func setPartGeneration(id *v2part.PartID, generation int) {
	copy(id[11:15], partGenerationTag[:])
	id[15] = byte(min(generation, 255))
}

// mergeTier is the selector's tier key; tests swap in size tiers.
var mergeTier = func(d v2part.Descriptor) int { return partGeneration(d.Scope.Part) }

// ClickHouse merge_with_ttl_timeout: at most one TTL rewrite per owner per
// four hours, so expiry work stays a small, bounded share of merge service.
const ttlRewriteSeconds = 4 * 3600

// When no tier merge is due, rewrite the local part whose earliest data
// expiry has passed (ClickHouse TTL merge). Parts whose latest expiry has
// passed are dropped whole by GC instead and are not rewritten.
func (o *coreOwner) selectTTLRewrite(snapshot *pebble.Snapshot, clock int64, limit uint64) ([]v2part.Descriptor, error) {
	o.gc.mu.Lock()
	inventory := o.gc.inventory
	o.gc.mu.Unlock()
	var chosen []v2part.Descriptor
	for _, value := range inventory.bindings {
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			return nil, err
		}
		c := d.Coverage
		if c.MinExpiry == 0 || c.MinExpiry >= clock || c.MaxExpiry < clock || d.Bytes > limit || d.Encoding != v2part.DenseEncoding || d.Scope.VShard != v2part.GroupVShard || d.Source.Incarnation != o.partSource.Incarnation || d.Source.Stream != o.partSource.Stream {
			continue
		}
		if chosen != nil && (c.MinExpiry > chosen[0].Coverage.MinExpiry || c.MinExpiry == chosen[0].Coverage.MinExpiry && bytes.Compare(d.SHA256[:], chosen[0].SHA256[:]) > 0) {
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
		if origin.Class == LivePart {
			chosen = []v2part.Descriptor{d}
		}
	}
	return chosen, nil
}

// The on-disk metadata sorts after a metric's runs. Buffer only that metric's
// encoded values per input to see its durable masks, then merge its slots. Each
// SST iterator moves strictly forward: there is no per-name metadata iterator or
// seek, and memory does not grow with the number of names in the part.
type mergeBlock struct {
	archive    uint32
	slot       int64
	start, end int
}

type mergeCursor struct {
	reader      *sstable.Reader
	cursor      *tableCursor
	key         v2part.Key
	decoder     v2part.KeyDecoder
	part        v2part.PartID
	metadata    RollupMetadata
	policyBytes []byte
	// config is the metadata's policy: from the part policy table for slim
	// rows, or parsed from the stored definition of older rows.
	config       *sealrollup.Config
	legacyConfig sealrollup.Config
	arena        []byte
	blocks       []mergeBlock
	records      []sealrollup.Record
	block, next  int
}

func (c *mergeCursor) readMetric(first bool) (bool, error) {
	valid := c.cursor.Next
	if first {
		valid = c.cursor.First
	}
	if !valid() {
		return false, c.cursor.Error()
	}
	c.arena, c.blocks = c.arena[:0], c.blocks[:0]
	c.records = c.records[:0]
	c.block, c.next = 0, 0
	k, err := c.decoder.Decode(c.cursor.Key())
	if err != nil {
		return false, err
	}
	c.key = k
	c.key.Part = c.part
	c.key.Archive, c.key.BlockStart, c.key.Ordinal = 0, 0, 0
	for {
		if k.Metric != c.key.Metric || k.VShard != c.key.VShard {
			return false, errors.New("merge metric lacks metadata")
		}
		if k.Archive == v2part.MetadataArchive {
			var slim bool
			c.metadata, slim, err = decodeRollupMetadataHeader(c.cursor.Value())
			if err != nil {
				return false, err
			}
			if slim {
				resolved, ok := v2part.ResolvePolicy(c.metadata.Frontier.Policy)
				if !ok {
					return false, errors.New("rollup metadata policy missing from part policy tables")
				}
				c.config = &resolved.Config
				return true, nil
			}
			c.policyBytes = append(c.policyBytes[:0], c.metadata.Policy...)
			c.metadata.Policy = c.policyBytes
			var policy RollupPolicy
			if err = json.Unmarshal(c.policyBytes, &policy); err != nil {
				return false, err
			}
			c.legacyConfig = policy.Config
			c.config = &c.legacyConfig
			return true, nil
		}
		if k.Ordinal != 0 {
			return false, errors.New("merge requires dense ordinal zero")
		}
		start := len(c.arena)
		c.arena = append(c.arena, c.cursor.Value()...)
		c.blocks = append(c.blocks, mergeBlock{k.Archive, k.BlockStart, start, len(c.arena)})
		if !c.cursor.Next() {
			if err = c.cursor.Error(); err != nil {
				return false, err
			}
			return false, errors.New("merge metric lacks metadata")
		}
		k, err = c.decoder.Decode(c.cursor.Key())
		if err != nil {
			return false, err
		}
	}
}

func (c *mergeCursor) advanceRecord(first bool) (bool, error) {
	if !first {
		c.next++
	}
	if c.next < len(c.records) {
		return true, nil
	}
	if c.block == len(c.blocks) {
		return false, nil
	}
	b := c.blocks[c.block]
	c.block++
	var err error
	c.records, err = v2part.DecodeSlotsInto(c.records[:0], b.archive, b.slot, c.arena[b.start:b.end])
	c.next = 0
	return err == nil, err
}

func (c *mergeCursor) record() sealrollup.Record { return c.records[c.next] }

type mergeHeap []*mergeCursor

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	// All inputs share group/partition and normalize to the output part. GCP3
	// orders the remaining identity by vshard, name byte length, then name bytes.
	a, b := h[i].key, h[j].key
	if a.VShard != b.VShard {
		return a.VShard < b.VShard
	}
	if len(a.Metric) != len(b.Metric) {
		return len(a.Metric) < len(b.Metric)
	}
	return a.Metric < b.Metric
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(v any)   { *h = append(*h, v.(*mergeCursor)) }
func (h *mergeHeap) Pop() any     { old := *h; v := old[len(old)-1]; *h = old[:len(old)-1]; return v }

type mergeRecordHeap []*mergeCursor

func (h mergeRecordHeap) Len() int           { return len(h) }
func (h mergeRecordHeap) Less(i, j int) bool { return mergeSlotLess(h[i].record(), h[j].record()) }
func (h mergeRecordHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeRecordHeap) Push(v any)        { *h = append(*h, v.(*mergeCursor)) }
func (h *mergeRecordHeap) Pop() any          { old := *h; v := old[len(old)-1]; *h = old[:len(old)-1]; return v }
func mergeSlotLess(a, b sealrollup.Record) bool {
	return a.Archive < b.Archive || a.Archive == b.Archive && a.Slot < b.Slot
}

// Merge same-source local originals with the shared timestamp/class/order rule.
// Resolved coarse records are copied, never aggregated. Publication and GC stay
// outside this streaming rewrite and retain the ordinary snapshot fences.
// A nonzero ttl is the durable owner ArchiveClock of a TTL rewrite: records
// whose expiry precedes it are dropped as whole-part retirement would.
func mergePartFiles(ctx context.Context, dir, path string, inputs []v2part.Descriptor, ttl int64, stop <-chan struct{}) (_ v2part.Descriptor, err error) {
	spec := v2part.Spec{Scope: inputs[0].Scope, Source: inputs[0].Source, MinTime: inputs[0].Coverage.MinTime, MaxTime: inputs[0].Coverage.MaxTime}
	if _, err = rand.Read(spec.Scope.Part[:]); err != nil {
		return v2part.Descriptor{}, err
	}
	// A merge output is one generation above its inputs; a TTL rewrite of one
	// part keeps its generation.
	generation := 0
	for _, d := range inputs {
		generation = max(generation, partGeneration(d.Scope.Part))
	}
	if ttl == 0 {
		generation++
	}
	setPartGeneration(&spec.Scope.Part, generation)
	seen := map[struct {
		archive uint32
		digest  v2part.Digest
	}]bool{}
	for _, d := range inputs {
		spec.MinTime = min(spec.MinTime, d.Coverage.MinTime)
		spec.MaxTime = max(spec.MaxTime, d.Coverage.MaxTime)
		spec.Source.MinOrder = min(spec.Source.MinOrder, d.Source.MinOrder)
		spec.Source.MaxOrder = max(spec.Source.MaxOrder, d.Source.MaxOrder)
		v2part.RegisterPolicies(d.Coverage.Policies)
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

	// Output envelopes omit the definition only for policies this part's own
	// table carries, so every merged part stays self-describing.
	tablePolicies := v2part.TablePolicies(spec.Policies)
	writer, err := v2part.NewDenseWriter(path, spec)
	if err != nil {
		return v2part.Descriptor{}, err
	}
	defer writer.Abort()
	limit, err := mergeMaxBytes()
	if err != nil {
		return v2part.Descriptor{}, err
	}
	writer.LimitBytes(limit)
	// Pebble's forward compaction iterator recycles compressed and decoded SST
	// blocks. A normal read iterator allocates/frees C memory for every block.
	// All inputs are advanced by this goroutine; this pool is never concurrent.
	var readBuffers sstable.BufferPool
	readBuffers.Init(2*len(inputs) + 2)
	defer readBuffers.Release()
	var bytesIterated uint64
	var cursors []*mergeCursor
	defer func() {
		for _, c := range cursors {
			err = errors.Join(err, c.cursor.Close(), c.reader.Close())
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
		it, e := reader.NewCompactionIter(&bytesIterated, sstable.TrivialReaderProvider{Reader: reader}, &readBuffers)
		if e != nil {
			_ = reader.Close()
			return v2part.Descriptor{}, e
		}

		c := &mergeCursor{reader: reader, cursor: &tableCursor{it: it}, part: spec.Scope.Part}
		cursors = append(cursors, c)
		valid, e := c.readMetric(true)
		if e != nil {
			return v2part.Descriptor{}, e
		}
		if valid {
			heap.Push(&h, c)
		}
	}

	var shards []uint16
	var members []*mergeCursor
	var records mergeRecordHeap
	var key, metaBytes []byte
	var config sealrollup.Config
	var expiry expiryBounds
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
		members = append(members[:0], c)
		for len(h) > 0 && h[0].key.VShard == c.key.VShard && h[0].key.Metric == c.key.Metric {
			members = append(members, heap.Pop(&h).(*mergeCursor))
		}
		// The definition is a function of the key (name, group) and the policy
		// digest, so equal IDs, groups and digests mean equal definitions.
		metadata, chosen := &c.metadata, c
		for _, input := range members[1:] {
			m := &input.metadata
			if m.ID != metadata.ID || input.key.Group != c.key.Group || m.Frontier.Policy != metadata.Frontier.Policy || (m.Policy != nil && metadata.Policy != nil && !bytes.Equal(m.Policy, metadata.Policy)) {
				return v2part.Descriptor{}, errors.New("merge changed metric identity/policy")
			}
			if compactMetadata(*m).after(compactMetadata(*metadata)) {
				metadata, chosen = m, input
			}
		}
		config = *chosen.config
		floor := max(metadata.LastUpdate/1e9, ttl)
		key, err = v2part.Encode(c.key)
		if err != nil {
			return v2part.Descriptor{}, err
		}
		tail := key[len(key)-16:]
		setSlot := func(archive uint32, slot int64) {
			binary.BigEndian.PutUint32(tail, archive)
			binary.BigEndian.PutUint64(tail[4:], uint64(slot)^(1<<63))
		}
		var winner sealrollup.Record
		occupied := false
		flush := func() error {
			if !occupied {
				return nil
			}
			setSlot(uint32(winner.Archive), winner.Slot)
			occupied = false
			expiry.data(rollupRecordExpiry(config, winner))
			return writer.SetRecordEncoded(key, winner)
		}
		records = records[:0]
		for _, input := range members {
			valid, e := input.advanceRecord(true)
			if e != nil {
				return v2part.Descriptor{}, e
			}
			if valid {
				heap.Push(&records, input)
			}
		}
		for len(records) > 0 {
			input := heap.Pop(&records).(*mergeCursor)
			r := input.record()
			if r.Archive >= len(config.Archives) {
				return v2part.Descriptor{}, errors.New("merge archive outside policy")
			}
			// A whole non-overlapping run can keep its exact dense encoding. Durable
			// reset/expiry masks must permit every slot; ties always use the slow path.
			last := input.records[len(input.records)-1]
			untouched := input.next == 0 && len(input.records) >= 16 && !metadata.Expired && (!occupied || mergeSlotLess(winner, r)) && (len(records) == 0 || mergeSlotLess(last, records[0].record()))
			if untouched {
				for _, item := range input.records {
					if item.Order <= metadata.ResetOrder || rollupRecordExpiry(config, item) < floor {
						untouched = false
						break
					}
				}
			}
			if untouched {
				if err = flush(); err != nil {
					return v2part.Descriptor{}, err
				}
				for _, item := range input.records {
					expiry.data(rollupRecordExpiry(config, item))
				}
				b := input.blocks[input.block-1]
				setSlot(b.archive, b.slot)
				if err = writer.SetSlotsEncoded(key, input.arena[b.start:b.end]); err != nil {
					return v2part.Descriptor{}, err
				}
				input.next = len(input.records) - 1
			} else if !metadata.Expired && r.Order > metadata.ResetOrder && rollupRecordExpiry(config, r) >= floor {
				if occupied && (winner.Archive != r.Archive || winner.Slot != r.Slot) {
					if err = flush(); err != nil {
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
			valid, e := input.advanceRecord(false)
			if e != nil {
				return v2part.Descriptor{}, e
			}
			if valid {
				heap.Push(&records, input)
			}
		}
		if err = flush(); err != nil {
			return v2part.Descriptor{}, err
		}
		setSlot(v2part.MetadataArchive, 0)
		expiry.envelope(rollupMetricExpiry(config, metadata.Frontier.MaxTime))
		if tablePolicies[metadata.Frontier.Policy] && !rollupWriteDefinitions.Load() {
			metaBytes = appendRollupMetadataSlim(metaBytes[:0], *metadata)
		} else {
			full := *metadata
			if full.Policy == nil {
				if full.Policy, _, err = appendRollupDefinition(nil, c.key.Metric, c.key.Group, full.Frontier.Policy); err != nil {
					return v2part.Descriptor{}, err
				}
			}
			metaBytes = AppendRollupMetadata(metaBytes[:0], full)
		}
		if err = writer.SetEncoded(key, metaBytes); err != nil {
			return v2part.Descriptor{}, err
		}
		if len(shards) == 0 || shards[len(shards)-1] != c.key.VShard {
			shards = append(shards, c.key.VShard)
		}
		for _, input := range members {
			valid, e := input.readMetric(false)
			if e != nil {
				return v2part.Descriptor{}, e
			}
			if valid {
				heap.Push(&h, input)
			}
		}
	}
	writer.SetExpiry(expiry.min, expiry.max)
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
	limit, configured, err := effectiveMergeLimit(o.dir)
	if err != nil {
		return false, err
	}
	inputs, err := o.selectMergeLimit(snapshot, limit)
	if err != nil {
		return false, err
	}
	var ttl int64
	if len(inputs) == 0 && limit < configured {
		// Disk-bound: tell "nothing due" from "due but no room".
		if due, e := o.selectMergeLimit(snapshot, configured); e == nil && len(due) > 0 {
			mergeDiskSkip.Add(1)
			slog.Warn("v2 merge skipped: not enough free disk", "dir", o.dir, "merge_limit_bytes", limit, "configured_cap_bytes", configured)
			return false, nil
		}
	}
	if len(inputs) == 0 {
		now := time.Now().Unix()
		if now < o.ttlNext.Load() {
			return false, nil
		}
		clock := o.retireClock()
		inputs, err = o.selectTTLRewrite(snapshot, clock, limit)
		if err != nil || len(inputs) == 0 {
			return false, err
		}
		ttl = clock
		o.ttlNext.Store(now + ttlRewriteSeconds)
	}
	var staged uint64
	for _, d := range inputs {
		staged += d.Bytes
	}
	mergeInflight.Add(staged)
	defer mergeInflight.Add(^(staged - 1))
	stage, err := os.MkdirTemp(o.dir, ".merge-")
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(stage)
	path := filepath.Join(stage, "output.sst")
	d, err := mergePartFiles(context.Background(), o.dir, path, inputs, ttl, stop)
	if errors.Is(err, context.Canceled) || errors.Is(err, v2part.ErrPartSizeLimit) {
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
