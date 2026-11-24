package v2core

// Background merges rewrite only same-source local originals. Imported parts
// retain their move-catalog authority and are never reclassified by compaction.
import (
	"bytes"
	"cmp"
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

const sealedMergeFanIn = 16

// Bound the sum of compressed inputs, rather than just each input. One owner
// has at most one merge in flight. The output may be smaller after pruning.
// The default matches ClickHouse max_bytes_to_merge_at_max_space_in_pool
// (150 GiB; VictoriaMetrics allows up to 1e12 B): monthly partitions then stay
// at O(log16 bytes) parts per owner instead of a terminal tier of capped parts.
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
			retry := time.Now().Add(10 * time.Second)
			for {
				if time.Now().After(retry) {
					clear(unchanged)
					retry = time.Now().Add(10 * time.Second)
				}
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
					changed, err := o.mergeSealedParts(e.gcStop, false)
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

// ForceMerge merges each owner's live local parts down to one part per group
// and time partition, like VictoriaMetrics /internal/force_merge or ClickHouse
// OPTIMIZE FINAL. Background merges keep running beside it. It returns the
// number of merges done; a caller repeats it until that number is zero.
func (e *coreEngine) ForceMerge() (int, error) {
	workers, err := mergeWorkerCount(len(e.owners))
	if err != nil {
		return 0, err
	}
	var merged atomic.Int64
	var failure atomic.Pointer[error]
	var wg sync.WaitGroup
	for worker := 0; worker < max(1, workers); worker++ {
		wg.Go(func() {
			for i := worker; i < len(e.owners); i += max(1, workers) {
				o := e.owners[i]
				if o.gc == nil || !o.cfg.Rollups {
					continue
				}
				for failure.Load() == nil {
					changed, err := o.mergeSealedParts(e.gcStop, true)
					if err != nil {
						failure.CompareAndSwap(nil, &err)
					}
					if !changed {
						break
					}
					merged.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if p := failure.Load(); p != nil {
		return int(merged.Load()), *p
	}
	return int(merged.Load()), nil
}

// Merges combine only Seq-adjacent live local parts of one group/partition
// (ClickHouse's block-number rule): a window never skips a live part sealed
// between its inputs, so the output's Seq range interleaves with no other
// part. Record order ranges may still interleave (a closure record can carry
// an older order than an earlier seal's records). A window of adjacent parts
// qualifies once its total is
// at least ratio/2 times its largest part, so a large input is not repeatedly
// combined with only a few much smaller inputs. Fresh windows also wait for
// ratio inputs; aged windows need only the size balance. Small outputs use ratio 4,
// so new seals fold into a small
// part every few seal intervals and few small parts stay live for renders.
// Larger outputs keep ratio sixteen. The cheapest qualifying range merges first.
const (
	smallMergeBytes = 64 << 20
	smallMergeRatio = 4
)

func mergeRatio(total uint64) uint64 {
	if total <= smallMergeBytes {
		return smallMergeRatio
	}
	return sealedMergeFanIn
}

// full keeps adjacency and the byte cap, but ignores background ratios and fan-in.
func (o *coreOwner) selectMergeLimit(snapshot *pebble.Snapshot, limit uint64, full bool) ([]v2part.Descriptor, error) {
	type scope struct {
		group     v2part.Group
		partition int64
	}
	scopes := map[scope][]v2part.Descriptor{}
	o.gc.mu.Lock()
	inventory := o.gc.inventory
	o.gc.mu.Unlock()
	for _, value := range inventory.bindings {
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			return nil, err
		}
		// Oversized parts stay in the sequence: a window stops at them.
		if d.Encoding != v2part.DenseEncoding || d.Scope.VShard != v2part.GroupVShard || d.Source.Incarnation != o.partSource.Incarnation || d.Source.Stream != o.partSource.Stream {
			continue
		}
		k, err := v2part.DecodeKey(d.FirstKey)
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
		s := scope{d.Scope.Group, d.Scope.Partition}
		scopes[s] = append(scopes[s], d)
	}
	var chosen []v2part.Descriptor
	var best uint64
	var bestScope scope
	for s, parts := range scopes {
		slices.SortFunc(parts, seqCompare)
		for i := range parts {
			var total, largest uint64
			// A window never spans a key generation change (name keys, then ID
			// keys): one merge writes one generation, and adjacency is kept.
			byID := descriptorByID(parts[i])
			for j := i; j < len(parts) && (full || j-i < sealedMergeFanIn) && parts[j].Bytes <= limit-total && descriptorByID(parts[j]) == byID; j++ {
				total += parts[j].Bytes
				largest = max(largest, parts[j].Bytes)
				if full {
					// Force the first maximal adjacent window that fits the cap.
					if j > i && (j+1 == len(parts) || parts[j+1].Bytes > limit-total || descriptorByID(parts[j+1]) != byID) {
						return slices.Clone(parts[i : j+1]), nil
					}
					continue
				}
				if j == i || 2*total < mergeRatio(total)*largest {
					continue
				}
				if uint64(j-i+1) < mergeRatio(total) && !o.mergeRangeOld(parts[i:j+1]) {
					continue
				}
				if chosen == nil || total < best || total == best && (s.group < bestScope.group || s.group == bestScope.group && s.partition < bestScope.partition) {
					chosen, best, bestScope = parts[i:j+1], total, s
				}
			}
		}
	}
	return slices.Clone(chosen), nil
}

// Fresh parts wait for a full batch. Once every input has waited two seal
// intervals, only the size balance remains, as in ClickHouse's age-lowered
// merge base. File age affects scheduling only; no persisted format changes.
func (o *coreOwner) mergeRangeOld(parts []v2part.Descriptor) bool {
	interval := o.cfg.SealInterval
	if interval == 0 {
		interval = 10 * time.Second
	}
	before := time.Now().Add(-2 * interval)
	for _, d := range parts {
		info, err := os.Stat(filepath.Join(o.dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst"))
		if err != nil || info.ModTime().After(before) {
			return false
		}
	}
	return true
}

// seqCompare orders one owner's parts of a scope by seal: Seq, then, for
// parts from producers without Seq (all zero), MinOrder, then digest.
func seqCompare(a, b v2part.Descriptor) int {
	if c := cmp.Compare(a.Source.Seq, b.Source.Seq); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Source.MinOrder, b.Source.MinOrder); c != 0 {
		return c
	}
	return bytes.Compare(a.SHA256[:], b.SHA256[:])
}

// ClickHouse merge_with_ttl_timeout: at most one TTL rewrite per owner per
// four hours, so expiry work stays a small, bounded share of merge service.
const ttlRewriteSeconds = 4 * 3600

// When no size-tier merge is due, rewrite the oldest local part of its scope
// whose earliest data expiry has passed (ClickHouse TTL merge): only a prefix
// merge drops expired records. Parts whose latest expiry has passed are
// dropped whole by GC instead and are not rewritten.
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
		k, err := v2part.DecodeKey(d.FirstKey)
		if err != nil {
			return nil, err
		}
		origin, err := readPartOrigin(snapshot, k)
		if err != nil {
			return nil, err
		}
		if origin.Class == LivePart && o.oldestInScope([]v2part.Descriptor{d}) {
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
	reader *sstable.Reader
	cursor *headerCursor
	// key is the current metric's identity with Metric left empty: the name
	// stays encoded in ident, the validated key bytes before the archive, and
	// ident[name:] is the GCP3 length-prefixed name or the GCI2 u32 ID.
	// Merges compare and copy these bytes; they never build a name string per
	// key. tail is the key generation's tail length after ident.
	key         v2part.Key
	ident       []byte
	name        int
	tail        int
	part        v2part.PartID
	seq         uint64
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
	encoded := c.cursor.Key()
	k, name, err := decodeMergeKey(encoded, c.key.Group)
	if err != nil {
		return false, err
	}
	c.tail = partKeyTail(k.ByID)
	c.ident = append(c.ident[:0], encoded[:len(encoded)-c.tail]...)
	// GCI2 decodes no name: ident[name:] is then the u32 ID.
	c.name = len(c.ident) - len(name) - 4
	c.key = k
	c.key.Part = c.part
	c.key.Archive, c.key.BlockStart, c.key.Ordinal = 0, 0, 0
	for {
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
		encoded = c.cursor.Key()
		if len(encoded) == len(c.ident)+c.tail && bytes.Equal(encoded[:len(c.ident)], c.ident) {
			setKeyTail(&k, encoded[len(c.ident):])
			continue
		}
		if k, name, err = decodeMergeKey(encoded, c.key.Group); err != nil {
			return false, err
		}
		if k.VShard != c.key.VShard || k.ByID != c.key.ByID || k.Local != c.key.Local || !bytes.Equal(name, c.ident[c.name+4:]) {
			return false, errors.New("merge metric lacks metadata")
		}
	}
}

// metricName is the current metric's encoded length-prefixed name.
func (c *mergeCursor) metricName() []byte { return c.ident[c.name:] }

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
	// orders the remaining identity by vshard, name byte length, then name bytes:
	// the encoded big-endian length followed by the name compares the same way.
	// GCI2 orders it by vshard, then ID: the big-endian u32 ID compares so.
	a, b := h[i], h[j]
	if a.key.VShard != b.key.VShard {
		return a.key.VShard < b.key.VShard
	}
	return bytes.Compare(a.metricName(), b.metricName()) < 0
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

// mergePartID draws a merge output's part ID. Byte differentials pin it.
var mergePartID = func(id *v2part.PartID) error {
	_, err := rand.Read(id[:])
	return err
}

// Merge same-source local originals with the shared timestamp/class/order rule.
// Resolved coarse records are copied, never aggregated. Publication and GC stay
// outside this streaming rewrite and retain the ordinary snapshot fences.
// retire is the owner's age-retirement clock (retireClock): a record is dropped
// only when its expiry precedes it, as whole-part retirement would, so a merge
// never removes a record the reader still returns.
func mergePartFiles(ctx context.Context, dir, path string, inputs []v2part.Descriptor, retire int64, stop <-chan struct{}) (v2part.Descriptor, error) {
	return mergeParts(ctx, dir, path, inputs, retire, 0, true, stop, nil, nil)
}

// mergeParts is mergePartFiles; prefix says the inputs hold their scope's
// oldest part, so every version up to their newest Seq: only such a
// merge writes windows down and drops the versions they carry (mergeVersions).
// dict and identities resolve ID-keyed inputs through the store dictionary.
func mergeParts(ctx context.Context, dir, path string, inputs []v2part.Descriptor, retire, bound int64, prefix bool, stop <-chan struct{}, dict *v2dict.Dict, identities *PolicyCatalog, excluded ...map[string]bool) (_ v2part.Descriptor, err error) {
	generation, err := mergeGeneration(inputs)
	if err != nil {
		return v2part.Descriptor{}, err
	}
	names := mergeNames{dict: dict, identities: identities}

	spec := v2part.Spec{Scope: inputs[0].Scope, Source: inputs[0].Source, MinClock: inputs[0].Coverage.MinClock, MinTime: inputs[0].Coverage.MinTime, MaxTime: inputs[0].Coverage.MaxTime}
	if err = mergePartID(&spec.Scope.Part); err != nil {
		return v2part.Descriptor{}, err
	}
	seen := map[struct {
		archive uint32
		digest  v2part.Digest
	}]bool{}
	for _, d := range inputs {
		spec.MinClock = min(spec.MinClock, d.Coverage.MinClock)
		spec.MinTime = min(spec.MinTime, d.Coverage.MinTime)
		spec.MaxTime = max(spec.MaxTime, d.Coverage.MaxTime)
		spec.Source.MinOrder = min(spec.Source.MinOrder, d.Source.MinOrder)
		spec.Source.MaxOrder = max(spec.Source.MaxOrder, d.Source.MaxOrder)
		spec.Source.Seq = max(spec.Source.Seq, d.Source.Seq)
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
	if generation != 0 {
		if err = writer.IDKeyed(generation); err != nil {
			return v2part.Descriptor{}, err
		}
	}
	writer.PredictOrders()
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

		c := &mergeCursor{reader: reader, cursor: newHeaderCursor(&tableCursor{it: it}, nil), part: spec.Scope.Part, seq: d.Source.Seq}
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
	var versions mergeVersions
	// A prefix merge that drops expired records writes their windows down
	// first, also for names without versions: the TTL write-down
	// (ClickHouse TTL merge), so a coarse window outlives its raw records.
	for _, d := range inputs {
		versions.ttl = versions.ttl || prefix && d.Coverage.MinExpiry != 0 && d.Coverage.MinExpiry < retire
	}
	var key, metaBytes []byte
	var config sealrollup.Config
	var expiry expiryBounds
	// nextMetric moves every member to its next metric.
	nextMetric := func() error {
		for _, input := range members {
			valid, e := input.readMetric(false)
			if e != nil {
				return e
			}
			if valid {
				heap.Push(&h, input)
			}
		}
		return nil
	}
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
		for len(h) > 0 && h[0].key.VShard == c.key.VShard && bytes.Equal(h[0].metricName(), c.metricName()) {
			members = append(members, heap.Pop(&h).(*mergeCursor))
		}
		if live, e := names.live(c.key, c.metadata.ID); e != nil {
			return v2part.Descriptor{}, e
		} else if !live {
			if err = nextMetric(); err != nil {
				return v2part.Descriptor{}, err
			}
			continue
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
		lifecycle := compactMetadata(*metadata)
		namePrefix := prefix
		if len(excluded) > 0 && excluded[0] != nil {
			name, e := names.definitionName(c.key, c.metricName()[4:])
			if e != nil {
				return v2part.Descriptor{}, e
			}
			if excluded[0][string(name)] {
				namePrefix = false
			}
		}
		// Only a prefix can write windows down before dropping their inputs.
		// No later event can read a record expired before the oldest outside
		// admission. Legacy parts retain the conservative per-name clock.
		expireAt := int64(math.MinInt64)
		if namePrefix {
			expireAt = retire
			if bound != 0 && spec.MinClock != 0 {
				expireAt = min(expireAt, bound)
			} else if metadata.LastUpdate > 0 {
				expireAt = min(expireAt, metadata.LastUpdate/1e9)
			}
		}
		// The output key is the input's validated identity with the output part:
		// Encode(c.key) byte for byte, without a name string or route re-hash.
		key = append(append(key[:0], c.ident...), make([]byte, c.tail)...)
		copy(key[c.name-16:c.name], c.part[:])
		tail := key[len(key)-c.tail:]
		setSlot := func(archive uint32, slot int64) { putKeyTail(tail, archive, slot) }
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
		versioned, e := versions.merge(members, config, metadata, expireAt, namePrefix)
		if e != nil {
			return v2part.Descriptor{}, e
		}
		// The plain stream resolves one Raw winner per slot. Buffered versions
		// must certify the complete name, including output run boundaries.
		summary := namePrefix && summaryPolicy(config, metadata)
		for _, member := range members {
			summary = summary && member.seq != 0
		}
		for i, r := range versioned {
			summary = summary && r.Kind == sealrollup.KindRaw && r.Upto == 0 && (i == 0 || mergeSlotLess(versioned[i-1], r))
		}
		orders := v2part.PredictedOrders
		if summary {
			orders = v2part.SummaryOrders
		}
		if err = writer.SetOrderEncoding(orders); err != nil {
			return v2part.Descriptor{}, err
		}
		for i := 0; i < len(versioned); {
			j := i + 1
			for j < len(versioned) && versioned[j].Archive == versioned[i].Archive {
				j++
			}
			for _, r := range versioned[i:j] {
				expiry.data(rollupRecordExpiry(config, r))
			}
			setSlot(uint32(versioned[i].Archive), versioned[i].Slot)
			if err = writer.AppendVersions(key, versioned[i:j]); err != nil {
				return v2part.Descriptor{}, err
			}
			i = j
		}
		records = records[:0]
		for _, input := range members {
			if versioned != nil {
				break
			}
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
			// A whole non-overlapping run skips per-record winner selection. Durable
			// reset/expiry masks must permit every slot; ties always use the slow path.
			// The writer coalesces it with adjacent runs of the same name and archive
			// up to MaxDenseSlots; only a full run keeps its exact dense encoding.
			last := input.records[len(input.records)-1]
			untouched := input.next == 0 && len(input.records) >= 16 && !metadata.Expired && (!occupied || mergeSlotLess(winner, r)) && (len(records) == 0 || mergeSlotLess(last, records[0].record()))
			if untouched {
				for _, item := range input.records {
					if lifecycle.beforeReset(input.seq, item.Order) || rollupRecordExpiry(config, item) < expireAt {
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
				if err = writer.AppendSlots(key, input.arena[b.start:b.end], input.records); err != nil {
					return v2part.Descriptor{}, err
				}
				input.next = len(input.records) - 1
			} else if !metadata.Expired && !lifecycle.beforeReset(input.seq, r.Order) && rollupRecordExpiry(config, r) >= expireAt {
				if occupied && (winner.Archive != r.Archive || winner.Slot != r.Slot) {
					if err = flush(); err != nil {
						return v2part.Descriptor{}, err
					}
				}
				r.Sample.Sequence = input.seq
				newer := !occupied
				if occupied {
					newer, err = newerOrigin(partOrigin{Class: LivePart}, r, partOrigin{Class: LivePart}, winner)
					if err != nil {
						return v2part.Descriptor{}, err
					}
					if !newer && input.seq == winner.Sample.Sequence && r.Sample.Time == winner.Sample.Time && r.Order == winner.Order && r.Delta != winner.Delta {
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
				name, e := names.definitionName(c.key, c.metricName()[4:])
				if e != nil {
					return v2part.Descriptor{}, e
				}
				if full.Policy, err = appendRollupDefinition(nil, name, c.key.Group, full.Frontier.Policy); err != nil {
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
		if err = nextMetric(); err != nil {
			return v2part.Descriptor{}, err
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

func (o *coreOwner) mergeSealedParts(stop <-chan struct{}, full bool) (bool, error) {
	o.mergeMu.Lock()
	defer o.mergeMu.Unlock()
	select {
	case <-stop:
		return false, nil
	default:
	}
	// Capture the pin and inventory under the same GC fence as publications.
	o.gc.mu.Lock()
	snapshot := o.db.NewSnapshot()
	moveEpoch := o.gc.moveEpoch
	o.gc.mu.Unlock()
	defer snapshot.Close()
	limit, configured, err := effectiveMergeLimit(o.dir)
	if err != nil {
		return false, err
	}
	inputs, err := o.selectMergeLimit(snapshot, limit, full)
	if err != nil {
		return false, err
	}
	retire := o.retireClock()
	if len(inputs) == 0 && limit < configured {
		// Disk-bound: tell "nothing due" from "due but no room".
		if due, e := o.selectMergeLimit(snapshot, configured, full); e == nil && len(due) > 0 {
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
		inputs, err = o.selectTTLRewrite(snapshot, retire, limit)
		if err != nil || len(inputs) == 0 {
			return false, err
		}
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
	var dict *v2dict.Dict
	var identities *PolicyCatalog
	if o.rollups != nil {
		identities = o.rollups.identities
		dict = identities.partDict()
	}
	bound, err := o.mergeAdmissionBound(snapshot, inputs, retire)
	if err != nil {
		return false, err
	}
	var excluded map[string]bool
	if moveEpoch != 0 {
		index, e := NewRollupSnapshot(context.Background(), snapshot, nil, identities, nil, 0)
		if e != nil {
			return false, e
		}
		excluded = map[string]bool{}
		for name, m := range index.metrics {
			if index.moves[m] != nil && index.moves[m].copied {
				excluded[name] = true
			}
		}
	}
	// Copied parts are outside the local merge inventory. Until they join its
	// lifecycle, retain this name's dependencies and do no partial write-down.
	d, err := mergeParts(context.Background(), o.dir, path, inputs, retire, bound, o.oldestInScope(inputs), stop, dict, identities, excluded)

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
	changed, err := o.gc.replaceMerged(inputs, path, d, moveEpoch)
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
// The output is linked and synced before the binding fence (g.mu): the caller's
// fileMu already excludes unlink, so publications wait only for the commit.
func (g *sealedPartGC) replaceMerged(inputs []v2part.Descriptor, path string, d v2part.Descriptor, moveEpoch uint64) (bool, error) {
	added, _, err := retainSealedPart(filepath.Dir(g.dir), path, d)
	if err != nil {
		return false, err
	}
	if g.stage != nil {
		g.stage("merge-retained")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.moveEpoch != moveEpoch {
		// Attachment changed which streams this merge must include. Keep the
		// original dependencies; the retained proposal is an ordinary orphan.
		g.pending[d.SHA256] = retiredPart{at: g.now()}
		g.pendingCount.Store(uint64(len(g.pending)))
		return false, nil
	}
	for _, old := range inputs {
		value, closer, err := g.db.Get(partBinding(sealedPartPrefix, old.SHA256))
		if errors.Is(err, pebble.ErrNotFound) {
			// The unpublished output is an orphan, as after a crash here.
			g.pending[d.SHA256] = retiredPart{at: g.now()}
			g.pendingCount.Store(uint64(len(g.pending)))
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

// oldestInScope reports whether inputs, Seq-adjacent parts of one scope,
// hold that scope's oldest part: no part of the scope from the owner's source
// sorts before them (seqCompare). Parts from another source (moves, imports)
// carry their windows as materialized values and are outside the merge's view.
func (o *coreOwner) oldestInScope(inputs []v2part.Descriptor) bool {
	first := inputs[0]
	for _, d := range inputs {
		if seqCompare(d, first) < 0 {
			first = d
		}
	}
	o.gc.mu.Lock()
	inventory := o.gc.inventory
	o.gc.mu.Unlock()
	for _, value := range inventory.bindings {
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			return false
		}
		if d.Scope.Group == inputs[0].Scope.Group && d.Scope.Partition == inputs[0].Scope.Partition && d.Source.Incarnation == o.partSource.Incarnation && d.Source.Stream == o.partSource.Stream && seqCompare(d, first) < 0 {
			return false
		}
	}
	return true
}

// mergeVersions merges one name whose inputs carry record versions: a run
// with kinds (versions, ghosts, windows), or a physical slot more than one
// input holds, or, in a prefix merge, an expired record. A prefix merge that drops a record (a superseded version, a
// ghost or an expired record) first writes windows down
// (sealrollup.WriteDown); any other merge keeps every version, expired ones
// too, so a later prefix merge still sees them. Rollups stay a pure function
// of the stored records. Each slot keeps the winner the per-slot readers chose
// before as its last Raw or seal Mat record, so their renders do not change;
// written-down windows are visible to the version reader only. The work is a
// walk of the inputs in slot order; the write-down runs only for names that
// drop records. Scratch is reused across names.
type mergeVersions struct {
	records, block, out []sealrollup.Record
	from                []int   // records[i]'s input
	order               []int32 // records by (archive, slot), then index
	versions            []sealrollup.Version
	view                []int // records the write-down sees (live incarnation)
	keep, gone          []bool
	win, last           []bool // records[i], out[i]: its slot's per-slot winner
	spans               []mergeSpan
	ttl                 bool // a prefix merge whose inputs hold expired records
}

// mergeSpan is one input run's archive and physical slot range.
type mergeSpan struct {
	archive     uint32
	first, last int64
}

// spanBefore reports that run a ends before run b starts.
func spanBefore(a, b mergeSpan) bool {
	return a.archive < b.archive || a.archive == b.archive && a.last < b.first
}

// merge returns the name's output records sorted by (archive, slot), or nil
// when no input carries versions and a prefix merge drops no expired record
// (the per-slot merge then runs unchanged).
func (m *mergeVersions) merge(members []*mergeCursor, config sealrollup.Config, metadata *RollupMetadata, retire int64, prefix bool) ([]sealrollup.Record, error) {
	// Most names hold no versions: runs without kinds whose slots no other
	// run holds. Their headers and slot columns say so without decoding.
	m.spans = m.spans[:0]
	plain, sorted := true, true
	for _, c := range members {
		for _, b := range c.blocks {
			if v2part.HasOrderSummary(c.arena[b.start:b.end]) && (len(config.Archives) != 1 || metadata.Version < 4 && metadata.ResetOrder != 0) {
				return nil, errors.New("order summary under history policy or legacy reset")
			}
			if !plain {
				continue
			}
			last, ok := v2part.PlainRunSpan(b.slot, c.arena[b.start:b.end])
			if plain = ok; !plain {
				continue
			}
			s := mergeSpan{b.archive, b.slot, last}
			if n := len(m.spans); n > 0 && !spanBefore(m.spans[n-1], s) {
				sorted = false
			}
			m.spans = append(m.spans, s)
		}
	}
	if plain && !sorted {
		slices.SortFunc(m.spans, func(a, b mergeSpan) int {
			if a.archive != b.archive {
				return cmp.Compare(a.archive, b.archive)
			}
			return cmp.Compare(a.first, b.first)
		})
		for i := 1; plain && i < len(m.spans); i++ {
			plain = spanBefore(m.spans[i-1], m.spans[i])
		}
	}
	if plain && !m.ttl {
		return nil, nil
	}
	m.records, m.from = m.records[:0], m.from[:0]
	for mi, c := range members {
		for _, b := range c.blocks {
			var err error
			if m.block, err = v2part.DecodeRecordsInto(m.block[:0], b.archive, b.slot, c.arena[b.start:b.end]); err != nil {
				return nil, err
			}
			for i := range m.block {
				if m.block[i].Archive >= len(config.Archives) {
					return nil, errors.New("merge archive outside policy")
				}
				m.block[i].Sample.Sequence = c.seq
				m.from = append(m.from, mi)
			}
			m.records = append(m.records, m.block...)
		}
	}
	return m.resolve(config, metadata, retire, prefix)
}

// resolve is merge over the decoded records of every input (records, from),
// each input's records in (archive, slot) order, a slot's versions oldest
// first.
func (m *mergeVersions) resolve(config sealrollup.Config, metadata *RollupMetadata, retire int64, prefix bool) ([]sealrollup.Record, error) {
	recs := m.records
	lifecycle := compactMetadata(*metadata)
	// Slot order: the inputs' records, stably merged by (archive, slot). Most
	// names arrive already in order (one input, or inputs of disjoint slots).
	m.order = m.order[:0]
	versioned, sorted := false, true
	for i := range recs {
		if recs[i].Kind != sealrollup.KindRaw {
			versioned = true
		}
		if i > 0 && !mergeSlotLess(recs[i-1], recs[i]) {
			sorted = false
		}
		m.order = append(m.order, int32(i))
	}
	if !sorted {
		slices.SortFunc(m.order, func(a, b int32) int {
			x, y := &recs[a], &recs[b]
			switch {
			case x.Archive != y.Archive:
				return x.Archive - y.Archive
			case x.Slot != y.Slot:
				return cmp.Compare(x.Slot, y.Slot)
			}
			return int(a - b)
		})
	}
	dead := func(i int) bool {
		return metadata.Expired || lifecycle.beforeReset(recs[i].Sample.Sequence, recs[i].Order)
	}
	expired := func(r *sealrollup.Record) bool { return rollupRecordExpiry(config, *r) < retire }
	// One pass over each slot's records: the per-slot winner, as the per-slot
	// merge picks it (of each input's per-slot view, its last Raw or seal Mat
	// record of a slot, the live record with the greatest time, sequence, then order),
	// and whether a prefix merge drops a record, which needs the write-down.
	m.win = append(m.win[:0], make([]bool, len(recs))...)
	drops, expiring := false, false
	for g := 0; g < len(m.order); {
		h := g + 1
		first := &recs[m.order[g]]
		for h < len(m.order) && recs[m.order[h]].Archive == first.Archive && recs[m.order[h]].Slot == first.Slot {
			h++
		}
		versioned = versioned || h-g > 1
		w, live := -1, 0
		for _, i := range m.order[g:h] {
			r := &recs[i]
			if dead(int(i)) {
				continue
			}
			live++
			ar := config.Archives[r.Archive]
			if r.Kind == sealrollup.KindGhost || expired(r) || r.Slot != r.Sample.Time/ar.Step%ar.Count {
				drops = true
			}
			expiring = expiring || expired(r)
			if r.Kind != sealrollup.KindRaw && (r.Kind != sealrollup.KindMat || r.Upto != 0) || expired(r) {
				continue
			}
			if w < 0 || m.from[w] == m.from[i] {
				w = int(i)
				continue
			}
			b := &recs[w]
			newer, err := newerOrigin(partOrigin{Class: LivePart}, *r, partOrigin{Class: LivePart}, *b)
			if err != nil {
				return nil, err
			}
			if !newer && r.Sample.Sequence == b.Sample.Sequence && r.Sample.Time == b.Sample.Time && r.Order == b.Order && r.Delta != b.Delta {
				return nil, errors.New("conflicting merge replacement order")
			}
			if newer {
				w = int(i)
			}
		}
		if live > 1 {
			drops = true
		}
		if w >= 0 {
			m.win[w] = true
		}
		g = h
	}
	if !versioned && !(prefix && expiring) {
		return nil, nil
	}
	m.keep = append(m.keep[:0], make([]bool, len(recs))...)
	var mats []sealrollup.Version
	switch {
	case prefix && drops:
		// The write-down sees the live incarnation; a reset deletes the rest.
		m.versions, m.view, m.gone = m.versions[:0], m.view[:0], m.gone[:0]
		for i := range recs {
			if r := &recs[i]; !dead(i) {
				v := r.Version()
				v.Seq, v.Class = r.Sample.Sequence, LivePart
				m.versions = append(m.versions, v)
				m.view = append(m.view, i)
				m.gone = append(m.gone, expired(r))
			}
		}
		var keep []bool
		keep, mats = sealrollup.WriteDown(config, m.versions, func(i int) bool { return m.gone[i] })
		for j, i := range m.view {
			m.keep[i] = keep[j]
		}
	default:
		// Without a drop the write-down keeps every live version and writes
		// nothing down.
		for i := range recs {
			m.keep[i] = !dead(i) && !expired(&recs[i])
		}
	}
	// Emit in slot order, each slot's winner last.
	m.out, m.last = m.out[:0], m.last[:0]
	for g := 0; g < len(m.order); {
		h, w := g, -1
		for ; h < len(m.order) && recs[m.order[h]].Archive == recs[m.order[g]].Archive && recs[m.order[h]].Slot == recs[m.order[g]].Slot; h++ {
			if i := m.order[h]; m.win[i] {
				w = int(i)
			} else if m.keep[i] {
				m.out = append(m.out, recs[i])
				m.last = append(m.last, false)
			}
		}
		if w >= 0 {
			m.out = append(m.out, recs[w])
			m.last = append(m.last, true)
		}
		g = h
	}
	if len(mats) == 0 {
		return m.out, nil
	}
	var seq uint64
	for _, r := range recs {
		seq = max(seq, r.Sample.Sequence)
	}
	for _, v := range mats {
		if r := v.Record(config); !metadata.Expired && !lifecycle.beforeReset(seq, r.Order) && !expired(&r) {
			m.out = append(m.out, r)
			m.last = append(m.last, false)
		}
	}
	// Sort the windows in: by (archive, slot), the per-slot winner last in its slot.
	sort.Stable(versionOrder{m.out, m.last})
	return m.out, nil
}

type versionOrder struct {
	recs []sealrollup.Record
	last []bool
}

func (v versionOrder) Len() int { return len(v.recs) }
func (v versionOrder) Less(i, j int) bool {
	a, b := v.recs[i], v.recs[j]
	if a.Archive != b.Archive || a.Slot != b.Slot {
		return mergeSlotLess(a, b)
	}
	return !v.last[i] && v.last[j]
}
func (v versionOrder) Swap(i, j int) {
	v.recs[i], v.recs[j] = v.recs[j], v.recs[i]
	v.last[i], v.last[j] = v.last[j], v.last[i]
}

// mergeAdmissionBound excludes the inputs, then bounds expiry by every live
// local part in their scope. Missing clocks force the legacy per-name bound.
func (o *coreOwner) mergeAdmissionBound(snapshot *pebble.Snapshot, inputs []v2part.Descriptor, retire int64) (int64, error) {
	o.gc.mu.Lock()
	inventory := o.gc.inventory
	o.gc.mu.Unlock()
	bound := retire
	for _, value := range inventory.bindings {
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			return 0, err
		}
		if slices.ContainsFunc(inputs, func(input v2part.Descriptor) bool { return input.SHA256 == d.SHA256 }) {
			continue
		}
		if d.Scope.Group != inputs[0].Scope.Group || d.Scope.Partition != inputs[0].Scope.Partition || d.Source.Incarnation != o.partSource.Incarnation || d.Source.Stream != o.partSource.Stream {
			continue
		}
		k, err := v2part.DecodeKey(d.FirstKey)
		if err != nil {
			return 0, err
		}
		origin, err := readPartOrigin(snapshot, k)
		if err != nil {
			return 0, err
		}
		if origin.Class == LivePart {
			bound = min(bound, d.Coverage.MinClock)
		}
	}
	return bound, nil
}
