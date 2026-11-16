package v2core

// The durable owner engine is an offline experiment. Callers own canonical ID
// allocation and the durable input log. An ID is an identity, never a name hash;
// only routing hashes the ID, so hash collisions cannot combine metric values.
import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

var coreMetaKey = []byte{0xff, 'f'}

type coreConfig struct {
	Rollups                    bool
	AppendParts                bool
	JournalIdentity            string
	Shards, Producers, MaxRows int
	Owners                     int
	PartitionSeconds           int64
	BlindBatch                 bool
	RawCodec                   string
}
type coreFrontier struct {
	Version       int
	Config        coreConfig
	Cursors       []uint64
	NextPart      uint64
	ExpiredBefore int64
}
type corePacket struct {
	rows       []row
	batches    []coreBatch
	cursor     uint64
	seal, stop bool
	ack        chan error
	lease      *coreDispatch
}

// Each producer owns a bounded ring. Owners release a lease after their last
// access; the producer cannot overwrite a slice still in use by any owner.
type coreDispatch struct {
	remaining atomic.Int32
	storage   []row
	parts     [][]row
	counts    []int
	routes    []int
	waitSince []time.Time
	packets   []corePacket
	router    *coreRouter
	bytes     uint64
}

const (
	coreQueueBytes  = 256 << 10
	coreStreamBytes = 8 << 20
	corePacketBytes = uint64(unsafe.Sizeof(corePacket{}))
)

type coreRouter struct {
	slots                       []coreDispatch
	bytes                       atomic.Uint64
	leaseWaitNS, leaseWaitCount atomic.Uint64
	leaseWaiting                atomic.Bool
	wake                        chan struct{}
}

// The producer and consumer own write/read respectively. A byte quota limits
// queued payloads; pointer slots cover even the smallest (empty) packets.
type coreQueue struct {
	slots       []atomic.Pointer[corePacket]
	limit       uint64
	written     uint64
	released    atomic.Uint64
	blockedNS   atomic.Uint64
	wake        chan struct{}
	write, read uint64
}

func newCoreQueue(limit uint64) *coreQueue {
	return &coreQueue{limit: limit, slots: make([]atomic.Pointer[corePacket], max(1, limit/corePacketBytes))}
}
func (p *corePacket) size() uint64 {
	n := corePacketBytes + uint64(len(p.rows))*uint64(unsafe.Sizeof(row{}))
	for _, b := range p.batches {
		n += uint64(unsafe.Sizeof(coreBatch{})) + uint64(len(b.Policy)) + uint64(len(b.Points))*uint64(unsafe.Sizeof(row{}))
	}
	return n
}
func (d *coreDispatch) release() {
	// Capture before publishing remaining=0: the producer may reuse d then.
	router, n := d.router, d.bytes
	if d.remaining.Add(-1) == 0 && router != nil {
		router.bytes.Add(-n)
		coreWake(router.wake)
	}
}
func (q *coreQueue) pop() *corePacket {
	p := q.slots[q.read%uint64(len(q.slots))].Swap(nil)
	if p != nil {
		q.released.Add(p.size())
		q.read++
		coreWake(q.wake)
	}
	return p
}
func (q *coreQueue) tryPush(p *corePacket) bool {
	n := p.size()
	used := q.written - q.released.Load()
	// An indivisible original group may exceed the quota, but only by itself.
	if used != 0 && (n > q.limit || used > q.limit-n) {
		return false
	}
	if !q.slots[q.write%uint64(len(q.slots))].CompareAndSwap(nil, p) {
		return false
	}
	q.written += n
	q.write++
	return true
}
func (q *coreQueue) push(p *corePacket, dead <-chan struct{}) error {
	var blocked time.Time
	for !q.tryPush(p) {
		if blocked.IsZero() {
			blocked = time.Now()
		}
		select {
		case <-dead:
			return errors.New("owner stopped")
		default:
		}
		time.Sleep(10 * time.Microsecond)
	}
	if !blocked.IsZero() {
		q.blockedNS.Add(uint64(time.Since(blocked)))
	}
	return nil
}

// Notifications are hints: the byte/lease counters remain the source of truth.
func coreWake(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}
func coreWait(wake chan struct{}) {
	if wake == nil {
		time.Sleep(10 * time.Microsecond)
		return
	}
	// Retry periodically to observe owner failure even without a release.
	timer := time.NewTimer(time.Millisecond)
	select {
	case <-wake:
	case <-timer.C:
	}
	timer.Stop()
}

type coreOwner struct {
	failure                      atomic.Pointer[coreFailure]
	hook                         func(*pebble.Snapshot, []row) error
	batchHook                    func(*pebble.Snapshot, []coreBatch, uint64) error
	plan                         func(coreSealInput) (coreSealPlan, error)
	frontier                     atomic.Pointer[coreFrontier]
	db                           *pebble.DB
	dir                          string
	cfg                          coreConfig
	meta                         coreFrontier
	queues                       []*coreQueue
	dead                         chan struct{}
	done                         chan error
	pub                          publication
	committed, parts, bytes      atomic.Uint64
	sortNS, encodeNS, ingestNS   atomic.Uint64
	syncNS, syncMaxNS, syncCalls atomic.Uint64
	preallocateNS, fileWriteNS   atomic.Uint64
	fileBuffer                   coreFileBuffer
	bucketSort                   bool
	radixSort                    bool
	sortScratch                  []row
	radixWork                    []row
	compact                      coreCompactWorkspace
	sealRows                     int
	sealCalls, sealNS            atomic.Uint64
	sealing                      atomic.Bool
	trace                        bool
}
type coreFailure struct{ err error }
type coreEngine struct {
	owners       []*coreOwner
	routers      []coreRouter
	cfg          coreConfig
	stalls       atomic.Uint64
	streamBytes  uint64
	indexScatter bool
	notifyWait   bool
}

// Hash only the canonical numeric ID, with stable distribution independent of
// admission order. The original ID remains in every key.
func coreShard(id uint64, n int) int {
	id ^= id >> 30
	id *= 0xbf58476d1ce4e5b9
	id ^= id >> 27
	id *= 0x94d049bb133111eb
	id ^= id >> 31
	return int(id % uint64(n))
}

// The optional hook owns its snapshot and receives a detached immutable head.
// It runs on the owner; callers must bound retained bytes and release snapshots.
func openCore(root string, cfg coreConfig) (*coreEngine, error) {
	return openCoreWithHooks(root, cfg, coreHooks{})
}
func openCoreWithHook(root string, cfg coreConfig, hook func(int, *pebble.Snapshot, []row) error) (*coreEngine, error) {
	return openCoreWithHooks(root, cfg, coreHooks{Publish: hook})
}
func openCoreWithHooks(root string, cfg coreConfig, hooks coreHooks) (*coreEngine, error) {
	if cfg.Rollups && hooks.PlanSeal != nil {
		return nil, errors.New("core rollups already own the seal planner")
	}
	hook := hooks.Publish
	if cfg.RawCodec == "" {
		cfg.RawCodec = os.Getenv("CORE_RAW_CODEC")
	}
	if cfg.RawCodec == "" {
		cfg.RawCodec = "run1"
	} else if cfg.RawCodec == "legacy" {
		// Preserve the original on-disk config for explicit legacy reopen.
		cfg.RawCodec = ""
	}
	if cfg.RawCodec != "" && cfg.RawCodec != "run1" {
		return nil, errors.New("unknown raw codec")
	}
	streamBytes := uint64(coreStreamBytes)
	if value := os.Getenv("CORE_STREAM_BYTES"); value != "" {
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil || n < corePacketBytes || n > 1<<30 {
			return nil, errors.New("CORE_STREAM_BYTES must be bytes between packet size and 1 GiB")
		}
		streamBytes = n
	}
	if cfg.Shards < 1 || cfg.Producers < 1 || cfg.MaxRows < 1 || cfg.PartitionSeconds < 1 || cfg.Owners < 0 {
		return nil, os.ErrInvalid
	}
	sealPhase := 0
	if value := os.Getenv("CORE_SEAL_PHASE"); value != "" {
		var err error
		sealPhase, err = strconv.Atoi(value)
		if err != nil || sealPhase < 0 || sealPhase >= 1024 {
			return nil, errors.New("CORE_SEAL_PHASE must be between 0 and 1023")
		}
	}
	trace := os.Getenv("CORE_TRACE_PHASES") == "1"
	if err := os.MkdirAll(root, 0750); err != nil {
		return nil, err
	}
	diskConfig := cfg
	diskConfig.Owners = 0 // execution geometry does not change routing or persisted bytes
	e := &coreEngine{cfg: cfg, routers: make([]coreRouter, cfg.Producers), streamBytes: streamBytes, indexScatter: os.Getenv("CORE_INDEX_SCATTER") == "1", notifyWait: os.Getenv("CORE_NOTIFY_WAIT") == "1"}
	fail := func(err error) (*coreEngine, error) {
		for _, o := range e.owners {
			_ = o.db.Close()
		}
		return nil, err
	}
	for s := 0; s < cfg.Shards; s++ {
		dir := filepath.Join(root, fmt.Sprintf("owner-%03d", s))
		opts := &pebble.Options{MemTableSize: 16 << 20, MaxConcurrentCompactions: func() int { return 2 }}
		opts.Experimental.TableCacheShards = 1
		opts.BytesPerSync = 64 << 20
		opts.EventListener = &pebble.EventListener{
			WriteStallBegin: func(pebble.WriteStallBeginInfo) { e.stalls.Add(1) },
			CompactionBegin: func(info pebble.CompactionInfo) { coreTrace(trace, "compaction_begin", dir, info.JobID) },
			CompactionEnd:   func(info pebble.CompactionInfo) { coreTrace(trace, "compaction_end", dir, info.JobID) },
		}
		db, err := pebble.Open(dir, opts)
		if err != nil {
			return fail(err)
		}
		o := &coreOwner{db: db, dir: dir, cfg: cfg, dead: make(chan struct{}), done: make(chan error, 1), trace: trace}
		o.sealRows = cfg.MaxRows
		if os.Getenv("CORE_STAGGER_SEAL") == "1" {
			// Include the process phase: one DB per process must stagger too.
			// This changes execution boundaries, never the persisted format.
			phase := (sealPhase + 1024*s/cfg.Shards) % 1024
			o.sealRows -= (cfg.MaxRows / 4) * phase / 1024
		}
		o.fileBuffer.direct = os.Getenv("CORE_SST_DIRECT") == "1"
		o.fileBuffer.preallocate = os.Getenv("CORE_SST_PREALLOCATE") == "1" || o.fileBuffer.direct
		o.bucketSort = os.Getenv("CORE_BUCKET_SORT") == "1"
		o.radixSort = os.Getenv("CORE_RADIX_SORT") == "1"
		e.owners = append(e.owners, o)
		if hook != nil {
			o.hook = func(snap *pebble.Snapshot, head []row) error { return hook(s, snap, head) }
		}
		if hooks.PublishBatches != nil {
			o.batchHook = func(snap *pebble.Snapshot, batches []coreBatch, generation uint64) error {
				return hooks.PublishBatches(s, snap, batches, generation)
			}
		}
		if hooks.PlanSeal != nil {
			o.plan = func(in coreSealInput) (coreSealPlan, error) { return hooks.PlanSeal(s, in) }
		}
		o.meta = coreFrontier{Version: 1, Config: diskConfig, Cursors: make([]uint64, cfg.Producers), ExpiredBefore: -1 << 63}
		b, c, err := db.Get(coreMetaKey)
		if err == nil {
			err = json.Unmarshal(b, &o.meta)
			_ = c.Close()
		} else if errors.Is(err, pebble.ErrNotFound) {
			err = nil
		}
		if err != nil {
			return fail(err)
		}
		if o.meta.Version != 1 || o.meta.Config != diskConfig || len(o.meta.Cursors) != cfg.Producers {
			return fail(errors.New("owner format/config mismatch"))
		}
		o.publishFrontier(o.meta)
		if cfg.Rollups {
			r, err := recoverCoreRollups(db)
			if err != nil {
				return fail(err)
			}
			o.plan = r.plan
		}
		// Only uncommitted staging files remain after successful atomic ingestion.
		files, err := filepath.Glob(filepath.Join(dir, "stage-*.sst"))
		if err != nil {
			return fail(err)
		}
		for _, p := range files {
			if err = os.Remove(p); err != nil {
				return fail(err)
			}
		}
		for range cfg.Producers {
			// The producer lease budget bounds total admitted bytes. Let each
			// edge hold its fair share when larger credit is requested.
			queueBytes := max(uint64(coreQueueBytes), streamBytes/uint64(cfg.Shards))
			o.queues = append(o.queues, newCoreQueue(queueBytes))
		}
	}
	workers := cfg.Owners
	if workers == 0 {
		workers = cfg.Shards
	}
	workers = min(workers, cfg.Shards)
	groups := make([][]*coreOwner, workers)
	if e.notifyWait {
		for p := range e.routers {
			wake := make(chan struct{}, 1)
			e.routers[p].wake = wake
			for _, o := range e.owners {
				o.queues[p].wake = wake
			}
		}
	}
	for i, o := range e.owners {
		o.pub.publish(o.db.NewSnapshot())
		if o.batchHook != nil {
			if err := o.batchHook(o.db.NewSnapshot(), nil, o.meta.NextPart); err != nil {
				return fail(err)
			}
		}
		groups[i%workers] = append(groups[i%workers], o)
	}
	for i, group := range groups {
		go runCoreOwnerGroup(group, hooks.OwnerCPUs, i)
	}
	return e, nil
}

// submit is called serially by each producer. Rows become owner-owned copies.
// Cursor is a durable log position in that producer's stream and strictly
// increases per batch. Broadcasting empty batches advances idle owners too.
func (e *coreEngine) submit(producer int, rows []row, cursor uint64, seal bool) error {
	if producer < 0 || producer >= e.cfg.Producers || cursor == 0 {
		return os.ErrInvalid
	}
	return e.submitPacket(producer, rows, nil, cursor, seal)
}
func (e *coreEngine) submitPacket(producer int, rows []row, batches []coreBatch, cursor uint64, seal bool) error {
	return e.submitPacketMode(producer, rows, batches, cursor, seal, true)
}

func (e *coreEngine) submissionError() error {
	for _, o := range e.owners {
		if failure := o.failure.Load(); failure != nil {
			return failure.err
		}
		select {
		case <-o.dead:
			return errors.New("owner stopped")
		default:
		}
	}
	return nil
}

func (e *coreEngine) submitPacketMode(producer int, rows []row, batches []coreBatch, cursor uint64, seal, waitSeal bool) error {
	if err := e.submissionError(); err != nil {
		return err
	}
	router := &e.routers[producer]
	streamBytes := e.streamBytes
	if streamBytes == 0 {
		streamBytes = coreStreamBytes
	}
	// Count every original packet's data, including detached apply groups.
	charge := uint64(len(e.owners))*corePacketBytes + uint64(len(rows))*uint64(unsafe.Sizeof(row{}))
	for _, b := range batches {
		charge += uint64(unsafe.Sizeof(coreBatch{})) + uint64(len(b.Policy)) + uint64(len(b.Points))*uint64(unsafe.Sizeof(row{}))
	}
	if len(router.slots) == 0 {
		router.slots = make([]coreDispatch, max(1, streamBytes/(uint64(len(e.owners))*corePacketBytes)))
	}
	var d *coreDispatch
	var waitStart time.Time
	defer func() {
		if !waitStart.IsZero() {
			router.leaseWaitNS.Add(uint64(time.Since(waitStart)))
			router.leaseWaiting.Store(false)
		}
	}()
	for d == nil {
		used := router.bytes.Load()
		if used == 0 || (charge <= streamBytes && used <= streamBytes-charge) {
			// Reuse free leases first, rather than allocating a fresh payload on
			// every lap of a large ring. No slow lease blocks unrelated free ones.
			for i := range router.slots {
				if router.slots[i].remaining.Load() == 0 {
					d = &router.slots[i]
					break
				}
			}
		}
		if d != nil {
			break
		}
		if waitStart.IsZero() {
			waitStart = time.Now()
			router.leaseWaitCount.Add(1)
			router.leaseWaiting.Store(true)
		}
		for _, o := range e.owners {
			if failure := o.failure.Load(); failure != nil {
				return failure.err
			}
			select {
			case <-o.dead:
				return errors.New("owner stopped during buffer reuse")
			default:
			}
		}
		coreWait(router.wake)
	}
	if !waitStart.IsZero() {
		router.leaseWaitNS.Add(uint64(time.Since(waitStart)))
		router.leaseWaiting.Store(false)
		waitStart = time.Time{}
	}
	if len(d.parts) == 0 {
		d.parts = make([][]row, e.cfg.Shards)
		d.counts = make([]int, e.cfg.Shards)
		d.waitSince = make([]time.Time, e.cfg.Shards)
		d.packets = make([]corePacket, e.cfg.Shards)
	}
	parts, counts := d.parts, d.counts
	clear(counts)
	if cap(d.routes) < len(rows) {
		d.routes = make([]int, len(rows))
	}
	routes := d.routes[:len(rows)]
	for i, r := range rows {
		s := coreShard(r.ID, e.cfg.Shards)
		routes[i] = s
		counts[s]++
	}
	for _, count := range counts {
		if count > e.cfg.MaxRows {
			return errors.New("producer batch exceeds owner head bound")
		}
	}
	// The reusable backing slice is split into disjoint full-capacity regions.
	// The lease prevents reuse until every owner has finished reading its packet.
	if cap(d.storage) < len(rows) {
		d.storage = make([]row, len(rows))
	}
	storage := d.storage[:len(rows)]
	if e.indexScatter {
		offset := 0
		for s, count := range counts {
			parts[s] = storage[offset : offset+count : offset+count]
			counts[s] = offset
			offset += count
		}
		for i := range rows {
			s := routes[i]
			storage[counts[s]] = rows[i]
			counts[s]++
		}
	} else {
		for s, count := range counts {
			parts[s] = storage[:0:count]
			storage = storage[count:]
		}
		for i, r := range rows {
			s := routes[i]
			parts[s] = append(parts[s], r)
		}
	}
	var byOwner [][]coreBatch
	if len(batches) > 0 {
		byOwner = make([][]coreBatch, e.cfg.Shards)
	}
	for _, b := range batches {
		owner := coreShard(b.ID, e.cfg.Shards)
		b.Points = slices.Clone(b.Points)
		b.Policy = slices.Clone(b.Policy)
		byOwner[owner] = append(byOwner[owner], b)
	}
	var acks []chan error
	if seal && waitSeal {
		acks = make([]chan error, len(e.owners))
	}
	d.router, d.bytes = router, charge
	router.bytes.Add(charge)
	d.remaining.Store(int32(len(e.owners)))
	for s := range e.owners {
		p := &d.packets[s]
		*p = corePacket{rows: parts[s], cursor: cursor, seal: seal, lease: d}
		if byOwner != nil {
			p.batches = byOwner[s]
		}
		if seal && waitSeal {
			p.ack = make(chan error, 1)
			acks[s] = p.ack
		}
		counts[s] = 1 // routing counts are no longer needed; track unsent packets
	}
	// Never wait for one owner's I/O before offering packets to ready owners.
	// Each producer finishes this fanout before its next batch, preserving FIFO.
	left := len(e.owners)
	for left > 0 {
		progress := false
		s := producer % len(e.owners)
		for range e.owners {
			if counts[s] != 0 {
				o := e.owners[s]
				if o.queues[producer].tryPush(&d.packets[s]) {
					if !d.waitSince[s].IsZero() {
						o.queues[producer].blockedNS.Add(uint64(time.Since(d.waitSince[s])))
						d.waitSince[s] = time.Time{}
					}
					counts[s] = 0
					left--
					progress = true
				} else {
					if d.waitSince[s].IsZero() {
						d.waitSince[s] = time.Now()
					}
					select {
					case <-o.dead:
						return errors.New("owner stopped during fanout")
					default:
					}
				}
			}
			s++
			if s == len(e.owners) {
				s = 0
			}
		}
		if left > 0 && !progress {
			coreWait(router.wake)
		}
	}

	for s, ack := range acks {
		select {
		case err := <-ack:
			if err != nil {
				return err
			}
		case <-e.owners[s].dead:
			return errors.New("owner stopped during seal")
		}
	}
	return nil
}
func (o *coreOwner) poller() func() (bool, bool) {
	head := make([]row, 0, o.cfg.MaxRows)
	pending := slices.Clone(o.meta.Cursors)
	var batches []coreBatch
	deadline := time.Now().Add(time.Second)
	var headPublication time.Time
	publishBatches := func() error {
		if o.batchHook == nil {
			return nil
		}
		// submitPacketMode already detached Points and Policy. Owners never mutate
		// those arenas; only the descriptor slice grows. Retain their immutable
		// storage instead of copying the full head for every publication.
		return o.batchHook(o.db.NewSnapshot(), slices.Clone(batches), o.meta.NextPart)
	}
	stopped := make([]bool, o.cfg.Producers)
	left := len(stopped)
	var failure error
	// One poll consumes at most one packet per producer. These are completion
	// handles only: rows remain in the existing bounded head and dispatch ring.
	type completion struct {
		ack   chan error
		lease *coreDispatch
	}
	completions := make([]completion, o.cfg.Producers)
	flush := func() error {
		if slices.Equal(pending, o.meta.Cursors) {
			return nil
		}
		next := o.meta
		next.Cursors = slices.Clone(pending)
		next.NextPart++
		n, err := o.sealBatches(head, batches, next)
		if err != nil {
			return err
		}
		o.meta = next
		o.publishFrontier(next)
		batches = nil
		o.committed.Add(uint64(len(head)))
		o.parts.Add(1)
		o.bytes.Add(n)
		head = head[:0]
		deadline = time.Now().Add(time.Second)
		o.pub.publish(o.db.NewSnapshot())
		if err := publishBatches(); err != nil {
			return err
		}
		if o.hook != nil {
			return o.hook(o.db.NewSnapshot(), nil)
		}
		return nil
	}
	return func() (bool, bool) {
		if left == 0 {
			if failure == nil {
				failure = flush()
			}
			if failure != nil {
				o.failure.CompareAndSwap(nil, &coreFailure{err: failure})
			}
			o.done <- failure
			close(o.dead)
			return true, true
		}
		found, sealPending := false, false
		for producer, q := range o.queues {
			if stopped[producer] {
				continue
			}
			p := q.pop()
			if p == nil {
				continue
			}
			found = true
			if p.stop {
				stopped[producer] = true
				left--
				continue
			}
			if failure == nil && o.plan != nil && len(p.rows) > 0 && len(p.batches) == 0 {
				failure = errors.New("seal planner requires original apply batches")
			}
			if failure == nil && p.cursor > pending[producer] {
				if len(head)+len(p.rows) > o.cfg.MaxRows {
					failure = flush()
				}
				if failure == nil {
					head = append(head, p.rows...)
					batches = append(batches, p.batches...)
					pending[producer] = p.cursor
				}
			}
			// The head owns copied rows and detached batch descriptors now.
			// Capture everything still needed before returning the packet: its
			// producer can immediately reuse and overwrite the leased storage.
			seal, ack, lease := p.seal, p.ack, p.lease
			// Pipelined seals keep their existing dispatch credit until durable
			// completion. There is no unbounded receipt or second work queue.
			holdLease := seal && ack == nil
			if lease != nil && !holdLease {
				lease.release()
			}
			sealPending = sealPending || seal
			if failure == nil && (len(head) >= o.sealRows || time.Now().After(deadline)) {
				failure = flush()
			}
			if failure == nil && o.hook != nil && !sealPending && time.Since(headPublication) >= time.Second {
				failure = o.hook(o.db.NewSnapshot(), slices.Clone(head))
				headPublication = time.Now()
			}
			if failure != nil {
				o.failure.CompareAndSwap(nil, &coreFailure{err: failure})
			}
			completions[producer].ack = ack
			if holdLease {
				completions[producer].lease = lease
			}
		}
		// Concurrent producers may have a seal ready in the same poll. Ingest
		// their shared head once before acknowledging any of them. Per-stream
		// FIFO, original apply batches and the dispatch byte budget are unchanged.
		if failure == nil && sealPending {
			failure = flush()
			if failure == nil && o.hook != nil {
				failure = o.hook(o.db.NewSnapshot(), slices.Clone(head))
				headPublication = time.Now()
			}
		}
		if failure == nil && found && !sealPending {
			failure = publishBatches()
		}
		if failure != nil {
			o.failure.CompareAndSwap(nil, &coreFailure{err: failure})
		}
		for i, c := range completions {
			if c.ack != nil {
				c.ack <- failure
			}
			if c.lease != nil {
				c.lease.release()
			}
			completions[i] = completion{}
		}
		if !found {
			if failure == nil && time.Now().After(deadline) {
				failure = flush()
				if failure != nil {
					o.failure.CompareAndSwap(nil, &coreFailure{err: failure})
				}
				deadline = time.Now().Add(time.Second)
			}
		}
		return false, found
	}
}

// One execution owner polls several independent databases. Database count is a
// storage geometry choice, independent of producer and execution-owner counts.
func runCoreOwnerGroup(owners []*coreOwner, cpus []int, index int) {
	unpin, err := coreAffinity(cpus, index)
	if err != nil {
		for _, o := range owners {
			o.done <- err
			close(o.dead)
		}
		return
	}
	defer unpin()
	polls := make([]func() (bool, bool), len(owners))
	for i, o := range owners {
		polls[i] = o.poller()
	}
	left := len(polls)
	for left > 0 {
		found := false
		for i, poll := range polls {
			if poll == nil {
				continue
			}
			done, work := poll()
			found = found || work
			if done {
				polls[i] = nil
				left--
			}
		}
		if !found {
			time.Sleep(100 * time.Microsecond)
		}
	}
}

// key = data-prefix / time-partition / metric-ID / archive / block-start /
// immutable-part / block-ordinal. Partition prefix permits one range deletion;
// the tuple inside a partition has the requested metric/archive/block layout.
func coreKey(r row, partition int64, part uint64, ordinal uint32) []byte {
	b := make([]byte, 41)
	coreKeyInto(b, r, partition, part, ordinal)
	return b
}
func coreKeyInto(b []byte, r row, partition int64, part uint64, ordinal uint32) {
	b[0] = 1
	binary.BigEndian.PutUint64(b[1:9], uint64(partition)^(1<<63))
	binary.BigEndian.PutUint64(b[9:17], r.ID)
	binary.BigEndian.PutUint32(b[17:21], 0) // raw archive; authoritative rollups are separate work
	binary.BigEndian.PutUint64(b[21:29], uint64(r.Time)^(1<<63))
	binary.BigEndian.PutUint64(b[29:37], part)
	binary.BigEndian.PutUint32(b[37:], ordinal)
}
func corePartition(t, seconds int64) int64 {
	q := t / seconds
	if t%seconds < 0 {
		q--
	}
	return q
}

// A one-second head normally fits a single time partition. Avoid the two
// signed divisions per sort comparison in that measured common case; mixed
// partitions retain the exact original comparator.
func sortCoreRows(head []row, seconds int64) {
	if len(head) < 2 {
		return
	}
	low, high := head[0].Time, head[0].Time
	for _, r := range head[1:] {
		low = min(low, r.Time)
		high = max(high, r.Time)
	}
	if corePartition(low, seconds) == corePartition(high, seconds) {
		slices.SortFunc(head, less)
		return
	}
	slices.SortFunc(head, func(a, b row) int {
		pa, pb := corePartition(a.Time, seconds), corePartition(b.Time, seconds)
		if pa < pb {
			return -1
		}
		if pa > pb {
			return 1
		}
		return less(a, b)
	})
}
func (o *coreOwner) seal(head []row, next coreFrontier) (uint64, error) {
	return o.sealBatches(head, nil, next)
}
func (o *coreOwner) sealBatches(head []row, batches []coreBatch, next coreFrontier) (uint64, error) {
	sealStart := time.Now()
	coreTrace(o.trace, "seal_begin", o.dir, 0)
	o.sealing.Store(true)
	defer func() {
		o.sealNS.Add(uint64(time.Since(sealStart)))
		o.sealCalls.Add(1)
		o.sealing.Store(false)
		coreTrace(o.trace, "seal_end", o.dir, 0)
	}()
	var plan coreSealPlan
	var err error
	durable := false
	defer func() {
		if !durable && plan.Abort != nil {
			plan.Abort()
		}
	}()
	if o.plan != nil {
		plan, err = o.plan(coreSealInput{Rows: head, Batches: batches, Next: next})
		if err != nil {
			return 0, err
		}
		if err = validateCoreSealPlan(plan); err != nil {
			return 0, err
		}
		if plan.ReplaceRaw && len(head) > 0 && len(plan.Files) == 0 {
			return 0, errors.New("raw replacement requires output for nonempty input")
		}
	}
	phaseStart := time.Now()
	if plan.ReplaceRaw {
		// The replacement planner owns ordering and retention in its format.
	} else if o.radixSort {
		o.sortScratch, o.radixWork = sortCoreRowsRadix(head, o.cfg.PartitionSeconds, o.sortScratch, o.radixWork)
	} else if o.bucketSort {
		o.sortScratch = sortCoreRowsBucketed(head, o.cfg.PartitionSeconds, o.sortScratch)
	} else {
		sortCoreRows(head, o.cfg.PartitionSeconds)
	}
	o.sortNS.Add(uint64(time.Since(phaseStart)))
	phaseStart = time.Now()
	// Part-first data files are append-only within a time partition. Their
	// frontier must live in another SST: putting a global ff key in every data
	// file would make every file's range overlap the rest of the database.
	batch := o.db.NewBatch()
	defer batch.Close()
	var writer *sstable.Writer
	var paths []string
	var total uint64
	stage := func(suffix string) (*sstable.Writer, string, error) {
		path := filepath.Join(o.dir, fmt.Sprintf("stage-%020d-%s.sst", o.meta.NextPart, suffix))
		f, err := vfs.Default.Create(path)
		if err != nil {
			return nil, "", err
		}
		paths = append(paths, path)
		return sstable.NewWriter(objstorageprovider.NewFileWritable(o.stageFile(f)), sstable.WriterOptions{Compression: sstable.SnappyCompression}), path, nil
	}
	defer func() {
		for _, p := range paths {
			_ = os.Remove(p)
		}
	}()
	var path string
	if !plan.ReplaceRaw && !o.cfg.BlindBatch && len(head) > 0 {
		writer, path, err = stage("data")
		if err != nil {
			return 0, err
		}
	}
	put := func(k, v []byte) error {
		if writer != nil {
			return writer.Set(k, v)
		}
		return batch.Set(k, v, nil)
	}
	var keyScratch [41]byte
	var valueScratch [256 * 24]byte
	for i := 0; !plan.ReplaceRaw && i < len(head); {
		partition := corePartition(head[i].Time, o.cfg.PartitionSeconds)
		if partition < next.ExpiredBefore {
			if writer != nil {
				_ = writer.Close()
			}
			return 0, errors.New("late point targets expired partition")
		}
		j := i + 1
		limit := 256
		if o.cfg.RawCodec == "run1" {
			limit = coreCompactRows
		}
		for j < len(head) && j < i+limit && (o.cfg.RawCodec == "run1" || head[j].ID == head[i].ID) && corePartition(head[j].Time, o.cfg.PartitionSeconds) == partition {
			j++
		}
		var value []byte
		if o.cfg.RawCodec == "run1" {
			value, err = o.compact.encode(head[i:j])
			if err != nil {
				break
			}
		} else {
			value = valueScratch[:(j-i)*24]
			for k, r := range head[i:j] {
				b := value[k*24:]
				binary.LittleEndian.PutUint64(b, uint64(r.Time))
				binary.LittleEndian.PutUint64(b[8:], r.Bits)
				binary.LittleEndian.PutUint64(b[16:], r.Seq)
			}
		}
		key := keyScratch[:]
		coreKeyInto(key, head[i], partition, o.meta.NextPart, uint32(i))
		if o.cfg.AppendParts {
			key[0] = 2
			binary.BigEndian.PutUint64(key[9:17], o.meta.NextPart)
			binary.BigEndian.PutUint64(key[17:25], head[i].ID)
			binary.BigEndian.PutUint32(key[25:29], 0)
			binary.BigEndian.PutUint64(key[29:37], uint64(head[i].Time)^(1<<63))
		}
		if o.cfg.RawCodec == "run1" {
			offset := 17
			if o.cfg.AppendParts {
				offset = 25
			}
			binary.BigEndian.PutUint32(key[offset:offset+4], 1)
		}
		if err = put(key, value); err != nil {
			break
		}
		i = j
	}
	if writer != nil {
		err = errors.Join(err, writer.Close())
		if err != nil {
			return 0, err
		}
		st, e := os.Stat(path)
		if e != nil {
			return 0, e
		}
		total += uint64(st.Size())
	}
	meta, e := json.Marshal(next)
	err = errors.Join(err, e)
	if err != nil {
		return 0, err
	}
	if o.cfg.BlindBatch {
		for _, file := range plan.Files {
			for _, kv := range file {
				if err = batch.Set(kv.Key, kv.Value, nil); err != nil {
					return 0, err
				}
			}
		}
		if err = batch.Set(coreMetaKey, meta, nil); err != nil {
			return 0, err
		}
		n := uint64(len(batch.Repr()))
		o.encodeNS.Add(uint64(time.Since(phaseStart)))
		ingestStart := time.Now()
		defer func() { o.ingestNS.Add(uint64(time.Since(ingestStart))) }()
		if err = batch.Commit(pebble.Sync); err != nil {
			return 0, err
		}
		durable = true
		if plan.Commit != nil {
			plan.Commit()
		}
		return n, nil
	}
	for i, file := range plan.Files {
		extra, xp, e := stage(fmt.Sprintf("extra-%03d", i))
		if e != nil {
			return 0, e
		}
		for _, kv := range file {
			if e = extra.Set(kv.Key, kv.Value); e != nil {
				break
			}
		}
		e = errors.Join(e, extra.Close())
		if e != nil {
			return 0, e
		}
		st, e := os.Stat(xp)
		if e != nil {
			return 0, e
		}
		total += uint64(st.Size())
	}
	metadata, mp, err := stage("frontier")
	if err != nil {
		return 0, err
	}
	err = metadata.Set(coreMetaKey, meta)
	err = errors.Join(err, metadata.Close())
	if err != nil {
		return 0, err
	}
	st, err := os.Stat(mp)
	if err != nil {
		return 0, err
	}
	total += uint64(st.Size())
	// One manifest edit atomically publishes all disjoint data/frontier SSTs.
	// An interrupted ingestion can never advance the frontier without its data.
	o.encodeNS.Add(uint64(time.Since(phaseStart)))
	ingestStart := time.Now()
	defer func() { o.ingestNS.Add(uint64(time.Since(ingestStart))) }()
	if err = o.db.Ingest(paths); err != nil {
		return 0, err
	}
	durable = true
	if plan.Commit != nil {
		plan.Commit()
	}
	return total, nil
}

func (e *coreEngine) close() error {
	var err error
	for _, o := range e.owners {
		for _, q := range o.queues {
			err = errors.Join(err, q.push(&corePacket{stop: true}, o.dead))
		}
	}
	for _, o := range e.owners {
		err = errors.Join(err, <-o.done)
		o.pub.clear()
		err = errors.Join(err, o.db.Close())
	}
	return err
}

// globalFrontier is conservative across shards and producers. Call only while
// opening, before starting producers, or after a completed seal barrier.
func (e *coreEngine) globalFrontier() []uint64 {
	out := slices.Clone(e.owners[0].meta.Cursors)
	for _, o := range e.owners[1:] {
		for p, c := range o.meta.Cursors {
			out[p] = min(out[p], c)
		}
	}
	return out
}
func coreRead(s *pebble.Snapshot, id uint64) ([]row, error) {
	it, err := s.NewIter(&pebble.IterOptions{LowerBound: []byte{1}, UpperBound: []byte{3}})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []row
	var compact coreCompactWorkspace
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if len(k) != 41 {
			return nil, errors.New("invalid core key")
		}
		offset := 9
		if k[0] == 2 {
			offset = 17
		}
		archive := 17
		if k[0] == 2 {
			archive = 25
		}
		codec := binary.BigEndian.Uint32(k[archive : archive+4])
		if codec == 1 {
			if err := compact.visit(it.Value(), func(r row) error {
				if r.ID == id {
					out = append(out, r)
				}
				return nil
			}); err != nil {
				return nil, err
			}
			continue
		}
		if codec != 0 {
			return nil, errors.New("unknown core raw codec")
		}
		if binary.BigEndian.Uint64(k[offset:offset+8]) != id {
			continue
		}
		v := it.Value()
		if len(v)%24 != 0 {
			return nil, errors.New("invalid core block")
		}
		for len(v) > 0 {
			out = append(out, row{id, int64(binary.LittleEndian.Uint64(v)), binary.LittleEndian.Uint64(v[8:]), binary.LittleEndian.Uint64(v[16:])})
			v = v[24:]
		}
	}
	slices.SortFunc(out, less)
	return out, it.Error()
}

// Expiry is a stopped-owner operation until authoritative rollups exist. It
// atomically drops a complete logical partition range and persists its floor.
// Physical bytes are reclaimed by Pebble compaction, not immediately unlinked.
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
