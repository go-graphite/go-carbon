package v2core

// The durable owner engine is an offline experiment. Callers own canonical ID
// allocation and the durable input log. An ID is an identity, never a name hash;
// only routing hashes the ID, so hash collisions cannot combine metric values.
import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"golang.org/x/sync/errgroup"
)

var coreMetaKey = []byte{0xff, 'f'}

type coreConfig struct {
	// SealInterval is a runtime threshold, independent of the persisted format.
	SealInterval time.Duration `json:"-"`
	// PublishInterval spaces head-only pending publications; zero is one second.
	PublishInterval            time.Duration `json:"-"`
	SealMaxRows                int           `json:"-"`
	Rollups                    bool
	RollupRoutingVersion       uint8
	AppendParts                bool
	JournalIdentity            string
	Shards, Producers, MaxRows int
	Owners                     int
	PartitionSeconds           int64
	BlindBatch                 bool
	RawCodec                   string
	// DeferMerges holds background part merges until ResumeMerges or Close, so
	// crash replay does not rewrite parts it is still sealing. Runtime only.
	DeferMerges bool `json:"-"`
	// KeepBaseRows is for tests only: it writes numeric raw base rows in rollups
	// mode, which otherwise never writes them. Archive records are the only
	// render, recovery, move and merge source and crash replay uses the input
	// journal, so tests use this to build stores in the older format and to
	// assert that they stay readable. Runtime-only.
	KeepBaseRows bool `json:"-"`
	// L0 writes each publication's new applications to an L0 run file before
	// readers see them (l0.go); restart loads them with LoadL0. Runtime only.
	L0 bool `json:"-"`
}

type coreFrontier struct {
	ArchiveClock  int64 `json:",omitempty"`
	Version       int
	Config        coreConfig
	Cursors       []uint64
	NextPart      uint64
	ExpiredBefore int64
}
type corePacket struct {
	operation  func(*coreOwner) error
	rows       []row
	batches    []coreBatch
	pending    *pendingRows // rollup applications, encoded by the producer (Outbox)
	l0         []string     // L0 files these loaded batches came from (LoadL0)
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
	pending   []pendingRows
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
	batchRows                   []row
	outbox                      *Outbox // encodes rollup batches (replay, tests)
	slots                       []coreDispatch
	bytes                       atomic.Uint64
	leaseWaitNS, leaseWaitCount atomic.Uint64
	leaseWaiting                atomic.Bool
}

// The producer and consumer own write/read respectively. A byte quota limits
// queued payloads; pointer slots cover even the smallest (empty) packets.
type coreQueue struct {
	ready       *atomic.Uint64
	readyMask   uint64
	work        chan struct{}
	slots       []atomic.Pointer[corePacket]
	limit       uint64
	written     uint64
	released    atomic.Uint64
	blockedNS   atomic.Uint64
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
	if p.pending != nil {
		n += p.pending.size()
	}
	return n
}

// points counts the packet's rows, numeric or encoded.
func (p *corePacket) points() int {
	if p.pending != nil {
		return len(p.rows) + len(p.pending.rows)
	}
	return len(p.rows)
}
func (d *coreDispatch) release() {
	// Capture before publishing remaining=0: the producer may reuse d then.
	router, n := d.router, d.bytes
	if d.remaining.Add(-1) == 0 && router != nil {
		router.bytes.Add(-n)
	}
}
func (q *coreQueue) pop() *corePacket {
	p := q.slots[q.read%uint64(len(q.slots))].Swap(nil)
	if p != nil {
		q.released.Add(p.size())
		q.read++
		if q.ready != nil && q.slots[q.read%uint64(len(q.slots))].Load() != nil {
			q.ready.Or(q.readyMask)
		}
	}
	return p
}

// peek returns the next packet without consuming it. Only the owner calls it.
func (q *coreQueue) peek() *corePacket {
	return q.slots[q.read%uint64(len(q.slots))].Load()
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
	if q.ready != nil && q.ready.Or(q.readyMask)&q.readyMask == 0 {
		coreWake(q.work)
	}
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

type coreOwner struct {
	ready          []atomic.Uint64
	work           chan struct{}
	nextPoll       time.Time // execution-owner local seal deadline
	bufferedPoints atomic.Int64
	bufferedSince  atomic.Int64
	rollups        *coreRollups
	gc             *sealedPartGC
	gcDirty        atomic.Bool
	mergeStop      <-chan struct{}
	mergeMu        sync.Mutex // one merge in flight per owner (background or ForceMerge)
	readGeneration uint64     // owner-local; includes manifest-only retirement publications

	partSource  v2part.Provenance
	ttlNext     atomic.Int64 // wall seconds before the next TTL rewrite
	retireNow   func() time.Time
	maintenance bool

	stage      func(string)
	visible    func(PendingView)
	lookup     func(uint64) *ApplicationPolicy
	sealTiming func(string, time.Duration)

	failure                      atomic.Pointer[coreFailure]
	hook                         func(*pebble.Snapshot, []row) error
	batchHook                    func(PendingView, uint64) error
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
	fileBuffer                   coreFileBuffer
	compact                      coreCompactWorkspace
	sealRows                     int
	sealCalls, sealNS            atomic.Uint64
	sealing                      atomic.Bool
	// Durable seals and their point-bearing names and points (seal size).
	durableSeals, sealNames atomic.Uint64
	sealPoints              atomic.Uint64
	// expiries counts Expire callbacks that returned true, on any goroutine.
	expiries atomic.Uint64
	index    int // owner index, staggers time-based seal deadlines
	// sealPubMu orders a seal's atomic part publication against pending
	// publications. A pin taken under it either predates the part and still
	// carries the frozen batches, or includes the part (sealDurable) and omits
	// them, so no point is ever invisible or counted twice.
	sealPubMu   sync.Mutex
	sealDurable bool
	// L0 files found at open (LoadL0 consumes them), the next file number,
	// the owner goroutine's encode buffer and bytes written.
	l0Files []string
	l0Next  uint64
	l0Buf   []byte
	l0Bytes atomic.Uint64
}
type coreFailure struct{ err error }
type coreEngine struct {
	identities *PolicyCatalog
	// dict is the store name dictionary of ID-keyed parts (nil: name keys).
	dict *v2dict.Dict

	gcStop    chan struct{}
	gcDone    chan struct{}
	mergeDone chan struct{}
	mergeHold chan struct{}
	resumed   atomic.Bool
	gcFailure atomic.Pointer[coreFailure]

	owners      []*coreOwner
	routers     []coreRouter
	cfg         coreConfig
	stalls      atomic.Uint64
	streamBytes uint64
	readOnly    bool
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

func openCoreWithHooks(root string, cfg coreConfig, hooks coreHooks) (*coreEngine, error) {
	return openCoreMode(root, cfg, hooks, false)
}
func openCoreMode(root string, cfg coreConfig, hooks coreHooks, readOnly bool) (*coreEngine, error) {
	if cfg.Rollups && hooks.PlanSeal != nil {
		return nil, errors.New("core rollups already own the seal planner")
	}
	if (hooks.Expire == nil) != (hooks.ExpiryTTL == nil) {
		return nil, errors.New("expiry needs both Expire and ExpiryTTL")
	}
	if cfg.Rollups {
		if cfg.RollupRoutingVersion == 0 {
			cfg.RollupRoutingVersion = 1
		}
		if cfg.RollupRoutingVersion != 1 {
			return nil, errors.New("unknown rollup routing version")
		}
	} else if cfg.RollupRoutingVersion != 0 {
		return nil, errors.New("rollup routing requires rollups")
	}
	if _, err := mergeMaxBytes(); err != nil {
		return nil, err
	}
	mergeWorkers, err := mergeWorkerCount(int(cfg.Shards))
	if err != nil {
		return nil, err
	}
	if readOnly {
		mergeWorkers = 0
	}
	hook := hooks.Publish
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
	if cfg.Shards < 1 || cfg.Producers < 1 || cfg.MaxRows < 1 || cfg.PartitionSeconds < 1 || cfg.Owners < 0 || cfg.SealInterval < 0 || cfg.PublishInterval < 0 || cfg.SealMaxRows < 0 {
		return nil, os.ErrInvalid
	}
	if err := os.MkdirAll(root, 0750); err != nil {
		return nil, err
	}
	diskConfig := cfg
	diskConfig.Owners = 0 // execution geometry does not change routing or persisted bytes
	diskConfig.SealInterval = 0
	diskConfig.SealMaxRows = 0
	diskConfig.DeferMerges = false
	diskConfig.KeepBaseRows = false
	identities := hooks.Identities
	if identities == nil {
		identities = &PolicyCatalog{}
	}
	e := &coreEngine{identities: identities, readOnly: readOnly, cfg: cfg, routers: make([]coreRouter, cfg.Producers), streamBytes: streamBytes}
	if hooks.IDKeys && cfg.Rollups && !readOnly {
		if !v2part.IDKeys.Load() {
			return nil, errors.New("ID-keyed seals need v2part.IDKeys on")
		}
		// Seals key parts by dictionary ID; recovery below already reads them.
		// A seal owner owns the vshards v with v % Shards == owner, so one
		// dictionary shard per owner gives each owner its own append lock.
		d, err := v2dict.Open(storeDictDir(root), v2dict.Options{Shards: cfg.Shards})
		if err != nil {
			return nil, err
		}
		e.dict = d
		identities.dict.Store(d)
	}
	fail := func(err error) (*coreEngine, error) {
		e.closeDict()
		for _, o := range e.owners {
			if o == nil {
				continue
			}
			o.pub.clear()
			_ = o.db.Close()
		}
		return nil, err
	}
	var partSource v2part.Provenance
	if cfg.Rollups {
		var err error
		partSource, err = openPartSource(root, cfg.JournalIdentity, hooks.PartSource)
		if err != nil {
			return nil, err
		}
	}
	e.owners = make([]*coreOwner, cfg.Shards)
	// Limit concurrent Pebble opens, rollup reconstruction and part validation.
	// No owner/merge/GC workers start until every owner has opened successfully.
	var opening errgroup.Group
	opening.SetLimit(min(48, runtime.GOMAXPROCS(0)))
	for s := 0; s < cfg.Shards; s++ {
		opening.Go(func() error {
			dir := filepath.Join(root, fmt.Sprintf("owner-%03d", s))
			opts := &pebble.Options{ReadOnly: readOnly, MemTableSize: 16 << 20, MaxConcurrentCompactions: func() int { return 2 }}

			opts.Experimental.TableCacheShards = 1
			opts.BytesPerSync = 64 << 20
			opts.EventListener = &pebble.EventListener{
				TableCreated: func(info pebble.TableCreateInfo) {
					if hooks.Stage != nil && info.Reason == "ingesting" {
						hooks.Stage("ingest")
					}
				},
				WriteStallBegin: func(pebble.WriteStallBeginInfo) { e.stalls.Add(1) },
			}
			db, err := pebble.Open(dir, opts)
			if err != nil {
				return err
			}
			o := &coreOwner{index: s, partSource: partSource, stage: hooks.Stage, sealTiming: hooks.SealTiming, db: db, dir: dir, cfg: cfg, dead: make(chan struct{}), done: make(chan error, 1), lookup: identities.lookup}
			if hooks.VisiblePending != nil {
				o.visible = func(v PendingView) { hooks.VisiblePending(s, v) }
			} else if hooks.Visible != nil {
				o.visible = func(v PendingView) { hooks.Visible(s, v.Batches()) }
			}
			sealMaxRows := cfg.SealMaxRows
			if sealMaxRows == 0 {
				sealMaxRows = 128 << 10
			}
			o.sealRows = min(cfg.MaxRows, sealMaxRows)
			e.owners[s] = o
			if cfg.L0 && !readOnly {
				if o.l0Files, o.l0Next, err = o.openL0(); err != nil {
					return err
				}
			}
			if hook != nil {
				o.hook = func(snap *pebble.Snapshot, head []row) error { return hook(s, snap, head) }
			}
			// PublishPending hands readers the pointer-free view. The older
			// []Batch hooks decode it per publication; they serve tests only.
			publishInventory := hooks.PublishPending
			if publishInventory == nil && hooks.PublishBatchesWithInventory != nil {
				publishInventory = func(s int, pin *pebble.Snapshot, v PendingView, generation uint64, inventory *RollupInventory) error {
					return hooks.PublishBatchesWithInventory(s, pin, v.Batches(), generation, inventory)
				}
			}
			if publishInventory != nil {
				// The pin is captured with the binding set under the GC fence. A
				// head-only publication (same sealed generation and binding set)
				// pins nothing: the reader keeps the snapshot it holds for them.
				var pinned bool
				var pinnedGeneration uint64
				var pinnedInventory *RollupInventory
				o.batchHook = func(pending PendingView, generation uint64) error {
					var pin *pebble.Snapshot
					var inventory *RollupInventory
					if o.gc != nil {
						o.gc.mu.Lock()
						inventory = o.gc.inventory
					}
					if !pinned || generation != pinnedGeneration || inventory != pinnedInventory {
						pin = o.db.NewSnapshot()
					}
					if o.gc != nil {
						o.gc.mu.Unlock()
					}
					err := publishInventory(s, pin, pending, generation, inventory)
					pinned, pinnedGeneration, pinnedInventory = err == nil, generation, inventory
					return err
				}
			} else if hooks.PublishBatches != nil {
				o.batchHook = func(pending PendingView, generation uint64) error {
					return hooks.PublishBatches(s, o.db.NewSnapshot(), pending.Batches(), generation)
				}
			}
			if hooks.PlanSeal != nil {
				o.plan = func(in coreSealInput) (coreSealPlan, error) {
					// PlanSeal hooks take decoded batches; they serve tests only.
					if in.Pending.Len() != 0 {
						in.Batches, in.Pending = in.Pending.Batches(), PendingView{}
					}
					return hooks.PlanSeal(s, in)
				}
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
				return err
			}
			if (o.meta.Version != 1 && o.meta.Version != 2) || o.meta.Config != diskConfig || len(o.meta.Cursors) != cfg.Producers {
				return errors.New("owner format/config mismatch")
			}
			o.publishFrontier(o.meta)
			if cfg.Rollups && readOnly {
				value, closer, err := db.Get(rollupDirectoryKey)
				if err == nil {
					absolute, e := filepath.Abs(dir)
					if e == nil && string(value) != absolute {
						e = errors.New("relocated archive owner must be opened writable once to rebind its local directory")
					}
					e = errors.Join(e, closer.Close())
					if e != nil {
						return e
					}
				} else if !errors.Is(err, pebble.ErrNotFound) {
					return err
				}
			}
			var moveEpoch uint64
			if cfg.Rollups && !readOnly {
				absolute, err := filepath.Abs(dir)
				if err != nil {
					return err
				}
				if err = db.Set(rollupDirectoryKey, []byte(absolute), pebble.Sync); err != nil {
					return err
				}
				r, copied, err := recoverCoreRollups(db, identities)
				if err != nil {
					return err
				}
				if copied {
					moveEpoch = 1
				}
				r.parallelism = max(1, min(8, runtime.GOMAXPROCS(0)/max(1, cfg.Shards)))
				r.stage = hooks.Stage
				if expire := hooks.Expire; expire != nil {
					r.expire = func(p RollupPolicy, m RollupMetadata, now time.Time) bool {
						if !expire(p, m, now) {
							return false
						}
						o.expiries.Add(1)
						return true
					}
				}
				r.now = hooks.Now
				if r.expire != nil {
					// Resolve inactivity deadlines under the current callback before intake.
					r.setExpiryTTL(hooks.ExpiryTTL)
				}
				o.retireNow = hooks.Now
				o.maintenance = hooks.Expire != nil
				o.plan = r.planParts
				o.rollups = r
			}
			// Only uncommitted staging files remain after successful atomic ingestion.
			files, err := filepath.Glob(filepath.Join(dir, "stage-*.sst"))
			if err != nil {
				return err
			}
			for _, p := range files {
				if err = os.Remove(p); err != nil {
					return err
				}
			}
			if cfg.Rollups {
				if !readOnly {
					if err = cleanupSealedMerges(dir); err != nil {
						return err
					}
				}
				if o.gc, err = openSealedPartGC(db, dir, hooks.Stage); err != nil {
					return err
				}
				o.gc.moveEpoch = moveEpoch
			}
			for range cfg.Producers {
				// The producer lease budget bounds total admitted bytes. Let each
				// edge hold its fair share when larger credit is requested.
				queueBytes := max(uint64(coreQueueBytes), streamBytes/uint64(cfg.Shards))
				o.queues = append(o.queues, newCoreQueue(queueBytes))
			}
			o.readGeneration = o.meta.NextPart
			o.pub.publish(o.db.NewSnapshot())
			if o.batchHook != nil {
				if err := o.batchHook(PendingView{}, o.readGeneration); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := opening.Wait(); err != nil {
		return fail(err)
	}

	workers := cfg.Owners
	if workers == 0 {
		workers = cfg.Shards
	}
	workers = min(workers, cfg.Shards)
	groups := make([][]*coreOwner, workers)
	for i, o := range e.owners {
		groups[i%workers] = append(groups[i%workers], o)
	}
	// One notification per execution group; rings and readiness bits remain
	// authoritative. Initialize before any producer or owner can run.
	for _, group := range groups {
		wake := make(chan struct{}, 1)
		for _, o := range group {
			o.initQueueSignals(wake)
		}
	}
	e.gcStop, e.gcDone = make(chan struct{}), make(chan struct{})
	go e.runPartGC()
	e.mergeDone = make(chan struct{})
	if e.cfg.DeferMerges {
		e.mergeHold = make(chan struct{})
	}
	for _, o := range e.owners {
		if mergeWorkers > 0 {
			o.mergeStop = e.gcStop
		}
	}
	go e.runPartMerges(mergeWorkers)
	for _, group := range groups {
		go runCoreOwnerGroup(group)
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
	return e.submitPacketMode(producer, rows, batches, nil, cursor, seal, true)
}

func (e *coreEngine) submissionError() error {
	if e.readOnly {
		return os.ErrPermission
	}
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

// submitPacketMode fans one producer packet out to every owner: numeric rows,
// detached batches, or an outbox of rows the producer encoded per owner. An
// outbox's arrays move into the dispatch lease, and the outbox receives the
// lease's released arrays in exchange.
func (e *coreEngine) submitPacketMode(producer int, rows []row, batches []coreBatch, out *Outbox, cursor uint64, seal, waitSeal bool) error {
	if err := e.submissionError(); err != nil {
		return err
	}
	router := &e.routers[producer]
	// Rollup applications arrive only as outboxes; batches are numeric-mode.
	routeID := func(id uint64) (int, error) {
		if !e.cfg.Rollups {
			return coreShard(id, e.cfg.Shards), nil
		}
		h := e.identities.lookup(id)
		if h == nil {
			return 0, errors.New("rollup row requires original application policy")
		}
		return int(partsformat.Route(h.policy.Name)) % e.cfg.Shards, nil
	}
	streamBytes := e.streamBytes
	if streamBytes == 0 {
		streamBytes = coreStreamBytes
	}
	// Count every original packet's data, including detached apply groups.
	charge := uint64(len(e.owners))*corePacketBytes + uint64(len(rows))*uint64(unsafe.Sizeof(row{}))
	for _, b := range batches {
		charge += uint64(unsafe.Sizeof(coreBatch{})) + uint64(len(b.Policy)) + uint64(len(b.Points))*uint64(unsafe.Sizeof(row{}))
	}
	if out != nil {
		for s := range out.owners {
			out.owners[s].closePacket()
			if len(out.owners[s].rows) > e.cfg.MaxRows {
				return errors.New("producer batch exceeds owner head bound")
			}
			charge += out.owners[s].size()
		}
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
		time.Sleep(10 * time.Microsecond)
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
		owner, err := routeID(r.ID)
		if err != nil {
			return err
		}
		routes[i] = owner
		counts[owner]++
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
	for s, count := range counts {
		parts[s] = storage[:0:count]
		storage = storage[count:]
	}
	for i, r := range rows {
		s := routes[i]
		parts[s] = append(parts[s], r)
	}
	var byOwner [][]coreBatch
	var pending []pendingRows
	if out != nil {
		// The owners read these arrays until they release the lease; the
		// outbox refills the arrays the lease's previous owners released.
		if len(d.pending) != len(out.owners) {
			d.pending = make([]pendingRows, len(out.owners))
		}
		d.pending, out.owners = out.owners, d.pending
		out.reset()
		pending = d.pending
	} else if len(batches) > 0 {
		// One immutable application and point arena per submitted packet. Owners
		// retain these after releasing the dispatch lease; no caller slices or
		// reusable routing storage escape into a seal or read snapshot.
		byOwner = make([][]coreBatch, e.cfg.Shards)
		clear(counts)
		pointCount, policyBytes := 0, 0
		for _, b := range batches {
			owner, err := routeID(b.ID)
			if err != nil {
				return err
			}
			counts[owner]++
			pointCount += len(b.Points)
			policyBytes += len(b.Policy)
		}
		descriptors := make([]coreBatch, len(batches))
		pointArena := make([]row, pointCount)
		policyArena := make([]byte, policyBytes)
		offset := 0
		for owner, n := range counts {
			byOwner[owner] = descriptors[offset : offset : offset+n]
			offset += n
		}
		pointOffset, policyOffset := 0, 0
		for _, b := range batches {
			owner, err := routeID(b.ID)
			if err != nil {
				return err
			}
			n := copy(pointArena[pointOffset:], b.Points)
			b.Points = pointArena[pointOffset : pointOffset+n : pointOffset+n]
			pointOffset += n
			n = copy(policyArena[policyOffset:], b.Policy)
			b.Policy = policyArena[policyOffset : policyOffset+n : policyOffset+n]
			policyOffset += n
			byOwner[owner] = append(byOwner[owner], b)
		}
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
		if pending != nil {
			p.pending = &pending[s]
		} else if byOwner != nil {
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
			time.Sleep(10 * time.Microsecond)
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

// Queue publication sets readiness after publishing its packet. The owner
// atomically takes each word and visits only producers that can have work.
func (o *coreOwner) initQueueSignals(work chan struct{}) {
	o.work = work
	o.ready = make([]atomic.Uint64, (len(o.queues)+63)/64)
	for p, q := range o.queues {
		q.ready, q.readyMask, q.work = &o.ready[p/64], uint64(1)<<(p%64), work
		if q.slots[q.read%uint64(len(q.slots))].Load() != nil {
			q.ready.Or(q.readyMask)
		}
	}
}

func (o *coreOwner) poller() func() (bool, bool) {
	// Every rollup owner closes idle tails through the existing maintenance
	// pass, including callers without an inactivity-expiry callback.
	if o.rollups != nil {
		o.maintenance = true
	}
	if o.ready == nil {
		o.initQueueSignals(nil)
	}
	if o.lookup == nil {
		// Without a catalog no ID has a handle, as with the engine's empty
		// PolicyCatalog; pending rows and views resolve through lookup.
		o.lookup = func(uint64) *ApplicationPolicy { return nil }
	}
	headCapacity := o.cfg.MaxRows
	if o.cfg.Rollups {
		// Rollups normally seal on a time or size threshold, below the
		// hard row limit. Grow with actual intake instead of reserving the
		// maximum buffer for every owner at startup.
		headCapacity = min(headCapacity, 64<<10)
	}
	// Without base rows a rollup seal reads only the batches, which carry the
	// same points. Its head keeps the point count (MaxRows, sealRows,
	// telemetry) and the oldest time (late-point admission), not the rows.
	keepRows := !o.cfg.Rollups || o.cfg.KeepBaseRows || o.hook != nil
	if !keepRows {
		headCapacity = 0
	}
	head := make([]row, 0, headCapacity)
	headPoints, headOldest := 0, int64(math.MaxInt64)
	pending := slices.Clone(o.meta.Cursors)
	// Pending applications live as pointer-free rows (name-once S5a-2). Each
	// head is a fresh pendingHead; published views share its immutable prefix.
	var pend pendingHead
	var pendingMerge pendingCompaction
	publishInterval := o.cfg.PublishInterval
	if publishInterval == 0 {
		publishInterval = time.Second
	}
	sealInterval := o.cfg.SealInterval
	if sealInterval == 0 {
		sealInterval = 10 * time.Second
	}
	// Stagger time-based seals by owner index across one interval, so owners
	// that start together never seal in one wave. Later deadlines restart
	// from each seal, which keeps the spread. Row-triggered seals are unchanged.
	deadline := time.Now().Add(sealInterval - sealInterval*time.Duration(o.index)/time.Duration(max(1, o.cfg.Shards)))
	o.nextPoll = deadline
	maintenanceDeadline := time.Now().Add(time.Second)
	var headPublication, batchPublication time.Time
	publishedBatches := 0
	// L0: positions [0, l0Written) of pend are in the files l0Cover; l0Loaded
	// marks a head holding rows LoadL0 restored, which the next seal covers.
	l0Written, l0Loaded := 0, false
	var l0Cover []string
	// Publication already shows each batch to o.visible once, and observers
	// dedupe repeats. A seal reports only batches no publication reported,
	// unless an expiry since its head began may have dropped an already
	// reported name: then it repeats the whole set, as before, so observers
	// re-add it. So visibility work is O(new batches), not O(head) per seal.
	headExpiries := o.expiries.Load()
	sealVisible := func(sealed PendingView, reported int, since uint64) {
		if o.visible == nil {
			return
		}
		if o.expiries.Load() != since {
			reported = 0
		}
		if reported < sealed.Len() {
			o.visible(sealed.Suffix(reported))
		}
	}
	type sealResult struct {
		n   uint64
		err error
	}
	// At most one frozen head per owner seals in the background. Its pending
	// view is immutable; intake appends to a fresh pendingHead, and
	// publications show both until the part is published.
	type sealFlight struct {
		rows     []row
		points   int
		oldest   int64
		pending  PendingView
		next     coreFrontier
		done     chan sealResult
		l0       []string // L0 files this seal covers
		reported int      // pending positions [0, reported) already reached o.visible
		expiries uint64   // o.expiries when this head began
	}
	var flight *sealFlight
	var spareRows []row
	var headSince int64
	// Row-hook publication clones the head; it keeps synchronous seals.
	asyncSeal := o.hook == nil
	publishBatches := func() error {
		// L0 files precede visibility: a render never shows a row a kill -9
		// loses. Without a publication hook they still follow its cadence.
		if o.cfg.L0 && pend.n > l0Written {
			path, err := o.writeL0(pend.rawView(nil).Suffix(l0Written))
			if err != nil {
				return err
			}
			l0Cover = append(l0Cover, path)
			l0Written = pend.n
			batchPublication = time.Now()
		}
		if o.batchHook == nil {
			return nil
		}
		// submitPacketMode already detached Points and Policy. Owners never mutate
		// those arenas, and between seals they only append descriptors; a seal
		// starts a new backing array instead of reusing or clearing the old one.
		// So a full slice expression publishes an immutable prefix that readers
		// share with the owner: publication costs O(new batches), not O(head).
		batchPublication = time.Now()
		prev := publishedBatches
		publishedBatches = pend.n
		if flight != nil {
			// Pin under the seal's publication lock. Once the frozen part is
			// durable the pin includes it, so its batches leave pending and the
			// sealed view moves to the next generation in the same publication.
			// The lock also orders the new-name pass against the seal's Commit.
			o.sealPubMu.Lock()
			defer o.sealPubMu.Unlock()
		}
		o.rollups.noteNewNames(&pend, o.lookup)
		pendingMerge.poll(&pend)
		published := pend.view(o.lookup)
		generation := o.readGeneration
		if flight != nil {
			if o.sealDurable {
				generation++
			} else {
				prev += flight.pending.Len()
				published = flight.pending.concat(published)
			}
		}
		err := o.batchHook(published, generation)
		if err == nil {
			pendingMerge.start(&pend)
		}
		if err == nil && o.visible != nil && prev < published.Len() {
			o.visible(published.Suffix(prev))
		}
		return err
	}
	// completeFlight retires a background seal exactly like a synchronous one:
	// frontier, counters, then one publication whose pin includes the part and
	// whose pending set no longer carries the frozen batches.
	completeFlight := func(r sealResult) error {
		f := flight
		flight = nil
		if r.err != nil {
			return r.err
		}
		o.meta = f.next
		o.readGeneration++
		o.publishFrontier(f.next)
		if err := unlinkL0(f.l0); err != nil {
			return err
		}
		o.committed.Add(uint64(f.points))
		o.parts.Add(1)
		o.bytes.Add(r.n)
		o.bufferedPoints.Store(int64(headPoints))
		o.bufferedSince.Store(headSince)
		// The intake head is already separate; readers that still pin the
		// frozen view keep it.
		// The seal never retains its input rows; reuse them for the next head.
		spareRows = f.rows[:0]
		o.pub.publish(o.db.NewSnapshot())
		if err := publishBatches(); err != nil {
			return err
		}
		sealVisible(f.pending, f.reported, f.expiries)
		return nil
	}
	awaitFlight := func() error {
		if flight == nil {
			return nil
		}
		return completeFlight(<-flight.done)
	}
	pollFlight := func() error {
		if flight == nil {
			return nil
		}
		select {
		case r := <-flight.done:
			return completeFlight(r)
		default:
			return nil
		}
	}
	// startSeal freezes the head and seals it in the background. The owner
	// keeps admitting rows into a new head and publishing it every second.
	// Callers ensure no seal is in flight.
	startSeal := func() error {
		if slices.Equal(pending, o.meta.Cursors) && !o.maintenance && !l0Loaded {
			deadline = time.Now().Add(sealInterval)
			return nil
		}
		if pend.n > publishedBatches {
			if err := publishBatches(); err != nil {
				return err
			}
		}
		next := o.meta
		if o.maintenance {
			next.Version = 2
		}
		next.Cursors = slices.Clone(pending)
		frozen := pend.view(o.lookup)
		next.ArchiveClock = frozen.maxClock(next.ArchiveClock)
		next.NextPart++
		f := &sealFlight{rows: head, points: headPoints, oldest: headOldest, pending: frozen, next: next, done: make(chan sealResult, 1), l0: l0Cover, reported: min(publishedBatches, pend.n), expiries: headExpiries}
		pend = pendingHead{}
		publishedBatches = 0
		l0Cover, l0Written, l0Loaded = nil, 0, false
		headExpiries = o.expiries.Load()
		o.sealPubMu.Lock()
		o.sealDurable = false
		o.sealPubMu.Unlock()
		flight = f
		head = spareRows
		if head == nil {
			head = make([]row, 0, headCapacity)
		}
		spareRows = nil
		headPoints, headOldest = 0, math.MaxInt64
		headSince = 0
		deadline = time.Now().Add(sealInterval)
		work := o.work
		go func() {
			if o.stage != nil {
				o.stage("background-seal")
			}
			// The planner reads the frozen rows in place and copies samples
			// into its own pointer-free scratch.
			n, err := o.sealBatches(f.rows, f.points, f.oldest, f.pending, f.next)
			f.done <- sealResult{n: n, err: err}
			if work != nil {
				select {
				case work <- struct{}{}:
				default:
				}
			}
		}()
		return nil
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
	completions := make([]completion, 0, o.cfg.Producers)
	readyProducers := make([]int, 0, o.cfg.Producers)
	flush := func() error {
		if err := awaitFlight(); err != nil {
			return err
		}
		if slices.Equal(pending, o.meta.Cursors) && !o.maintenance && !l0Loaded {
			return nil
		}
		// Publish the last intake tail before synchronous rollup/disk work. A
		// larger seal must not postpone visibility until that work completes.
		if pend.n > publishedBatches {
			if err := publishBatches(); err != nil {
				return err
			}
		}
		next := o.meta
		if o.maintenance {
			// An older runtime cannot preserve logical reset authority. Fence it
			// through the atomically replaced owner frontier before expiry acts.
			next.Version = 2
		}
		next.Cursors = slices.Clone(pending)
		sealedPending := pend.view(o.lookup)
		next.ArchiveClock = sealedPending.maxClock(next.ArchiveClock)
		next.NextPart++
		reported, since := min(publishedBatches, pend.n), headExpiries
		n, err := o.sealBatches(head, headPoints, headOldest, sealedPending, next)
		if err != nil {
			return err
		}
		headExpiries = o.expiries.Load()
		o.meta = next
		o.readGeneration++
		o.publishFrontier(next)
		if err := unlinkL0(l0Cover); err != nil {
			return err
		}
		// Readers may still hold the published prefix of sealedPending. Never
		// overwrite or clear it; the next head appends to fresh slices.
		pend = pendingHead{}
		publishedBatches = 0
		l0Cover, l0Written, l0Loaded = nil, 0, false
		o.committed.Add(uint64(headPoints))
		o.parts.Add(1)
		o.bytes.Add(n)
		head = head[:0]
		headPoints, headOldest = 0, math.MaxInt64
		o.bufferedPoints.Store(0)
		o.bufferedSince.Store(0)
		deadline = time.Now().Add(sealInterval)
		o.pub.publish(o.db.NewSnapshot())
		if err := publishBatches(); err != nil {
			return err
		}
		sealVisible(sealedPending, reported, since)
		if o.hook != nil {
			return o.hook(o.db.NewSnapshot(), nil)
		}
		return nil
	}
	// thresholdSeal applies the size and time thresholds. A due seal never blocks on
	// the one in flight: intake continues and the next seal starts as soon as
	// the frozen part is published. A packet that would pass the hard MaxRows
	// limit stays queued until then, while the loop keeps publishing.
	thresholdSeal := func() error {
		if !asyncSeal {
			return flush()
		}
		if err := pollFlight(); err != nil {
			return err
		}
		if flight != nil {
			return nil
		}
		return startSeal()
	}
	// fullHead reports a data packet that the head cannot admit below MaxRows.
	fullHead := func(p *corePacket, producer int) bool {
		return p != nil && p.operation == nil && !p.stop && p.cursor > pending[producer] && headPoints+p.points() > o.cfg.MaxRows
	}
	return func() (bool, bool) {
		if failure == nil && flight != nil {
			failure = pollFlight()
			if failure == nil && flight == nil && (headPoints >= o.sealRows || time.Now().After(deadline)) {
				failure = startSeal()
			}
			if failure != nil {
				o.failure.CompareAndSwap(nil, &coreFailure{err: failure})
			}
		}
		if failure == nil && flight == nil && o.gcDirty.Swap(false) {
			o.readGeneration++
			o.pub.publish(o.db.NewSnapshot())
			failure = publishBatches()
			if failure == nil && o.hook != nil {
				failure = o.hook(o.db.NewSnapshot(), slices.Clone(head))
			}
		}
		if left == 0 {
			if failure == nil {
				failure = flush()
			}
			if flight != nil {
				// Never close the database under a background seal.
				<-flight.done
				flight = nil
			}
			pendingMerge.wait(&pend)
			if failure != nil {
				o.failure.CompareAndSwap(nil, &coreFailure{err: failure})
			}
			o.done <- failure
			close(o.dead)
			return true, true
		}
		found, sealPending := false, false
		readyProducers = readyProducers[:0]
		for word := range o.ready {
			for ready := o.ready[word].Swap(0); ready != 0; ready &= ready - 1 {
				readyProducers = append(readyProducers, word*64+bits.TrailingZeros64(ready))
			}
		}
		for _, producer := range readyProducers {
			q := o.queues[producer]
			if stopped[producer] {
				continue
			}
			if asyncSeal && failure == nil && flight != nil && fullHead(q.peek(), producer) {
				// Backpressure without blocking the loop: the packet stays queued
				// and its producer waits on the full queue, as for any full queue.
				// The loop keeps publishing the admitted head every second and
				// polling the flight; the seal goroutine wakes it when done.
				if failure = pollFlight(); failure != nil {
					o.failure.CompareAndSwap(nil, &coreFailure{err: failure})
				} else if flight != nil {
					q.ready.Or(q.readyMask)
					continue
				}
			}
			p := q.pop()
			if p == nil {
				continue
			}
			found = true
			if p.operation != nil {
				err := failure
				if err == nil {
					err = flush()
				}
				if err == nil {
					err = p.operation(o)
				}
				if err == nil {
					o.readGeneration++
					o.pub.publish(o.db.NewSnapshot())
					err = publishBatches()
					if err == nil && o.hook != nil {
						err = o.hook(o.db.NewSnapshot(), nil)
					}
				}
				p.ack <- err
				continue
			}
			if p.stop {
				stopped[producer] = true
				left--
				continue
			}
			if failure == nil && o.plan != nil && len(p.rows) > 0 && len(p.batches) == 0 && p.pending == nil {
				failure = errors.New("seal planner requires original apply batches")
			}
			if failure == nil && (p.cursor > pending[producer] || p.l0 != nil) {
				points := p.points()
				if headPoints+points > o.cfg.MaxRows {
					if asyncSeal {
						// The loop above left this packet queued while a flight ran;
						// the flight is published, so freeze the full head now.
						failure = awaitFlight()
						if failure == nil {
							failure = startSeal()
						}
					} else {
						failure = flush()
					}
				}
				if failure == nil {
					if headPoints == 0 && points > 0 {
						headSince = time.Now().UnixNano()
						if flight == nil {
							o.bufferedSince.Store(headSince)
						}
					}
					if keepRows {
						head = append(head, p.rows...)
						if p.pending != nil {
							for _, b := range p.pending.decodeFrom(o.lookup) {
								head = append(head, b.Points...)
							}
						}
					}
					for _, r := range p.rows {
						headOldest = min(headOldest, r.Time)
					}
					if p.pending != nil && len(p.pending.rows) > 0 {
						headOldest = min(headOldest, p.pending.oldest)
					}
					headPoints += points
					buffered := headPoints
					if flight != nil {
						buffered += flight.points
					}
					o.bufferedPoints.Store(int64(buffered))
					if p.pending != nil {
						pend.addRows(p.pending)
					} else {
						pend.add(p.batches, o.lookup)
					}
					if p.l0 != nil {
						// Loaded rows are in their files already; publish them, do
						// not write them again, and let the next seal unlink them.
						l0Cover = append(l0Cover, p.l0...)
						l0Written, l0Loaded = pend.n, true
					}
					pending[producer] = max(pending[producer], p.cursor)
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
			if failure == nil && (headPoints >= o.sealRows || time.Now().After(deadline)) {
				failure = thresholdSeal()
			}
			if failure == nil && o.hook != nil && !sealPending && time.Since(headPublication) >= time.Second {
				failure = o.hook(o.db.NewSnapshot(), slices.Clone(head))
				headPublication = time.Now()
			}
			if failure != nil {
				o.failure.CompareAndSwap(nil, &coreFailure{err: failure})
			}
			if ack != nil || holdLease {
				c := completion{ack: ack}
				if holdLease {
					c.lease = lease
				}
				completions = append(completions, c)
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
		if failure == nil && !found && o.hook != nil && len(head) > 0 && time.Since(headPublication) >= time.Second {
			failure = o.hook(o.db.NewSnapshot(), slices.Clone(head))
			headPublication = time.Now()
		}
		if failure == nil && !sealPending && pend.n > publishedBatches && time.Since(batchPublication) >= publishInterval {
			failure = publishBatches()
		}
		if failure != nil {
			o.failure.CompareAndSwap(nil, &coreFailure{err: failure})
		}
		for _, c := range completions {
			if c.ack != nil {
				c.ack <- failure
			}
			if c.lease != nil {
				c.lease.release()
			}
		}
		clear(completions)
		completions = completions[:0]
		if !found {
			if failure == nil && time.Now().After(deadline) {
				if asyncSeal {
					failure = thresholdSeal()
				} else {
					failure = flush()
					deadline = time.Now().Add(sealInterval)
				}
				if failure != nil {
					o.failure.CompareAndSwap(nil, &coreFailure{err: failure})
				}
			}
		}
		// Rollup metadata belongs to the seal planner while a seal is in flight.
		if failure == nil && flight == nil && o.maintenance && !time.Now().Before(maintenanceDeadline) {
			// Keep inactivity detection independent of the larger seal threshold.
			// The application callback requests its existing intake fence before
			// allowing expiry; only an allowed expiry or new metadata needs a seal.
			r := o.rollups
			now := time.Now()
			if r.now != nil {
				now = r.now()
			}
			due := failure == nil && ((r.expire != nil && r.expiryDue(now)) || r.pendingMetadataDue(&pend, o.lookup))
			if due {
				failure = flush()
				if failure != nil {
					o.failure.CompareAndSwap(nil, &coreFailure{err: failure})
				}
			}
			maintenanceDeadline = time.Now().Add(time.Second)
		}
		o.nextPoll = deadline
		if o.maintenance && maintenanceDeadline.Before(o.nextPoll) {
			o.nextPoll = maintenanceDeadline
		}
		if headPoints > 0 {
			if o.batchHook != nil && pend.n > publishedBatches {
				if next := batchPublication.Add(publishInterval); next.Before(o.nextPoll) {
					o.nextPoll = next
				}
			}
			if o.hook != nil {
				if next := headPublication.Add(time.Second); next.Before(o.nextPoll) {
					o.nextPoll = next
				}
			}
		}
		if flight != nil {
			// The seal goroutine wakes the execution group when it finishes; a due
			// seal waits for that wake instead of spinning on a passed deadline.
			now := time.Now()
			limit := now.Add(time.Second)
			if o.work == nil {
				limit = now.Add(time.Millisecond)
			}
			if o.nextPoll.Before(now) || limit.Before(o.nextPoll) {
				o.nextPoll = limit
			}
		}
		if failure != nil {
			o.nextPoll = time.Now().Add(time.Second)
		}
		return false, found
	}
}

// One execution owner polls several independent databases. Database count is a
// storage geometry choice, independent of producer and execution-owner counts.
func runCoreOwnerGroup(owners []*coreOwner) {
	polls := make([]func() (bool, bool), len(owners))
	for i, o := range owners {
		polls[i] = o.poller()
	}
	left := len(polls)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
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
		if !found && left > 0 {
			deadline := time.Now().Add(time.Second)
			for i, o := range owners {
				if polls[i] != nil && o.nextPoll.Before(deadline) {
					deadline = o.nextPoll
				}
			}
			timer.Reset(max(0, time.Until(deadline)))
			select {
			case <-owners[0].work:
			case <-timer.C:
			}
			timer.Stop()
		}
	}
}

// key = data-prefix / time-partition / metric-ID / archive / block-start /
// immutable-part / block-ordinal. Partition prefix permits one range deletion;
// the tuple inside a partition has the requested metric/archive/block layout.
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

// sealBatches seals points admitted rows whose earliest time is oldest.
// Without base rows the rollup planner reads pending directly and head is empty.
func (o *coreOwner) sealBatches(head []row, points int, oldest int64, pending PendingView, next coreFrontier) (uint64, error) {
	if err := o.observeSealedMergeBudget(); err != nil {
		return 0, err
	}
	// GC must not unlink a replayed original between link reuse and atomic
	// descriptor publication. This owner-local lock never reaches readers or
	// producer buffers. Retirement itself can run from the seal Commit hook.
	if o.gc != nil {
		o.gc.fileMu.Lock()
		defer o.gc.fileMu.Unlock()
	}
	sealStart := time.Now()
	planEnd := sealStart
	defer func() {
		if o.sealTiming != nil {
			end := time.Now()
			o.sealTiming("seal_total", end.Sub(sealStart))
			o.sealTiming("rollup_plan", planEnd.Sub(sealStart))
			o.sealTiming("publication", end.Sub(planEnd))
		}
	}()
	if o.stage != nil {
		o.stage("seal")
	}
	o.sealing.Store(true)
	defer func() {
		o.sealNS.Add(uint64(time.Since(sealStart)))
		o.sealCalls.Add(1)
		o.sealing.Store(false)
	}()
	var plan coreSealPlan
	var err error
	durable := false
	defer func() {
		if !durable && plan.Abort != nil {
			plan.Abort()
		}
		if durable {
			o.durableSeals.Add(1)
			o.sealNames.Add(uint64(plan.Names))
			o.sealPoints.Add(uint64(points))
		}
	}()
	if o.plan != nil {
		plan, err = o.plan(coreSealInput{Rows: head, Pending: pending, Next: next})
		if err != nil {
			return 0, err
		}
		if err = validateCoreSealPlan(plan); err != nil {
			return 0, err
		}
		if plan.ReplaceRaw && points > 0 && len(plan.Files) == 0 {
			return 0, errors.New("raw replacement requires output for nonempty input")
		}
	}
	planEnd = time.Now()
	rollupWrites := o.startRollupPartWrites(plan, next.NextPart)
	defer rollupWrites.cleanup()
	phaseStart := time.Now()
	omitRaw := plan.ReplaceRaw || (o.cfg.Rollups && !o.cfg.KeepBaseRows)
	if omitRaw && !plan.ReplaceRaw && points > 0 {
		// Keep the base-row admission rule without sorting or encoding rows.
		// Partitions are monotonic in time, so the oldest row decides.
		if corePartition(oldest, o.cfg.PartitionSeconds) < next.ExpiredBefore {
			return 0, errors.New("late point targets expired partition")
		}
	}
	switch {
	case omitRaw:
		// The replacement planner owns ordering and retention in its format;
		// without base rows the archive planner already consumed the input.
	case o.stage != nil:
		observed := false
		slices.SortFunc(head, func(a, b row) int {
			if !observed {
				observed = true
				o.stage("sort")
			}
			pa, pb := corePartition(a.Time, o.cfg.PartitionSeconds), corePartition(b.Time, o.cfg.PartitionSeconds)
			if pa < pb {
				return -1
			}
			if pa > pb {
				return 1
			}
			return less(a, b)
		})
	default:
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
	var paths, stagedPaths []string
	var total uint64
	stage := func(suffix string) (*sstable.Writer, string, error) {
		path := filepath.Join(o.dir, fmt.Sprintf("stage-%020d-%s.sst", o.meta.NextPart, suffix))
		f, err := vfs.Default.Create(path)
		if err != nil {
			return nil, "", err
		}
		paths = append(paths, path)
		stagedPaths = append(stagedPaths, path)
		return sstable.NewWriter(objstorageprovider.NewFileWritable(o.stageFile(f)), sstable.WriterOptions{Compression: sstable.SnappyCompression}), path, nil
	}
	defer func() {
		for _, p := range stagedPaths {
			_ = os.Remove(p)
		}
	}()
	var path string
	if !omitRaw && (!o.cfg.BlindBatch || o.cfg.Rollups) && len(head) > 0 {
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
	for i := 0; !omitRaw && i < len(head); {
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
	if o.cfg.BlindBatch && !o.cfg.Rollups {
		for _, file := range plan.Files {
			for _, kv := range file {
				if err = batch.Set(kv.Key, kv.encodedValue(), nil); err != nil {
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
		o.sealPubMu.Lock()
		err = batch.Commit(pebble.Sync)
		o.sealDurable = err == nil
		if err == nil && plan.Commit != nil {
			plan.Commit() // installs under the lock the new-name pass holds
		}
		o.sealPubMu.Unlock()
		if err != nil {
			return 0, err
		}
		durable = true
		return n, nil
	}
	var descriptors []coreKV
	var retained []string
	defer func() {
		if !durable {
			for _, p := range retained {
				_ = os.Remove(p)
			}
		}
	}()
	retainPart := func(i int) error {
		<-rollupWrites.done
		result := rollupWrites.results[i]
		xp, d, e := result.path, result.descriptor, result.err
		if e != nil {
			return e
		}
		// The shared writer closes and syncs its SST exactly once before return.
		o.syncCalls.Add(1)
		kv, saved, e := retainSealedPart(o.dir, xp, d)
		retained = append(retained, saved...)
		if e != nil {
			return e
		}
		descriptors = append(descriptors, kv)
		total += d.Bytes
		if o.stage != nil {
			o.stage("part-retained")
		}
		return nil
	}
	for i := range plan.Parts {
		if e := retainPart(len(plan.Files) + i); e != nil {
			return 0, e
		}
	}
	for i, file := range plan.Files {
		if isRollupFile(file) {
			if e := retainPart(i); e != nil {
				return 0, e
			}
			continue
		}
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
	if len(descriptors) > 0 {
		slices.SortFunc(descriptors, func(a, b coreKV) int { return bytes.Compare(a.Key, b.Key) })
		w, _, e := stage("parts")
		if e != nil {
			return 0, e
		}
		for _, kv := range descriptors {
			if e = w.Set(kv.Key, kv.Value); e != nil {
				break
			}
		}
		e = errors.Join(e, w.Close())
		if e != nil {
			return 0, e
		}
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
	o.sealPubMu.Lock()
	if o.gc != nil {
		o.gc.mu.Lock()
	}
	err = o.db.Ingest(paths)
	if o.gc != nil {
		if err == nil {
			o.gc.inventory = o.gc.inventory.with(descriptors, nil)
		}
		o.gc.mu.Unlock()
	}
	o.sealDurable = err == nil
	if err == nil && plan.Commit != nil {
		plan.Commit() // installs under the lock the new-name pass holds
	}
	o.sealPubMu.Unlock()
	if err != nil {
		return 0, err
	}
	durable = true
	if o.stage != nil {
		o.stage("commit")
	}
	return total, nil
}

func (e *coreEngine) close() error {
	close(e.gcStop)
	<-e.gcDone
	<-e.mergeDone
	// Forced merges can outlive HTTP shutdown. Drain each owner's merge before
	// closing its database; queued merges observe gcStop under the same lock.
	for _, o := range e.owners {
		o.mergeMu.Lock()
		o.mergeMu.Unlock()
	}
	var err error
	if f := e.gcFailure.Load(); f != nil {
		err = f.err
	}
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
	return errors.Join(err, e.closeDict())
}

// closeDict closes the store dictionary after the owners stopped sealing.
func (e *coreEngine) closeDict() error {
	if e.dict == nil {
		return nil
	}
	e.identities.dict.CompareAndSwap(e.dict, nil)
	d := e.dict
	e.dict = nil
	return d.Close()
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
