// Package v2backend composes the experimental immutable-parts daemon backend.
// This is a single-origin store: import, deletion and replica ownership changes
// are deliberately unavailable until a fenced incarnation protocol exists.
package v2backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/internal/mixedread"
	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/partsread"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
	"golang.org/x/sync/semaphore"
)

var ErrClosed = errors.New("v2 backend closed")

type frame struct {
	name     string
	resolved *resolvedMetric
	group    partsformat.Group
	policy   daemonstore.Metric
	clock    int64
	cursor   uint64
	order    uint64
	points   []points.Point
	payload  []byte
}
type metricState struct {
	Name     []byte
	Group    []byte
	Policy   daemonstore.Metric
	Frontier sealrollup.Frontier
	slots    []map[int64]sealrollup.Sample
	prepared *sealrollup.Prepared
	config   sealrollup.Config
}
type checkpoint struct {
	Version        int
	Journal        string
	Cursor, Points uint64
	Order          uint64
	Metrics        []*metricState
	Revisions      map[string]uint64
}
type command struct {
	frame  *frame
	batch  []frame
	seal   []frame
	op     byte
	answer chan answer
}
type answer struct {
	frames []frame
	points []points.Point
	err    error
	live   *liveCapture
}
type owner struct {
	id             int
	s              *Store
	db             *pebble.DB
	dir            string
	requests       chan command
	admission      *semaphore.Weighted
	done           chan struct{}
	active         []frame
	pending        []frame
	coreCursor     uint64
	nextOrder      uint64
	journalEncoder receiptEncoder
	workspace      sealrollup.Workspace
	batchScratch   []sealrollup.Batch
	sampleScratch  []sealrollup.Sample
	receiptScratch []partlog.DictionaryFrame
	sealPending    bool
	commitScratch  sealScratch
	spare          []frame
	meta           checkpoint
	metrics        map[string]*metricState
	pub            partsread.Publication
	readCatalog    atomic.Pointer[catalog]
	readBase       atomic.Pointer[liveBase]
}
type Store struct {
	core                          *v2core.Engine
	coreRoutes                    []uint64
	coreCatalog                   sync.Map
	coreSequence                  atomic.Uint64
	coreReplay                    atomic.Bool
	coreViewsClosed               atomic.Bool
	opts                          daemonstore.Options
	resolved                      sync.Map
	admissionContext              context.Context
	cancelAdmission               context.CancelFunc
	beforeSeal                    func(int)
	now                           func() time.Time
	log                           *partlog.Log
	dbs                           []*pebble.DB
	owners                        []*owner
	budget                        *partsread.Budget
	legacy                        *store.Store
	legacyCatalog                 *catalog
	mixed                         *mixedread.Reader
	errors                        chan daemonstore.Failure
	failure                       atomic.Pointer[daemonstore.Failure]
	closing                       atomic.Bool
	closed                        atomic.Bool
	flushMu                       sync.Mutex
	closeOnce                     sync.Once
	closeErr                      error
	stop, done                    chan struct{}
	watchStop, watchDone          chan struct{}
	ingested, parts, stalls, debt atomic.Uint64
	sealNS                        atomic.Int64
	pins                          atomic.Int64
}

var _ daemonstore.Backend = (*Store)(nil)

func Open(opts daemonstore.Options) (daemonstore.Backend, error) { return openAt(opts, time.Now) }

func openAt(opts daemonstore.Options, now func() time.Time) (_ *Store, err error) {
	if opts.WriteEngine != "" && opts.WriteEngine != "standalone" && opts.WriteEngine != "core" {
		return nil, errors.New("unknown v2 write engine")
	}
	if opts.WriteEngine == "core" && opts.Owners != opts.Shards {
		return nil, errors.New("core write engine requires owners equal shards")
	}
	if err := validJournalCodec(opts.JournalCodec); err != nil {
		return nil, err
	}
	if opts.DataDir == "" || opts.Owners < 1 || opts.Shards < 1 || opts.PartInterval <= 0 || opts.Policy == nil {
		return nil, errors.New("invalid v2 backend options")
	}
	if err := os.MkdirAll(opts.DataDir, 0750); err != nil {
		return nil, err
	}
	s := &Store{opts: opts, now: now, errors: make(chan daemonstore.Failure, 1),
		budget: partsread.NewBudget(256 << 20), stop: make(chan struct{}), done: make(chan struct{}),
		watchStop: make(chan struct{}), watchDone: make(chan struct{})}
	s.admissionContext, s.cancelAdmission = context.WithCancel(context.Background())
	defer func() {
		if err != nil {
			s.abortOpen()
		}
	}()
	// Each database's LOCK fences simultaneous processes before journal recovery.
	for i := 0; opts.WriteEngine != "core" && i < opts.Shards; i++ {
		dir := filepath.Join(opts.DataDir, fmt.Sprintf("db-%04d", i))
		db, e := pebble.Open(dir, &pebble.Options{MemTableSize: 16 << 20, MaxConcurrentCompactions: func() int { return 2 },
			EventListener: &pebble.EventListener{WriteStallBegin: func(pebble.WriteStallBeginInfo) { s.stalls.Add(1) }}})
		if e != nil {
			return nil, e
		}
		s.dbs = append(s.dbs, db)
	}
	// Geometry and origin are restart invariants, never inferred from local IDs.
	format := []byte(fmt.Sprintf("experimental-v2-single-origin-v1\nowners=%d\nshards=%d\n", opts.Owners, opts.Shards))
	if opts.WriteEngine == "core" {
		format = append(format, "write-engine=core-v1\n"...)
	}
	marker := filepath.Join(opts.DataDir, "V2FORMAT")
	if old, e := os.ReadFile(marker); e == nil {
		if !bytes.Equal(old, format) {
			return nil, errors.New("v2 format/geometry mismatch")
		}
	} else if errors.Is(e, os.ErrNotExist) {
		f, e := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
		if e != nil {
			return nil, e
		}
		_, e = f.Write(format)
		if e == nil {
			e = f.Sync()
		}
		e = errors.Join(e, f.Close())
		if e != nil {
			return nil, e
		}
		d, e := os.Open(opts.DataDir)
		if e != nil {
			return nil, e
		}
		e = errors.Join(d.Sync(), d.Close())
		if e != nil {
			return nil, e
		}
	} else {
		return nil, e
	}
	s.log, err = partlog.Open(filepath.Join(opts.DataDir, "journal"), partlog.Options{Streams: opts.Owners, SyncInterval: time.Second})
	if err != nil {
		return nil, err
	}
	if opts.WriteEngine == "core" {
		if err = s.openCore(); err != nil {
			return nil, err
		}
	}
	for i := 0; i < opts.Owners; i++ {
		o := &owner{id: i, s: s, db: s.dbs[i%opts.Shards], dir: filepath.Join(opts.DataDir, fmt.Sprintf("owner-%04d", i)),
			admission: semaphore.NewWeighted(256 << 20), requests: make(chan command, 64), done: make(chan struct{}), metrics: make(map[string]*metricState)}
		if e := os.MkdirAll(o.dir, 0750); e != nil {
			return nil, e
		}
		s.owners = append(s.owners, o)
		if e := o.recover(); e != nil {
			return nil, fmt.Errorf("owner %d: %w", i, e)
		}
		o.nextOrder = max(o.meta.Order, o.meta.Cursor)
		o.coreCursor = o.meta.Cursor
		if s.core != nil && s.core.DurableFrontiers()[i][i] != o.meta.Cursor {
			return nil, errors.New("core/backend atomic frontier mismatch")
		}
	}
	if err = s.openLegacy(); err != nil {
		return nil, err
	}
	if err = s.log.Sync(); err != nil {
		return nil, err
	}
	s.coreReplay.Store(true)
	// Replay only collects while the journal stream is borrowed. Syncing from a
	// replay callback would recursively wait on that same stream.
	for _, o := range s.owners {
		pendingPoints := 0
		replayCursor := o.meta.Cursor
		var decoder receiptDecoder
		defer decoder.close()
		// Definitions can precede the durable apply frontier. Decode the retained
		// epoch prefix but never reapply a checkpointed receipt.
		if e := s.log.Replay(o.id, 0, func(r partlog.Record) error {
			fs, e := decoder.decode(r.Payload)
			if e != nil {
				return e
			}
			if r.Cursor <= replayCursor {
				return nil
			}
			for _, f := range fs {
				f.cursor = r.Cursor
				if f.clock == 0 {
					f.clock = r.Clock
				}
				if s.route(f.name) != o.id {
					return errors.New("journal metric owner mismatch")
				}
				current, e := opts.Policy(f.name)
				if e != nil {
					return e
				}
				if !reflect.DeepEqual(current, f.policy) {
					return errors.New("replay policy changed")
				}
				o.nextOrder++
				f.order = o.nextOrder
				f.payload = nil
				if s.core != nil {
					f.resolved, e = s.resolve(f.name)
					if e != nil {
						return e
					}
				}
				o.active = append(o.active, f)
				pendingPoints += len(f.points)
				s.ingested.Add(uint64(len(f.points)))
			}
			if s.core != nil {
				if e := o.appendCore(o.active); e != nil {
					return e
				}
				o.active = nil
				pendingPoints = 0
			}
			// Never checkpoint a partial journal record: all its frames share Cursor.
			if pendingPoints >= 1<<20 {
				if e := o.commit(o.active); e != nil {
					return e
				}
				o.active = nil
				pendingPoints = 0
			}
			return nil
		}); e != nil {
			return nil, e
		}
	}
	if err = s.log.Sync(); err != nil {
		return nil, err
	}
	for _, o := range s.owners {
		if s.core != nil {
			if o.coreCursor > 0 {
				err = s.core.Append(o.id, nil, o.coreCursor, true)
			}
		} else {
			err = o.commit(o.active)
		}
		if err != nil {
			return nil, err
		}
		o.active = nil
		if err = o.publish(); err != nil {
			return nil, err
		}
	}
	s.coreReplay.Store(false)
	if err = s.openMixed(); err != nil {
		return nil, err
	}
	for _, o := range s.owners {
		go o.run()
	}
	go s.maintain()
	go s.watchErrors()
	return s, nil
}

func (s *Store) abortOpen() {
	s.coreViewsClosed.Store(true)
	for _, o := range s.owners {
		o.pub.Close()
		if b := o.readBase.Swap(nil); b != nil {
			b.pub.Close()
		}
	}
	if s.core != nil {
		_ = s.core.Close()
	}
	if s.log != nil {
		_ = s.log.Close()
	}
	if s.legacy != nil {
		_ = s.legacy.Close()
	}
	if s.core == nil {
		for _, db := range s.dbs {
			_ = db.Close()
		}
	}
}
func (s *Store) route(name string) int {
	return int(partsformat.Route(partsformat.ID(name))) % s.opts.Owners
}
func (s *Store) Errors() <-chan daemonstore.Failure { return s.errors }
func (s *Store) fail(op string, e error) error {
	if e == nil {
		return nil
	}
	f := &daemonstore.Failure{Operation: op, Err: e}
	if s.failure.CompareAndSwap(nil, f) {
		s.cancelAdmission()
		s.errors <- *f
	}
	return e
}
func (s *Store) status() error {
	if f := s.failure.Load(); f != nil {
		return fmt.Errorf("%s: %w", f.Operation, f.Err)
	}
	if s.closing.Load() {
		return ErrClosed
	}
	return nil
}
func (s *Store) Append(p *points.Points) error {
	if p == nil || len(p.Data) == 0 {
		return s.status()
	}
	f, e := s.makeFrame(p.Metric, p.Data, s.now().Unix())
	if e != nil {
		return e
	}
	return s.admit([][]frame{{f}}, []int{s.route(p.Metric)})
}
func (o *owner) run() {
	o.journalEncoder = newReceiptEncoder(o.s.opts.JournalCodec)
	defer close(o.done)
	defer o.journalEncoder.Close()
	var deferred *command
	var commands []command
	var frameScratch []frame
	for {
		var c command
		if deferred != nil {
			c = *deferred
			deferred = nil
		} else {
			c = <-o.requests
		}
		var a answer
		switch c.op {
		case 's':
			if o.coreCursor > 0 {
				a.err = o.s.core.Append(o.id, nil, o.coreCursor, true)
			}
		case 'f':
			a.frames = o.active
			o.active = o.spare[:0]
			o.spare = nil
			o.pending = a.frames
		case 'd':
			clear(o.pending)
			o.spare = o.pending[:0]
			if cap(o.spare) > retainedFrames {
				o.spare = nil
			}
			o.pending = nil
		case 'r':
			for _, fs := range [][]frame{o.pending, o.active} {
				for _, f := range fs {
					if f.name == c.frame.name {
						a.points = append(a.points, f.points...)
					}
				}
			}
		case 'v':
			a.live, a.err = o.captureLive(c.frame.name)
		case 'q':
			c.answer <- a
			return
		default:
			commands = append(commands[:0], c)
			fs := append(frameScratch[:0], c.batch...)
			if c.frame != nil {
				fs = append(fs, *c.frame)
			}
			wireBytes := encodedFramesSize(fs)
		drain:
			for len(fs) < 4096 {
				select {
				case next := <-o.requests:
					if next.op != 0 {
						deferred = &next
						break drain
					}
					incoming := next.batch
					if next.frame != nil {
						incoming = []frame{*next.frame}
					}
					n := encodedFramesSize(incoming) - 8
					if wireBytes+n > 8<<20 {
						deferred = &next
						break drain
					}
					wireBytes += n
					commands = append(commands, next)
					if next.frame != nil {
						fs = append(fs, *next.frame)
					} else {
						fs = append(fs, next.batch...)
					}
				default:
					break drain
				}
			}
			a.err = o.s.status()
			if a.err == nil {
				var payload []byte
				payload, a.err = o.encodeReceipts(fs)
				var cursor uint64
				if a.err == nil {
					cursor, a.err = o.s.log.Append(o.id, fs[0].clock, payload)
				}
				if a.err == nil {
					var n uint64
					for i := range fs {
						o.nextOrder++
						fs[i].order = o.nextOrder
						fs[i].cursor = cursor
						fs[i].payload = nil
						n += uint64(len(fs[i].points))
					}
					if o.s.core != nil {
						a.err = o.appendCore(fs)
					}
					if a.err == nil {
						o.active = append(o.active, fs...)
						o.s.ingested.Add(n)
					}
				}
			}
			for _, cc := range commands {
				cc.answer <- a
			}
			clear(commands)
			clear(fs)
			frameScratch = fs[:0]
			if a.err != nil && !errors.Is(a.err, ErrClosed) {
				o.s.fail("owner", a.err)
			}
			continue
		}
		if a.err != nil && !errors.Is(a.err, ErrClosed) {
			o.s.fail("owner", a.err)
		}
		c.answer <- a
	}
}
func (s *Store) flush() error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	if f := s.failure.Load(); f != nil {
		return f.Err
	}
	frozen := make([][]frame, len(s.owners))
	acks := make([]chan answer, len(s.owners))
	for i, o := range s.owners {
		acks[i] = make(chan answer, 1)
		o.requests <- command{op: 'f', answer: acks[i]}
	}
	for i := range acks {
		frozen[i] = (<-acks[i]).frames
	}
	anyFrames := false
	for _, fs := range frozen {
		anyFrames = anyFrames || len(fs) > 0
	}
	if !anyFrames {
		return nil
	}
	// All frozen records are stable before ANY durable part cursor advances.
	if e := s.log.Sync(); e != nil {
		return s.fail("sync", e)
	}
	for i, o := range s.owners {
		go func(o *owner, fs []frame, ack chan answer) {
			if s.beforeSeal != nil && len(fs) > 0 {
				s.beforeSeal(o.id)
			}
			var e error
			if s.core != nil {
				e = o.awaitCorePublication(fs)
			} else {
				e = o.commit(fs)
			}
			if e == nil {
				o.admission.Release(frameCharge(fs))
			}
			if e != nil {
				s.fail("seal", e)
			}
			done := make(chan answer, 1)
			o.requests <- command{op: 'd', answer: done}
			<-done
			ack <- answer{err: e}
		}(o, frozen[i], acks[i])
	}
	var err error
	for i := range acks {
		err = errors.Join(err, (<-acks[i]).err)
	}
	var debt uint64
	for _, db := range s.dbs {
		debt += db.Metrics().Compact.EstimatedDebt
	}
	s.debt.Store(debt)
	// Retain the journal for now. Trimming requires an independently proven
	// power-failure frontier/retention protocol, not only a SIGKILL test.
	return err
}
func (s *Store) Flush() error {
	if s.closing.Load() {
		return ErrClosed
	}
	return s.flush()
}
func (s *Store) maintain() {
	defer close(s.done)
	t := time.NewTicker(s.opts.PartInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			if e := s.flush(); e != nil {
				s.fail("seal", e)
				return
			}
		}
	}
}
func (s *Store) watchErrors() {
	defer close(s.watchDone)
	for {
		select {
		case <-s.watchStop:
			return
		case e := <-s.log.Errors():
			if e != nil {
				s.fail("journal", e)
				return
			}
		}
	}
}
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closing.Store(true)
		s.cancelAdmission()
		close(s.stop)
		<-s.done
		s.closeErr = s.flush()
		s.flushMu.Lock()
		defer s.flushMu.Unlock()
		s.closed.Store(true)
		for _, o := range s.owners {
			a := make(chan answer, 1)
			o.requests <- command{op: 'q', answer: a}
			<-a
			<-o.done
		}
		s.coreViewsClosed.Store(true)
		for _, o := range s.owners {
			o.pub.Close()
			if b := o.readBase.Swap(nil); b != nil {
				b.pub.Close()
			}
		}
		if s.core != nil {
			s.closeErr = errors.Join(s.closeErr, s.core.Close())
		}
		close(s.watchStop)
		<-s.watchDone
		s.closeErr = errors.Join(s.closeErr, s.log.Close())
		if s.core == nil {
			for _, db := range s.dbs {
				s.closeErr = errors.Join(s.closeErr, db.Close())
			}
		}
		if s.legacy != nil {
			s.closeErr = errors.Join(s.closeErr, s.legacy.Close())
		}
	})
	return s.closeErr
}
func (s *Store) Stats() daemonstore.Stats {
	return daemonstore.Stats{IngestedPoints: s.ingested.Load(), Parts: int64(s.parts.Load()), WriteStalls: s.stalls.Load(), CompactionDebtBytes: s.debt.Load(), SealLatencySeconds: float64(s.sealNS.Load()) / 1e9, SnapshotPins: s.pins.Load()}
}

func rollupConfig(m daemonstore.Metric) (sealrollup.Config, error) {
	c := sealrollup.Config{XFilesFactor: m.XFilesFactor}
	for i, n := range []string{"average", "sum", "last", "max", "min", "first"} {
		if m.AggregationMethod == n {
			c.Method = sealrollup.Method(i + 1)
		}
	}
	for _, a := range m.Archives {
		c.Archives = append(c.Archives, sealrollup.Archive{Step: int64(a.Step), Count: int64(a.Points)})
	}
	return c, c.Validate()
}
func encodeFrame(f frame) ([]byte, error) {
	p, e := json.Marshal(f.policy)
	if e != nil {
		return nil, e
	}
	b, e := partlog.EncodeBatch(partlog.Batch{Metric: f.name, PolicyEpoch: 1, Points: f.points})
	if e != nil {
		return nil, e
	}
	out := []byte{'V', '2', 'B', 1}
	out = binary.BigEndian.AppendUint32(out, uint32(len(p)))
	out = binary.BigEndian.AppendUint32(out, uint32(len(f.group)))
	out = append(out, p...)
	out = append(out, []byte(f.group)...)
	return append(out, b...), nil
}
func decodeFrame(b []byte) (frame, error) {
	var f frame
	if len(b) < 12 || !bytes.Equal(b[:4], []byte{'V', '2', 'B', 1}) {
		return f, errors.New("invalid v2 journal envelope")
	}
	n, g := uint64(binary.BigEndian.Uint32(b[4:8])), uint64(binary.BigEndian.Uint32(b[8:12]))
	if n+g > uint64(len(b)-12) {
		return f, errors.New("invalid v2 envelope lengths")
	}
	if e := json.Unmarshal(b[12:12+n], &f.policy); e != nil {
		return f, e
	}
	f.group = partsformat.Group(string(b[12+n : 12+n+g]))
	x, e := partlog.DecodeBatch(b[12+n+g:])
	if e != nil {
		return f, e
	}
	if x.PolicyEpoch != 1 {
		return f, errors.New("unsupported policy epoch")
	}
	f.name = x.Metric
	f.points = x.Points
	f.payload = slices.Clone(b)
	if _, e = rollupConfig(f.policy); e != nil {
		return f, e
	}
	if f.group == "" || len(f.group) > partsformat.MaxNameBytes {
		return f, errors.New("invalid persisted placement group")
	}
	return f, nil
}

// CacheQuery borrows no mutable head: the admission owner copies its accepted
// unsealed receipts while the caller waits on a bounded command exchange.
func (s *Store) CacheQuery(ctx context.Context, name string) ([]points.Point, error) {
	if e := s.status(); e != nil {
		return nil, e
	}
	o := s.owners[s.route(name)]
	a := make(chan answer, 1)
	select {
	case o.requests <- command{op: 'r', frame: &frame{name: name}, answer: a}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-o.done:
		return nil, ErrClosed
	}
	select {
	case r := <-a:
		return r.points, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-o.done:
		return nil, ErrClosed
	}
}
