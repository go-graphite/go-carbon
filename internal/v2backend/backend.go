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
	frame    *frame
	batch    []frame
	seal     []frame
	op       byte
	answer   chan answer
	activate func() error
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
	nextOrder      uint64
	recovering     bool
	journalEncoder partlog.DictionaryDeltaEncoder
	workspace      sealrollup.Workspace
	batchScratch   []sealrollup.Batch
	sampleScratch  []sealrollup.Sample
	meta           checkpoint
	metrics        map[string]*metricState
	pub            partsread.Publication
	readCatalog    atomic.Pointer[catalog]
	readBase       atomic.Pointer[liveBase]
}
type Store struct {
	ready                         chan struct{}
	readyOK                       atomic.Bool
	firstAccepted                 chan struct{}
	firstAcceptNS                 atomic.Int64
	accepted                      atomic.Uint64
	legacyReady                   atomic.Pointer[catalog]
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

func openAt(opts daemonstore.Options, now func() time.Time) (*Store, error) {
	return openAdmissionAt(opts, now, false)
}

func (s *Store) initialize() (err error) {
	opts := s.opts
	// Each database's LOCK fences simultaneous processes before journal recovery.
	for i := 0; i < opts.Shards; i++ {
		dir := filepath.Join(opts.DataDir, fmt.Sprintf("db-%04d", i))
		db, e := pebble.Open(dir, &pebble.Options{MemTableSize: 16 << 20, MaxConcurrentCompactions: func() int { return 2 },
			EventListener: &pebble.EventListener{WriteStallBegin: func(pebble.WriteStallBeginInfo) { s.stalls.Add(1) }}})
		if e != nil {
			return e
		}
		s.dbs = append(s.dbs, db)
	}
	// Geometry and origin are restart invariants, never inferred from local IDs.
	format := []byte(fmt.Sprintf("experimental-v2-single-origin-v1\nowners=%d\nshards=%d\n", opts.Owners, opts.Shards))
	marker := filepath.Join(opts.DataDir, "V2FORMAT")
	if old, e := os.ReadFile(marker); e == nil {
		if !bytes.Equal(old, format) {
			return errors.New("v2 format/geometry mismatch")
		}
	} else if errors.Is(e, os.ErrNotExist) {
		f, e := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
		if e != nil {
			return e
		}
		_, e = f.Write(format)
		if e == nil {
			e = f.Sync()
		}
		e = errors.Join(e, f.Close())
		if e != nil {
			return e
		}
		d, e := os.Open(opts.DataDir)
		if e != nil {
			return e
		}
		e = errors.Join(d.Sync(), d.Close())
		if e != nil {
			return e
		}
	} else {
		return e
	}
	for _, o := range s.owners {
		o.db = s.dbs[o.id%opts.Shards]
		if e := os.MkdirAll(o.dir, 0750); e != nil {
			return e
		}
		if e := o.recover(); e != nil {
			return fmt.Errorf("owner %d: %w", o.id, e)
		}
		o.nextOrder = max(o.meta.Order, o.meta.Cursor)
	}
	if err = s.openLegacy(); err != nil {
		return err
	}
	s.legacyReady.Store(s.legacyCatalog)
	for _, o := range s.owners {
		if e := s.recoverOwner(o); e != nil {
			return e
		}
	}
	return s.openMixed()
}

func (s *Store) abortOpen() {
	for _, o := range s.owners {
		o.pub.Close()
		if b := o.readBase.Swap(nil); b != nil {
			b.pub.Close()
		}
	}
	if s.log != nil {
		_ = s.log.Close()
	}
	if s.legacy != nil {
		_ = s.legacy.Close()
	}
	for _, db := range s.dbs {
		_ = db.Close()
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
	defer close(o.done)
	defer o.journalEncoder.Close()
	var deferred *command
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
		case 'a':
			a.err = c.activate()
			if a.err == nil {
				o.recovering = false
			}
		case 'f':
			a.frames = o.active
			o.active = nil
			o.pending = a.frames
		case 'd':
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
			commands := []command{c}
			fs := c.batch
			if c.frame != nil {
				fs = []frame{*c.frame}
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
				if a.err == nil && o.s.firstAcceptNS.Load() == 0 {
					a.err = o.s.log.Sync()
					if a.err == nil && o.s.firstAcceptNS.CompareAndSwap(0, time.Now().UnixNano()) {
						close(o.s.firstAccepted)
					}
				}
				if a.err == nil {
					var n uint64
					for i := range fs {
						if !o.recovering {
							o.nextOrder++
							fs[i].order = o.nextOrder
						}
						fs[i].cursor = cursor
						fs[i].payload = nil
						n += uint64(len(fs[i].points))
					}
					o.s.accepted.Add(n)
					if o.recovering {
						o.admission.Release(frameCharge(fs))
					} else {
						o.active = append(o.active, fs...)
						o.s.ingested.Add(n)
					}
				}
			}
			for _, cc := range commands {
				cc.answer <- a
			}
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
			e := o.commit(fs)
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
	if e := s.WaitReady(context.Background()); e != nil {
		return e
	}
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
		<-s.ready
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
			o.pub.Close()
			if b := o.readBase.Swap(nil); b != nil {
				b.pub.Close()
			}
		}
		close(s.watchStop)
		<-s.watchDone
		s.closeErr = errors.Join(s.closeErr, s.log.Close())
		for _, db := range s.dbs {
			s.closeErr = errors.Join(s.closeErr, db.Close())
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
	if e := s.readStatus(); e != nil {
		return nil, e
	}
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
