package v2backend

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/partsread"
	"golang.org/x/sync/semaphore"
)

// ErrRecovering is explicit: no incomplete historical render is returned.
var ErrRecovering = errors.New("v2 historical recovery not ready")

// OpenAdmission durably fences the journal before starting historical work.
// Admission owners retain no recovery backlog in RAM: the journal is its source
// of truth. Each owner switches to parts only at an ordered suffix barrier.
func OpenAdmission(opts daemonstore.Options) (daemonstore.Backend, error) {
	return openAdmissionAt(opts, time.Now, true)
}

func openAdmissionAt(opts daemonstore.Options, now func() time.Time, async bool) (*Store, error) {
	return openAdmissionWithHook(opts, now, async, nil)
}

// beforeOwner is a deterministic recovery fault-injection hook; normal opens
// pass nil. It runs before replay, outside admission and core owner goroutines.
func openAdmissionWithHook(opts daemonstore.Options, now func() time.Time, async bool, beforeOwner func(int)) (*Store, error) {
	if opts.WriteEngine != "" && opts.WriteEngine != "standalone" && opts.WriteEngine != "core" {
		return nil, errors.New("unknown v2 write engine")
	}
	if opts.WriteEngine == "core" && opts.Owners < opts.Shards {
		return nil, errors.New("core write engine requires at least one journal owner per shard")
	}

	if err := validJournalCodec(opts.JournalCodec); err != nil {
		return nil, err
	}
	if opts.DataDir == "" || opts.Owners < 1 || opts.Shards < 1 || opts.PartInterval <= 0 || opts.Policy == nil {
		return nil, errors.New("invalid v2 backend options")
	}
	if e := os.MkdirAll(opts.DataDir, 0750); e != nil {
		return nil, e
	}
	s := &Store{opts: opts, now: now, errors: make(chan daemonstore.Failure, 1),
		budget: partsread.NewBudget(256 << 20), stop: make(chan struct{}), done: make(chan struct{}),
		watchStop: make(chan struct{}), watchDone: make(chan struct{}), ready: make(chan struct{}), firstAccepted: make(chan struct{})}
	s.admissionContext, s.cancelAdmission = context.WithCancel(context.Background())
	var e error
	s.log, e = partlog.OpenAdmission(filepath.Join(opts.DataDir, "journal"), partlog.Options{Streams: opts.Owners, SyncInterval: time.Second})
	if e != nil {
		s.cancelAdmission()
		return nil, e
	}
	for i := 0; i < opts.Owners; i++ {
		o := &owner{id: i, s: s, dir: filepath.Join(opts.DataDir, fmt.Sprintf("owner-%04d", i)), recovering: true,
			admission: semaphore.NewWeighted(256 << 20), requests: make(chan command, 64), done: make(chan struct{}), metrics: make(map[string]*metricState)}
		s.owners = append(s.owners, o)
		go o.run()
	}
	go s.watchErrors()
	go func() {
		e := s.initialize(beforeOwner)
		if e != nil {
			s.fail("recovery", e)
		} else {
			s.readyOK.Store(true)
		}
		close(s.ready)
		s.maintain()
	}()
	if !async {
		if e = s.WaitReady(context.Background()); e != nil {
			_ = s.Close()
			return nil, e
		}
	}
	return s, nil
}

func (s *Store) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ready:
	}
	if f := s.failure.Load(); f != nil {
		return f.Err
	}
	return nil
}
func (s *Store) readStatus() error {
	if e := s.status(); e != nil {
		return e
	}
	if !s.readyOK.Load() {
		return ErrRecovering
	}
	return nil
}

// FirstAccepted closes after the first journal write and explicit sync. Ordinary
// subsequent appends retain the once-per-second group-sync contract.
func (s *Store) FirstAccepted() <-chan struct{} { return s.firstAccepted }
func (s *Store) FirstAcceptedUnixNano() int64   { return s.firstAcceptNS.Load() }
func (s *Store) AcceptedPoints() uint64         { return s.accepted.Load() }

func (s *Store) recoverOwner(o *owner) error {
	decoder := receiptDecoder{catalog: partlog.CatalogDecoder{Catalog: o.journalCatalog}}
	defer decoder.close()
	var last uint64
	pending := 0
	apply := func(r partlog.Record) error {
		if e := s.admissionContext.Err(); e != nil {
			return e
		}
		fs, e := decoder.decode(r.Payload)
		if e != nil {
			return e
		}
		last = r.Cursor
		if r.Cursor <= o.meta.Cursor {
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
			current, e := s.opts.Policy(f.name)
			if e != nil {
				return e
			}
			if !reflect.DeepEqual(current, f.policy) {
				return errors.New("replay policy changed")
			}
			if legacy := s.legacyReady.Load(); legacy != nil {
				if old, ok := legacy.Lookup(f.name); ok && (!reflect.DeepEqual(toDaemonMetric(old), f.policy) || len(f.policy.Archives) > 1) {
					return errors.New("legacy policy cannot seed recovered writes")
				}
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
			pending += len(f.points)
			s.ingested.Add(uint64(len(f.points)))
		}
		if s.core != nil && len(o.active) > 0 {
			if e := o.appendCore(o.active); e != nil {
				return e
			}
			o.active = nil
			pending = 0
		}
		if pending >= 1<<20 {
			if e := s.log.Sync(); e != nil {
				return e
			}
			if e := o.commit(o.active); e != nil {
				return e
			}
			o.active = nil
			pending = 0
		}
		return nil
	}
	tails, e := s.log.RecoveredTails(s.admissionContext)
	if e != nil {
		return e
	}
	if o.meta.Cursor > tails[o.id] {
		return errors.New("parts frontier exceeds historical journal")
	}
	prefixDone := errors.New("fixed historical prefix complete")
	// Replay fixes a complete-record snapshot. A second snapshot under the owner
	// command barrier consumes exactly its remaining suffix using the same decoder.
	if e := s.log.Replay(o.id, 0, func(r partlog.Record) error {
		if r.Cursor > tails[o.id] {
			return prefixDone
		}
		return apply(r)
	}); e != nil && !errors.Is(e, prefixDone) {
		return e
	}
	ack := make(chan answer, 1)
	o.requests <- command{op: 'a', answer: ack, activate: func() error {
		if e := s.log.Replay(o.id, last, apply); e != nil {
			return e
		}
		if e := s.log.Sync(); e != nil {
			return e
		}
		if s.core != nil {
			if o.coreCursor > 0 {
				if e := s.core.Append(o.id, nil, o.coreCursor, true); e != nil {
					return e
				}
			}
			o.active = nil
			return nil
		}
		if e := o.commit(o.active); e != nil {
			return e
		}
		o.active = nil
		return o.publish()
	}}
	return (<-ack).err
}
func (s *Store) RecoveryDone() <-chan struct{}  { return s.ready }
func (s *Store) AdmissionDone() <-chan struct{} { return s.watchDone }
