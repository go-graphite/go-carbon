// Package v2daemon wires producer-local catalogs and journals to the numeric core.
// It is an offline experiment; receiver ownership is separate work.
package v2daemon

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
	"golang.org/x/sync/errgroup"
)

// Config is fixed for the lifetime of a store. Layer is 0 (numeric), 1 (names)
// or 3 (rollups over L0 run files); there is no producer journal. Each
// producer has one caller, including Drain.
type Config struct {
	// BaselinePath names a verified converted native checkpoint, installed once
	// before core publication, journal replay, or admission.
	BaselinePath string

	Layer, Producers, Shards, MaxPendingRows int
	GroupInterval                            time.Duration
	SealInterval                             time.Duration
	SealMaxRows                              int
	// KeepBaseRows writes raw base rows next to layer-3 archive records. Only
	// tests and the fixed-corpus verifier set it; production never does.
	KeepBaseRows bool
	Hooks        v2core.Hooks
	// Mirror receives each drained application batch before core submission
	// (classic dual write; the classic engine's own input log recovers it).
	// Each producer calls serially; different producers may call concurrently.
	// The callback must finish consuming its borrowed batches before returning.
	Mirror func(int, []v2core.Batch) error
	// Prepare runs under the root lock before opening core or replaying input.
	Prepare func() error
	// MaxCoreRows bounds each core owner's staging buffer. Zero retains the
	// experiment default; small correctness fixtures can bound their memory.
	MaxCoreRows int
}

// Store owns core and its serial producers. Stop all producers before Close.
// Producer methods share no daemon mutex. Core owns all physical data layout.
type Store struct {
	Core      *v2core.Engine
	Producers []*Producer
	cfg       Config
	lock      *os.File
}
type packet struct {
	out     *v2core.Outbox // direct L3 applications
	rows    []v2core.Row
	batches []v2core.Batch
	cursor  uint64
	seal    bool
}

// Producer owns its catalog, pending rows and the packets it hands to core.
type Producer struct {
	engine                          *v2core.Engine
	mirror                          func(int, []v2core.Batch) error
	index, count, layer, maxPending int
	catalog                         *partlog.NameCatalog
	packets                         []packet
	outboxes                        []*v2core.Outbox            // free; one in steady intake
	applications                    []*v2core.ApplicationPolicy // by catalog ID
	cursor                          uint64
	applyOrder                      uint64
	journalDir                      string
	pendingRows                     int
	nextSync                        time.Time
	interval                        time.Duration
	failed                          error
	ForcedGroups                    uint64
	CatalogSyncTime, CoreSubmitTime time.Duration
}

func coreConfig(c Config) v2core.Config {
	maxRows := c.MaxCoreRows
	if maxRows == 0 {
		maxRows = 2 << 20
	}
	return v2core.Config{L0: c.Layer == 3, SealInterval: c.SealInterval, SealMaxRows: c.SealMaxRows, KeepBaseRows: c.KeepBaseRows, Rollups: c.Layer == 3, JournalIdentity: "core-first-fixed-v1", Producers: c.Producers, Shards: c.Shards, Owners: c.Shards, MaxRows: maxRows, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1"}
}

// Open recovers durable journal layers through the normal core write queues.
func Open(root string, c Config) (*Store, error) { return openWithHooks(root, c, c.Hooks) }

func openWithHooks(root string, c Config, hooks v2core.Hooks) (*Store, error) {
	// One recovery worker per scheduler CPU: each producer replays serially, so
	// a lower cap (32) left two thirds of a 96-CPU host idle during replay. The
	// existing per-producer core queues retain their ordinary admission budget.
	return openWithRecoveryWorkers(root, c, hooks, runtime.GOMAXPROCS(0))
}

func openWithRecoveryWorkers(root string, c Config, hooks v2core.Hooks, workers int) (*Store, error) {
	if c.Mirror != nil && c.Layer != 3 {
		return nil, errors.New("classic mirror requires the rollup layer")
	}

	if c.Layer < 0 || c.Layer > 3 || c.Layer == 2 || c.Producers < 1 || c.Shards < 1 {
		return nil, errors.New("invalid layer or geometry")
	}
	if c.MaxPendingRows == 0 {
		c.MaxPendingRows = 1 << 20
	}
	if c.MaxPendingRows < 1 {
		return nil, errors.New("invalid pending limit")
	}
	if c.GroupInterval == 0 {
		c.GroupInterval = time.Second
	}
	if c.GroupInterval < 0 {
		return nil, errors.New("invalid group interval")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	lock, _, err := lockRoot(root, c)
	if err != nil {
		return nil, err
	}
	if c.Prepare != nil {
		if err := c.Prepare(); err != nil {
			_ = lock.Close()
			return nil, err
		}
	}
	baselineOrder, err := installBaseline(root, c)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	// Merges wait until every producer has replayed; replay seals stay append-only.
	cc := coreConfig(c)
	cc.DeferMerges = true
	e, err := v2core.Open(filepath.Join(root, "placement-group-00000001", "core"), cc, hooks)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	s := &Store{Core: e, cfg: c, lock: lock}
	// Register admission sinks serially; each recovery worker exclusively owns one producer.
	for i := 0; i < c.Producers; i++ {
		p := &Producer{applyOrder: baselineOrder, engine: e, index: i, count: c.Producers, layer: c.Layer, maxPending: c.MaxPendingRows, interval: c.GroupInterval, nextSync: time.Now().Add(c.GroupInterval)}
		p.mirror = c.Mirror
		s.Producers = append(s.Producers, p)
	}
	group, ctx := errgroup.WithContext(context.Background())
	group.SetLimit(max(1, min(workers, c.Producers)))
	for i, p := range s.Producers {
		group.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if c.Layer == 0 {
				return nil
			}
			p.journalDir = filepath.Join(root, "placement-group-00000001", "journal", fmt.Sprintf("producer-%03d", i))
			var err error
			if c.Layer == 3 {
				err = p.recoverL0(p.journalDir)
			} else {
				err = p.openCatalog()
			}
			if err != nil {
				p.failed = err
				return fmt.Errorf("recover producer %d: %w", i, err)
			}
			return nil
		})
	}
	// Join every worker before closing catalogs, queues or the root lock.
	if err := group.Wait(); err != nil {
		_ = s.Close()
		return nil, err
	}
	if c.Layer == 3 {
		// Continue each producer's order above every loaded application. An
		// owner's frontier is the last order of each producer it sealed.
		frontiers := e.DurableFrontiers()
		err := e.LoadL0(func(owner int, b *v2core.Batch) (bool, error) {
			producer := int((b.ID - 1) % uint64(c.Producers))
			p := s.Producers[producer]
			p.applyOrder = max(p.applyOrder, b.Order)
			p.cursor = p.applyOrder
			if b.Order <= frontiers[owner][producer] {
				return false, nil
			}
			return true, p.openCatalog()
		})
		if err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("load L0 run files: %w", err)
		}
	}
	e.ResumeMerges()

	return s, nil
}

// openCatalog runs on the producer's serial caller, on recovery only if a
// live L0 data needs names. Core parts remain authoritative for sealed data.
func (p *Producer) openCatalog() error {
	if p.catalog != nil {
		return nil
	}
	var err error
	if p.layer == 3 {
		recovery := p.engine.NewPolicyRecovery()
		binding := &catalogPolicies{engine: p.engine, producer: p.index, producers: p.count, recovery: recovery}
		p.catalog, err = partlog.OpenNameCatalog(filepath.Join(p.journalDir, "catalog"), binding)
		if err == nil {
			err = recovery.Finish()
		}
		binding.recovery = nil
	} else {
		p.catalog, err = partlog.OpenNameCatalog(filepath.Join(p.journalDir, "catalog"))
	}
	return p.fail(err)
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func (p *Producer) fail(err error) error {
	if err != nil && p.failed == nil {
		p.failed = err
	}
	return p.failed
}

// NumericID is an injective mapping of persisted stream-local IDs. It is not a hash.
func NumericID(local uint64, producer, producers int) uint64 {
	return (local-1)*uint64(producers) + uint64(producer) + 1
}

// AppendNumeric is the L0 control and does not admit names or journal input.
func (p *Producer) AppendNumeric(rows []v2core.Row) error {
	if p.failed != nil {
		return p.failed
	}
	if p.layer != 0 {
		return p.fail(errors.New("numeric append requires layer zero"))
	}
	p.cursor += uint64(len(rows))
	return p.fail(p.engine.Append(p.index, rows, p.cursor, false))
}

// appendNamed resolves every name through the durable catalog. Returned
// success only accepts memory. Inputs are borrowed for this call only. Sequence is the exact per-name input order supplied by caller.
func (p *Producer) appendNamed(names []string, data []points.Point, sequence []uint64, clock int64) error {
	if p.failed != nil {
		return p.failed
	}
	if p.layer == 3 {
		return p.fail(errors.New("rollups require original AppendApplications groups"))
	}
	if p.layer == 0 || len(names) != len(data) || len(names) != len(sequence) || len(names) == 0 || len(names) > 32768 {
		return p.fail(errors.New("invalid named batch"))
	}
	rows := make([]v2core.Row, len(names))
	ids, err := p.catalog.InternBatch(names, nil)
	if err != nil {
		return p.fail(err)
	}
	for i, id := range ids {
		rows[i] = v2core.Row{ID: NumericID(id, p.index, p.count), Time: data[i].Timestamp, Bits: math.Float64bits(data[i].Value), Seq: sequence[i]}
	}
	p.cursor += uint64(len(data))
	p.pendingRows += len(data)
	p.packets = append(p.packets, packet{rows: rows, cursor: p.cursor})
	if p.pendingRows >= p.maxPending {
		p.ForcedGroups++
		return p.Drain()
	}
	return nil
}

// Append is the named layer's intake (raw TCP owners on layer 1).
func (p *Producer) Append(names []string, _ []uint64, data []points.Point, sequence []uint64, clock int64) error {
	return p.appendNamed(names, data, sequence, clock)
}

// Seal queues a core barrier. L1 syncs new catalog entries then submits it;
// L3 hands it to core with the next drain.
func (p *Producer) Seal() error {
	if p.failed != nil {
		return p.failed
	}
	if p.cursor == 0 {
		return nil
	}
	if p.layer == 0 {
		return p.fail(p.engine.Append(p.index, nil, p.cursor, true))
	}
	p.packets = append(p.packets, packet{cursor: p.cursor, seal: true})
	if p.layer == 1 || !time.Now().Before(p.nextSync) {
		return p.Drain()
	}
	return nil
}

// Drain syncs the catalog, then sends the exact queued order to core. On
// layer 3 core writes it to L0 run files at its next publication.
func (p *Producer) Drain() error {
	if p.failed != nil {
		return p.failed
	}
	if p.layer == 0 {
		return nil
	}
	if len(p.packets) == 0 {
		return nil
	}
	start := time.Now()
	if p.catalog != nil {
		if err := p.catalog.Sync(); err != nil {
			return p.fail(err)
		}
	}
	p.CatalogSyncTime += time.Since(start)
	start = time.Now()
	for _, packet := range p.packets {
		if len(packet.batches) > 0 && p.mirror != nil {
			if err := p.mirror(p.index, packet.batches); err != nil {
				return p.fail(err)
			}
		}
		var err error
		switch {
		case packet.out != nil:
			// The outbox comes back empty, holding the arrays owners released.
			err = p.engine.AppendOutboxPipelined(p.index, packet.out, packet.cursor, packet.seal)
			p.outboxes = append(p.outboxes, packet.out)
		case packet.batches != nil || p.layer == 3:
			err = p.engine.AppendBatchesPipelined(p.index, packet.batches, packet.cursor, packet.seal)
		default:
			err = p.engine.AppendPipelined(p.index, packet.rows, packet.cursor, packet.seal)
		}
		if err != nil {
			return p.fail(err)
		}
	}
	p.CoreSubmitTime += time.Since(start)
	clear(p.packets)
	p.packets = p.packets[:0]
	p.pendingRows = 0
	p.nextSync = time.Now().Add(p.interval)
	return nil
}

// Close drains stopped producers and closes their resources even after an error.
func (s *Store) Close() error {
	var result error
	for _, p := range s.Producers {
		result = errors.Join(result, p.Drain())
		if p.catalog != nil {
			result = errors.Join(result, p.catalog.Close())
			p.catalog = nil
		}
	}
	if s.Core != nil {
		result = errors.Join(result, s.Core.Close())
		s.Core = nil
	}
	if s.lock != nil {
		result = errors.Join(result, s.lock.Close())
		s.lock = nil
	}
	return result
}
