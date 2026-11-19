// Package v2daemon wires producer-local catalogs and journals to the numeric core.
// It is an offline experiment; receiver ownership is separate work.
package v2daemon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2placement"
	"github.com/go-graphite/go-carbon/points"
	"golang.org/x/sync/errgroup"
)

// Config is fixed for the lifetime of a store. Layer is 0 (numeric), 1 (names),
// 2 (journal), or 3 (rollups). Each producer has one caller, including Drain.
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
	// Placement requires stamped intake; one daemon root owns one PG.
	Placement Placement
	Hooks     v2core.Hooks
	// DurableApplications receives the synced input prefix before core submission.
	// It is also called during recovery, independently of core frontiers.
	// Each producer calls serially; different producers may call concurrently.
	// The callback must finish consuming its borrowed batches before returning.
	DurableApplications func(int, []v2core.Batch) error
	// Prepare runs under the root lock before opening core or replaying input.
	Prepare func() error
	// MaxCoreRows bounds each core owner's staging buffer. Zero retains the
	// experiment default; small correctness fixtures can bound their memory.
	MaxCoreRows int
	// JournalSegmentBytes rotates a producer's journal to a new segment after a
	// group fsync once the current segment holds at least this many bytes. Each
	// segment starts with fresh predictor state, so restart can skip every
	// segment that ends at or below all owners' durable frontiers. Zero keeps one
	// segment per process run. Rotation never deletes a segment.
	JournalSegmentBytes int64
	// fullReplay disables segment skipping; tests use it as the oracle.
	fullReplay bool
}

// Store owns core and its serial producers. Stop all producers before Close.
// Producer methods share no daemon mutex. Core owns all physical data layout.
type Store struct {
	relayActive atomic.Bool
	Core        *v2core.Engine
	Producers   []*Producer
	cfg         Config
	lock        *os.File
}
type packet struct {
	rows    []v2core.Row
	batches []v2core.Batch
	cursor  uint64
	seal    bool
}

// Producer owns its catalog, predictors, pending rows and journal stream.
type Producer struct {
	relayPositions                  map[v2placement.Stamp]relayPosition
	admission                       v2placement.Sink
	placement                       Placement
	engine                          *v2core.Engine
	durableApplications             func(int, []v2core.Batch) error
	index, count, layer, maxPending int
	catalog                         *partlog.NameCatalog
	encoder                         partlog.CatalogEncoder
	journal                         *os.File
	writer                          *bufio.Writer
	packets                         []packet
	records                         [][]byte
	spareRecords                    [][]byte
	spareRows                       [][]v2core.Row
	spareBatches                    [][]v2core.Batch
	applicationFrames               []partlog.CatalogFrame
	applicationIDs                  []uint64
	applications                    []*RegisteredApplication // by catalog ID
	cursor                          uint64
	applyOrder                      uint64
	journalDir                      string
	segment                         int
	segmentBytes, segmentLimit      int64
	fullReplay                      bool
	// Recovery counts journal segments skipped below the durable frontier and
	// segments decoded; set once by Open.
	RecoverySkippedSegments, RecoveryReplayedSegments            int
	pendingRows                                                  int
	nextSync                                                     time.Time
	interval                                                     time.Duration
	failed                                                       error
	stage                                                        func(string)
	JournalBytes, SyncGroups, ForcedGroups                       uint64
	EncodeTime, CatalogSyncTime, JournalSyncTime, CoreSubmitTime time.Duration
}

func coreConfig(c Config) v2core.Config {
	maxRows := c.MaxCoreRows
	if maxRows == 0 {
		maxRows = 2 << 20
	}
	return v2core.Config{SealInterval: c.SealInterval, SealMaxRows: c.SealMaxRows, KeepBaseRows: c.KeepBaseRows, Rollups: c.Layer == 3, JournalIdentity: "core-first-fixed-v1", Producers: c.Producers, Shards: c.Shards, Owners: c.Shards, MaxRows: maxRows, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1"}
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
	if c.DurableApplications != nil && c.Layer != 3 {
		return nil, errors.New("classic mirror requires application journal layer")
	}
	if c.Placement != nil && c.Layer < 2 {
		return nil, errors.New("placement requires a durable journal layer")
	}

	if c.Layer < 0 || c.Layer > 3 || c.Producers < 1 || c.Shards < 1 {
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
		p := &Producer{applyOrder: baselineOrder, engine: e, index: i, count: c.Producers, layer: c.Layer, maxPending: c.MaxPendingRows, interval: c.GroupInterval, nextSync: time.Now().Add(c.GroupInterval), segmentLimit: c.JournalSegmentBytes, fullReplay: c.fullReplay}
		if c.Placement != nil {
			p.placement = c.Placement
			p.admission = c.Placement.Register()
		}
		p.durableApplications = c.DurableApplications
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
			if c.Layer >= 2 {
				err = p.recoverProducer(ctx, p.journalDir)
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
	e.ResumeMerges()

	return s, nil
}

// openCatalog runs on the producer's serial caller, on recovery only if a
// journal tail needs names. Core parts remain authoritative for sealed data.
func (p *Producer) openCatalog() error {
	if p.catalog != nil {
		return nil
	}
	var err error
	if p.layer == 3 {
		p.catalog, err = partlog.OpenNameCatalog(filepath.Join(p.journalDir, "catalog"), func(id uint64, name string, definition []byte) (string, []byte, error) {
			return p.engine.BindDefinition(NumericID(id, p.index, p.count), name, definition)
		})
	} else {
		p.catalog, err = partlog.OpenNameCatalog(filepath.Join(p.journalDir, "catalog"))
	}
	if err == nil {
		p.encoder = partlog.CatalogEncoder{Catalog: p.catalog, SeriesClocks: true, SparseClocks: true}
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
	if p.admission != nil {
		return v2placement.ErrUnversioned
	}
	if p.failed != nil {
		return p.failed
	}
	if p.layer != 0 {
		return p.fail(errors.New("numeric append requires layer zero"))
	}
	p.cursor += uint64(len(rows))
	return p.fail(p.engine.Append(p.index, rows, p.cursor, false))
}

// Append resolves every name through the durable catalog. Returned success only
// accepts memory: Drain is the durability acknowledgment. Inputs are borrowed
// for this call only. Sequence is the exact per-name input order supplied by caller.
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
	if p.layer == 2 {
		apps := make([]Application, len(names))
		for i, name := range names {
			apps[i] = Application{Name: name, Order: p.applyOrder + uint64(i) + 1, Clock: clock, Points: data[i : i+1], Sequence: sequence[i : i+1]}
		}
		return p.appendApplications(apps)
	}
	var rows []v2core.Row
	if n := len(p.spareRows); n > 0 {
		rows = p.spareRows[n-1]
		p.spareRows[n-1] = nil
		p.spareRows = p.spareRows[:n-1]
	}
	rows = slices.Grow(rows[:0], len(names))[:len(names)]
	ids, err := p.catalog.InternBatch(names, nil)
	if err != nil {
		return p.fail(err)
	}
	for i, id := range ids {
		rows[i] = v2core.Row{ID: NumericID(id, p.index, p.count), Time: data[i].Timestamp, Bits: math.Float64bits(data[i].Value), Seq: sequence[i]}
	}
	if p.layer >= 2 {
		start := time.Now()
		record, err := p.encoder.EncodePoints(ids, data, clock)
		p.EncodeTime += time.Since(start)
		if err != nil {
			return p.fail(err)
		}
		p.records = append(p.records, record)
	}
	p.cursor += uint64(len(data))
	p.pendingRows += len(data)
	p.packets = append(p.packets, packet{rows: rows, cursor: p.cursor})
	if p.pendingRows >= p.maxPending {
		p.ForcedGroups++
		return p.Drain()
	}
	if p.layer == 2 && !time.Now().Before(p.nextSync) {
		return p.Drain()
	}
	return nil
}

// Seal retains a logical barrier without forcing a journal fsync on every seal.
// L1 syncs new catalog entries then submits the seal; L2 releases it at group sync.
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

// Drain syncs catalog before journal, then sends the exact queued order to core.
// It is a per-producer durable journal acknowledgment, not a global core seal.
func (p *Producer) Drain() error {
	if p.failed != nil {
		return p.failed
	}
	if p.layer == 0 || len(p.packets) == 0 {
		return nil
	}
	start := time.Now()
	if p.catalog != nil {
		if err := p.catalog.Sync(); err != nil {
			return p.fail(err)
		}
	}
	p.CatalogSyncTime += time.Since(start)
	if p.layer >= 2 {
		for _, record := range p.records {
			var h [4]byte
			binary.LittleEndian.PutUint32(h[:], uint32(len(record)))
			if _, err := p.writer.Write(h[:]); err != nil {
				return p.fail(err)
			}
			if p.stage != nil {
				if err := p.writer.Flush(); err != nil {
					return p.fail(err)
				}
				p.stage("append")
			}
			if _, err := p.writer.Write(record); err != nil {
				return p.fail(err)
			}
			p.JournalBytes += uint64(4 + len(record))
			p.segmentBytes += int64(4 + len(record))
		}
		start = time.Now()
		if err := p.writer.Flush(); err != nil {
			return p.fail(err)
		}
		if err := p.journal.Sync(); err != nil {
			return p.fail(err)
		}
		p.JournalSyncTime += time.Since(start)
		p.SyncGroups++
		// Write/Flush and fsync have finished consuming these detached records.
		// No core packet aliases the serialized application envelope.
		for _, record := range p.records {
			// Placed envelopes currently allocate independently and never take
			// a spare. Keep this pool bounded by recycling only plain frames.
			if bytes.HasPrefix(record, applicationMagic) {
				p.spareRecords = append(p.spareRecords, record[:0])
			}
		}
		clear(p.records)
		p.records = p.records[:0]
		// Every record of this segment is synced and none is encoded ahead.
		if p.segmentLimit > 0 && p.segmentBytes >= p.segmentLimit {
			if err := p.rotateJournal(); err != nil {
				return p.fail(err)
			}
		}
	}
	start = time.Now()
	for _, packet := range p.packets {
		if len(packet.batches) > 0 && p.durableApplications != nil {
			if err := p.durableApplications(p.index, packet.batches); err != nil {
				return p.fail(err)
			}
		}
		var err error
		if packet.batches != nil || p.layer == 3 {
			err = p.engine.AppendBatchesPipelined(p.index, packet.batches, packet.cursor, packet.seal)
		} else {
			err = p.engine.AppendPipelined(p.index, packet.rows, packet.cursor, packet.seal)
		}
		if err != nil {
			return p.fail(err)
		}
		if packet.rows != nil {
			p.spareRows = append(p.spareRows, packet.rows[:0])
		}
		if packet.batches != nil {
			clear(packet.batches)
			p.spareBatches = append(p.spareBatches, packet.batches[:0])
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
		p.encoder.Close()
		if p.journal != nil {
			result = errors.Join(result, p.journal.Close())
			p.journal = nil
		}
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
