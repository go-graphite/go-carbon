// Package v2daemon wires producer-local catalogs and journals to the numeric core.
// It is an offline experiment; receiver ownership and crash replay are separate work.
package v2daemon

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

// Config is fixed for the lifetime of a store. Layer is 0 (numeric), 1 (names),
// or 2 (journal). Each producer must have exactly one caller, including Drain.
type Config struct {
	Layer, Producers, Shards, MaxPendingRows int
	GroupInterval                            time.Duration
	coreMaxRows                              int // private fixture bound; zero retains the experiment buffer size
}

// Store owns core and its serial producers. Stop all producers before Close.
// Producer methods share no daemon mutex. Core owns all physical data layout.
type Store struct {
	Core      *v2core.Engine
	Producers []*Producer
	cfg       Config
}
type packet struct {
	rows    []v2core.Row
	batches []v2core.Batch
	cursor  uint64
	seal    bool
}

// Producer owns its catalog, predictors, pending rows and journal stream.
type Producer struct {
	engine                                                       *v2core.Engine
	index, count, layer, maxPending                              int
	catalog                                                      *partlog.NameCatalog
	encoder                                                      partlog.CatalogEncoder
	journal                                                      *os.File
	writer                                                       *bufio.Writer
	packets                                                      []packet
	records                                                      [][]byte
	spareRows                                                    [][]v2core.Row
	cursor                                                       uint64
	applyOrder                                                   uint64
	pendingRows                                                  int
	nextSync                                                     time.Time
	interval                                                     time.Duration
	failed                                                       error
	JournalBytes, SyncGroups, ForcedGroups                       uint64
	EncodeTime, CatalogSyncTime, JournalSyncTime, CoreSubmitTime time.Duration
}

func coreConfig(c Config) v2core.Config {
	maxRows := c.coreMaxRows
	if maxRows == 0 {
		maxRows = 2 << 20
	}
	return v2core.Config{Rollups: c.Layer == 3, JournalIdentity: "core-first-fixed-v1", Producers: c.Producers, Shards: c.Shards, Owners: c.Shards, MaxRows: maxRows, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1"}
}

// Open requires a fresh experiment root. Reopening writable journals without
// replay is deliberately refused. Core alone may be reopened for read-back.
func Open(root string, c Config) (*Store, error) {
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
	// An exclusive marker prevents accidentally appending a fresh predictor epoch
	// or reusing IDs/cursors on an existing store. Recovery is not implemented here.
	f, err := os.OpenFile(filepath.Join(root, "experiment.lock"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	e, err := v2core.Open(filepath.Join(root, "placement-group-00000001", "core"), coreConfig(c), v2core.Hooks{})
	if err != nil {
		return nil, err
	}
	s := &Store{Core: e, cfg: c}
	for i := 0; i < c.Producers; i++ {
		p := &Producer{engine: e, index: i, count: c.Producers, layer: c.Layer, maxPending: c.MaxPendingRows, interval: c.GroupInterval, nextSync: time.Now().Add(c.GroupInterval)}
		s.Producers = append(s.Producers, p)
		if c.Layer == 0 {
			continue
		}
		dir := filepath.Join(root, "placement-group-00000001", "journal", fmt.Sprintf("producer-%03d", i))
		p.catalog, err = partlog.OpenNameCatalog(filepath.Join(dir, "catalog"))
		if err != nil {
			_ = s.Close()
			return nil, err
		}
		p.encoder = partlog.CatalogEncoder{Catalog: p.catalog, SeriesClocks: true, SparseClocks: true}
		if c.Layer >= 2 {
			p.journal, err = os.OpenFile(filepath.Join(dir, "segment.pdc4"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err == nil {
				err = syncDir(dir)
			}
			if err != nil {
				_ = s.Close()
				return nil, err
			}
			p.writer = bufio.NewWriterSize(p.journal, 1<<20)
		}
	}
	return s, nil
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

// Append resolves every name through the durable catalog. Returned success only
// accepts memory: Drain is the durability acknowledgment. Inputs are borrowed
// for this call only. Sequence is the exact per-name input order supplied by caller.
func (p *Producer) Append(names []string, data []points.Point, sequence []uint64, clock int64) error {
	if p.failed != nil {
		return p.failed
	}
	if p.layer == 3 {
		return p.fail(errors.New("rollups require original AppendApplications groups"))
	}
	if p.layer == 0 || len(names) != len(data) || len(names) != len(sequence) || len(names) == 0 || len(names) > 32768 {
		return p.fail(errors.New("invalid named batch"))
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
	if err := p.catalog.Sync(); err != nil {
		return p.fail(err)
	}
	p.CatalogSyncTime += time.Since(start)
	if p.layer >= 2 {
		for _, record := range p.records {
			var h [4]byte
			binary.LittleEndian.PutUint32(h[:], uint32(len(record)))
			if _, err := p.writer.Write(h[:]); err != nil {
				return p.fail(err)
			}
			if _, err := p.writer.Write(record); err != nil {
				return p.fail(err)
			}
			p.JournalBytes += uint64(4 + len(record))
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
		clear(p.records)
		p.records = p.records[:0]
	}
	start = time.Now()
	for _, packet := range p.packets {
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
	return result
}
