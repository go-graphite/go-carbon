package main

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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

var coreMetaKey = []byte{0xff, 'f'}

type coreConfig struct {
	JournalIdentity            string
	Shards, Producers, MaxRows int
	PartitionSeconds           int64
	BlindBatch                 bool
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
	cursor     uint64
	seal, stop bool
	ack        chan error
}
type coreQueue struct {
	slots       [2]atomic.Pointer[corePacket]
	write, read uint64 // exactly one producer and one owner respectively
}

func (q *coreQueue) pop() *corePacket {
	p := q.slots[q.read%2].Swap(nil)
	if p != nil {
		q.read++
	}
	return p
}
func (q *coreQueue) push(p *corePacket, dead <-chan struct{}) error {
	for !q.slots[q.write%2].CompareAndSwap(nil, p) {
		select {
		case <-dead:
			return errors.New("owner stopped")
		default:
		}
		time.Sleep(10 * time.Microsecond)
	}
	q.write++
	return nil
}

type coreOwner struct {
	hook                    func(*pebble.Snapshot, []row) error
	db                      *pebble.DB
	dir                     string
	cfg                     coreConfig
	meta                    coreFrontier
	queues                  []*coreQueue
	dead                    chan struct{}
	done                    chan error
	pub                     publication
	committed, parts, bytes atomic.Uint64
}
type coreEngine struct {
	owners []*coreOwner
	cfg    coreConfig
	stalls atomic.Uint64
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
	return openCoreWithHook(root, cfg, nil)
}
func openCoreWithHook(root string, cfg coreConfig, hook func(int, *pebble.Snapshot, []row) error) (*coreEngine, error) {
	if cfg.Shards < 1 || cfg.Producers < 1 || cfg.MaxRows < 1 || cfg.PartitionSeconds < 1 {
		return nil, os.ErrInvalid
	}
	if err := os.MkdirAll(root, 0750); err != nil {
		return nil, err
	}
	e := &coreEngine{cfg: cfg}
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
		opts.EventListener = &pebble.EventListener{WriteStallBegin: func(pebble.WriteStallBeginInfo) { e.stalls.Add(1) }}
		db, err := pebble.Open(dir, opts)
		if err != nil {
			return fail(err)
		}
		o := &coreOwner{db: db, dir: dir, cfg: cfg, dead: make(chan struct{}), done: make(chan error, 1)}
		e.owners = append(e.owners, o)
		if hook != nil {
			o.hook = func(snap *pebble.Snapshot, head []row) error { return hook(s, snap, head) }
		}
		o.meta = coreFrontier{Version: 1, Config: cfg, Cursors: make([]uint64, cfg.Producers), ExpiredBefore: -1 << 63}
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
		if o.meta.Version != 1 || o.meta.Config != cfg || len(o.meta.Cursors) != cfg.Producers {
			return fail(errors.New("owner format/config mismatch"))
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
			o.queues = append(o.queues, new(coreQueue))
		}
	}
	for _, o := range e.owners {
		o.pub.publish(o.db.NewSnapshot())
		go o.run()
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
	parts := make([][]row, e.cfg.Shards)
	for _, r := range rows {
		s := coreShard(r.ID, e.cfg.Shards)
		parts[s] = append(parts[s], r)
	}
	for _, rs := range parts {
		if len(rs) > e.cfg.MaxRows {
			return errors.New("producer batch exceeds owner head bound")
		}
	}
	var acks []chan error
	for s, o := range e.owners {
		p := &corePacket{rows: parts[s], cursor: cursor, seal: seal}
		if seal {
			p.ack = make(chan error, 1)
			acks = append(acks, p.ack)
		}
		if err := o.queues[producer].push(p, o.dead); err != nil {
			return err
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
func (o *coreOwner) run() {
	defer close(o.dead)
	head := make([]row, 0, o.cfg.MaxRows)
	pending := slices.Clone(o.meta.Cursors)
	deadline := time.Now().Add(time.Second)
	var headPublication time.Time
	stopped := make([]bool, o.cfg.Producers)
	left := len(stopped)
	var failure error
	flush := func() error {
		if slices.Equal(pending, o.meta.Cursors) {
			return nil
		}
		next := o.meta
		next.Cursors = slices.Clone(pending)
		next.NextPart++
		n, err := o.seal(head, next)
		if err != nil {
			return err
		}
		o.meta = next
		o.committed.Add(uint64(len(head)))
		o.parts.Add(1)
		o.bytes.Add(n)
		head = head[:0]
		deadline = time.Now().Add(time.Second)
		o.pub.publish(o.db.NewSnapshot())
		if o.hook != nil {
			return o.hook(o.db.NewSnapshot(), nil)
		}
		return nil
	}
	for left > 0 {
		found := false
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
			if failure == nil && p.cursor > pending[producer] {
				if len(head)+len(p.rows) > o.cfg.MaxRows {
					failure = flush()
				}
				if failure == nil {
					head = append(head, p.rows...)
					pending[producer] = p.cursor
				}
			}
			if failure == nil && (p.seal || len(head) >= o.cfg.MaxRows || time.Now().After(deadline)) {
				failure = flush()
			}
			if failure == nil && o.hook != nil && (p.seal || time.Since(headPublication) >= time.Second) {
				failure = o.hook(o.db.NewSnapshot(), slices.Clone(head))
				headPublication = time.Now()
			}
			if p.ack != nil {
				p.ack <- failure
			}
		}
		if !found {
			if failure == nil && time.Now().After(deadline) {
				failure = flush()
				deadline = time.Now().Add(time.Second)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
	if failure == nil {
		failure = flush()
	}
	o.done <- failure
}

// key = data-prefix / time-partition / metric-ID / archive / block-start /
// immutable-part / block-ordinal. Partition prefix permits one range deletion;
// the tuple inside a partition has the requested metric/archive/block layout.
func coreKey(r row, partition int64, part uint64, ordinal uint32) []byte {
	b := make([]byte, 41)
	b[0] = 1
	binary.BigEndian.PutUint64(b[1:9], uint64(partition)^(1<<63))
	binary.BigEndian.PutUint64(b[9:17], r.ID)
	binary.BigEndian.PutUint32(b[17:21], 0) // raw archive; authoritative rollups are separate work
	binary.BigEndian.PutUint64(b[21:29], uint64(r.Time)^(1<<63))
	binary.BigEndian.PutUint64(b[29:37], part)
	binary.BigEndian.PutUint32(b[37:], ordinal)
	return b
}
func corePartition(t, seconds int64) int64 {
	q := t / seconds
	if t%seconds < 0 {
		q--
	}
	return q
}
func (o *coreOwner) seal(head []row, next coreFrontier) (uint64, error) {
	slices.SortFunc(head, func(a, b row) int {
		pa, pb := corePartition(a.Time, o.cfg.PartitionSeconds), corePartition(b.Time, o.cfg.PartitionSeconds)
		if pa < pb {
			return -1
		}
		if pa > pb {
			return 1
		}
		return less(a, b)
	})
	// Both sinks receive exactly the same ordered blind puts, including frontier.
	batch := o.db.NewBatch()
	defer batch.Close()
	var writer *sstable.Writer
	path := filepath.Join(o.dir, fmt.Sprintf("stage-%020d.sst", o.meta.NextPart))
	var err error
	if !o.cfg.BlindBatch {
		f, e := vfs.Default.Create(path)
		if e != nil {
			return 0, e
		}
		writer = sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.SnappyCompression})
		defer os.Remove(path)
	}
	put := func(k, v []byte) error {
		if writer != nil {
			return writer.Set(k, v)
		}
		return batch.Set(k, v, nil)
	}
	for i := 0; i < len(head); {
		p := corePartition(head[i].Time, o.cfg.PartitionSeconds)
		if p < next.ExpiredBefore {
			return 0, errors.New("late point targets expired partition")
		}
		j := i + 1
		for j < len(head) && j < i+256 && head[j].ID == head[i].ID && corePartition(head[j].Time, o.cfg.PartitionSeconds) == p {
			j++
		}
		value := make([]byte, (j-i)*24)
		for k, r := range head[i:j] {
			b := value[k*24:]
			binary.LittleEndian.PutUint64(b, uint64(r.Time))
			binary.LittleEndian.PutUint64(b[8:], r.Bits)
			binary.LittleEndian.PutUint64(b[16:], r.Seq)
		}
		if err = put(coreKey(head[i], p, o.meta.NextPart, uint32(i)), value); err != nil {
			break
		}
		i = j
	}
	meta, e := json.Marshal(next)
	err = errors.Join(err, e)
	if err == nil {
		err = put(coreMetaKey, meta)
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
		// Pebble publishes every key in this SST atomically and durably in its
		// manifest. A frontier can never become visible without its blocks.
		if err = o.db.Ingest([]string{path}); err != nil {
			return 0, err
		}
		return uint64(st.Size()), nil
	}
	if err != nil {
		return 0, err
	}
	n := uint64(len(batch.Repr()))
	return n, batch.Commit(pebble.Sync)
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
	it, err := s.NewIter(&pebble.IterOptions{LowerBound: []byte{1}, UpperBound: []byte{2}})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []row
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if len(k) != 41 {
			return nil, errors.New("invalid core key")
		}
		if binary.BigEndian.Uint64(k[9:17]) != id {
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
	upper := make([]byte, 9)
	upper[0] = 1
	binary.BigEndian.PutUint64(upper[1:], uint64(before)^(1<<63))
	batch := db.NewBatch()
	defer batch.Close()
	if err = batch.DeleteRange([]byte{1}, upper, nil); err != nil {
		return err
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
