package main

// Atomic seal integration experiment. This adapter accepts the original ordered
// metric batches, never the flattened prototype rows. Its numeric-ID key format
// is deliberately private to the experiment: production callers must use the
// placement-group/vshard codec and a durable ordered input-log adapter.
import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

type atomicRollupInput struct {
	ID      uint64
	Config  sealrollup.Config
	Batches []sealrollup.Batch
}
type atomicRollupState struct {
	Frontier sealrollup.Frontier
	Config   sealrollup.Config
	Slots    map[[2]int64]sealrollup.Sample
}
type atomicRollupOwner struct {
	db    *pebble.DB
	dir   string
	state map[uint64]*atomicRollupState
	part  uint64
	// Owner-local, exported by the enclosing engine's metrics collector.
	Errors uint64
	// Fault injection never publishes state before a successful atomic ingest.
	beforeIngest func() error
	afterIngest  func() error
	poisoned     bool
}
type rollupKV struct{ key, value []byte }

var atomicRollupPartKey = []byte{255, 'r'}

func rollupMetaKey(kind byte, id uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{255, kind}, id)
}
func rollupDataKey(part, id uint64, kind byte, a uint32, slot uint64) []byte {
	b := binary.BigEndian.AppendUint64([]byte{'S', 1}, part)
	b = binary.BigEndian.AppendUint64(b, id)
	b = append(b, kind)
	b = binary.BigEndian.AppendUint32(b, a)
	return binary.BigEndian.AppendUint64(b, slot)
}
func encodeRollupBatch(b sealrollup.Batch) []byte {
	out := binary.BigEndian.AppendUint64(nil, b.Order)
	out = binary.BigEndian.AppendUint64(out, uint64(b.Now))
	for _, p := range b.Points {
		out = binary.BigEndian.AppendUint64(out, uint64(p.Time))
		out = binary.BigEndian.AppendUint64(out, p.Bits)
		out = binary.BigEndian.AppendUint64(out, p.Sequence)
	}
	return out
}
func rollupView(s *atomicRollupState) sealrollup.View {
	if s == nil {
		return nil
	}
	return func(a int, slot int64) (sealrollup.Sample, bool) {
		p, ok := s.Slots[[2]int64{int64(a), slot}]
		return p, ok
	}
}

// openAtomicRollupOwner performs the only DB reads: reconstructing a committed
// immutable owner view on restart. A production implementation needs a bounded
// sealed-block cache; this experimental map retains every occupied ring slot.
func openAtomicRollupOwner(db *pebble.DB, dir string) (*atomicRollupOwner, error) {
	o := &atomicRollupOwner{db: db, dir: dir, state: map[uint64]*atomicRollupState{}}
	get := func(id uint64) *atomicRollupState {
		s := o.state[id]
		if s == nil {
			s = &atomicRollupState{Slots: map[[2]int64]sealrollup.Sample{}}
			o.state[id] = s
		}
		return s
	}
	it, err := db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	orders := map[uint64]map[[2]int64]uint64{}
	for it.First(); it.Valid(); it.Next() {
		k, v := it.Key(), it.Value()
		switch {
		case bytes.Equal(k, atomicRollupPartKey) || bytes.Equal(k, []byte{'T', 'r'}):
			if len(v) != 8 {
				return nil, errors.New("invalid rollup part frontier")
			}
			o.part = binary.BigEndian.Uint64(v)
		case len(k) == 10 && (k[0] == 255 || k[0] == 'T') && k[1] == 'p':
			if err = json.Unmarshal(v, &get(binary.BigEndian.Uint64(k[2:])).Config); err != nil {
				return nil, err
			}
		case len(k) == 10 && (k[0] == 255 || k[0] == 'T') && k[1] == 's':
			if err = json.Unmarshal(v, &get(binary.BigEndian.Uint64(k[2:])).Frontier); err != nil {
				return nil, err
			}
		case len(k) == 31 && k[0] == 'S' && k[1] == 1 && k[18] == 'a':
			id := binary.BigEndian.Uint64(k[10:18])
			a := int(binary.BigEndian.Uint32(k[19:23]))
			slot := int64(binary.BigEndian.Uint64(k[23:]))
			r, e := sealrollup.DecodeRecord(a, slot, v)
			if e != nil {
				return nil, e
			}
			if orders[id] == nil {
				orders[id] = map[[2]int64]uint64{}
			}
			pos := [2]int64{int64(a), slot}
			old, ok := get(id).Slots[pos]
			if r.Order == orders[id][pos] && ok && (old.Time != r.Sample.Time || old.Bits != r.Sample.Bits) {
				return nil, errors.New("conflicting rollup order")
			}
			if r.Order > orders[id][pos] {
				get(id).Slots[pos] = r.Sample
				orders[id][pos] = r.Order
			}
		}
	}
	if err = it.Error(); err != nil {
		return nil, err
	}
	for _, s := range o.state {
		policy, e := sealrollup.PolicyDigest(s.Config)
		if e != nil {
			return nil, e
		}
		if policy != s.Frontier.Policy {
			return nil, errors.New("rollup policy/frontier mismatch")
		}
	}
	return o, nil
}

// Seal writes original raw batches, all authoritative archives (including fine
// slots), exact policy, and replay frontier in ONE Pebble manifest edit. There is
// no disk read before writing. Plans see only the owner's immutable prior view.
// The caller invokes this on its owner thread, normally every second. Sources
// at/below the persisted frontier must be skipped before admission on replay.
func (o *atomicRollupOwner) Seal(inputs []atomicRollupInput) (written uint64, err error) {
	defer func() {
		if err != nil {
			o.Errors++
			log.Printf("ERROR immutable seal rollup failed: %v", err)
		}
	}()
	if o.poisoned {
		return 0, errors.New("rollup owner requires reopen after uncertain commit")
	}
	next := o.part + 1
	if next == 0 {
		return 0, errors.New("rollup part overflow")
	}
	plans := make([]sealrollup.SealPlan, len(inputs))
	seen := map[uint64]bool{}
	var data, meta []rollupKV
	for i, in := range inputs {
		if seen[in.ID] {
			return 0, errors.New("duplicate metric in rollup seal")
		}
		seen[in.ID] = true
		old := o.state[in.ID]
		var prior sealrollup.Frontier
		if old != nil {
			prior = old.Frontier
		}
		plans[i], err = sealrollup.Plan(in.Config, prior, rollupView(old), in.Batches)
		if err != nil {
			return 0, err
		}
		for _, b := range in.Batches {
			data = append(data, rollupKV{rollupDataKey(next, in.ID, 'r', 0, b.Order), encodeRollupBatch(b)})
		}
		for _, r := range plans[i].Records {
			data = append(data, rollupKV{rollupDataKey(next, in.ID, 'a', uint32(r.Archive), uint64(r.Slot)), sealrollup.EncodeRecord(r)})
		}
		frontier, e := json.Marshal(plans[i].Next)
		if e != nil {
			return 0, e
		}
		meta = append(meta, rollupKV{rollupMetaKey('s', in.ID), frontier})
		if old == nil {
			policy, e := json.Marshal(in.Config)
			if e != nil {
				return 0, e
			}
			meta = append(meta, rollupKV{rollupMetaKey('p', in.ID), policy})
		}
	}
	meta = append(meta, rollupKV{atomicRollupPartKey, binary.BigEndian.AppendUint64(nil, next)})
	var paths []string
	defer func() {
		for _, p := range paths {
			_ = os.Remove(p)
		}
	}()
	stage := func(kind string, rows []rollupKV) error {
		if len(rows) == 0 {
			return nil
		}
		slices.SortFunc(rows, func(a, b rollupKV) int { return bytes.Compare(a.key, b.key) })
		path := filepath.Join(o.dir, fmt.Sprintf("seal-stage-%020d-%s.sst", next, kind))
		paths = append(paths, path)
		f, e := vfs.Default.Create(path)
		if e != nil {
			return e
		}
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.SnappyCompression})
		for _, kv := range rows {
			if e = w.Set(kv.key, kv.value); e != nil {
				_ = w.Close()
				return e
			}
		}
		if e = w.Close(); e != nil {
			return e
		}
		st, e := os.Stat(path)
		if e != nil {
			return e
		}
		written += uint64(st.Size())
		return nil
	}
	if err = stage("data", data); err != nil {
		return 0, err
	}
	if err = stage("frontier", meta); err != nil {
		return 0, err
	}
	if o.beforeIngest != nil {
		if err = o.beforeIngest(); err != nil {
			return 0, err
		}
	}
	if err = o.db.Ingest(paths); err != nil {
		o.poisoned = true
		return 0, err
	}
	if o.afterIngest != nil {
		if err = o.afterIngest(); err != nil {
			o.poisoned = true
			return 0, err
		}
	}
	// No retained mutable input slices: configs and samples belong to this owner.
	for i, in := range inputs {
		s := o.state[in.ID]
		if s == nil {
			s = &atomicRollupState{Config: in.Config, Slots: map[[2]int64]sealrollup.Sample{}}
			s.Config.Archives = slices.Clone(in.Config.Archives)
			o.state[in.ID] = s
		}
		for _, r := range plans[i].Records {
			s.Slots[[2]int64{int64(r.Archive), r.Slot}] = r.Sample
		}
		s.Frontier = plans[i].Next
	}
	o.part = next
	return written, nil
}
