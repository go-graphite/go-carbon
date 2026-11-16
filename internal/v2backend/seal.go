package v2backend

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/partsops"
	"github.com/go-graphite/go-carbon/internal/partsrun"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

type kv struct{ key, value []byte }

func metaKey(id int) []byte {
	return binary.BigEndian.AppendUint32([]byte{255, 'V', '2', 1}, uint32(id))
}
func refPrefix(id int, name string) []byte {
	b := binary.BigEndian.AppendUint32([]byte{253, 'V', '2', 1}, uint32(id))
	b = binary.BigEndian.AppendUint32(b, uint32(len(name)))
	return append(b, []byte(name)...)
}
func (o *owner) recover() error {
	o.meta = checkpoint{Version: 2, Journal: o.s.log.Identity()}
	data, c, e := o.db.Get(metaKey(o.id))
	if e == nil {
		e = o.readCheckpoint(data)
		e = errors.Join(e, c.Close())
	} else if errors.Is(e, pebble.ErrNotFound) {
		e = nil
	}
	if e != nil {
		return e
	}
	if (o.meta.Version != 1 && o.meta.Version != 2) || o.meta.Journal != o.s.log.Identity() {
		return errors.New("owner checkpoint origin mismatch")
	}
	for _, m := range o.meta.Metrics {
		name := string(m.Name)
		if o.s.route(name) != o.id {
			return errors.New("catalog metric owner mismatch")
		}
		if _, ok := o.metrics[name]; ok {
			return errors.New("duplicate catalog metric")
		}
		cfg, e := rollupConfig(m.Policy)
		if e != nil {
			return e
		}
		digest, e := sealrollup.PolicyDigest(cfg)
		if e != nil {
			return e
		}
		if digest != m.Frontier.Policy || m.Frontier.Order > max(o.meta.Order, o.meta.Cursor) {
			return errors.New("catalog policy/frontier mismatch")
		}
		policy, e := o.s.opts.Policy(name)
		if e != nil {
			return e
		}
		if !reflect.DeepEqual(policy, m.Policy) {
			return errors.New("stored policy differs from configured policy")
		}
		m.slots = make([]map[int64]sealrollup.Sample, len(m.Policy.Archives))
		for i := range m.slots {
			if i+1 < len(m.slots) {
				m.slots[i] = make(map[int64]sealrollup.Sample)
			}
		}
		o.metrics[name] = m
	}
	// Recover the reducer's physical slot view once, before any intake. Normal
	// Append and seal never perform a Get/read-before-write against the database.
	it, e := o.db.NewIter(&pebble.IterOptions{LowerBound: []byte{'G', 'C', 'P', 3}, UpperBound: []byte{'G', 'C', 'P', 4}})
	if e != nil {
		return e
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		k, e := partsformat.Decode(it.Key())
		if e != nil {
			return e
		}
		if k.Archive == math.MaxUint32 || o.s.route(string(k.Metric)) != o.id {
			continue
		}
		m := o.metrics[string(k.Metric)]
		if m == nil || string(m.Group) != string(k.Group) || k.Archive != compactArchive && int(k.Archive) >= len(m.slots) {
			return errors.New("part has no matching durable policy")
		}
		install := func(r sealrollup.Record) error {
			a := m.Policy.Archives[r.Archive]
			if r.Order > m.Frontier.Order || r.Sample.Time%int64(a.Step) != 0 || (r.Sample.Time/int64(a.Step))%int64(a.Points) != r.Slot {
				return errors.New("invalid durable physical slot")
			}
			prior, ok := m.slots[r.Archive][r.Slot]
			if ok && prior.Sequence == r.Order && (prior.Time != r.Sample.Time || prior.Bits != r.Sample.Bits) {
				return errors.New("conflicting durable slot order")
			}
			if !ok || r.Order > prior.Sequence {
				p := r.Sample
				p.Sequence = r.Order
				if m.slots[r.Archive] != nil {
					m.slots[r.Archive][r.Slot] = p
				}
			}
			return nil
		}
		if k.Archive == compactArchive {
			if e := visitCompactRecords(it.Value(), m.Policy, nil, install); e != nil {
				return e
			}
		} else {
			r, e := sealrollup.DecodeRecord(int(k.Archive), k.BlockStart, it.Value())
			if e != nil {
				return e
			}
			if e = install(r); e != nil {
				return e
			}
		}
	}
	o.s.ingested.Add(o.meta.Points)
	return it.Error()
}

// sealTransaction owns borrowed staging buffers until exactly one terminal call.
// Preparation never advances catalogues, reducer slots, receipts or read views.
// The storage engine must atomically ingest every file before calling commit;
// a failed write/ingest calls abort instead. Publication is a separate step.
type sealTransaction struct {
	owner *owner
	files [][]kv
	apply func()
	done  bool
}

func (tx *sealTransaction) release() {
	if tx.owner == nil {
		return
	}
	o := tx.owner
	o.commitScratch.reset()
	clear(o.batchScratch)
	if cap(o.batchScratch) > retainedFrames {
		o.batchScratch = nil
	}
	if cap(o.sampleScratch) > 65536 {
		o.sampleScratch = nil
	}
	o.sealPending = false
	tx.files = nil
	tx.apply = nil
}
func (tx *sealTransaction) commit() {
	if tx.done {
		return
	}
	tx.done = true
	defer tx.release()
	if tx.apply != nil {
		tx.apply()
	}
}
func (tx *sealTransaction) abort() {
	if tx.done {
		return
	}
	tx.done = true
	tx.release()
}

func (o *owner) commit(frames []frame) error {
	tx, err := o.prepareSeal(frames)
	if err != nil {
		return err
	}
	defer tx.abort()
	if len(tx.files) == 0 {
		return nil
	}
	var e error
	var paths []string
	defer func() {
		for _, p := range paths {
			_ = os.Remove(p)
		}
	}()
	stage := func(rows []kv) error {
		if len(rows) == 0 {
			return nil
		}
		path := filepath.Join(o.dir, fmt.Sprintf("stage-%d-%04d.sst", time.Now().UnixNano(), len(paths)))
		f, e := vfs.Default.Create(path)
		if e != nil {
			return e
		}
		paths = append(paths, path)
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.SnappyCompression})
		for i, row := range rows {
			if i > 0 && bytes.Equal(rows[i-1].key, row.key) {
				_ = w.Close()
				return errors.New("duplicate key in immutable seal")
			}
			if e = w.Set(row.key, row.value); e != nil {
				_ = w.Close()
				return e
			}
		}
		return w.Close()
	}
	for _, rows := range tx.files {
		if e = stage(rows); e != nil {
			return e
		}
	}
	// Ingest synchronizes the manifest publication. Journal Sync has already
	// completed for every included original batch before this command was sent.
	if e = o.db.Ingest(paths); e != nil {
		return e
	}
	tx.commit()
	return o.publish()
}

func (o *owner) prepareSeal(frames []frame) (_ *sealTransaction, err error) {
	if len(frames) == 0 {
		return &sealTransaction{}, nil
	}
	if o.sealPending {
		return nil, errors.New("owner already has an unresolved seal")
	}
	started := time.Now()
	var part partsformat.PartID
	if _, e := rand.Read(part[:]); e != nil {
		return nil, e
	}
	scratch := &o.commitScratch
	byName := scratch.group(frames)
	tx := &sealTransaction{owner: o}
	o.sealPending = true
	defer func() {
		if err != nil {
			tx.abort()
		}
	}()
	var count uint64
	for _, f := range frames {
		if f.cursor <= o.meta.Cursor {
			return nil, errors.New("replayed committed batch in live seal")
		}
		count += uint64(len(f.points))
	}
	// Every data file covers exactly one portable (PG,vshard,partition) range.
	dataFiles := make(map[string][]kv)
	refs := make([]kv, 0, len(byName))
	var ops []kv
	scopes := make(map[string]*partsops.Batch)
	next := make(map[string]*metricState, len(byName))
	plans := make(map[string]sealrollup.SealPlan)
	var runWorkspace partsrun.Workspace
	for name, span := range byName {
		fs := scratch.frames[span.start:span.end]
		last := fs[len(fs)-1]
		m := o.metrics[name]
		if m == nil {
			m = &metricState{Name: []byte(name), Group: []byte(last.group), Policy: last.policy, slots: make([]map[int64]sealrollup.Sample, len(last.policy.Archives))}
			for i := range m.slots {
				if i+1 < len(m.slots) {
					m.slots[i] = make(map[int64]sealrollup.Sample)
				}
			}
		}
		if string(m.Group) != string(last.group) || !samePolicy(m.Policy, last.policy) {
			return nil, errors.New("metric placement/policy changed")
		}
		if m.prepared == nil {
			copyMetric := *m
			m = &copyMetric
			var e error
			m.config, e = rollupConfig(m.Policy)
			if e != nil {
				return nil, e
			}
			m.prepared, e = sealrollup.Prepare(m.config)
			if e != nil {
				return nil, e
			}
		}
		cfg, prepared := m.config, m.prepared
		total := 0
		for _, f := range fs {
			total += len(f.points)
		}
		if cap(o.sampleScratch) < total {
			o.sampleScratch = make([]sealrollup.Sample, total)
		} else {
			o.sampleScratch = o.sampleScratch[:total]
		}
		samples := o.sampleScratch
		batches := slices.Grow(o.batchScratch[:0], len(fs))
		for _, f := range fs {
			if string(f.group) != string(m.Group) || (f.resolved != last.resolved || f.resolved == nil) && !samePolicy(f.policy, m.Policy) {
				return nil, errors.New("batch policy or placement changed inside seal")
			}
			b := sealrollup.Batch{Order: f.order, Now: f.clock, Points: samples[:len(f.points):len(f.points)]}
			for i, p := range f.points {
				b.Points[i] = sealrollup.Sample{Time: p.Timestamp, Bits: math.Float64bits(p.Value), Sequence: uint64(i + 1)}
			}
			batches = append(batches, b)
			samples = samples[len(f.points):]
		}
		plan, e := prepared.PlanWithWorkspace(m.Frontier, func(a int, slot int64) (sealrollup.Sample, bool) { p, ok := m.slots[a][slot]; return p, ok }, nil, batches, &o.workspace)
		clear(batches)
		o.batchScratch = batches[:0]
		if e != nil {
			return nil, e
		}
		plans[name] = plan
		copyMetric := *m
		copyMetric.Frontier = plan.Next
		next[name] = &copyMetric
		vs := partsformat.Route(partsformat.ID(name))
		partition := last.clock / 86400
		base := partsformat.Key{Group: partsformat.Group(m.Group), VShard: vs, Partition: partition, Metric: partsformat.ID(name), Part: part}
		prefix, e := partsformat.PartitionPrefixForGroup(partsformat.Group(m.Group), vs, partition)
		if e != nil {
			return nil, e
		}
		file := string(prefix)
		key, e := partsformat.Encode(base)
		if e != nil {
			return nil, e
		}
		if o.s.core != nil {
			for start := 0; start < len(plan.Records); start += compactRecords {
				base.Archive, base.BlockStart, base.Ordinal = compactArchive, 0, uint32(start/compactRecords)
				runKey, e := partsformat.Encode(base)
				if e != nil {
					return nil, e
				}
				value, e := appendCompactRecords(nil, plan.Records[start:min(start+compactRecords, len(plan.Records))], &runWorkspace)
				if e != nil {
					return nil, e
				}
				dataFiles[file] = append(dataFiles[file], kv{scratch.bytes.copy(runKey), scratch.bytes.copy(value)})
			}
		} else {
			for _, r := range plan.Records {
				encodedKey := scratch.bytes.copy(key)
				// GCP3's final 16 bytes are archive, signed physical slot, ordinal.
				tail := encodedKey[len(encodedKey)-16:]
				binary.BigEndian.PutUint32(tail, uint32(r.Archive))
				binary.BigEndian.PutUint64(tail[4:], uint64(r.Slot)^(1<<63))
				dataFiles[file] = append(dataFiles[file], kv{encodedKey, scratch.bytes.copy(sealrollup.EncodeRecord(r))})
			}
		}
		// Receipts remain in the retained durable journal. Data, catalogue and
		// replay cursor/order still publish in a single atomic ingest.
		rkey := append(refPrefix(o.id, name), part[:]...)
		refs = append(refs, kv{rkey, slices.Clone(key[:len(key)-16])})
		// Generation 1 denotes this store's initial single origin only. This
		// backend has no import/recreate/delete API and grants no replica epoch.
		metadata := store.Metadata{MetricConfig: store.MetricConfig{Name: name, AggregationMethod: store.AggregationMethod(cfg.Method), XFilesFactor: cfg.XFilesFactor}, Generation: 1, Revision: last.cursor, LastUpdate: time.Unix(last.clock, 0)}
		for _, a := range m.Policy.Archives {
			metadata.Retentions = append(metadata.Retentions, store.Retention{Step: a.Step, Count: a.Points})
		}
		scope := partsops.Scope{PlacementGroup: partsformat.Group(m.Group), VShard: vs}
		keyScope := fmt.Sprintf("%x/%d", []byte(m.Group), vs)
		b := scopes[keyScope]
		if b == nil {
			rev := o.meta.Revisions[keyScope]
			b = &partsops.Batch{Scope: scope, ExpectedRevision: rev, Revision: rev + 1}
			scopes[keyScope] = b
		}
		b.Upserts = append(b.Upserts, partsops.Metric{Scope: scope, Metadata: metadata})
	}
	for _, b := range scopes {
		edits, e := partsops.EncodeEdits(*b)
		if e != nil {
			return nil, e
		}
		for _, edit := range edits {
			ops = append(ops, kv{edit.Key, edit.Value})
		}
	}
	meta := o.meta
	meta.Revisions = make(map[string]uint64, len(o.meta.Revisions)+len(scopes))
	for k, v := range o.meta.Revisions {
		meta.Revisions[k] = v
	}
	for k, b := range scopes {
		meta.Revisions[k] = b.Revision
	}
	meta.Cursor = frames[len(frames)-1].cursor
	meta.Order = frames[len(frames)-1].order
	meta.Points += count
	// Upgrade a legacy full JSON checkpoint once. Thereafter only changed
	// metrics and touched scope revisions are replaced alongside the small header.
	checkpointRows := make([]kv, 0, len(byName)+len(scopes)+1)
	metricPrefix, revisionPrefix := checkpointPrefix(o.id, 'M'), checkpointPrefix(o.id, 'R')
	for name, m := range next {
		if _, changed := byName[name]; changed || o.meta.Version == 1 {
			key := append(append(make([]byte, 0, len(metricPrefix)+len(name)), metricPrefix...), name...)
			checkpointRows = append(checkpointRows, kv{key, encodeMetric(m)})
		}
	}
	if o.meta.Version == 1 {
		for name, m := range o.metrics {
			if _, changed := next[name]; !changed {
				checkpointRows = append(checkpointRows, kv{append(append(make([]byte, 0, len(metricPrefix)+len(name)), metricPrefix...), name...), encodeMetric(m)})
			}
		}
	}
	for name, revision := range meta.Revisions {
		if _, changed := scopes[name]; changed || o.meta.Version == 1 {
			key := append(append(make([]byte, 0, len(revisionPrefix)+len(name)), revisionPrefix...), name...)
			checkpointRows = append(checkpointRows, kv{key, binary.BigEndian.AppendUint64(nil, revision)})
		}
	}
	meta.Version = 2
	meta.Metrics = nil // owner.metrics owns the live state; do not retain a second slice.
	checkpointRows = append(checkpointRows, kv{metaKey(o.id), encodeCheckpoint(meta)})
	for _, rows := range dataFiles {
		tx.files = append(tx.files, rows)
	}
	for _, rows := range [][]kv{ops, refs, checkpointRows} {
		if len(rows) > 0 {
			tx.files = append(tx.files, rows)
		}
	}
	for _, rows := range tx.files {
		sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })
		for i := 1; i < len(rows); i++ {
			if bytes.Equal(rows[i-1].key, rows[i].key) {
				return nil, errors.New("duplicate key in immutable seal")
			}
		}
	}
	sort.Slice(tx.files, func(i, j int) bool { return bytes.Compare(tx.files[i][0].key, tx.files[j][0].key) < 0 })
	for i := 1; i < len(tx.files); i++ {
		previous := tx.files[i-1]
		if bytes.Compare(previous[len(previous)-1].key, tx.files[i][0].key) >= 0 {
			return nil, errors.New("overlapping immutable seal files")
		}
	}
	tx.apply = func() {
		for name, plan := range plans {
			m := next[name]
			for _, r := range plan.Records {
				p := r.Sample
				p.Sequence = r.Order
				if m.slots[r.Archive] != nil {
					m.slots[r.Archive][r.Slot] = p
				}
			}
		}
		for name, m := range next {
			o.metrics[name] = m
		}
		o.meta = meta
		o.s.parts.Add(uint64(len(dataFiles)))
		o.s.sealNS.Store(int64(time.Since(started)))
	}
	return tx, nil
}
