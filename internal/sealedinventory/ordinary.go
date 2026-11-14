package sealedinventory

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Ordinary rows are disjoint; every ordinary byte belongs to exactly one row.
// Position is relative to the sampled archive, never a claim about unseen SSTs.
type ordinaryRow struct {
	Archive                                                         uint16
	Step, RetentionSeconds                                          uint64
	SchemaSource, Layout, Position, Descriptor, Temperature, Reason string
	tally
}
type nativeID struct {
	file    uint64
	archive uint16
	block   uint32
}
type ordinaryDescriptor struct {
	count, bucket, packed uint32
	bucketed              bool
}
type archiveSchema struct{ step, count uint64 }
type sampleValue struct {
	value []byte
	seq   uint64
	found bool
}
type metricSample struct {
	last        map[nativeID]int64
	descriptors map[nativeID]ordinaryDescriptor
	schemas     map[[2]uint64]archiveSchema
	newest      map[[2]uint64]nativeID
	incomplete  bool
}
type cachedSample struct {
	metric string
	sample *metricSample
}
type ordinaryIndex struct {
	cutoff          int64
	readers         []*sstable.Reader
	cache           map[string]*list.Element
	order           *list.List
	runner          *runner
	metadataPaths   []string
	metadataReaders map[string]*list.Element
	readerOrder     *list.List
}

func splitContinuationKey(key []byte) (metric, suffix []byte, bucketed bool, bucket uint32) {
	if len(key) >= 22 && key[0] == 't' && key[5] == 'u' {
		return key[6:22], key[22:], true, binary.BigEndian.Uint32(key[1:5])
	}
	if len(key) >= 17 && key[0] == 'u' {
		return key[1:17], key[17:], false, 0
	}
	return nil, key, false, 0
}
func parseNative(suffix []byte) nativeID {
	return nativeID{binary.BigEndian.Uint64(suffix[1:9]), binary.BigEndian.Uint16(suffix[10:12]), binary.BigEndian.Uint32(suffix[12:16])}
}
func (r *runner) openOrdinaryIndex(paths []string) (*ordinaryIndex, error) {
	x := &ordinaryIndex{cache: map[string]*list.Element{}, order: list.New()}
	for _, path := range paths {
		reader, _, err := r.open(path)
		if err != nil {
			x.close()
			return nil, err
		}
		x.readers = append(x.readers, reader)
	}
	return x, nil
}

type metadataReader struct {
	path   string
	reader *sstable.Reader
}

// Only a small working set of metadata SST readers stays open. All source IO
// shares runner.open's O_RDONLY/O_NOFOLLOW and physical-byte rate limiter.
func (x *ordinaryIndex) metadataReader(path string) (*sstable.Reader, error) {
	if x.metadataReaders == nil {
		x.metadataReaders = map[string]*list.Element{}
		x.readerOrder = list.New()
	}
	if el := x.metadataReaders[path]; el != nil {
		x.readerOrder.MoveToFront(el)
		return el.Value.(metadataReader).reader, nil
	}
	if x.readerOrder.Len() == 32 {
		el := x.readerOrder.Back()
		old := el.Value.(metadataReader)
		if err := old.reader.Close(); err != nil {
			return nil, err
		}
		delete(x.metadataReaders, old.path)
		x.readerOrder.Remove(el)
	}
	r, _, err := x.runner.open(path)
	if err != nil {
		return nil, err
	}
	x.metadataReaders[path] = x.readerOrder.PushFront(metadataReader{path, r})
	return r, nil
}
func (x *ordinaryIndex) close() {
	for _, el := range x.metadataReaders {
		_ = el.Value.(metadataReader).reader.Close()
	}
	for _, r := range x.readers {
		_ = r.Close()
	}
}
func (x *ordinaryIndex) lookup(key []byte) (sampleValue, error) {
	var best sampleValue
	for _, reader := range x.readers {
		it, err := reader.NewIter(nil, nil)
		if err != nil {
			return best, err
		}
		k, v := it.SeekGE(key, 0)
		if k != nil && bytes.Equal(k.UserKey, key) && (!best.found || k.SeqNum() > best.seq) {
			value, _, e := v.Value(nil)
			if e != nil {
				_ = it.Close()
				return best, e
			}
			// SET only. Newer deletions/merges invalidate knowledge, not physical bytes.
			best = sampleValue{seq: k.SeqNum(), found: true}
			if k.Kind() == 1 {
				best.value = bytes.Clone(value)
			}
		}
		err = errors.Join(it.Error(), it.Close())
		if err != nil {
			return best, err
		}
	}
	return best, nil
}
func readArchiveSchemas(value []byte) ([]archiveSchema, error) {
	if len(value) < 2 || value[0] < 1 || value[0] > 2 || value[1] > 1 {
		return nil, errors.New("invalid sampled manifest")
	}
	body := value[2:]
	read := func() (uint64, error) {
		v, n := binary.Uvarint(body)
		if n <= 0 {
			return 0, errors.New("invalid sampled manifest integer")
		}
		body = body[n:]
		return v, nil
	}
	for i := 0; i < 2; i++ {
		if _, e := read(); e != nil {
			return nil, e
		}
	}
	n, e := read()
	if e != nil || n < 1 || n > 64 {
		return nil, errors.New("invalid sampled archive count")
	}
	a := make([]archiveSchema, int(n))
	for i := range a {
		for j := 0; j < 5; j++ {
			v, e := read()
			if e != nil {
				return nil, e
			}
			if j == 1 {
				a[i].step = v
			}
			if j == 2 {
				a[i].count = v
			}
		}
	}
	return a, nil
}
func (x *ordinaryIndex) metric(metric []byte, file uint64) (*metricSample, error) {
	name := string(binary.BigEndian.AppendUint64(bytes.Clone(metric), file))
	if el := x.cache[name]; el != nil {
		x.order.MoveToFront(el)
		return el.Value.(cachedSample).sample, nil
	}
	m := &metricSample{last: map[nativeID]int64{}, descriptors: map[nativeID]ordinaryDescriptor{}, schemas: map[[2]uint64]archiveSchema{}, newest: map[[2]uint64]nativeID{}}
	filePrefix := binary.BigEndian.AppendUint64(append(append([]byte{'u'}, metric...), 'f'), file)
	values := map[string]sampleValue{}
	// Read [file/b,file/c) and the exact file/h manifest. Never iterate point
	// groups from unsampled SSTs. The source directory must be an immutable pin;
	// this tool does not consult MANIFEST, memtables or LOCK.
	total := 0
	readMetadata := func(reader *sstable.Reader) error {
		for _, kind := range []byte{'b', 'h'} {
			lower := append(bytes.Clone(filePrefix), kind)
			upper := append(bytes.Clone(filePrefix), kind+1)
			it, e := reader.NewIter(lower, upper)
			if e != nil {
				return e
			}
			for k, v := it.SeekGE(lower, 0); k != nil && bytes.Compare(k.UserKey, upper) < 0; k, v = it.Next() {
				_, suffix, _, _ := splitContinuationKey(k.UserKey)
				if !(len(suffix) == 16 && kind == 'b' || len(suffix) == 10 && kind == 'h') {
					continue
				}
				if len(values) >= 8192 || total >= 2<<20 {
					m.incomplete = true
					break
				}
				old := values[string(suffix)]
				if old.found && old.seq >= k.SeqNum() {
					continue
				}
				value, _, err := v.Value(nil)
				if err != nil {
					_ = it.Close()
					return err
				}
				if len(value) > 2<<20-total {
					m.incomplete = true
					break
				}
				item := sampleValue{seq: k.SeqNum(), found: true}
				if k.Kind() == 1 {
					item.value = bytes.Clone(value)
				}
				values[string(suffix)] = item
				total += len(suffix) + len(value)
			}
			if err := errors.Join(it.Error(), it.Close()); err != nil {
				return err
			}
		}
		return nil
	}
	if len(x.metadataPaths) > 0 {
		for _, path := range x.metadataPaths {
			reader, err := x.metadataReader(path)
			if err != nil {
				return nil, err
			}
			if err := readMetadata(reader); err != nil {
				return nil, err
			}
		}
	} else {
		for _, reader := range x.readers {
			if err := readMetadata(reader); err != nil {
				return nil, err
			}
		}
	}
	for key, item := range values {
		suffix, v := []byte(key), item.value
		if len(v) == 0 {
			continue
		}
		if suffix[9] == 'h' {
			schemas, e := readArchiveSchemas(v)
			if e != nil {
				return nil, e
			}
			file := binary.BigEndian.Uint64(suffix[1:9])
			for i, a := range schemas {
				m.schemas[[2]uint64{file, uint64(i)}] = a
			}
			continue
		}
		if len(v) != 4 && len(v) != 8 && len(v) != 12 {
			return nil, errors.New("invalid sampled descriptor")
		}
		d := ordinaryDescriptor{count: binary.BigEndian.Uint32(v)}
		if len(v) >= 8 {
			d.bucketed = true
			d.bucket = binary.BigEndian.Uint32(v[4:8])
		}
		if len(v) == 12 {
			d.packed = binary.BigEndian.Uint32(v[8:])
		}
		m.descriptors[parseNative(suffix)] = d
	}
	newestTime := map[[2]uint64]int64{}
	for id, d := range m.descriptors {
		if d.count == 0 {
			continue
		}
		groupSize := uint32(128)
		if d.packed > 0 {
			groupSize = d.packed
		}
		suffix := make([]byte, 20)
		suffix[0] = 'f'
		binary.BigEndian.PutUint64(suffix[1:9], id.file)
		suffix[9] = 'c'
		binary.BigEndian.PutUint16(suffix[10:12], id.archive)
		binary.BigEndian.PutUint32(suffix[12:16], id.block)
		binary.BigEndian.PutUint32(suffix[16:], (d.count-1)/groupSize)
		key := append(append([]byte{'u'}, metric...), suffix...)
		if d.bucketed {
			key = append(binary.BigEndian.AppendUint32([]byte{'t'}, d.bucket), key...)
		}
		tail, e := x.lookup(key)
		if e != nil {
			return nil, e
		}
		if len(tail.value) == 0 {
			continue
		}
		var last int64
		if d.packed > 0 {
			if tail.value[0] != 3 {
				continue
			}
			points, e := chunkstore.DecodeSealedPoints(tail.value)
			if e != nil {
				return nil, e
			}
			if len(points) > 0 {
				last = points[len(points)-1].Timestamp
			}
		} else {
			if tail.value[0] != 2 {
				continue
			}
			var b chunkstore.PointBlock
			if e := chunkstore.DecodePointBlock(tail.value, &b); e != nil {
				return nil, e
			}
			last = b.Points[(d.count-1)%128].Timestamp
		}
		m.last[id] = last
		a := [2]uint64{id.file, uint64(id.archive)}
		if last > newestTime[a] {
			newestTime[a] = last
			m.newest[a] = id
		}
	}
	el := x.order.PushFront(cachedSample{name, m})
	x.cache[name] = el
	if x.order.Len() > 128 {
		last := x.order.Back()
		delete(x.cache, last.Value.(cachedSample).metric)
		x.order.Remove(last)
	}
	return m, nil
}
func (x *ordinaryIndex) row(key []byte) (ordinaryRow, error) {
	metric, suffix, bucketed, bucket := splitContinuationKey(key)
	row := ordinaryRow{Temperature: "unknown", SchemaSource: "key-layout/archive-only", Layout: "metric-prefixed", Position: "unknown", Descriptor: "missing", Reason: "descriptor_missing"}
	if bucketed {
		row.Layout = "time-bucketed"
	}
	if len(metric) == 0 || len(suffix) != 20 {
		return row, nil
	}
	id := parseNative(suffix)
	row.Archive = id.archive
	if suffix[9] == 'p' {
		row.Descriptor = "classic/no-native-descriptor"
		row.Reason = "classic_or_sidecar"
		return row, nil
	}
	m, err := x.metric(metric, id.file)
	if err != nil {
		return row, err
	}
	a := [2]uint64{id.file, uint64(id.archive)}
	if s, ok := m.schemas[a]; ok {
		row.Step = s.step
		row.RetentionSeconds = s.step * s.count
		row.SchemaSource = "store-manifest"
	}
	if newest, ok := m.newest[a]; ok {
		if newest == id {
			row.Position = "newest-observed/open-candidate"
		} else {
			row.Position = "older-than-observed-newest"
		}
	}
	if last := m.last[id]; last > 0 && x.cutoff > 0 {
		row.Temperature = "within-write-window"
		if last < x.cutoff {
			row.Temperature = "cold"
		}
	}
	d, ok := m.descriptors[id]
	if !ok {
		if m.incomplete {
			row.Reason = "metadata_budget"
		}
		return row, nil
	}
	row.Descriptor = fmt.Sprintf("bucketed=%t,count_le_128=%t,packed=%d,count_gt_max=%t", d.bucketed, d.count <= 128, d.packed, d.count > chunkstore.MaxColdPackRangePoints)
	group := binary.BigEndian.Uint32(suffix[16:])
	switch {
	case d.bucketed != bucketed || bucketed && d.bucket != bucket || uint64(group)*128 >= uint64(d.count):
		row.Reason = "descriptor_layout_or_count_mismatch"
	case d.packed != 0:
		row.Reason = "descriptor_packed/ordinary-version"
	case !d.bucketed:
		row.Reason = "non_bucketed/pre_relayout"
	case d.count <= 128:
		row.Reason = "count_le_128"
	case d.count > chunkstore.MaxColdPackRangePoints:
		row.Reason = "count_gt_max"
	case row.Temperature == "within-write-window":
		row.Reason = "hot_native_range"
	case row.Temperature == "cold":
		row.Reason = "cold_eligible_shape/benefit_or_backlog_unknown"
	default:
		row.Reason = "eligible_shape/temperature_or_benefit_unknown"
	}
	return row, nil
}
func ordinaryRowKey(r ordinaryRow) string {
	return fmt.Sprintf("%d/%d/%d/%s/%s/%s/%s/%s/%s", r.Archive, r.Step, r.RetentionSeconds, r.SchemaSource, r.Layout, r.Position, r.Descriptor, r.Temperature, r.Reason)
}
