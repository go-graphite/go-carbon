package carbon

import (
	"bytes"
	"fmt"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

// A batch never crosses a source/epoch boundary. Entries retain first-seen
// metric order and every metric's original point order, including corrections.
// Tagged names are ordering barriers: different spellings can normalize to the
// same cache key, so they must retain their original interleaving.
type captureGroup struct {
	metric cache.RecoveryMetric
	name   string
	data   []points.Point
}
type captureBatch struct {
	decoder     points.FrameDecoder
	point       points.Points
	index       map[string]int
	groups      []captureGroup
	used, count int
	cost        int64
}

func (b *captureBatch) prepare(raw []byte, sink *startupSink) error {
	if b.index == nil {
		b.index = make(map[string]int)
	}
	clear(b.index)
	for i := range b.used {
		b.groups[i].name = ""
		b.groups[i].metric = cache.RecoveryMetric{}
		b.groups[i].data = b.groups[i].data[:0]
	}
	b.used, b.count, b.cost = 0, 0, 0
	resolver, _ := sink.admission.(interface {
		ResolveRecoveryMetric([]byte) cache.RecoveryMetric
	})
	return b.decoder.Read(raw, func(name []byte, data []points.Point) {
		tagged := bytes.IndexByte(name, ';') >= 0
		if tagged {
			clear(b.index)
		}
		i, exists := b.index[string(name)]
		if !exists {
			i = b.used
			b.used++
			if i == len(b.groups) {
				b.groups = append(b.groups, captureGroup{})
			}
			g := &b.groups[i]
			if resolver != nil {
				g.metric = resolver.ResolveRecoveryMetric(name)
				g.name = g.metric.Name()
				b.cost += g.metric.OverheadBytes()
			} else {
				if namer, ok := sink.admission.(interface{ RecoveryMetricName([]byte) string }); ok {
					g.name = namer.RecoveryMetricName(name)
				} else {
					g.name = string(name)
				}
				b.cost += sink.admission.RecoveryMetricBytes(name)
			}
			if !tagged {
				b.index[g.name] = i
			}
		}
		b.groups[i].data = append(b.groups[i].data, data...)
		b.count += len(data)
	})
}
func (b *captureBatch) applyGroup(i int, source *cache.RecoverySource, sink *startupSink, live bool) {
	b.applyRange(i, 0, len(b.groups[i].data), source, sink, live)
}
func (b *captureBatch) applyRange(i, start, end int, source *cache.RecoverySource, sink *startupSink, live bool) {
	g := &b.groups[i]
	b.point.Metric, b.point.Data = g.name, g.data[start:end]
	if sink.groupedAdd != nil {
		sink.groupedAdd(g.metric, &b.point, source, live)
	} else {
		sink.add(&b.point, source)
	}
}
func (b *captureBatch) apply(source *cache.RecoverySource, sink *startupSink, live bool) {
	for i := range b.used {
		b.applyGroup(i, source, sink, live)
	}
}
func (b *captureBatch) checkCount(n int) error {
	if b.count != n {
		return fmt.Errorf("capture batch point count: got %d want %d", b.count, n)
	}
	return nil
}

// Capture append uses a bounded owned-name memo, independent of the replay
// owner's memo. No cache access is needed before startup attaches the sink.
type captureWriteNames struct {
	names map[string]string
	bytes int
}

func (*captureWriteNames) HasRecoveryCapacityBatch(int, int64) bool { return true }
func (*captureWriteNames) RecoveryMetricBytes([]byte) int64         { return 0 }
func (m *captureWriteNames) RecoveryMetricName(name []byte) string {
	if canonical, ok := m.names[string(name)]; ok {
		return canonical
	}
	if m.names == nil {
		m.names = make(map[string]string)
	}
	if len(m.names) >= 4096 || m.bytes+len(name) > 4<<20 {
		clear(m.names)
		m.bytes = 0
	}
	canonical := string(name)
	m.names[canonical] = canonical
	m.bytes += len(name)
	return canonical
}
func (b *captureBatch) appendBinary(dst []byte) []byte {
	for i := range b.used {
		g := &b.groups[i]
		p := points.Points{Metric: g.name, Data: g.data}
		dst = p.AppendBinary(dst)
	}
	return dst
}
