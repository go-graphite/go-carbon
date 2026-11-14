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
	return points.ReadBinaryBytesBorrowed(raw, func(name []byte, data []points.Point) {
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
				g.name = string(name)
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
func (b *captureBatch) apply(source *cache.RecoverySource, sink *startupSink, live bool) {
	for i := range b.used {
		g := &b.groups[i]
		if sink.groupedAdd != nil {
			sink.groupedAdd(g.metric, g.data, source, live)
		} else {
			p := points.Points{Metric: g.name, Data: g.data}
			// The cache consumes data synchronously, just like the borrowed decoder.
			sink.add(&p, source)
		}
	}
}
func (b *captureBatch) checkCount(n int) error {
	if b.count != n {
		return fmt.Errorf("capture batch point count: got %d want %d", b.count, n)
	}
	return nil
}
