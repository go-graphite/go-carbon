// Package malloc returns C heap memory that glibc keeps after frees, and
// reports the C heap's size. In cgo builds Pebble allocates its block cache
// and memtables with malloc; glibc gives busy threads their own arenas and
// keeps freed chunks in them, so resident memory can grow well past what is
// in use (60 GiB in 975 arenas for 31 GiB of live data on one store host).
package malloc

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Stats is the C heap as glibc reports it.
type Stats struct {
	System uint64 // bytes obtained from the system: arenas plus mmapped chunks
	InUse  uint64 // bytes in allocated chunks
	Free   uint64 // bytes in free chunks, held by the arenas
}

// Supported reports whether this build can trim and report the C heap.
func Supported() bool { return supported }

// Trimmer calls malloc_trim periodically.
type Trimmer struct {
	trim  func()
	stats func() (Stats, bool) // nil unless stats were requested
	last  atomic.Pointer[Stats]
	stop  chan struct{}
	done  chan struct{}
	trims atomic.Uint64
	nanos atomic.Uint64
}

// Start trims every interval and, with withStats, reads the heap's stats after
// each trim. Both walk every arena's free chunks under that arena's lock, which
// takes seconds on a large fragmented heap, so neither runs on a scrape. It
// returns nil when interval is not positive or the build cannot trim.
func Start(interval time.Duration, withStats bool) *Trimmer {
	if interval <= 0 || !supported {
		return nil
	}
	var read func() (Stats, bool)
	if withStats {
		read = stats
	}
	return start(interval, trim, read)
}

func start(interval time.Duration, fn func(), read func() (Stats, bool)) *Trimmer {
	t := &Trimmer{trim: fn, stats: read, stop: make(chan struct{}), done: make(chan struct{})}
	go t.run(interval)
	return t
}

func (t *Trimmer) run(interval time.Duration) {
	defer close(t.done)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-tick.C:
			began := time.Now()
			t.trim()
			t.nanos.Add(uint64(time.Since(began)))
			t.trims.Add(1)
			if t.stats != nil {
				if s, ok := t.stats(); ok {
					t.last.Store(&s)
				}
			}
		}
	}
}

// Stop ends the trimmer and waits for a running trim to finish.
func (t *Trimmer) Stop() {
	if t == nil {
		return
	}
	close(t.stop)
	<-t.done
}

// Collector exports the trimmer's work and the heap stats it last read.
type Collector struct {
	trimmer atomic.Pointer[Trimmer]
}

// NewCollector returns a collector for the app's trimmer.
func NewCollector() *Collector { return &Collector{} }

// SetTrimmer makes the collector report t's trims; nil stops reporting them.
func (c *Collector) SetTrimmer(t *Trimmer) { c.trimmer.Store(t) }

var (
	systemDesc    = prometheus.NewDesc("malloc_system_bytes", "C heap bytes glibc obtained from the system: arenas plus mmapped chunks.", nil, nil)
	inUseDesc     = prometheus.NewDesc("malloc_inuse_bytes", "C heap bytes in allocated chunks.", nil, nil)
	freeDesc      = prometheus.NewDesc("malloc_free_bytes", "C heap bytes in free chunks that glibc's arenas hold.", nil, nil)
	trimsDesc     = prometheus.NewDesc("malloc_trims_total", "malloc_trim calls by the periodic trimmer.", nil, nil)
	trimNanosDesc = prometheus.NewDesc("malloc_trim_seconds_total", "Time spent in malloc_trim by the periodic trimmer.", nil, nil)
)

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{systemDesc, inUseDesc, freeDesc, trimsDesc, trimNanosDesc} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	t := c.trimmer.Load()
	if t == nil {
		return
	}
	ch <- prometheus.MustNewConstMetric(trimsDesc, prometheus.CounterValue, float64(t.trims.Load()))
	ch <- prometheus.MustNewConstMetric(trimNanosDesc, prometheus.CounterValue, float64(t.nanos.Load())/1e9)
	if s := t.last.Load(); s != nil {
		ch <- prometheus.MustNewConstMetric(systemDesc, prometheus.GaugeValue, float64(s.System))
		ch <- prometheus.MustNewConstMetric(inUseDesc, prometheus.GaugeValue, float64(s.InUse))
		ch <- prometheus.MustNewConstMetric(freeDesc, prometheus.GaugeValue, float64(s.Free))
	}
}
