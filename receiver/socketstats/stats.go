// Package socketstats samples TCP intake pressure without touching the receive path.
package socketstats

import (
	"net"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/prometheus/client_golang/prometheus"
)

type metric struct {
	name, help string
	counter    bool
}

var hostMetrics = map[string]metric{
	"ListenOverflows":      {"host_tcp_listen_overflows_total", "Connections rejected because the accept queue was full.", true},
	"ListenDrops":          {"host_tcp_listen_drops_total", "Connections dropped by listening sockets.", true},
	"TCPBacklogDrop":       {"host_tcp_backlog_drops_total", "Packets dropped from TCP socket backlogs.", true},
	"TCPRcvQDrop":          {"host_tcp_receive_queue_drops_total", "Packets dropped because TCP receive queues were full.", true},
	"PruneCalled":          {"host_tcp_prune_called_total", "TCP receive memory pruning attempts.", true},
	"RcvPruned":            {"host_tcp_receive_pruned_total", "TCP receive queues pruned under memory pressure.", true},
	"OfoPruned":            {"host_tcp_out_of_order_pruned_total", "TCP out-of-order queues pruned under memory pressure.", true},
	"TCPZeroWindowDrop":    {"host_tcp_zero_window_drops_total", "TCP packets dropped while the receive window was zero.", true},
	"TCPToZeroWindowAdv":   {"host_tcp_to_zero_window_advertisements_total", "TCP transitions to advertising a zero receive window.", true},
	"TCPWantZeroWindowAdv": {"host_tcp_want_zero_window_advertisements_total", "TCP attempts to advertise a zero receive window.", true},
	"InErrs":               {"host_tcp_input_errors_total", "TCP segments received with errors.", true},
	"InCsumErrors":         {"host_tcp_input_checksum_errors_total", "TCP segments received with checksum errors.", true},
}
var listenerMetrics = []metric{
	{"receiver_tcp_accept_queue_length", "Connections waiting to be accepted.", false},
	{"receiver_tcp_accept_queue_limit", "Maximum connections waiting to be accepted.", false},
	{"receiver_tcp_receive_queue_bytes", "Unread bytes in live connections on the listener address and port.", false},
	{"receiver_tcp_connections", "Live connections on the listener address and port, excluding pending handshakes and TIME_WAIT.", false},
}

var sampleMetrics = []metric{
	{"host_tcp_sample_success", "Whether the last TCP host counter sample succeeded.", false},
	{"receiver_tcp_sample_success", "Whether the last TCP socket sample succeeded.", false},
	{"receiver_tcp_sample_duration_seconds", "Time spent sampling TCP counters and socket queues.", false},
}

type queues struct{ length, limit, bytes, connections uint64 }
type snapshot struct {
	host              map[string]uint64
	listeners         map[string]queues
	hostOK, socketsOK bool
	duration          float64
}

// Collector owns a single sampler shared by Graphite stats and Prometheus.
// Host counters cover the current network namespace, including other processes.
// Listener gauges cover the bound address/port, including shared-port sockets.
type Collector struct {
	mu        sync.RWMutex
	listeners map[string]*net.TCPAddr
	current   snapshot
	stop      chan struct{}
	done      chan struct{}
	once      sync.Once
}

// New starts a ten-second sampler on Linux and returns nil elsewhere.
// listeners must contain the actual bound addresses (including assigned ports).
func New(listeners map[string]*net.TCPAddr) *Collector {
	if !supported {
		return nil
	}
	c := &Collector{listeners: listeners, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(c.done)
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			s := sample(c.listeners)
			c.mu.Lock()
			c.current = s
			c.mu.Unlock()
			select {
			case <-c.stop:
				return
			case <-tick.C:
			}
		}
	}()
	return c
}
func (c *Collector) Stop() { c.once.Do(func() { close(c.stop) }); <-c.done }
func (c *Collector) values(emit func(metric, string, float64)) {
	c.mu.RLock()
	s := c.current
	c.mu.RUnlock()
	for key, value := range s.host {
		emit(hostMetrics[key], "", float64(value))
	}
	for name, q := range s.listeners {
		for i, value := range []uint64{q.length, q.limit, q.bytes, q.connections} {
			emit(listenerMetrics[i], name, float64(value))
		}
	}
	boolValue := func(v bool) float64 {
		if v {
			return 1
		}
		return 0
	}

	emit(sampleMetrics[0], "", boolValue(s.hostOK))
	emit(sampleMetrics[1], "", boolValue(s.socketsOK))
	emit(sampleMetrics[2], "", s.duration)
}
func (c *Collector) Stat(send helper.StatCallback) {
	c.values(func(m metric, listener string, value float64) {
		name := m.name
		if listener != "" {
			name = listener + "." + name
		}
		send(name, value)
	})
}
func desc(m metric, listener bool) *prometheus.Desc {
	var labels []string
	if listener {
		labels = []string{"receiver"}
	}
	return prometheus.NewDesc(m.name, m.help, labels, nil)
}
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range hostMetrics {
		ch <- desc(m, false)
	}
	for _, m := range listenerMetrics {
		ch <- desc(m, true)
	}
	for _, m := range sampleMetrics {
		ch <- desc(m, false)
	}
}
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.values(func(m metric, listener string, value float64) {
		typ := prometheus.GaugeValue
		if m.counter {
			typ = prometheus.CounterValue
		}
		var labels []string
		if listener != "" {
			labels = []string{listener}
		}
		ch <- prometheus.MustNewConstMetric(desc(m, listener != ""), typ, value, labels...)
	})
}
