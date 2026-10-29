package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// OTLP/HTTP JSON, built by hand: no OTLP library is vendored. Only the fields
// used here are modelled; int64 values and timestamps are decimal strings as
// the OTLP JSON mapping requires.
type otlpKV struct {
	Key   string   `json:"key"`
	Value otlpAnyV `json:"value"`
}

type otlpAnyV struct {
	StringValue string `json:"stringValue"`
}

type otlpPoint struct {
	Attributes        []otlpKV `json:"attributes,omitempty"`
	StartTimeUnixNano string   `json:"startTimeUnixNano,omitempty"`
	TimeUnixNano      string   `json:"timeUnixNano"`
	AsInt             *string  `json:"asInt,omitempty"`
	AsDouble          *float64 `json:"asDouble,omitempty"`
}

type otlpSum struct {
	AggregationTemporality int         `json:"aggregationTemporality"` // 2: cumulative
	IsMonotonic            bool        `json:"isMonotonic"`
	DataPoints             []otlpPoint `json:"dataPoints"`
}

type otlpGauge struct {
	DataPoints []otlpPoint `json:"dataPoints"`
}

type otlpMetric struct {
	Name        string     `json:"name"`
	Description string     `json:"description,omitempty"`
	Sum         *otlpSum   `json:"sum,omitempty"`
	Gauge       *otlpGauge `json:"gauge,omitempty"`
}

type otlpRequest struct {
	ResourceMetrics []otlpResourceMetrics `json:"resourceMetrics"`
}

type otlpResourceMetrics struct {
	Resource struct {
		Attributes []otlpKV `json:"attributes"`
	} `json:"resource"`
	ScopeMetrics []otlpScopeMetrics `json:"scopeMetrics"`
}

type otlpScopeMetrics struct {
	Scope struct {
		Name string `json:"name"`
	} `json:"scope"`
	Metrics []otlpMetric `json:"metrics"`
}

// metricSet accumulates data points by metric name in insertion order.
type metricSet struct {
	start, now string
	base       []otlpKV
	metrics    []otlpMetric
	index      map[string]int
}

func newMetricSet(start, now time.Time, base []otlpKV) *metricSet {
	return &metricSet{start: strconv.FormatInt(start.UnixNano(), 10), now: strconv.FormatInt(now.UnixNano(), 10), base: base, index: map[string]int{}}
}

func (m *metricSet) point(labels []string) otlpPoint {
	p := otlpPoint{TimeUnixNano: m.now, Attributes: append([]otlpKV(nil), m.base...)}
	for i := 0; i+1 < len(labels); i += 2 {
		p.Attributes = append(p.Attributes, otlpKV{labels[i], otlpAnyV{labels[i+1]}})
	}
	return p
}

func (m *metricSet) metric(name, help string, sum bool) *otlpMetric {
	if i, ok := m.index[name]; ok {
		return &m.metrics[i]
	}
	om := otlpMetric{Name: name, Description: help}
	if sum {
		om.Sum = &otlpSum{AggregationTemporality: 2, IsMonotonic: true}
	} else {
		om.Gauge = &otlpGauge{}
	}
	m.index[name] = len(m.metrics)
	m.metrics = append(m.metrics, om)
	return &m.metrics[len(m.metrics)-1]
}

// counter adds a cumulative monotonic sum point. Names carry no _total
// suffix: the Prometheus translation appends it.
func (m *metricSet) counter(name, help string, v int64, labels ...string) {
	p := m.point(labels)
	p.StartTimeUnixNano = m.start
	s := strconv.FormatInt(v, 10)
	p.AsInt = &s
	om := m.metric(name, help, true)
	om.Sum.DataPoints = append(om.Sum.DataPoints, p)
}

func (m *metricSet) counterFloat(name, help string, v float64, labels ...string) {
	p := m.point(labels)
	p.StartTimeUnixNano = m.start
	p.AsDouble = &v
	om := m.metric(name, help, true)
	om.Sum.DataPoints = append(om.Sum.DataPoints, p)
}

func (m *metricSet) gauge(name, help string, v int64, labels ...string) {
	p := m.point(labels)
	s := strconv.FormatInt(v, 10)
	p.AsInt = &s
	om := m.metric(name, help, false)
	om.Gauge.DataPoints = append(om.Gauge.DataPoints, p)
}

func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// hostPort is the replica label: the host:port of a carbonserver URL.
func hostPort(base string) string {
	if u, err := url.Parse(base); err == nil && u.Host != "" {
		return u.Host
	}
	return base
}

// otlpMetrics snapshots the run's progress.
func (e *engine) otlpMetrics(start, now time.Time, base []otlpKV, running bool) []otlpMetric {
	m := newMetricSet(start, now, base)
	s := &e.stats
	mode := "write"
	if e.cfg.dryRun {
		mode = "dry_run"
	}
	m.gauge("replica_backfill_running", "1 while a backfill runs.", b2i(running), "mode", mode)
	m.gauge("replica_backfill_metrics_planned", "Metrics this run will scan (selected minus those a checkpoint skips).", s.names.Load()-s.resumed.Load())
	m.counter("replica_backfill_metrics_scanned", "Metrics compared with the local host.", s.scanned.Load())
	m.counter("replica_backfill_metrics_inactive", "Metrics skipped without a local point in the lookback.", s.inactive.Load())
	m.counter("replica_backfill_metrics_with_gaps", "Metrics with empty local slots in the window.", s.withGaps.Load())
	m.counter("replica_backfill_points_missing", "Empty local slots in the window.", s.missingSlots.Load())
	m.counter("replica_backfill_points_fillable", "Empty local slots a replica has.", s.fillable.Load())
	var sent int64
	if e.out != nil {
		sent = e.out.stats.points.Load()
	}
	m.counter("replica_backfill_points_sent", "Points written to the local receiver.", sent)
	m.counter("replica_backfill_step_mismatches", "Metrics a replica returned at another step.", s.stepMismatch.Load())
	type target struct {
		kind string
		c    *client
	}
	targets := []target{{"local", e.local}}
	for _, r := range e.replicas {
		targets = append(targets, target{"replica", r})
	}
	for _, t := range targets {
		hp := hostPort(t.c.base)
		st := &t.c.stats
		for _, r := range []struct {
			result string
			v      int64
		}{{"ok", st.ok.Load()}, {"error", st.errors.Load()}, {"retry", st.retryAttempts.Load()}, {"throttled", st.throttled.Load()}} {
			m.counter("replica_backfill_requests", "HTTP request attempts by result.", r.v, "target", t.kind, "replica", hp, "result", r.result)
		}
		m.counterFloat("replica_backfill_request_seconds", "Time spent in HTTP request attempts.", time.Duration(st.nanos.Load()).Seconds(), "target", t.kind, "replica", hp)
		m.counter("replica_backfill_response_bytes", "HTTP response body bytes.", st.bytesIn.Load(), "target", t.kind, "replica", hp)
	}
	var paused bool
	var pausedFor time.Duration
	if e.out != nil {
		paused, pausedFor = e.out.gate.pausedFor()
	}
	m.gauge("replica_backfill_write_paused", "1 while writes wait for room in the local cache.", b2i(paused))
	m.counterFloat("replica_backfill_write_paused_seconds", "Time writes waited for room in the local cache.", pausedFor.Seconds())
	m.gauge("replica_backfill_window_start_timestamp_seconds", "Start of the backfill window.", e.cfg.from)
	m.gauge("replica_backfill_window_end_timestamp_seconds", "End of the backfill window.", e.cfg.until)
	return m.metrics
}

// otlpPusher pushes the run's metrics to an OTLP/HTTP receiver on an interval
// and once more on close. Failures are logged and never stop the run.
type otlpPusher struct {
	url      string
	interval time.Duration
	http     *http.Client
	start    time.Time
	metrics  func(now time.Time, running bool) []otlpMetric
	stop     chan struct{}
	done     chan struct{}
}

func newOTLPPusher(endpoint string, interval time.Duration, attrs map[string]string, metrics func(start, now time.Time, base []otlpKV, running bool) []otlpMetric) *otlpPusher {
	u := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(u, "/v1/metrics") {
		u += "/v1/metrics"
	}
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var base []otlpKV
	for _, k := range keys {
		base = append(base, otlpKV{k, otlpAnyV{attrs[k]}})
	}
	p := &otlpPusher{url: u, interval: interval, http: &http.Client{Timeout: 5 * time.Second}, start: time.Now(),
		stop: make(chan struct{}), done: make(chan struct{})}
	p.metrics = func(now time.Time, running bool) []otlpMetric { return metrics(p.start, now, base, running) }
	return p
}

func (p *otlpPusher) run() {
	defer close(p.done)
	if p.interval <= 0 {
		return
	}
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			p.push(true)
		}
	}
}

// close stops the interval pushes and sends the final one (running = 0).
func (p *otlpPusher) close() {
	close(p.stop)
	<-p.done
	p.push(false)
}

func (p *otlpPusher) push(running bool) {
	var req otlpRequest
	rm := otlpResourceMetrics{}
	rm.Resource.Attributes = []otlpKV{{"service.name", otlpAnyV{"replica_backfill"}}}
	sm := otlpScopeMetrics{Metrics: p.metrics(time.Now(), running)}
	sm.Scope.Name = "replica-backfill"
	rm.ScopeMetrics = []otlpScopeMetrics{sm}
	req.ResourceMetrics = []otlpResourceMetrics{rm}
	body, err := json.Marshal(req)
	if err == nil {
		err = p.post(body)
	}
	if err != nil {
		log.Printf("otlp push to %s failed: %v", p.url, err)
	}
}

func (p *otlpPusher) post(body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// carbonLabels reads only the [prometheus.labels] table of a go-carbon config.
func carbonLabels(path string) (map[string]string, error) {
	var c struct {
		Prometheus struct {
			Labels map[string]string `toml:"labels"`
		} `toml:"prometheus"`
	}
	if _, err := toml.DecodeFile(path, &c); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return c.Prometheus.Labels, nil
}
