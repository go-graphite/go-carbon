package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// otlpReceiver accepts OTLP/HTTP JSON pushes and validates their shape.
type otlpReceiver struct {
	t      *testing.T
	mu     sync.Mutex
	pushes []map[string]float64 // series key -> value, per push
	srv    *httptest.Server
}

var nanos = regexp.MustCompile(`^[0-9]{19}$`)

func newOTLPReceiver(t *testing.T) *otlpReceiver {
	r := &otlpReceiver{t: t}
	r.srv = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *otlpReceiver) serve(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/v1/metrics" || req.Method != http.MethodPost || req.Header.Get("Content-Type") != "application/json" {
		r.t.Errorf("unexpected request %s %s %q", req.Method, req.URL.Path, req.Header.Get("Content-Type"))
		http.Error(w, "bad", http.StatusBadRequest)
		return
	}
	body, _ := io.ReadAll(req.Body)
	// Decode generically: the checks must not depend on the sender's types.
	var doc struct {
		ResourceMetrics []struct {
			Resource struct {
				Attributes []struct {
					Key   string
					Value map[string]any
				}
			}
			ScopeMetrics []struct {
				Metrics []map[string]json.RawMessage
			}
		}
	}
	if err := json.Unmarshal(body, &doc); err != nil || len(doc.ResourceMetrics) != 1 || len(doc.ResourceMetrics[0].ScopeMetrics) != 1 {
		r.t.Errorf("invalid OTLP document: %v %s", err, body)
		return
	}
	rm := doc.ResourceMetrics[0]
	if len(rm.Resource.Attributes) != 1 || rm.Resource.Attributes[0].Key != "service.name" || rm.Resource.Attributes[0].Value["stringValue"] != "replica_backfill" {
		r.t.Errorf("resource attributes %+v", rm.Resource.Attributes)
	}
	type point struct {
		Attributes []struct {
			Key   string
			Value map[string]string
		}
		StartTimeUnixNano *string
		TimeUnixNano      *string
		AsInt             *string
		AsDouble          *float64
	}
	push := map[string]float64{}
	for _, m := range rm.ScopeMetrics[0].Metrics {
		var name string
		_ = json.Unmarshal(m["name"], &name)
		if _, ok := m["unit"]; ok || strings.HasSuffix(name, "_total") || !strings.HasPrefix(name, "replica_backfill_") {
			r.t.Errorf("metric %q: no unit, no _total suffix, replica_backfill_ prefix expected", name)
		}
		var points []point
		switch {
		case m["sum"] != nil:
			var s struct {
				AggregationTemporality *int
				IsMonotonic            *bool
				DataPoints             []point
			}
			if err := json.Unmarshal(m["sum"], &s); err != nil || s.AggregationTemporality == nil || *s.AggregationTemporality != 2 || s.IsMonotonic == nil || !*s.IsMonotonic {
				r.t.Errorf("sum %s: want cumulative monotonic: %s", name, m["sum"])
			}
			points = s.DataPoints
			for _, p := range points {
				if p.StartTimeUnixNano == nil || !nanos.MatchString(*p.StartTimeUnixNano) {
					r.t.Errorf("sum %s: start time %v", name, p.StartTimeUnixNano)
				}
			}
		case m["gauge"] != nil:
			var g struct{ DataPoints []point }
			if err := json.Unmarshal(m["gauge"], &g); err != nil {
				r.t.Errorf("gauge %s: %v", name, err)
			}
			points = g.DataPoints
		default:
			r.t.Errorf("metric %s is neither sum nor gauge", name)
		}
		if len(points) == 0 {
			r.t.Errorf("metric %s has no data points", name)
		}
		for _, p := range points {
			if p.TimeUnixNano == nil || !nanos.MatchString(*p.TimeUnixNano) {
				r.t.Errorf("%s: time %v", name, p.TimeUnixNano)
			}
			key := name
			for _, a := range p.Attributes {
				if a.Key == "carbon_cluster" || a.Key == "carbon_location" || a.Key == "carbon_ring" {
					continue
				}
				key += "|" + a.Key + "=" + a.Value["stringValue"]
			}
			if got := attr(p.Attributes, "carbon_cluster") + "/" + attr(p.Attributes, "carbon_location") + "/" + attr(p.Attributes, "carbon_ring"); got != "c1/region_a/override" {
				r.t.Errorf("%s: carbon labels %q", name, got)
			}
			switch {
			case p.AsInt != nil:
				v, err := strconv.ParseInt(*p.AsInt, 10, 64)
				if err != nil {
					r.t.Errorf("%s: asInt %q", name, *p.AsInt)
				}
				push[key] = float64(v)
			case p.AsDouble != nil:
				push[key] = *p.AsDouble
			default:
				r.t.Errorf("%s: no value", name)
			}
		}
	}
	r.mu.Lock()
	r.pushes = append(r.pushes, push)
	r.mu.Unlock()
}

func attr(attrs []struct {
	Key   string
	Value map[string]string
}, key string) string {
	for _, a := range attrs {
		if a.Key == key {
			return a.Value["stringValue"]
		}
	}
	return ""
}

func TestOTLPPushMatchesReport(t *testing.T) {
	t.Parallel()
	local, rep := newFake(t), newFake(t)
	for i := range 30 {
		n := "o." + strconv.Itoa(10+i)
		local.fill(n, base-5*step, func(int64) float64 { return 1 }, func(ts int64) bool { return ts == base+step || ts == base+2*step })
		rep.fill(n, base, func(int64) float64 { return 2 }, nil)
	}
	local.set("o.inactive", base-86400, 1)
	rep.fail = func(_ []string, n int) int {
		if n == 1 {
			return http.StatusTooManyRequests
		}
		return 0
	}
	recv := newOTLPReceiver(t)
	cfgPath := filepath.Join(t.TempDir(), "go-carbon.conf")
	conf := "[common]\nmax-cpu = 4\n\n[prometheus]\nenabled = true\n\n[prometheus.labels]\ncarbon_cluster = \"c1\"\ncarbon_location = \"region_a\"\ncarbon_ring = \"r1\"\n"
	if err := os.WriteFile(cfgPath, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	in := newIngest(t, nil)
	o := testOptions(local, in.ln.Addr().String(), rep)
	o.batch, o.workers, o.replicaRPS = 10, 1, 8 // about 0.5s: several interval pushes
	o.otlpEndpoint, o.otlpInterval, o.carbonConfig = recv.srv.URL, 40*time.Millisecond, cfgPath
	o.otlpLabels = labelList{"carbon_ring": "override"}
	r := runOK(t, o)

	recv.mu.Lock()
	pushes := append([]map[string]float64(nil), recv.pushes...)
	recv.mu.Unlock()
	if len(pushes) < 3 {
		t.Fatalf("%d pushes, want interval pushes and a final one", len(pushes))
	}
	if pushes[0]["replica_backfill_running|mode=write"] != 1 {
		t.Fatalf("first push running = %v", pushes[0]["replica_backfill_running|mode=write"])
	}
	final := pushes[len(pushes)-1]
	repHost := strings.TrimPrefix(rep.srv.URL, "http://")
	localHost := strings.TrimPrefix(local.srv.URL, "http://")
	want := map[string]float64{
		"replica_backfill_running|mode=write":                                               0,
		"replica_backfill_metrics_planned":                                                  float64(r.Metrics["selected"]),
		"replica_backfill_metrics_scanned":                                                  float64(r.Metrics["scanned"]),
		"replica_backfill_metrics_inactive":                                                 1,
		"replica_backfill_metrics_with_gaps":                                                float64(r.Metrics["with_gaps"]),
		"replica_backfill_points_missing":                                                   float64(r.Metrics["missing_slots"]),
		"replica_backfill_points_fillable":                                                  float64(r.Metrics["fillable_points"]),
		"replica_backfill_points_sent":                                                      float64(r.Write["points_sent"]),
		"replica_backfill_step_mismatches":                                                  0,
		"replica_backfill_requests|target=replica|replica=" + repHost + "|result=ok":        3,
		"replica_backfill_requests|target=replica|replica=" + repHost + "|result=throttled": 1,
		"replica_backfill_requests|target=replica|replica=" + repHost + "|result=error":     0,
		"replica_backfill_requests|target=local|replica=" + localHost + "|result=ok":        float64(r.Local["requests"]),
		"replica_backfill_response_bytes|target=replica|replica=" + repHost:                 float64(r.Replicas[rep.srv.URL]["bytes_in"]),
		"replica_backfill_write_paused":                                                     0,
		"replica_backfill_write_paused_seconds":                                             0,
		"replica_backfill_window_start_timestamp_seconds":                                   float64(r.From),
		"replica_backfill_window_end_timestamp_seconds":                                     float64(r.Until),
	}
	for k, v := range want {
		if got, ok := final[k]; !ok || got != v {
			t.Errorf("%s = %v (present %v), want %v", k, got, ok, v)
		}
	}
	if final["replica_backfill_points_sent"] != 60 || final["replica_backfill_request_seconds|target=replica|replica="+repHost] <= 0 {
		t.Errorf("final push %v", final)
	}
	// Cumulative sums never decrease between pushes.
	gauges := map[string]bool{"replica_backfill_running|mode=write": true, "replica_backfill_metrics_planned": true, "replica_backfill_write_paused": true}
	for i := 1; i < len(pushes); i++ {
		for k, v := range pushes[i-1] {
			if !gauges[k] && pushes[i][k] < v {
				t.Errorf("%s decreased from %v to %v", k, v, pushes[i][k])
			}
		}
	}
}

func TestOTLPPushFailureIsNotFatal(t *testing.T) {
	t.Parallel()
	local, rep := newFake(t), newFake(t)
	local.fill("x", base-5*step, func(int64) float64 { return 1 }, func(ts int64) bool { return ts == base+step })
	rep.fill("x", base, func(int64) float64 { return 2 }, nil)
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "down", http.StatusServiceUnavailable) }))
	defer dead.Close()
	in := newIngest(t, nil)
	o := testOptions(local, in.ln.Addr().String(), rep)
	o.otlpEndpoint, o.otlpInterval = dead.URL, 10*time.Millisecond
	if r := runOK(t, o); r.Write["points_sent"] != 1 {
		t.Fatalf("report %+v", r.Write)
	}
	if _, err := run(context.Background(), o, time.Unix(base+3600, 0)); err != nil {
		t.Fatal(err)
	}
}
