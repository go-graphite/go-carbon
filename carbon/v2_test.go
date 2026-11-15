package carbon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/helper/carbonpb"
	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/go-graphite/go-whisper"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

// oracleBackend tests the daemon boundary with a real classic Whisper engine.
// It deliberately is NOT an immutable-parts implementation or a v2 parity proof.
type oracleBackend struct {
	mu                            sync.Mutex
	w                             *whisper.Whisper
	count                         uint64
	accepted                      chan struct{}
	cacheEntered                  chan struct{}
	cacheCanceled                 chan struct{}
	cacheReturn                   chan struct{}
	errors                        chan daemonstore.Failure
	appendErr, flushErr, closeErr error
	closed                        bool
	events                        []string
}

func (b *oracleBackend) Append(p *points.Points) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.appendErr != nil {
		return b.appendErr
	}
	batch := make([]*whisper.TimeSeriesPoint, len(p.Data))
	for i, d := range p.Data {
		batch[i] = &whisper.TimeSeriesPoint{Time: int(d.Timestamp), Value: d.Value}
	}
	if b.w != nil {
		if err := b.w.UpdateMany(batch); err != nil {
			return err
		}
	}
	b.count += uint64(len(p.Data))
	b.events = append(b.events, "append")
	if b.accepted != nil {
		select {
		case b.accepted <- struct{}{}:
		default:
		}
	}
	return nil
}
func (b *oracleBackend) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, "flush")
	return b.flushErr
}
func (b *oracleBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.events = append(b.events, "close")
	if b.w != nil {
		_ = b.w.Close()
	}
	return b.closeErr
}
func (b *oracleBackend) Errors() <-chan daemonstore.Failure { return b.errors }
func (b *oracleBackend) Stats() daemonstore.Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return daemonstore.Stats{IngestedPoints: b.count}
}
func (b *oracleBackend) Fetch(_ context.Context, name string, from, until int) (*daemonstore.Series, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if name != "routing.value" {
		return nil, daemonstore.ErrNotFound
	}
	s, err := b.w.Fetch(from, until)
	if err != nil || s == nil {
		return nil, err
	}
	return &daemonstore.Series{Metric: oracleMetric(), From: s.FromTime(), Until: s.UntilTime(), Step: s.Step(), Values: s.Values()}, nil
}
func oracleMetric() daemonstore.Metric {
	return daemonstore.Metric{AggregationMethod: "average", XFilesFactor: 0.5, Archives: []daemonstore.Archive{{Step: 1, Points: 3600}}}
}
func (b *oracleBackend) Info(context.Context, string) (daemonstore.Metric, error) {
	return oracleMetric(), nil
}
func (b *oracleBackend) Find(_ context.Context, pattern string) ([]daemonstore.Match, error) {
	if pattern == "routing.value" || pattern == "routing.*" {
		return []daemonstore.Match{{Name: "routing.value", Leaf: true}}, nil
	}
	return nil, nil
}
func (b *oracleBackend) CacheQuery(ctx context.Context, _ string) ([]points.Point, error) {
	if b.cacheEntered != nil {
		close(b.cacheEntered)
		<-ctx.Done()
		if b.cacheCanceled != nil {
			close(b.cacheCanceled)
			<-b.cacheReturn
		}
		return nil, ctx.Err()
	}
	return []points.Point{{Timestamp: 123, Value: math.Copysign(0, -1)}}, nil
}

func TestV2StopCancelsAndJoinsBackendReads(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	b := &oracleBackend{cacheEntered: make(chan struct{}), cacheCanceled: make(chan struct{}), cacheReturn: make(chan struct{})}
	release := sync.OnceFunc(func() { close(b.cacheReturn) })
	defer release()
	app := v2Fixture(t, b)
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	reader := app.v2
	done := make(chan error, 1)
	go func() {
		_, err := reader.CacheQuery(context.Background(), "routing.value")
		done <- err
	}()
	select {
	case <-b.cacheEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("backend query was not entered")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- app.DumpStop() }()
	select {
	case <-b.cacheCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not cancel backend read")
	}
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		t.Fatal("backend closed before read returned")
	}
	select {
	case <-stopped:
		t.Fatal("stop did not wait for active read")
	default:
	}
	release()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not join completed read")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("stop did not cancel backend read", err)
	}
	if _, err := reader.Find(context.Background(), "routing.*"); !errors.Is(err, context.Canceled) {
		t.Fatal("closed backend admitted another read", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		t.Fatal("backend close was skipped")
	}
}

func v2Fixture(t *testing.T, b *oracleBackend) *App {
	t.Helper()
	file := testConfig(t, t.TempDir())
	app := New(file)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	c := app.Config
	c.V2.Enabled = true
	c.V2.DataDir = filepath.Join(t.TempDir(), "parts")
	c.Dump.Enabled = false
	c.InputLog.Enabled = false
	c.Tags.Enabled = false
	c.Buckyd.Enabled = false
	c.Common.MetricEndpoint = "tcp://127.0.0.1:1" // no self metrics in input fixture
	c.Carbonserver.Enabled = true
	c.Carbonserver.TrieIndex = false
	c.Carbonserver.TrigramIndex = false
	c.Carbonserver.CacheScan = false
	c.Carbonserver.QueryCacheEnabled = false
	c.Carbonserver.StreamingQueryCacheEnabled = false
	c.Carbonserver.FindCacheEnabled = false
	c.Carbonserver.GlobCacheEnabled = false
	c.Carbonserver.NoServiceWhenIndexIsNotReady = true
	c.Prometheus.Enabled = true
	c.Udp.Enabled = false
	c.Pickle.Enabled = false
	c.Tcp.Enabled = true
	app.v2Open = func(o daemonstore.Options) (daemonstore.Backend, error) {
		if o.Owners < 1 || o.Shards < 1 || o.PartInterval <= 0 || o.Policy == nil {
			t.Fatal("missing open options")
		}
		return b, nil
	}
	t.Cleanup(app.Stop)
	return app
}

func TestV2DaemonRoutingWithClassicOracle(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	ret, err := whisper.ParseRetentionDefs("1s:1h")
	if err != nil {
		t.Fatal(err)
	}
	w, err := whisper.Create(filepath.Join(t.TempDir(), "oracle.wsp"), ret, whisper.Average, 0.5)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := whisper.Create(filepath.Join(t.TempDir(), "reference.wsp"), ret, whisper.Average, 0.5)
	if err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	defer reference.Close()
	b := &oracleBackend{w: w, accepted: make(chan struct{}, 32)}
	app := v2Fixture(t, b)
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	if app.Persister != nil || app.MetricStore != nil {
		t.Fatal("v2 opened a legacy writer")
	}
	now := time.Now().Unix()
	from, until := now-20, now-1
	values := []float64{1.2345678901234567, math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64, -7.25}
	referencePoints := make([]*whisper.TimeSeriesPoint, len(values))
	for i, value := range values {
		referencePoints[i] = &whisper.TimeSeriesPoint{Time: int(from) + i + 1, Value: value}
	}
	if err := reference.UpdateMany(referencePoints); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", app.Receivers[0].Receiver.(*tcp.TCP).Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range values {
		if _, err := fmt.Fprintf(conn, "routing.value %.17g %d\n", v, from+int64(i)+1); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.Close()
	deadline := time.After(5 * time.Second)
	for b.Stats().IngestedPoints < uint64(len(values)) {
		select {
		case <-b.accepted:
		case <-deadline:
			t.Fatal("TCP input did not reach backend")
		}
	}
	if !app.Cache.IsEmpty() {
		t.Fatal("v2 input leaked into legacy cache")
	}
	address := "http://" + app.Carbonserver.HTTPListener().Addr().String()
	request := &protov3.MultiFetchRequest{Metrics: []*protov3.FetchRequest{{Name: "routing.value", PathExpression: "routing.value", StartTime: from, StopTime: until}}}
	encoded, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	res, err := client.Post(address+"/render/?format=carbonapi_v3_pb", "application/x-carbonapi-v3-pb", bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("render %d: %s", res.StatusCode, body)
	}
	var payload protov3.MultiFetchResponse
	if err := proto.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode: %v: %s", err, body)
	}
	if len(payload.Metrics) != 1 {
		t.Fatalf("metrics %v", payload.Metrics)
	}
	want, err := reference.Fetch(int(from), int(until))
	if err != nil {
		t.Fatal(err)
	}
	got := payload.Metrics[0]
	if got.StartTime != int64(want.FromTime()) || got.StopTime != int64(want.UntilTime()) || got.StepTime != int64(want.Step()) || len(got.Values) != len(want.Values()) {
		t.Fatal("response grid differs")
	}
	for i, v := range want.Values() {
		if math.Float64bits(got.Values[i]) != math.Float64bits(v) {
			t.Fatalf("slot %d bits %x != %x", i, math.Float64bits(got.Values[i]), math.Float64bits(v))
		}
	}
	for _, path := range []string{"/metrics/find/?query=routing.*&format=json", "/info/?target=routing.value&format=json"} {
		r, e := client.Get(address + path)
		if e != nil {
			t.Fatal(e)
		}
		data, e := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if e != nil || r.StatusCode != 200 || !strings.Contains(string(data), "routing.value") {
			t.Fatalf("%s %d %s %v", path, r.StatusCode, data, e)
		}
	}
	cache, err := app.Api.CacheQuery(context.Background(), &carbonpb.CacheRequest{Metrics: []string{"routing.value"}})
	if err != nil || len(cache.Metrics) != 1 || math.Float64bits(cache.Metrics[0].Points[0].Value) != math.Float64bits(math.Copysign(0, -1)) {
		t.Fatalf("gRPC cache query %v %v", cache, err)
	}
	families, err := app.PromRegistry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	for _, name := range []string{"http_request_duration_seconds_exp", "http_handler_duration_seconds_exp", "disk_wait_seconds_exp", "go_carbon_v2_ingested_points_total", "go_carbon_v2_read_duration_seconds"} {
		if !names[name] {
			t.Fatal("missing metric", name)
		}
	}
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed || len(b.events) < 2 || strings.Join(b.events[len(b.events)-2:], ",") != "flush,close" {
		t.Fatal("incorrect flush/close order", b.events)
	}
}

func TestV2FlagFailsClosedWithoutBackend(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	app := v2Fixture(t, &oracleBackend{})
	app.v2Open = nil
	if err := app.Start(); !errors.Is(err, daemonstore.ErrUnavailable) {
		t.Fatal("expected unavailable backend", err)
	}
	if len(app.Receivers) != 0 || app.Persister != nil || app.MetricStore != nil {
		t.Fatal("failed startup exposed a writer/listener")
	}
	families, err := app.PromRegistry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == "go_carbon_v2_startup_errors_total" && f.Metric[0].GetCounter().GetValue() == 1 {
			return
		}
	}
	t.Fatal("startup failure was not counted")
}

func TestV2StopReportsFlushAndCloseFailures(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	failure := errors.New("durability barrier failed")
	for _, op := range []string{"flush", "close"} {
		t.Run(op, func(t *testing.T) {
			b := &oracleBackend{}
			if op == "flush" {
				b.flushErr = failure
			} else {
				b.closeErr = failure
			}
			app := v2Fixture(t, b)
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			if err := app.DumpStop(); !errors.Is(err, failure) {
				t.Fatal("stop lost storage failure", err)
			}
			if !b.closed || !errors.Is(app.ExitError(), failure) {
				t.Fatal("storage failure skipped close or exit status")
			}
		})
	}
}

func TestV2FailureStopsAndRetainsExitError(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	failure := errors.New("journal sync failed")
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprint(async), func(t *testing.T) {
			b := &oracleBackend{errors: make(chan daemonstore.Failure, 1)}
			if !async {
				b.appendErr = failure
			}
			app := v2Fixture(t, b)
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			app.RLock()
			stopped := app.exit
			app.RUnlock()
			if async {
				b.errors <- daemonstore.Failure{Operation: "sync", Err: failure}
			} else {
				app.v2.append(&points.Points{Metric: "routing.value", Data: []points.Point{{Value: 1, Timestamp: 1}}})
			}
			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				t.Fatal("failure did not stop daemon")
			}
			if !errors.Is(app.ExitError(), failure) {
				t.Fatal("failure lost", app.ExitError())
			}
			b.mu.Lock()
			defer b.mu.Unlock()
			if !b.closed {
				t.Fatal("backend left open")
			}
		})
	}
}

func TestV2MetricRegistration(t *testing.T) {
	b := &oracleBackend{}
	app := New("")
	v := newV2Runtime(app, b)
	defer func() { v.stopReads(); close(v.stopErrors); <-v.errorsDone }()
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(v)
	if _, err := registry.Gather(); err != nil {
		t.Fatal(err)
	}
}
