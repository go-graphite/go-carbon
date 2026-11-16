//go:build linux || darwin

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

func TestRealOracleChecksEveryBit(t *testing.T) {
	values := []float64{math.Copysign(0, -1), math.Float64frombits(0x7ff8000000000042), 1}
	q := realQuery{Name: "sample", Start: 10, Stop: 13, Step: 1}
	for _, v := range values {
		q.Bits = append(q.Bits, math.Float64bits(v))
	}
	if err := compareRealSeries(q, 10, 13, 1, values); err != nil {
		t.Fatal(err)
	}
	for i, replacement := range []float64{0, math.Float64frombits(0x7ff8000000000043), math.Nextafter(1, 2)} {
		changed := append([]float64(nil), values...)
		changed[i] = replacement
		if err := compareRealSeries(q, 10, 13, 1, changed); err == nil {
			t.Fatalf("ignored changed bits at %d", i)
		}
	}
	if err := compareRealSeries(q, 9, 12, 1, values); err == nil {
		t.Fatal("ignored changed time range")
	}
}

// This is a harness test, not evidence for the real-data qualification gate.
func TestRealOracleMixedAndSourceRoundTrip(t *testing.T) {
	for _, rangeOrder := range []bool{false, true} {
		t.Run(fmt.Sprintf("id_order_%t", rangeOrder), func(t *testing.T) { realOracleRoundTrip(t, rangeOrder) })
	}
}
func realOracleRoundTrip(t *testing.T, rangeOrder bool) {
	ctx := context.Background()
	root := t.TempDir()
	legacyPath, output, oraclePath := filepath.Join(root, "legacy"), filepath.Join(root, "parts"), filepath.Join(root, "oracle.json")
	now := time.Unix(1794700800, 0)
	s, err := chunkstore.Open(legacyPath, chunkstore.Options{ExactBlocks: true, Now: func() time.Time { return now }, CacheSize: 8 << 20, MemTableSize: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i, count := 0, 0; count < 2001; i++ {
		name := fmt.Sprintf("sample.%08d", i)
		// Keep the persisted journal small while exercising multiple routing
		// shards. Exhaustive per-shard recovery belongs to partsconvert tests.
		if partsformat.Route(partsformat.ID(name)) >= 16 {
			continue
		}
		count++
		if _, err = s.Create(ctx, chunkstore.MetricConfig{Name: name, Retentions: []chunkstore.Retention{{Step: 60, Count: 120}}, AggregationMethod: chunkstore.Average, XFilesFactor: .5}); err != nil {
			t.Fatal(err)
		}
		var input []chunkstore.Point
		for j := range 119 {
			input = append(input, chunkstore.Point{Timestamp: now.Unix() - int64(120-j)*60, Value: float64(i*120 + j)})
		}
		if err = s.UpdateMany(ctx, name, input); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = captureRealAt(ctx, legacyPath, oraclePath, now.Unix()); err != nil {
		t.Fatal(err)
	}
	var oracle realOracle
	b, err := os.ReadFile(oraclePath)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &oracle); err != nil {
		t.Fatal(err)
	}
	if len(oracle.Queries) != 2000 || oracle.CatalogNames != 2001 {
		t.Fatalf("incorrect sample: %d/%d", len(oracle.Queries), oracle.CatalogNames)
	}
	if len(oracle.CatalogNamespaces) != 1 || oracle.CatalogNamespaces["sample"] != 2001 || len(oracle.CatalogPolicies) != 1 {
		t.Fatalf("coverage must include all 2001 names: namespaces=%v policies=%v", oracle.CatalogNamespaces, oracle.CatalogPolicies)
	}
	for _, count := range oracle.CatalogPolicies {
		if count != oracle.CatalogNames {
			t.Fatalf("policy coverage=%d, catalogue=%d", count, oracle.CatalogNames)
		}
	}
	if _, err = convertLegacyCopy(ctx, legacyPath, output, 4096, 256<<20, copyOptions{RangeOrder: rangeOrder}); err != nil {
		t.Fatal(err)
	}
	queries := make(map[string]realQuery, len(oracle.Queries))
	for _, q := range oracle.Queries {
		queries[q.Name] = q
	}
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/render/" || r.URL.Query().Get("format") != "carbonapi_v3_pb" {
			t.Errorf("unexpected source request: %s %s", r.Method, r.URL)
			http.Error(w, "bad request", 400)
			return
		}
		body, e := io.ReadAll(r.Body)
		var request pb.MultiFetchRequest
		if e != nil || request.UnmarshalVT(body) != nil || len(request.Metrics) != 1 {
			t.Error("invalid render protobuf")
			http.Error(w, "bad protobuf", 400)
			return
		}
		q, ok := queries[request.Metrics[0].Name]
		if !ok || request.Metrics[0].StartTime != q.From || request.Metrics[0].StopTime != q.Until {
			t.Error("source range differs from frozen oracle")
			http.Error(w, "bad range", 400)
			return
		}
		m := &pb.FetchResponse{Name: q.Name, StartTime: q.Start, StopTime: q.Stop, StepTime: q.Step}
		for _, bits := range q.Bits {
			m.Values = append(m.Values, math.Float64frombits(bits))
		}
		response := pb.MultiFetchResponse{Metrics: []*pb.FetchResponse{m}}
		body, e = response.MarshalVT()
		if e != nil {
			t.Error(e)
			return
		}
		requests.Add(1)
		_, _ = w.Write(body)
	}))
	defer server.Close()
	if err = verifyReal(ctx, legacyPath, output, oraclePath, server.URL); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2000 {
		t.Fatalf("source render requests: %d", requests.Load())
	}
	// A corrupted frozen value must fail, even when the source serves the same
	// original response and the rest of the archive is unchanged.
	oracle.Queries[0].Bits[0] ^= 1
	b, err = json.Marshal(oracle)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(oraclePath, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err = verifyReal(ctx, legacyPath, output, oraclePath, ""); err == nil {
		t.Fatal("accepted corrupted frozen render")
	}
	oracle.Queries[0].Bits[0] ^= 1
	oracle.Queries[0].Metric.Archives[0].Step++
	b, err = json.Marshal(oracle)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(oraclePath, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err = verifyReal(ctx, legacyPath, output, oraclePath, ""); err == nil {
		t.Fatal("accepted oracle schema instead of checking converted policy")
	}
}
