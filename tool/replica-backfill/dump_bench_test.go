package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

// BenchmarkSourceBackfill100K measures complete dry runs, including dump
// validation/indexing, against local httptest carbonservers. Each of 100k
// metrics has three slots, two gaps and an authoritative local value. It
// excludes fixture creation and includes local HTTP, gap detection and stats;
// dry-run excludes TCP ingestion and all pacing is disabled.
func BenchmarkSourceBackfill100K(b *testing.B) {
	const count = 100_000
	dir := b.TempDir()
	dumpPath, namesPath := filepath.Join(dir, "source.jsonl"), filepath.Join(dir, "names")
	dump, err := os.Create(dumpPath)
	if err != nil {
		b.Fatal(err)
	}
	names, err := os.Create(namesPath)
	if err != nil {
		b.Fatal(err)
	}
	dw, nw := bufio.NewWriter(dump), bufio.NewWriter(names)
	for i := range count {
		name := fmt.Sprintf("synthetic.metric.%07d", i)
		fmt.Fprintln(nw, name)
		fmt.Fprintf(dw, `{"name":%q,"archives":[{"step":60,"count":10000000,"points":[{"t":%d,"bits":"3ff0000000000000"},{"t":%d,"bits":"4000000000000000"},{"t":%d,"bits":"4008000000000000"}]}]}`+"\n", name, base, base+60, base+120)
	}
	if err := dw.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := nw.Flush(); err != nil {
		b.Fatal(err)
	}
	dump.Close()
	names.Close()

	local, donor := syntheticServer(true), syntheticServer(false)
	defer local.Close()
	defer donor.Close()
	for _, mode := range []string{"HTTP", "Dump"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				o := &options{local: local.URL, replicas: urlList{donor.URL}, from: fmt.Sprint(base), until: fmt.Sprint(base + 120), namesFile: namesPath, batch: 500, workers: 4, replicaRPS: 1e9, timeout: time.Minute, dryRun: true}
				if mode == "Dump" {
					o.replicas = nil
					o.sourceDumps = dumpPaths{dumpPath}
				}
				r, err := run(context.Background(), o, time.Unix(base+180, 0))
				if err != nil {
					b.Fatal(err)
				}
				if r.Metrics["scanned"] != count || r.Metrics["fillable_points"] != 2*count {
					b.Fatalf("unexpected report: %+v", r)
				}
			}
			b.ReportMetric(float64(count*b.N)/b.Elapsed().Seconds(), "names/s")
		})
	}
}

func syntheticServer(local bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req protov3.MultiFetchRequest
		if err := req.UnmarshalVT(body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		resp := protov3.MultiFetchResponse{}
		for _, m := range req.Metrics {
			vals := []float64{1, 2, 3}
			if local {
				vals = []float64{-99, math.NaN(), math.NaN()}
			}
			resp.Metrics = append(resp.Metrics, &protov3.FetchResponse{Name: m.Name, StartTime: base, StopTime: base + 180, StepTime: 60, Values: vals})
		}
		out, _ := resp.MarshalVT()
		_, _ = w.Write(out)
	}))
}
